#  Copyright 2025 Collate
#  Licensed under the Collate Community License, Version 1.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#  https://github.com/open-metadata/OpenMetadata/blob/main/ingestion/LICENSE
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

"""
Source connection handler
"""

from __future__ import annotations

from dataclasses import replace
from typing import TYPE_CHECKING

from metadata.core.connections.test_connection import (
    Diagnosis,
    ErrorPack,
    Evidence,
    Matchers,
    check,
    when,
)
from metadata.core.connections.test_connection.check import CheckError
from metadata.core.connections.test_connection.checks.dashboard import DashboardStep
from metadata.core.connections.test_connection.checks.rest import (
    fetch_list,
    http_status,
    verify_access,
)
from metadata.core.connections.test_connection.classifier import chain_text
from metadata.core.connections.test_connection.network import NETWORK_ERRORS
from metadata.generated.schema.entity.services.connections.dashboard.powerBIConnection import (
    PowerBIConnection as PowerBIConnectionConfig,
)
from metadata.ingestion.api.steps import InvalidSourceException
from metadata.ingestion.connections.connection import BaseConnection
from metadata.ingestion.source.dashboard.powerbi.client import (
    PowerBiApiClient,
    PowerBiClient,
)
from metadata.ingestion.source.dashboard.powerbi.file_client import (
    PowerBiFileClient,
    PowerBIFileConfigException,
    pbit_files_only,
)

READ_PBIT_FILES = "read .pbit files"

if TYPE_CHECKING:
    from metadata.core.connections.lifetime import Borrowed
    from metadata.core.connections.test_connection import ChecksProvider
    from metadata.core.connections.test_connection.classifier import Matcher
    from metadata.ingestion.source.dashboard.powerbi.models import PbitDatasets


def _contains_any(*tokens: str) -> Matcher:
    """Match when any of ``tokens`` appears in the error (or its cause chain).

    MSAL reports a bad authority through more than one message shape - a malformed
    tenant, a blank tenant - so a single ``Matchers.contains`` token would miss
    some; this ORs the stable tokens across those shapes."""
    lowered = tuple(token.lower() for token in tokens)

    def match(error: BaseException) -> bool:
        chain = chain_text(error)
        return any(token in chain for token in lowered)

    return match


ENTRA_ERRORS_DOC = "https://learn.microsoft.com/en-us/entra/identity-platform/reference-error-codes"


def _token_error(code: str) -> Matcher:
    """Match an AADSTS code in a token-acquisition failure.

    Gated on InvalidSourceException so a status-coded REST error echoing an AADSTS
    code still classifies by its status.
    """
    lowered = code.lower()
    return lambda error: Matchers.exception(InvalidSourceException)(error) and lowered in chain_text(error)


POWERBI_ERRORS = ErrorPack(
    # MSAL returns its error as a dict, which the client interpolates into the
    # InvalidSourceException message - that is what makes the code matchable text.
    # Codes: ENTRA_ERRORS_DOC.
    when(_token_error("AADSTS7000215")).diagnose(
        "Invalid client secret",
        fix="Microsoft Entra rejected the Client Secret (AADSTS7000215). Check it was copied "
        "whole, has not expired, and is the secret *value* rather than the secret ID.",
        doc=ENTRA_ERRORS_DOC,
    ),
    when(_token_error("AADSTS700016")).diagnose(
        "Application not found in this tenant",
        fix="The app registration was not found in this directory (AADSTS700016). Check the "
        "Client ID and Tenant ID match the same tenant, and that an administrator has "
        "installed or consented to the application there.",
        doc=ENTRA_ERRORS_DOC,
    ),
    when(_token_error("AADSTS90002")).diagnose(
        "Tenant not found",
        fix="Microsoft Entra does not know this tenant (AADSTS90002). Check the Tenant ID is the "
        "correct GUID or tenant name for your organization.",
        doc=ENTRA_ERRORS_DOC,
    ),
    # CheckAccess acquires the OAuth token, so a bad secret fails there as an
    # InvalidSourceException - not here. Any 401/403 from a REST call therefore
    # means the token was accepted but Power BI would not authorize the call, so
    # neither diagnosis points at the credentials. Per Microsoft's REST API
    # troubleshooting guide, a service principal that is not enabled for the
    # (admin) APIs returns 401, while a missing workspace/resource grant returns
    # 403 - so the two split on tenant-enablement vs. workspace access.
    when(Matchers.exception(PowerBIFileConfigException)).diagnose(
        "Could not read the .pbit files",
        fix="Check the .pbit files source: the local path, or the bucket, prefix and credentials of the "
        "cloud storage it points at.",
    ),
    when(Matchers.exception(InvalidSourceException)).diagnose(
        "Authentication failed",
        fix="Could not acquire an OAuth token. Check the Client ID, Client Secret, and Tenant ID, "
        "and that the app registration is allowed to request the configured scope.",
    ),
    when(http_status(401)).diagnose(
        "Power BI did not authorize the service principal",
        fix="The token was accepted but Power BI rejected the call (401). In the Fabric admin "
        "portal enable 'Allow service principals to use Power BI APIs' (and the read-only admin "
        "APIs setting when Use Admin APIs is on), add the service principal to the allowed "
        "security group, and grant the app registration the required API permission.",
    ),
    when(http_status(403)).diagnose(
        "Insufficient permissions",
        fix="The service principal is authenticated but not authorized for this resource (403). "
        "Add it as a member (or admin) of the target workspace and grant the permissions the "
        "call needs.",
    ),
    when(http_status(404)).diagnose(
        "Resource not found",
        fix="The requested resource was not found (404). Check the API URL and that the configured "
        "tenant/workspace exists and is visible to the service principal.",
    ),
    when(http_status(429)).diagnose(
        "Rate limited by Power BI",
        fix="Power BI is throttling this service principal (429). Retry once the current window "
        "has elapsed; the admin APIs throttle per user per time window.",
    ),
    # Kept last: authority/instance-discovery failures are MSAL ValueErrors that
    # carry no HTTP status, so a broad substring match here must not shadow a
    # status-coded 401/403/404 whose message happens to echo the authority URL.
    when(_contains_any("authority", "should consist of an https url")).diagnose(
        "Invalid tenant or authority",
        fix="The authority could not be resolved. Check the Tenant ID (a valid GUID or "
        "tenant name) and the Authority URI, e.g. https://login.microsoftonline.com/.",
    ),
).including(NETWORK_ERRORS)


class PowerBIChecks:
    """Test-connection checks for PowerBI.

    ``CheckAccess`` is the gate: it acquires an OAuth token, so a bad service
    principal fails fast before any list endpoint is dialled. ``GetDashboards``
    then exercises list access.
    """

    errors = POWERBI_ERRORS

    def __init__(self, powerbi: Borrowed[PowerBiClient]) -> None:
        self._powerbi = powerbi

    def _client(self) -> PowerBiApiClient:
        return self._powerbi.client.api_client

    @check(DashboardStep.CheckAccess)
    def check_access(self) -> Evidence:
        return verify_access(
            lambda: self._client().get_auth_token(),
            command="acquire OAuth token",
        )

    @check(DashboardStep.GetDashboards)
    def get_dashboards(self) -> Evidence:
        return fetch_list(
            lambda: self._client().test_fetch_dashboards(),
            noun="dashboard",
            command="fetch dashboards",
            empty_caveat=Diagnosis(
                title="No dashboards visible",
                remediation="The connection works but returned no dashboards. Confirm the service "
                "principal has access to a workspace that contains dashboards - an empty result can "
                "also mean the listing was filtered or could not be read.",
            ),
        )


class PowerBIFileChecks:
    """Test-connection checks for a PowerBI service read from .pbit files alone.

    ``CheckAccess`` reads the files. ``GetDashboards`` passes with a caveat: dashboards
    exist only in the Power BI service, so none are read without a service principal.
    """

    errors = POWERBI_ERRORS

    def __init__(self, powerbi: Borrowed[PowerBiClient]) -> None:
        self._powerbi = powerbi

    def _read(self) -> PbitDatasets:
        file_client = self._powerbi.client.file_client
        read = file_client.get_datasets() if file_client else None
        if read is None:
            raise PowerBIFileConfigException("The .pbit files could not be read")
        return read

    @check(DashboardStep.CheckAccess)
    def check_access(self) -> Evidence:
        try:
            read = self._read()
        except Exception as cause:
            raise CheckError(cause, Evidence(command=READ_PBIT_FILES)) from cause
        evidence = fetch_list(
            lambda: read.datasets,
            noun="data model",
            command=READ_PBIT_FILES,
            empty_caveat=Diagnosis(
                title="No .pbit data models found",
                remediation="The .pbit files source was read but held no data model. Check its path, or "
                "bucket and prefix, and that the files are templates exported from Power BI Desktop "
                "(File > Export > Power BI template).",
            ),
        )
        if read.unread_files:
            return replace(
                evidence,
                caveat=Diagnosis(
                    title="Some .pbit files could not be read",
                    remediation=f"Not read: {', '.join(read.unread_files)}. Check that each is a whole template "
                    "exported from Power BI Desktop (File > Export > Power BI template).",
                ),
            )
        return evidence

    @check(DashboardStep.GetDashboards)
    def get_dashboards(self) -> Evidence:
        return Evidence(
            summary="dashboards are not read from .pbit files",
            caveat=Diagnosis(
                title="Dashboards not ingested",
                remediation="Without a Client ID, Client Secret and Tenant ID only the data models of the .pbit "
                "files, their lineage and their metrics are ingested. Dashboards and reports exist only in the "
                "Power BI service.",
            ),
        )


class PowerBIConnection(BaseConnection[PowerBIConnectionConfig, PowerBiClient]):
    def _get_client(self) -> PowerBiClient:
        """
        Create connection
        """
        file_client = None
        if self.service_connection.pbitFilesSource:
            file_client = PowerBiFileClient(self.service_connection)
            self._on_close(file_client.delete_tmp_files)
        return PowerBiClient(
            api_client=PowerBiApiClient(self.service_connection),
            file_client=file_client,
        )

    def checks(self) -> ChecksProvider:
        if pbit_files_only(self.service_connection):
            return PowerBIFileChecks(powerbi=self.borrow())
        return PowerBIChecks(powerbi=self.borrow())
