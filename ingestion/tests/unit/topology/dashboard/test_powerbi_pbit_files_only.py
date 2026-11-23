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
PowerBI read from .pbit files alone, with no service principal.
"""

import copy
import json
import shutil
import uuid
import zipfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from metadata.core.connections.lifetime import Borrowed
from metadata.core.connections.test_connection.check import CheckError
from metadata.generated.schema.api.data.createDashboardDataModel import CreateDashboardDataModelRequest
from metadata.generated.schema.api.lineage.addLineage import AddLineageRequest
from metadata.generated.schema.entity.data.dashboardDataModel import DashboardDataModel
from metadata.generated.schema.entity.data.table import Column, DataType, Table
from metadata.generated.schema.entity.services.connections.dashboard.powerbi.s3Config import S3Config
from metadata.generated.schema.entity.services.connections.dashboard.powerBIConnection import (
    PowerBIConnection as PowerBIConnectionConfig,
)
from metadata.generated.schema.metadataIngestion.workflow import OpenMetadataWorkflowConfig
from metadata.generated.schema.type.basic import FullyQualifiedEntityName
from metadata.generated.schema.type.entityReference import EntityReference
from metadata.ingestion.api.steps import InvalidSourceException
from metadata.ingestion.ometa.ometa_api import OpenMetadata
from metadata.ingestion.ometa.utils import model_str
from metadata.ingestion.source.dashboard.powerbi.client import PowerBiApiClient, PowerBiClient
from metadata.ingestion.source.dashboard.powerbi.connection import (
    POWERBI_ERRORS,
    PowerBIChecks,
    PowerBIConnection,
    PowerBIFileChecks,
)
from metadata.ingestion.source.dashboard.powerbi.file_client import (
    PowerBiFileClient,
    PowerBIFileConfigException,
    get_pbit_files,
    pbit_files_only,
)
from metadata.ingestion.source.dashboard.powerbi.metadata import PowerbiSource

SERVICE = "powerbi_pbit_test"
CREDENTIALS = {"clientId": "client_id", "clientSecret": "secret", "tenantId": "tenant_id"}
SALES_DATASET_ID = "c5bf4b57-1de4-4c7f-ae3a-b151f36a8260"

SALES_MODEL = {
    "name": "6f0a2d4e-1b3c-4d5e-8f90-1a2b3c4d5e6f",
    "compatibilityLevel": 1550,
    "model": {
        "tables": [
            {
                "name": "Sales",
                "columns": [
                    {"name": "Amount", "dataType": "double", "sourceColumn": "Amount"},
                    {"name": "Discount", "dataType": "double", "sourceColumn": "Discount"},
                    {
                        "type": "calculated",
                        "name": "Net Amount",
                        "dataType": "double",
                        "expression": ["Sales[Amount]", "    - Sales[Discount]"],
                    },
                ],
                "measures": [{"name": "Total Net", "expression": "SUM(Sales[Net Amount])"}],
                "partitions": [
                    {
                        "name": "Sales",
                        "mode": "import",
                        "source": {
                            "type": "m",
                            "expression": [
                                "let",
                                '    Source = Sql.Database("sql.example.com", "sales"),',
                                '    invoice_line = Source{[Schema="dbo",Item="Sales"]}[Data]',
                                "in",
                                "    invoice_line",
                            ],
                        },
                    }
                ],
            }
        ],
        "expressions": [
            {"name": "Server", "kind": "m", "expression": '"sql.example.com" meta [IsParameterQuery=true]'}
        ],
    },
}


def write_pbit(folder: Path, name: str, model: dict, dataset_id: str | None = SALES_DATASET_ID) -> None:
    """A .pbit as Power BI Desktop writes it: a zip whose DataModelSchema is UTF-16 JSON."""
    artifacts = [{"DatasetId": dataset_id, "ReportId": str(uuid.uuid4())}] if dataset_id else []
    path = folder / f"{name}.pbit"
    path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(path, "w") as pbit:
        pbit.writestr("Connections", json.dumps({"Version": 3, "RemoteArtifacts": artifacts}))
        pbit.writestr("DataModelSchema", json.dumps(model).encode("utf-16-le"))


def files_only_config(folder: Path, **credentials: str) -> PowerBIConnectionConfig:
    return PowerBIConnectionConfig(
        pbitFilesSource={"pbitFileConfigType": "local", "path": str(folder)},
        **credentials,
    )


def powerbi_client(config: PowerBIConnectionConfig) -> PowerBiClient:
    return PowerBiClient(api_client=PowerBiApiClient(config), file_client=PowerBiFileClient(config))


@pytest.mark.parametrize(
    ("pbit_source", "credentials", "expected"),
    [
        (True, {}, True),
        (True, CREDENTIALS, False),
        (True, {"clientId": "client_id"}, False),
        (False, {}, False),
    ],
)
def test_a_pbit_source_without_any_credential_is_read_from_the_files_alone(
    tmp_path, pbit_source, credentials, expected
):
    config = files_only_config(tmp_path, **credentials) if pbit_source else PowerBIConnectionConfig(**credentials)
    assert pbit_files_only(config) is expected


def test_the_api_client_needs_a_service_principal_only_when_it_calls_the_api(tmp_path):
    with (
        patch("metadata.ingestion.source.dashboard.powerbi.client.msal.ConfidentialClientApplication") as msal_app,
        patch("metadata.ingestion.source.dashboard.powerbi.client.sleep") as sleep,
    ):
        api_client = PowerBiApiClient(files_only_config(tmp_path))
        with pytest.raises(InvalidSourceException, match="Client ID, Client Secret and Tenant ID"):
            api_client.get_auth_token()

    msal_app.assert_not_called()
    sleep.assert_not_called()


def test_each_pbit_file_is_read_as_the_dataset_it_was_published_as(tmp_path):
    write_pbit(tmp_path, "Sales Analytics", SALES_MODEL)

    read = PowerBiFileClient(files_only_config(tmp_path)).get_datasets()

    assert read is not None
    assert read.unread_files == []
    [dataset] = read.datasets
    assert (dataset.id, dataset.name) == (SALES_DATASET_ID, "Sales Analytics")
    [table] = dataset.tables or []
    assert "Sql.Database" in (table.source or [])[0].expression
    assert [(column.name, column.expression) for column in table.columns or []] == [
        ("Amount", None),
        ("Discount", None),
        ("Net Amount", "Sales[Amount]\n    - Sales[Discount]"),
    ]
    assert [expression.name for expression in dataset.expressions or []] == ["Server"]


def test_a_pbit_never_published_gets_an_id_that_is_stable_per_file(tmp_path):
    write_pbit(tmp_path, "Draft", SALES_MODEL, dataset_id=None)
    write_pbit(tmp_path, "Other Draft", SALES_MODEL, dataset_id=None)
    config = files_only_config(tmp_path)

    first = {dataset.name: dataset.id for dataset in _datasets(PowerBiFileClient(config))}
    second = {dataset.name: dataset.id for dataset in _datasets(PowerBiFileClient(config))}

    assert first == second
    assert len(set(first.values())) == 2


def _datasets(file_client: PowerBiFileClient) -> list:
    read = file_client.get_datasets()
    assert read is not None
    return read.datasets


def test_pbit_files_of_one_published_dataset_are_one_dataset_read_from_the_first_file(tmp_path):
    regional = copy.deepcopy(SALES_MODEL)
    regional["model"]["tables"][0]["name"] = "Regional Sales"
    write_pbit(tmp_path, "Sales Regional", regional)
    write_pbit(tmp_path, "Sales Analytics", SALES_MODEL)

    [dataset] = _datasets(PowerBiFileClient(files_only_config(tmp_path)))

    assert (dataset.id, dataset.name) == (SALES_DATASET_ID, "Sales Analytics")
    assert [table.name for table in dataset.tables or []] == ["Sales"]


def test_files_with_dotted_names_or_one_name_in_two_folders_are_apart(tmp_path):
    for name in ("Sales.EU", "Sales.US", "team-a/Finance", "team-b/Finance"):
        write_pbit(tmp_path, name, SALES_MODEL, dataset_id=None)

    datasets = _datasets(PowerBiFileClient(files_only_config(tmp_path)))

    assert sorted(dataset.name for dataset in datasets) == ["Finance", "Finance", "Sales.EU", "Sales.US"]
    assert len({dataset.id for dataset in datasets}) == 4


def test_a_removed_pbit_file_is_not_read_again(tmp_path):
    write_pbit(tmp_path, "Kept", SALES_MODEL, dataset_id=None)
    write_pbit(tmp_path, "Removed", SALES_MODEL, dataset_id=None)
    config = files_only_config(tmp_path)
    assert sorted(dataset.name for dataset in _datasets(PowerBiFileClient(config))) == ["Kept", "Removed"]

    (tmp_path / "Removed.pbit").unlink()

    assert [dataset.name for dataset in _datasets(PowerBiFileClient(config))] == ["Kept"]
    assert sorted(path.name for path in tmp_path.iterdir()) == ["Kept.pbit"]


def test_a_pbit_named_like_a_template_member_is_read(tmp_path):
    write_pbit(tmp_path, "Customer Connections", SALES_MODEL, dataset_id=None)
    write_pbit(tmp_path, "Partner Connections/Sales", SALES_MODEL, dataset_id=None)

    read = PowerBiFileClient(files_only_config(tmp_path)).get_datasets()

    assert read is not None
    assert sorted(dataset.name for dataset in read.datasets) == ["Customer Connections", "Sales"]
    assert read.unread_files == []


def test_a_cloud_source_reads_only_the_files_it_downloaded(tmp_path):
    extract_dir = tmp_path / "pbitFiles"
    write_pbit(extract_dir / "left-behind", "Other Service", SALES_MODEL, dataset_id=None)
    s3_config = MagicMock(spec=S3Config)
    s3_config.pbitFilesExtractDir = str(extract_dir)
    s3_config.prefixConfig = None
    s3_config.securityConfig = MagicMock()

    def download(path: str, local_file_path: str, **_) -> None:
        write_pbit(Path(local_file_path).parent, Path(local_file_path).stem, SALES_MODEL, dataset_id=None)

    with (
        patch("metadata.ingestion.source.dashboard.powerbi.file_client.AWSClient") as aws,
        patch(
            "metadata.ingestion.source.dashboard.powerbi.file_client.list_s3_objects",
            return_value=[{"Key": "reports/Sales.pbit"}],
        ),
        patch("metadata.ingestion.source.dashboard.powerbi.file_client.get_reader") as get_reader,
    ):
        aws.return_value.get_client.return_value.list_buckets.return_value = {"Buckets": [{"Name": "bucket-a"}]}
        get_reader.return_value.download.side_effect = download
        read = get_pbit_files(s3_config)

    assert read is not None
    assert [schema.file_path for schema in read.schemas] == ["bucket-a/reports/Sales"]


def test_a_cloud_download_lost_before_the_read_is_reported_unread(tmp_path):
    extract_dir = tmp_path / "pbitFiles"
    s3_config = MagicMock(spec=S3Config)
    s3_config.pbitFilesExtractDir = str(extract_dir)
    s3_config.prefixConfig = None
    s3_config.securityConfig = MagicMock()
    downloads = []

    def download(path: str, local_file_path: str, **_) -> None:
        write_pbit(Path(local_file_path).parent, Path(local_file_path).stem, SALES_MODEL, dataset_id=None)
        downloads.append(path)
        if len(downloads) == 1:
            # Another service's connection closes and removes the shared extract dir, as delete_tmp_files does.
            shutil.rmtree(extract_dir)

    with (
        patch("metadata.ingestion.source.dashboard.powerbi.file_client.AWSClient") as aws,
        patch(
            "metadata.ingestion.source.dashboard.powerbi.file_client.list_s3_objects",
            return_value=[{"Key": "reports/Finance.pbit"}, {"Key": "reports/Sales.pbit"}],
        ),
        patch("metadata.ingestion.source.dashboard.powerbi.file_client.get_reader") as get_reader,
    ):
        aws.return_value.get_client.return_value.list_buckets.return_value = {"Buckets": [{"Name": "bucket-a"}]}
        get_reader.return_value.download.side_effect = download
        read = get_pbit_files(s3_config)

    assert read is not None
    assert [schema.file_path for schema in read.schemas] == ["bucket-a/reports/Sales"]
    assert read.unread_files == ["bucket-a/reports/Finance"]


def test_a_pbit_file_that_cannot_be_read_is_reported(tmp_path):
    write_pbit(tmp_path, "Sales Analytics", SALES_MODEL)
    (tmp_path / "Broken.pbit").write_bytes(b"PK\x03\x04 truncated upload")

    read = PowerBiFileClient(files_only_config(tmp_path)).get_datasets()

    assert read is not None
    assert [dataset.name for dataset in read.datasets] == ["Sales Analytics"]
    assert read.unread_files == ["Broken"]


def test_a_missing_pbit_folder_is_not_read_as_an_empty_one(tmp_path):
    assert PowerBiFileClient(files_only_config(tmp_path / "missing")).get_datasets() is None


def test_a_tenant_msal_cannot_discover_fails_once_with_its_diagnosis(tmp_path):
    with (
        patch(
            "metadata.ingestion.source.dashboard.powerbi.client.msal.ConfidentialClientApplication",
            side_effect=ValueError("Unable to get authority configuration for https://login.microsoftonline.com/bad"),
        ) as msal_app,
        patch("metadata.ingestion.source.dashboard.powerbi.client.sleep") as sleep,
    ):
        api_client = PowerBiApiClient(PowerBIConnectionConfig(**CREDENTIALS))
        checks = PowerBIChecks(powerbi=Borrowed.of(MagicMock(api_client=api_client)))
        with pytest.raises(CheckError) as failure:
            checks.check_access()

    diagnosis = POWERBI_ERRORS.classify(failure.value.cause)
    assert diagnosis is not None
    assert diagnosis.title == "Invalid tenant or authority"
    msal_app.assert_called_once()
    sleep.assert_not_called()


def test_the_connection_tests_the_files_when_there_are_no_credentials(tmp_path):
    assert isinstance(PowerBIConnection(files_only_config(tmp_path)).checks(), PowerBIFileChecks)
    assert isinstance(PowerBIConnection(files_only_config(tmp_path, **CREDENTIALS)).checks(), PowerBIChecks)


def test_the_file_checks_read_the_files_and_note_that_dashboards_are_not_read(tmp_path):
    write_pbit(tmp_path, "Sales Analytics", SALES_MODEL)
    checks = PowerBIFileChecks(powerbi=Borrowed.of(powerbi_client(files_only_config(tmp_path))))

    access = checks.check_access()
    dashboards = checks.get_dashboards()

    assert (access.summary, access.caveat) == ("1 data model enumerated", None)
    assert dashboards.caveat is not None
    assert dashboards.caveat.title == "Dashboards not ingested"


def test_the_file_checks_warn_when_the_source_holds_no_pbit_file(tmp_path):
    checks = PowerBIFileChecks(powerbi=Borrowed.of(powerbi_client(files_only_config(tmp_path))))

    access = checks.check_access()

    assert access.caveat is not None
    assert access.caveat.title == "No .pbit data models found"


def test_the_file_checks_name_the_files_they_could_not_read(tmp_path):
    write_pbit(tmp_path, "Sales Analytics", SALES_MODEL)
    (tmp_path / "Broken.pbit").write_bytes(b"not a zip")
    checks = PowerBIFileChecks(powerbi=Borrowed.of(powerbi_client(files_only_config(tmp_path))))

    access = checks.check_access()

    assert access.summary == "1 data model enumerated"
    assert access.caveat is not None
    assert access.caveat.title == "Some .pbit files could not be read"
    assert "Broken" in (access.caveat.remediation or "")


def test_the_file_checks_fail_when_the_files_cannot_be_read():
    file_client = MagicMock(spec=PowerBiFileClient)
    file_client.get_datasets.return_value = None
    checks = PowerBIFileChecks(powerbi=Borrowed.of(MagicMock(file_client=file_client)))

    with pytest.raises(CheckError) as failure:
        checks.check_access()

    assert isinstance(failure.value.cause, PowerBIFileConfigException)
    diagnosis = POWERBI_ERRORS.classify(failure.value.cause)
    assert diagnosis is not None
    assert diagnosis.title == "Could not read the .pbit files"


@pytest.fixture
def files_only_source(tmp_path):
    """A PowerBI source configured with a .pbit folder and no service principal."""
    write_pbit(tmp_path, "Sales Analytics", SALES_MODEL)
    config = {
        "source": {
            "type": "powerbi",
            "serviceName": SERVICE,
            "serviceConnection": {"config": files_only_config(tmp_path).model_dump(mode="json", exclude_none=True)},
            "sourceConfig": {"config": {"type": "DashboardMetadata"}},
        },
        "sink": {"type": "metadata-rest", "config": {}},
        "workflowConfig": {
            "openMetadataServerConfig": {
                "hostPort": "http://localhost:8585/api",
                "authProvider": "openmetadata",
                "securityConfig": {"jwtToken": "token"},
            }
        },
    }
    with (
        patch("metadata.ingestion.source.dashboard.dashboard_service.DashboardServiceSource.test_connection"),
        patch("metadata.ingestion.source.dashboard.dashboard_service.create_connection"),
    ):
        workflow_config = OpenMetadataWorkflowConfig.model_validate(copy.deepcopy(config))
        source = PowerbiSource.create(
            config["source"], OpenMetadata(workflow_config.workflowConfig.openMetadataServerConfig)
        )
    source.context.get().__dict__["dashboard_service"] = SERVICE
    source.client = powerbi_client(source.service_connection)
    # The real seen-sets are class attributes shared by every source in the process.
    source.dashboard_source_state = set()
    source.datamodel_source_state = set()
    return source


def _enter_workspace(source: PowerbiSource):
    [workspace] = list(source._prepare_workspace_data())
    source.state.enter(workspace)
    source.context.get().__dict__["workspace"] = workspace
    return workspace


def _data_model_entity(request: CreateDashboardDataModelRequest) -> DashboardDataModel:
    model_fqn = f"{SERVICE}.model.{model_str(request.name)}"
    return DashboardDataModel(
        id=uuid.uuid4(),
        name=request.name,
        fullyQualifiedName=model_fqn,
        service=EntityReference(id=uuid.uuid4(), type="dashboardService"),
        dataModelType=request.dataModelType,
        columns=[
            table.model_copy(
                update={
                    "fullyQualifiedName": FullyQualifiedEntityName(f"{model_fqn}.{table.name.root}"),
                    "children": [
                        child.model_copy(
                            update={
                                "fullyQualifiedName": FullyQualifiedEntityName(
                                    f"{model_fqn}.{table.name.root}.{child.name.root}"
                                )
                            }
                        )
                        for child in table.children or []
                    ],
                }
            )
            for table in request.columns
        ],
    )


def test_files_only_ingestion_reads_one_workspace_of_the_files_datasets(files_only_source):
    workspace = _enter_workspace(files_only_source)

    assert [dataset.id for dataset in workspace.datasets or []] == [SALES_DATASET_ID]
    assert workspace.dashboards == []
    assert workspace.reports == []


def test_a_files_only_data_model_has_no_service_url_or_workspace(files_only_source):
    workspace = _enter_workspace(files_only_source)

    [request] = [either.right for either in files_only_source.yield_datamodel(workspace) if either.right]

    assert (request.sourceUrl, request.project) == (None, None)
    [sales] = request.columns
    assert [child.name.root for child in sales.children or []] == ["Amount", "Discount", "Net Amount", "Total Net"]


def test_files_only_lineage_links_the_warehouse_columns_to_the_calculated_column(files_only_source):
    workspace = _enter_workspace(files_only_source)
    [request] = [either.right for either in files_only_source.yield_datamodel(workspace) if either.right]
    data_model = _data_model_entity(request)
    warehouse = Table(
        id=uuid.uuid4(),
        name="Sales",
        fullyQualifiedName="mssql.sales.dbo.Sales",
        columns=[
            Column(name=name, dataType=DataType.DOUBLE, fullyQualifiedName=f"mssql.sales.dbo.Sales.{name}")
            for name in ("Amount", "Discount")
        ],
    )

    with (
        patch.object(files_only_source.metadata, "get_by_name", return_value=data_model),
        patch.object(files_only_source.metadata, "search_in_any_service", return_value=warehouse),
        patch.object(files_only_source, "create_table_datamodel_lineage_from_files") as reread_files,
    ):
        lineage = [
            either.right for either in files_only_source.yield_dashboard_lineage_details(workspace) if either.right
        ]

    reread_files.assert_not_called()
    [edge] = [request for request in lineage if isinstance(request, AddLineageRequest)]
    columns_lineage = edge.edge.lineageDetails.columnsLineage or []
    by_target = {model_str(entry.toColumn): entry for entry in columns_lineage}
    model_fqn = model_str(data_model.fullyQualifiedName)
    net_amount = by_target[f"{model_fqn}.Sales.Net Amount"]
    assert [model_str(column) for column in net_amount.fromColumns or []] == [
        "mssql.sales.dbo.Sales.Amount",
        "mssql.sales.dbo.Sales.Discount",
    ]
    assert model_str(net_amount.function) == "Sales[Amount]\n    - Sales[Discount]"
    assert by_target[f"{model_fqn}.Sales.Amount"].function is None


def test_files_that_cannot_be_read_mark_the_listing_failed_instead_of_reading_nothing(files_only_source):
    file_client = MagicMock(spec=PowerBiFileClient)
    file_client.get_datasets.side_effect = PowerBIFileConfigException("bucket not found")
    files_only_source.client = PowerBiClient(
        api_client=PowerBiApiClient(files_only_source.service_connection), file_client=file_client
    )

    with patch.object(files_only_source, "_mark_workspace_listing_failed") as listing_failed:
        workspaces = list(files_only_source._prepare_workspace_data())

    assert workspaces == []
    listing_failed.assert_called_once()


def _run_workspaces(source: PowerbiSource) -> None:
    """Drive the workspace node as the topology runner does, with its data model stage."""
    for workspace in source.get_dashboard():
        list(source.yield_datamodel(workspace))


def test_a_files_only_run_confirms_its_data_models_but_never_its_dashboards(files_only_source):
    _run_workspaces(files_only_source)

    assert files_only_source.datamodel_listing.is_complete
    assert not files_only_source.datamodel_listing.has_failed
    assert not files_only_source.dashboard_listing.confirms_nothing_to_ingest


def test_a_missing_pbit_folder_confirms_nothing(files_only_source, tmp_path):
    files_only_source.client = powerbi_client(files_only_config(tmp_path / "missing"))

    _run_workspaces(files_only_source)

    assert not files_only_source.datamodel_listing.confirms_nothing_to_ingest
    assert not files_only_source.dashboard_listing.confirms_nothing_to_ingest


def test_an_unreadable_pbit_file_keeps_the_stale_pass_from_deleting_its_data_model(files_only_source, tmp_path):
    (tmp_path / "Broken.pbit").write_bytes(b"not a zip")

    _run_workspaces(files_only_source)
    with patch("metadata.ingestion.source.dashboard.dashboard_service.delete_entity_from_source") as delete_stale:
        deletions = list(files_only_source.mark_datamodels_as_deleted())

    assert files_only_source.datamodel_listing.has_failed
    assert deletions == []
    delete_stale.assert_not_called()


def test_a_readable_pbit_folder_runs_the_stale_pass(files_only_source):
    _run_workspaces(files_only_source)
    with patch(
        "metadata.ingestion.source.dashboard.dashboard_service.delete_entity_from_source", return_value=iter(())
    ) as delete_stale:
        list(files_only_source.mark_datamodels_as_deleted())

    delete_stale.assert_called_once()
