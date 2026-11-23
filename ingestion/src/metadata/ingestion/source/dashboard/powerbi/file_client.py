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
File Client for PowerBi
"""

import json
import os
import shutil
import tempfile
import traceback
import uuid
import zipfile
from collections import defaultdict
from contextlib import suppress
from functools import singledispatch
from pathlib import Path, PurePosixPath

from metadata.clients.aws_client import AWSClient
from metadata.clients.azure_client import AzureClient
from metadata.generated.schema.entity.services.connections.dashboard.powerbi.azureConfig import (
    AzureConfig,
)
from metadata.generated.schema.entity.services.connections.dashboard.powerbi.gcsConfig import (
    GCSConfig,
)
from metadata.generated.schema.entity.services.connections.dashboard.powerbi.s3Config import (
    S3Config,
)
from metadata.generated.schema.entity.services.connections.dashboard.powerBIConnection import (
    LocalConfig,
    PowerBIConnection,
)
from metadata.ingestion.connections.source_api_client import TrackedREST
from metadata.ingestion.source.dashboard.powerbi.models import (
    ConnectionFile,
    DataModelSchema,
    Dataset,
    DatasetExpression,
    PbitDatasets,
    PbitFilesRead,
    PowerBiTable,
)
from metadata.readers.file.config_source_factory import get_reader
from metadata.readers.file.local import LocalReader
from metadata.utils.credentials import (
    get_gcp_impersonate_credentials_or_none,
    set_google_credentials,
)
from metadata.utils.logger import utils_logger
from metadata.utils.s3_utils import list_s3_objects

logger = utils_logger()

# Namespace of the ids given to the datasets of .pbit files that name no published dataset
PBIT_DATASET_NAMESPACE = uuid.uuid5(uuid.NAMESPACE_URL, "https://open-metadata.org/powerbi/pbit")


def pbit_files_only(config: PowerBIConnection) -> bool:
    """A .pbit files source and no service principal: the service is read from the files alone."""
    return config.pbitFilesSource is not None and not (config.clientId or config.clientSecret or config.tenantId)


def get_prefix_config(config) -> tuple[str | None, str | None]:
    """
    Return (bucket, prefix) tuple
    """
    if config.prefixConfig:
        return (
            config.prefixConfig.bucketName,
            config.prefixConfig.objectPrefix,
        )
    return None, None


def _ensure_extract_dir(path: str | None) -> None:
    if path is None:
        raise PowerBIFileConfigException("PowerBI .pbit extraction directory is not configured")
    Path(path).mkdir(parents=True, exist_ok=True)


def _run_dir(extract_dir: str | None) -> str:
    """A directory of this read alone inside ``extract_dir``, so files another run left there are not read."""
    _ensure_extract_dir(extract_dir)
    return tempfile.mkdtemp(dir=extract_dir)


def get_blobs_grouped_by_dir(blobs: list[str]) -> dict[str, list[str]]:
    """
    Method to group the objs by the dir
    """
    blob_grouped_by_directory = defaultdict(list)
    for blob in blobs or []:
        subdirectory = blob.rsplit("/", 1)[0] if "/" in blob else ""
        blob_file_name = blob.rsplit("/", 1)[1] if "/" in blob else blob
        if blob_file_name.lower().endswith(".pbit"):
            blob_grouped_by_directory[subdirectory].append(blob)
    return blob_grouped_by_directory


def _safe_local_path(extract_dir: str, blob: str) -> str:
    """Resolve the local download path for a blob and confirm it stays inside
    ``extract_dir``.

    Blob names come from the cloud storage listing and may contain ``..`` or
    absolute-path components. Joining them naively would let a crafted object key
    write outside the extract directory (path traversal / arbitrary file write), so
    the resolved path is validated against the resolved extract directory.
    """
    base = Path(extract_dir).resolve()
    target = (base / blob).resolve()
    if not target.is_relative_to(base):
        raise PowerBIFileConfigException(f"Skipping .pbit object key that escapes the extract directory: {blob}")
    return str(target)


def download_pbit_files(
    blob_grouped_by_directory: dict,
    config,
    client,
    bucket_name: str | None,
    extract_dir: str,
) -> list[str]:
    """
    Method to download the files from sources, returning the local path of each
    """
    downloaded = []
    for blobs in blob_grouped_by_directory.values():
        kwargs = {}
        if bucket_name:
            kwargs = {"bucket_name": bucket_name}
        for blob in blobs:
            if not blob:
                continue
            try:
                # validate per blob so one escaping key does not skip the rest
                local_file_path = _safe_local_path(extract_dir, blob)
            except PowerBIFileConfigException as exc:
                logger.warning(exc)
                continue
            reader = get_reader(config_source=config, client=client)
            # create the required dir before downloading
            os.makedirs(os.path.dirname(local_file_path), exist_ok=True)  # noqa: PTH103, PTH120
            reader.download(path=blob, local_file_path=local_file_path, **kwargs)
            downloaded.append(local_file_path)
    return downloaded


def _extract_pbit(zip_ref: zipfile.ZipFile, extract_dir: str) -> None:
    """Extract a .pbit archive, skipping any member whose path escapes ``extract_dir``.

    A .pbit is a ZIP whose member names are attacker-controlled when the file comes
    from a shared bucket or folder. ``extractall`` happens to rewrite ``..`` and
    absolute names today, but validating each member keeps the guarantee explicit
    and reports the crafted entry instead of silently relocating it.
    """
    base = Path(extract_dir).resolve()
    for member in zip_ref.infolist():
        if not (base / member.filename).resolve().is_relative_to(base):
            logger.warning("Skipping .pbit entry that escapes the extract directory: %r", member.filename)
            continue
        zip_ref.extract(member, base)


def _get_datamodel_schema_list(path: str) -> list[DataModelSchema] | None:
    """
    Method maps the json to datamodel schema model
    """
    extracted = Path(path) / "extracted"
    reader = LocalReader(extracted)
    connection_files = sorted(reader.get_local_files(search_key="Connections"))
    datamodel_schema_list = []
    for connection_file in connection_files:
        try:
            datamodel_schema = DataModelSchema(file_path=Path(connection_file).parent.relative_to(extracted).as_posix())
            with open(connection_file, "rb") as file:  # noqa: PTH123
                connection_json_file = json.load(file)
                datamodel_schema.connectionFile = ConnectionFile(**connection_json_file)

            datamodel_schema_file = str(Path(connection_file).with_name("DataModelSchema"))
            with open(datamodel_schema_file, "rb") as file:  # noqa: PTH123
                data_model_schema_json_file = json.load(file)
                model = data_model_schema_json_file.get("model")
                datamodel_schema.tables = [PowerBiTable(**table) for table in model["tables"] or []]
                datamodel_schema.expressions = [
                    DatasetExpression(**expression) for expression in model.get("expressions") or []
                ]
            if datamodel_schema.tables and datamodel_schema.connectionFile:
                datamodel_schema_list.append(datamodel_schema)
        except Exception as exc:
            logger.debug(traceback.format_exc())
            logger.error(f"Error reading and mapping the datamodel schema file for {connection_file}: {exc}")
    return datamodel_schema_list


def get_datamodel_schema_files_from_pbit(
    path: str, extract_dir: str | None = None, file_paths: list[str] | None = None
) -> PbitFilesRead | None:
    """
    Method to unzip the locally saved pbit files and get the schema files

    Each file is extracted, by its path under ``path``, into a directory of this run alone inside
    ``extract_dir``, so a file removed since an earlier run is not read again and files of one name in
    two folders stay apart. ``file_paths`` are the files expected under ``path``, when they are known
    from elsewhere than ``path`` itself: one missing from it is reported as unread.
    """
    try:
        source = Path(path)
        if not source.is_dir():
            logger.error("The .pbit files path is not a directory: %s", path)
            return None
        file_paths = sorted(
            LocalReader(source).get_local_files(search_key=".pbit") if file_paths is None else file_paths
        )
        names = [
            Path(file_path).resolve().relative_to(source.resolve()).with_suffix("").as_posix()
            for file_path in file_paths
        ]
        if extract_dir:
            Path(extract_dir).mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(dir=extract_dir) as extract_root:
            for file_path, name in zip(file_paths, names, strict=True):
                # One unreadable .pbit must not drop the schemas of the others. Damaged
                # archives fail during extraction with many types (BadZipFile, zlib.error,
                # EOFError, RuntimeError for encrypted entries, NotImplementedError for
                # unsupported compression), so any failure skips only this file.
                try:
                    with zipfile.ZipFile(file_path, "r") as zip_ref:
                        _extract_pbit(zip_ref, f"{extract_root}/extracted/{name}")
                except Exception as exc:
                    logger.debug(traceback.format_exc())
                    logger.warning("Skipping .pbit file %s: %s", file_path, exc)
            schemas = _get_datamodel_schema_list(extract_root) or []
        read = {schema.file_path for schema in schemas}
        return PbitFilesRead(schemas=schemas, unread_files=[name for name in names if name not in read])

    except Exception as exc:
        logger.debug(traceback.format_exc())
        logger.error(f"Error extracting pbit files: {exc}")
    return None


@singledispatch
def get_pbit_files(config) -> PbitFilesRead | None:
    """
    Single dispatch method to get the pbit files from different sources
    """

    if config:
        raise NotImplementedError(f"Config not implemented for type {type(config)}: {config}")


@get_pbit_files.register
def _(config: S3Config):
    try:
        run_dir = _run_dir(config.pbitFilesExtractDir)
        downloaded: list[str] = []
        bucket_name, prefix = get_prefix_config(config)

        client = AWSClient(config.securityConfig).get_client(service_name="s3")

        if not bucket_name:
            buckets = client.list_buckets()["Buckets"]
        else:
            buckets = [{"Name": bucket_name}]
        for bucket in buckets:
            kwargs = {"Bucket": bucket["Name"]}
            if prefix:
                kwargs["Prefix"] = prefix if prefix.endswith("/") else f"{prefix}/"

            # Download the pbit files and store them in the local path
            downloaded += download_pbit_files(
                blob_grouped_by_directory=get_blobs_grouped_by_dir(
                    blobs=[key["Key"] for key in list_s3_objects(client, **kwargs)]
                ),
                config=config,
                client=client,
                bucket_name=bucket["Name"],
                extract_dir=f"{run_dir}/{bucket['Name']}",
            )
        # Extract the datamodel schema files from pbit files and return the list of datamodel schema objects
        return get_datamodel_schema_files_from_pbit(path=run_dir, extract_dir=run_dir, file_paths=downloaded)

    except Exception as exc:
        logger.debug(traceback.format_exc())
        raise PowerBIFileConfigException(f"Error fetching .pbit files from s3: {exc}")  # noqa: B904


@get_pbit_files.register
def _(config: AzureConfig):
    try:
        run_dir = _run_dir(config.pbitFilesExtractDir)
        downloaded: list[str] = []
        bucket_name, prefix = get_prefix_config(config)

        client = AzureClient(config.securityConfig).create_blob_client()

        if not bucket_name:
            container_dicts = client.list_containers()
            containers = [client.get_container_client(container["name"]) for container in container_dicts]
        else:
            container_client = client.get_container_client(bucket_name)
            containers = [container_client]
        for container_client in containers:
            if prefix:
                blob_list = container_client.list_blobs(name_starts_with=prefix)
            else:
                blob_list = container_client.list_blobs()

            # Download the pbit files and store them in the local path
            downloaded += download_pbit_files(
                blob_grouped_by_directory=get_blobs_grouped_by_dir(blobs=[blob.name for blob in blob_list]),
                config=config,
                client=client,
                bucket_name=container_client.container_name,
                extract_dir=f"{run_dir}/{container_client.container_name}",
            )
        # Extract the datamodel schema files from pbit files and return the list of datamodel schema objects
        return get_datamodel_schema_files_from_pbit(path=run_dir, extract_dir=run_dir, file_paths=downloaded)

    except Exception as exc:
        logger.debug(traceback.format_exc())
        raise PowerBIFileConfigException(  # noqa: B904
            f"Error fetching .pbit files from Azure: {exc}"
        )


@get_pbit_files.register
def _(config: GCSConfig):
    try:
        run_dir = _run_dir(config.pbitFilesExtractDir)
        downloaded: list[str] = []
        bucket_name, prefix = get_prefix_config(config)
        from google.cloud import storage  # pylint: disable=import-outside-toplevel

        set_google_credentials(gcp_credentials=config.securityConfig)

        client = storage.Client(credentials=get_gcp_impersonate_credentials_or_none(config.securityConfig))
        if not bucket_name:
            buckets = client.list_buckets()
        else:
            buckets = [client.get_bucket(bucket_name)]
        for bucket in buckets:
            if prefix:
                obj_list = client.list_blobs(bucket.name, prefix=prefix)
            else:
                obj_list = client.list_blobs(bucket.name)

            downloaded += download_pbit_files(
                blob_grouped_by_directory=get_blobs_grouped_by_dir(blobs=[blob.name for blob in obj_list]),
                config=config,
                client=client,
                bucket_name=bucket.name,
                extract_dir=f"{run_dir}/{bucket.name}",
            )
        # Extract the datamodel schema files from pbit files and return the list of datamodel schema objects
        return get_datamodel_schema_files_from_pbit(path=run_dir, extract_dir=run_dir, file_paths=downloaded)

    except Exception as exc:
        logger.debug(traceback.format_exc())
        raise PowerBIFileConfigException(f"Error fetching .pbit files from GCS: {exc}")  # noqa: B904


@get_pbit_files.register
def _(config: LocalConfig):
    try:
        return get_datamodel_schema_files_from_pbit(path=config.path, extract_dir=config.pbitFilesExtractDir)
    except Exception as exc:
        logger.debug(traceback.format_exc())
        logger.error(f"Error getting pbit files from local: {exc}")
    return None


def _dataset_from_schema(schema: DataModelSchema) -> Dataset:
    """A .pbit's model as a dataset: the published dataset's id, or one derived from the file's path."""
    file_path = schema.file_path or ""
    artifacts = (schema.connectionFile.RemoteArtifacts if schema.connectionFile else None) or []
    dataset_id = artifacts[0].DatasetId if artifacts else str(uuid.uuid5(PBIT_DATASET_NAMESPACE, file_path))
    return Dataset(
        id=dataset_id,
        name=PurePosixPath(file_path).name,
        tables=schema.tables or [],
        expressions=schema.expressions or [],
    )


class PowerBIFileConfigException(Exception):  # noqa: N818
    """
    Raise when encountering errors while extracting pbit files
    """


class PowerBiFileClient:
    """
    File client for PowerBi
    """

    client: TrackedREST

    def __init__(self, config: PowerBIConnection):
        self.config = config

    def get_data_model_schema_mappings(self) -> list[DataModelSchema] | None:
        """
        Get the data model schema mappings
        """
        read = get_pbit_files(self.config.pbitFilesSource)
        return read.schemas if read else None

    def get_datasets(self) -> PbitDatasets | None:
        """The .pbit files' models as the datasets the Power BI API returns, one per published dataset."""
        read = get_pbit_files(self.config.pbitFilesSource)
        if read is None:
            return None
        datasets: dict[str, Dataset] = {}
        for schema in read.schemas:
            dataset = _dataset_from_schema(schema)
            if dataset.id in datasets:
                logger.warning(
                    "Skipping .pbit file %s: %s, read first, is the same dataset %s",
                    schema.file_path,
                    datasets[dataset.id].name,
                    dataset.id,
                )
                continue
            datasets[dataset.id] = dataset
        return PbitDatasets(datasets=list(datasets.values()), unread_files=read.unread_files)

    def delete_tmp_files(self):
        """
        Method to remove the files after ingestion is completed
        """
        source = self.config.pbitFilesSource
        extract_dir = source.pbitFilesExtractDir if source is not None else None
        if extract_dir is not None:
            with suppress(FileNotFoundError):
                shutil.rmtree(extract_dir)
