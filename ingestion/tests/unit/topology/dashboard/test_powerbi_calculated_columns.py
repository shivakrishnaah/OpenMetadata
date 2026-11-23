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
A PowerBI calculated column's DAX on the column lineage from the warehouse columns it reads.
"""

import uuid
from unittest.mock import patch

import pytest

from metadata.generated.schema.entity.data.dashboardDataModel import DashboardDataModel, DataModelType
from metadata.generated.schema.entity.data.table import Column, DataType, Table
from metadata.generated.schema.metadataIngestion.workflow import OpenMetadataWorkflowConfig
from metadata.generated.schema.type.entityReference import EntityReference
from metadata.ingestion.ometa.ometa_api import OpenMetadata
from metadata.ingestion.ometa.utils import model_str
from metadata.ingestion.source.dashboard.powerbi.metadata import PowerbiSource
from metadata.ingestion.source.dashboard.powerbi.metrics import calculated_column_inputs
from metadata.ingestion.source.dashboard.powerbi.models import (
    ConnectionFile,
    DataModelSchema,
    Dataset,
    Group,
    PowerBiColumns,
    PowerBiMeasures,
    PowerBiTable,
    RemoteArtifacts,
)

MODEL_FQN = "powerbi.model.ds-sales"
CONFIG = {
    "source": {
        "type": "powerbi",
        "serviceName": "powerbi",
        "serviceConnection": {
            "config": {"type": "PowerBI", "clientId": "client_id", "clientSecret": "secret", "tenantId": "tenant_id"}
        },
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

SALES = PowerBiTable(
    name="Sales",
    columns=[
        PowerBiColumns(name="Amount"),
        PowerBiColumns(name="Discount"),
        PowerBiColumns(name="Order Date"),
        PowerBiColumns(name="Net Amount", expression="[Amount] - Sales[Discount]"),
        PowerBiColumns(name="Net Doubled", expression="[Net Amount] * 2"),
        PowerBiColumns(name="Region Name", expression="RELATED(Region[Name])"),
        PowerBiColumns(name="Year", expression="YEAR('Sales'[Order Date])"),
        PowerBiColumns(name="Loop", expression="[Loop] + 1"),
        PowerBiColumns(name="Loaded On", expression="TODAY()"),
        PowerBiColumns(name="Over Total", expression="[Amount] / [Total Sales]"),
    ],
    measures=[PowerBiMeasures(name="Total Sales", expression="SUM(Sales[Amount])")],
)
REGION = PowerBiTable(
    name="Region",
    columns=[
        PowerBiColumns(name="Name"),
        PowerBiColumns(name="Opened"),
        PowerBiColumns(name="Year", expression="YEAR([Opened])"),
    ],
)
MODEL_TABLES = [SALES, REGION]


def _column(table: PowerBiTable, name: str) -> PowerBiColumns:
    return next(column for column in table.columns or [] if column.name == name)


@pytest.mark.parametrize(
    ("table", "column", "expected"),
    [
        (SALES, "Net Amount", [("Sales", "Amount"), ("Sales", "Discount")]),
        (SALES, "Net Doubled", [("Sales", "Amount"), ("Sales", "Discount")]),
        (SALES, "Region Name", [("Region", "Name")]),
        (SALES, "Year", [("Sales", "Order Date")]),
        (SALES, "Loop", []),
        (SALES, "Loaded On", []),
        (SALES, "Over Total", [("Sales", "Amount")]),
        (REGION, "Year", [("Region", "Opened")]),
    ],
)
def test_a_calculated_column_reads_the_data_columns_behind_its_references(table, column, expected):
    assert sorted(calculated_column_inputs(MODEL_TABLES, table, _column(table, column))) == expected


@pytest.fixture
def source() -> PowerbiSource:
    with (
        patch("metadata.ingestion.source.dashboard.dashboard_service.DashboardServiceSource.test_connection"),
        patch("metadata.ingestion.source.dashboard.dashboard_service.create_connection"),
    ):
        workflow_config = OpenMetadataWorkflowConfig.model_validate(CONFIG)
        return PowerbiSource.create(
            CONFIG["source"], OpenMetadata(workflow_config.workflowConfig.openMetadataServerConfig)
        )


def _warehouse(name: str, columns: list[str]) -> Table:
    fqn = f"mssql.sales.dbo.{name}"
    return Table(
        id=uuid.uuid4(),
        name=name,
        fullyQualifiedName=fqn,
        columns=[
            Column(name=column, dataType=DataType.STRING, fullyQualifiedName=f"{fqn}.{column}") for column in columns
        ],
    )


def _data_model() -> DashboardDataModel:
    def table(powerbi_table: PowerBiTable) -> Column:
        table_fqn = f"{MODEL_FQN}.{powerbi_table.name}"
        return Column(
            name=powerbi_table.name,
            dataType=DataType.TABLE,
            fullyQualifiedName=table_fqn,
            children=[
                Column(name=column.name, dataType=DataType.STRING, fullyQualifiedName=f"{table_fqn}.{column.name}")
                for column in powerbi_table.columns or []
            ],
        )

    return DashboardDataModel(
        id=uuid.uuid4(),
        name="ds-sales",
        fullyQualifiedName=MODEL_FQN,
        service=EntityReference(id=uuid.uuid4(), type="dashboardService"),
        dataModelType=DataModelType.PowerBIDataModel,
        columns=[table(SALES), table(REGION)],
    )


def _calculated_entries(source: PowerbiSource, table: PowerBiTable, warehouse: Table, model_tables) -> dict:
    with patch.object(source.metadata, "search_in_any_service", return_value=warehouse):
        [lineage] = list(
            source._get_table_and_datamodel_lineage(
                db_service_prefix=None,
                table=table,
                datamodel_entity=_data_model(),
                model_tables=model_tables,
            )
        )
    return {
        model_str(entry.toColumn): (
            [model_str(column) for column in entry.fromColumns or []],
            model_str(entry.function),
        )
        for entry in lineage.right.edge.lineageDetails.columnsLineage or []
        if entry.function is not None
    }


def test_the_sales_edge_carries_the_calculated_columns_that_read_sales(source):
    # The warehouse table has a Name too, which RELATED(Region[Name]) must not resolve to.
    warehouse = _warehouse("Sales", ["Amount", "Discount", "Order Date", "Name"])
    entries = _calculated_entries(source, SALES, warehouse, MODEL_TABLES)

    amount, discount, order_date = (f"mssql.sales.dbo.Sales.{name}" for name in ("Amount", "Discount", "Order Date"))
    assert entries == {
        f"{MODEL_FQN}.Sales.Net Amount": ([amount, discount], "[Amount] - Sales[Discount]"),
        f"{MODEL_FQN}.Sales.Net Doubled": ([amount, discount], "[Net Amount] * 2"),
        f"{MODEL_FQN}.Sales.Year": ([order_date], "YEAR('Sales'[Order Date])"),
        f"{MODEL_FQN}.Sales.Over Total": ([amount], "[Amount] / [Total Sales]"),
    }


def test_a_column_read_from_another_table_goes_on_that_tables_edge(source):
    entries = _calculated_entries(source, REGION, _warehouse("Region", ["Name", "Opened"]), MODEL_TABLES)

    assert entries == {
        f"{MODEL_FQN}.Sales.Region Name": (["mssql.sales.dbo.Region.Name"], "RELATED(Region[Name])"),
        f"{MODEL_FQN}.Region.Year": (["mssql.sales.dbo.Region.Opened"], "YEAR([Opened])"),
    }


def test_without_the_model_tables_only_the_tables_own_columns_are_resolved(source):
    entries = _calculated_entries(source, REGION, _warehouse("Region", ["Name", "Opened"]), None)

    assert entries == {f"{MODEL_FQN}.Region.Year": (["mssql.sales.dbo.Region.Opened"], "YEAR([Opened])")}


def test_the_pbit_lineage_path_resolves_against_the_files_tables(source):
    source.datamodel_file_mappings = [
        DataModelSchema(
            tables=MODEL_TABLES,
            connectionFile=ConnectionFile(RemoteArtifacts=[RemoteArtifacts(DatasetId="ds-sales", ReportId="report")]),
        )
    ]

    with patch.object(source, "_get_table_and_datamodel_lineage", return_value=iter(())) as table_lineage:
        list(source.create_table_datamodel_lineage_from_files(db_service_prefix=None, datamodel_entity=_data_model()))

    assert [call.kwargs["model_tables"] for call in table_lineage.call_args_list] == [MODEL_TABLES, MODEL_TABLES]


def test_the_api_lineage_path_resolves_against_the_whole_dataset(source):
    dataset = Dataset(id="ds-sales", name="Sales Model", tables=MODEL_TABLES)
    workspace = Group(id="ws-1", name="Sales", datasets=[dataset])
    source.state.enter(workspace)
    source.context.get().__dict__["workspace"] = workspace
    source.context.get().__dict__["dashboard_service"] = "powerbi"

    with (
        patch.object(source.metadata, "get_by_name", return_value=_data_model()),
        patch.object(source, "_get_table_and_datamodel_lineage", return_value=iter(())) as table_lineage,
    ):
        list(source.yield_dashboard_lineage_details(workspace))

    assert [call.kwargs["model_tables"] for call in table_lineage.call_args_list] == [MODEL_TABLES, MODEL_TABLES]


def test_a_calculated_column_is_not_also_paired_by_name(source):
    # Year is calculated; the warehouse table's own year column must not be read as its copy.
    with patch.object(
        source.metadata,
        "search_in_any_service",
        return_value=_warehouse("Sales", ["Amount", "Discount", "Order Date", "Year"]),
    ):
        [lineage] = list(
            source._get_table_and_datamodel_lineage(
                db_service_prefix=None, table=SALES, datamodel_entity=_data_model(), model_tables=MODEL_TABLES
            )
        )

    year = [
        (
            [model_str(column) for column in entry.fromColumns or []],
            model_str(entry.function) if entry.function else None,
        )
        for entry in lineage.right.edge.lineageDetails.columnsLineage or []
        if model_str(entry.toColumn) == f"{MODEL_FQN}.Sales.Year"
    ]
    assert year == [(["mssql.sales.dbo.Sales.Order Date"], "YEAR('Sales'[Order Date])")]
