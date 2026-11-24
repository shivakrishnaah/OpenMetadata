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
A Power Query navigation step names its database, schema and table however the query spaces it.
"""

import uuid

import pytest
from tests.unit.topology.dashboard.test_powerbi_source_table_match import source  # noqa: F401

from metadata.generated.schema.entity.data.dashboardDataModel import DashboardDataModel, DataModelType
from metadata.generated.schema.type.entityReference import EntityReference
from metadata.ingestion.source.dashboard.powerbi.metadata import (
    BIGQUERY_STEP,
    DATABASE_STEP,
    SCHEMA_STEP,
    TABLE_STEP,
)
from metadata.ingestion.source.dashboard.powerbi.models import (
    Dataset,
    DatasetExpression,
    Group,
    PowerBiTable,
    PowerBITableSource,
)

SPACED_DATABRICKS = """let
    Source = Databricks.Catalogs(ServerHostname, HttpPath, [Catalog = null, Database = null]),
    Catalog = Source{[Name = CatalogName, Kind = "Database"]}[Data],
    Schema = Catalog{[Name = SchemaName, Kind = "Schema"]}[Data],
    Data = Schema{[Name = "orders", Kind = "Table"]}[Data]
in
    Data"""
COMPACT_DATABRICKS = """let
    Source = Databricks.Catalogs("adb-1.azuredatabricks.net", "/sql/1.0/warehouses/1", [Catalog=null]),
    Catalog = Source{[Name="sales",Kind="Database"]}[Data],
    Schema = Catalog{[Name="gold",Kind="Schema"]}[Data],
    Data = Schema{[Name="orders",Kind="Table"]}[Data]
in
    Data"""
PARAMETERS = [
    DatasetExpression(name="CatalogName", expression='"sales" meta [IsParameterQuery=true, Type="Text"]'),
    DatasetExpression(name="SchemaName", expression='"gold" meta [IsParameterQuery=true, Type="Text"]'),
]


@pytest.mark.parametrize(
    ("step", "text", "groups"),
    [
        (DATABASE_STEP, '{[Name="sales",Kind="Database"]}', ("sales", None)),
        (DATABASE_STEP, '{[Name = "sales", Kind = "Database"]}', ("sales", None)),
        (DATABASE_STEP, '{[ Name = CatalogName , Kind = "Database" ]}', (None, "CatalogName")),
        (SCHEMA_STEP, '{[Name=SchemaName,Kind="Schema"]}', (None, "SchemaName")),
        (TABLE_STEP, '{[Name = "orders", Kind = "Table"]}', ("orders", None)),
        (BIGQUERY_STEP, '{[Name = "dataset", Kind = "Schema"]}', ("dataset", "Schema")),
        (BIGQUERY_STEP, '{[Name="project"]}', ("project", None)),
    ],
)
def test_a_navigation_step_is_read_however_it_is_spaced(step, text, groups):
    match = step.search(text)
    assert match is not None
    assert match.groups() == groups


@pytest.mark.parametrize("expression", [SPACED_DATABRICKS, COMPACT_DATABRICKS], ids=["spaced", "compact"])
def test_a_databricks_catalog_source_names_its_table_however_it_is_spaced(source, expression):  # noqa: F811
    dataset_id = str(uuid.uuid4())
    table = PowerBiTable(name="Orders", source=[PowerBITableSource(expression=expression)], columns=[])
    source.state.enter(
        Group(id="workspace", datasets=[Dataset(id=dataset_id, name="Sales", tables=[table], expressions=PARAMETERS)])
    )
    data_model = DashboardDataModel(
        id=uuid.uuid4(),
        name=dataset_id,
        service=EntityReference(id=uuid.uuid4(), type="dashboardService"),
        dataModelType=DataModelType.PowerBIDataModel,
        columns=[],
    )
    assert source._parse_table_info_from_source_exp(table, data_model) == [
        {"database": "sales", "schema": "gold", "table": "orders"}
    ]
