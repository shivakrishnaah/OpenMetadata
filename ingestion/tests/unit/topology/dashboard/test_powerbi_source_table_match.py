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
A PowerBI table links to the one warehouse table its reference names across the dbServicePrefixes, and to
nothing when no single table is confirmed (ADR:2026-11-24-a-powerbi-table-links-only-to-the-one-warehouse-table-its-reference-names).
"""

import logging
import uuid
from fnmatch import fnmatch
from unittest.mock import patch

import pytest

from metadata.entity_resolution.table_binding import UnsupportedTableReferenceError
from metadata.generated.schema.entity.data.dashboardDataModel import DashboardDataModel, DataModelType
from metadata.generated.schema.entity.data.table import Table
from metadata.generated.schema.metadataIngestion.dashboardServiceMetadataPipeline import LineageInformation
from metadata.generated.schema.metadataIngestion.workflow import OpenMetadataWorkflowConfig
from metadata.generated.schema.type.entityReference import EntityReference
from metadata.ingestion.ometa.ometa_api import OpenMetadata
from metadata.ingestion.ometa.utils import model_str
from metadata.ingestion.source.dashboard.powerbi import metadata as powerbi_metadata
from metadata.ingestion.source.dashboard.powerbi.metadata import PowerbiSource
from metadata.ingestion.source.dashboard.powerbi.models import (
    Dataflow,
    DataflowExportResponse,
    PowerBiTable,
    PowerBITableSource,
)

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
DATA_MODEL = DashboardDataModel(
    id=uuid.uuid4(),
    name="ds-sales",
    fullyQualifiedName="powerbi.model.ds-sales",
    service=EntityReference(id=uuid.uuid4(), type="dashboardService"),
    dataModelType=DataModelType.PowerBIDataModel,
    columns=[],
)
DATAFLOW_MODEL = DashboardDataModel(
    id=uuid.uuid4(),
    name="df-sales",
    fullyQualifiedName="powerbi.model.df-sales",
    service=EntityReference(id=uuid.uuid4(), type="dashboardService"),
    dataModelType=DataModelType.PowerBIDataFlow,
    columns=[],
)
ORDERS = PowerBiTable(name="Orders", columns=[])
QUALIFIED = [{"database": "sales", "schema": "dbo", "table": "Orders"}]


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


def _table(fqn: str) -> Table:
    return Table(id=uuid.uuid4(), name=fqn.rsplit(".", 1)[-1], fullyQualifiedName=fqn, columns=[])


def _catalog(source: PowerbiSource, tables: list[Table], prefixes: list[str] | None = None) -> list[str]:
    """OpenMetadata's search answers from `tables` with its wildcard matching; returns the table searches made."""
    searches: list[str] = []

    def es_search_from_fqn(entity_type, fqn_search_string, **_):
        if entity_type is not Table:
            return []
        searches.append(fqn_search_string)
        return [
            table for table in tables if fnmatch(model_str(table.fullyQualifiedName).lower(), fqn_search_string.lower())
        ]

    source.metadata.es_search_from_fqn = es_search_from_fqn
    source.metadata.get_by_name = lambda **_: None
    source.source_config.lineageInformation = LineageInformation(dbServicePrefixes=prefixes)
    return searches


def _linked(results) -> list[str]:
    return [model_str(either.right.edge.fromEntity.fullyQualifiedName) for either in results if either.right]


def _model_results(source: PowerbiSource, table_info: list[dict] | None, table: PowerBiTable = ORDERS) -> list:
    """What the data model's lineage yields over every dbServicePrefixes entry, as the base class runs it."""
    with patch.object(source, "_parse_table_info_from_source_exp", return_value=table_info):
        return [
            either
            for prefix in source.get_db_service_prefixes() or [None]
            for either in source._get_table_and_datamodel_lineage(
                db_service_prefix=prefix, table=table, datamodel_entity=DATA_MODEL
            )
        ]


def _model_lineage(source: PowerbiSource, table_info: list[dict] | None, table: PowerBiTable = ORDERS) -> list[str]:
    """The tables linked to the data model over every dbServicePrefixes entry."""
    return _linked(_model_results(source, table_info, table))


def _dataflow_results(source: PowerbiSource, tables: list[dict]) -> list:
    """What the dataflow's lineage yields over every dbServicePrefixes entry, as the base class runs it."""
    parsed = [{"entity_name": "Orders", "tables": tables, "sql": None}]
    with patch.object(source, "_parse_dataflow_m_document", return_value=parsed):
        return [
            either
            for prefix in source.get_db_service_prefixes() or [None]
            for either in source.create_dataflow_table_lineage(
                datamodel=Dataflow(objectId="df-sales", name="Sales"),
                datamodel_entity=DATAFLOW_MODEL,
                dataflow_export=DataflowExportResponse(name="Sales"),
                db_service_prefix=prefix,
            )
        ]


def _dataflow_lineage(source: PowerbiSource, tables: list[dict]) -> list[str]:
    """The tables linked to the dataflow over every dbServicePrefixes entry."""
    return _linked(_dataflow_results(source, tables))


def test_a_table_its_reference_names_is_linked(source):
    _catalog(source, [_table("mssql.sales.dbo.Orders"), _table("mssql.sales.staging.Orders")])
    assert _model_lineage(source, QUALIFIED) == ["mssql.sales.dbo.Orders"]


def test_a_table_named_only_by_power_bi_links_to_nothing(source):
    searches = _catalog(source, [_table("mssql.sales.dbo.Orders")])
    assert _model_lineage(source, None) == []
    assert searches == []


def test_a_parsed_table_without_a_schema_links_to_nothing(source, caplog):
    _catalog(source, [_table("mssql.sales.dbo.Orders")])
    with caplog.at_level(logging.INFO):
        assert _model_lineage(source, [{"database": "sales", "table": "Orders"}]) == []
    assert "its schema is unknown" in caplog.text


def test_a_prefix_gives_the_schema_of_a_table_named_only_by_power_bi(source):
    _catalog(source, [_table("mssql.sales.dbo.Orders")], prefixes=["mssql.sales.dbo"])
    assert _model_lineage(source, None) == ["mssql.sales.dbo.Orders"]


def test_a_prefix_whose_schema_differs_links_to_nothing(source):
    _catalog(source, [_table("mssql.sales.dbo.Orders")], prefixes=["mssql.sales.staging"])
    assert _model_lineage(source, QUALIFIED) == []


def test_a_table_two_services_hold_links_to_nothing(source, caplog):
    _catalog(source, [_table("mssql.sales.dbo.Orders"), _table("mssql_copy.sales.dbo.Orders")])
    with caplog.at_level(logging.WARNING):
        assert _model_lineage(source, QUALIFIED) == []
    assert "mssql.sales.dbo.Orders, mssql_copy.sales.dbo.Orders" in caplog.text
    assert "dbServicePrefixes" in caplog.text


def test_a_table_two_prefixes_find_links_to_nothing(source):
    _catalog(
        source,
        [_table("mssql.sales.dbo.Orders"), _table("mssql_copy.sales.dbo.Orders")],
        prefixes=["mssql", "mssql_copy"],
    )
    assert _model_lineage(source, QUALIFIED) == []


def test_a_table_one_of_the_prefixes_finds_is_linked_once(source):
    _catalog(source, [_table("mssql.sales.dbo.Orders"), _table("other.sales.dbo.Orders")], prefixes=["mssql", "pg"])
    assert _model_lineage(source, QUALIFIED) == ["mssql.sales.dbo.Orders"]


def test_one_table_two_prefixes_find_is_one_match(source):
    _catalog(source, [_table("mssql.sales.dbo.Orders")], prefixes=["mssql.sales", "mssql.sales.dbo"])
    assert _model_lineage(source, QUALIFIED) == ["mssql.sales.dbo.Orders"]


def test_a_failed_search_is_an_error_not_a_missing_table(source):
    def search_down(**_):
        raise RuntimeError("search is down")

    source.metadata.es_search_from_fqn = search_down
    source.metadata.get_by_name = lambda **_: None
    with patch.object(source, "_parse_table_info_from_source_exp", return_value=QUALIFIED):
        results = list(
            source._get_table_and_datamodel_lineage(db_service_prefix=None, table=ORDERS, datamodel_entity=DATA_MODEL)
        )
    [error] = [either.left.error for either in results if either.left]
    assert "search is down" in error


def test_a_dataflow_entity_links_to_the_table_its_reference_names(source):
    _catalog(source, [_table("mssql.sales.dbo.Orders"), _table("mssql.sales.staging.Orders")])
    assert _dataflow_lineage(source, QUALIFIED) == ["mssql.sales.dbo.Orders"]


def test_a_dataflow_reference_without_a_schema_links_to_nothing(source):
    _catalog(source, [_table("mssql.sales.dbo.Orders")])
    assert _dataflow_lineage(source, [{"database": "sales", "table": "Orders"}]) == []


def test_a_dataflow_reference_two_services_hold_links_to_nothing(source):
    _catalog(source, [_table("mssql.sales.dbo.Orders"), _table("mssql_copy.sales.dbo.Orders")])
    assert _dataflow_lineage(source, QUALIFIED) == []


def test_a_dataflow_reference_without_a_table_name_is_skipped(source):
    searches = _catalog(source, [_table("mssql.sales.dbo.Orders")])
    parsed = [{"entity_name": "Orders", "tables": [{"schema": "dbo"}], "sql": None}]
    with patch.object(source, "_parse_dataflow_m_document", return_value=parsed):
        results = list(
            source.create_dataflow_table_lineage(
                datamodel=Dataflow(objectId="df-sales", name="Sales"),
                datamodel_entity=DATAFLOW_MODEL,
                dataflow_export=DataflowExportResponse(name="Sales"),
                db_service_prefix=None,
            )
        )
    assert results == []
    assert searches == []


def test_each_reference_is_resolved_on_its_own(source):
    _catalog(source, [_table("mssql.sales.dbo.Orders"), _table("mssql.sales.staging.Orders")])
    assert _model_lineage(source, QUALIFIED) == ["mssql.sales.dbo.Orders"]
    staging = [{"database": "sales", "schema": "staging", "table": "Orders"}]
    assert _model_lineage(source, staging) == ["mssql.sales.staging.Orders"]


QUOTED = {"database": "sales", "schema": "dbo", "table": 'Order"s'}
CUSTOMERS = {"database": "sales", "schema": "dbo", "table": "Customers"}


def test_a_name_the_fqn_grammar_rejects_is_skipped_without_an_error(source):
    _catalog(source, [_table("mssql.sales.dbo.Orders")])
    for results in (_model_results(source, [QUOTED, *QUALIFIED]), _dataflow_results(source, [QUOTED, *QUALIFIED])):
        assert _linked(results) == ["mssql.sales.dbo.Orders"]
        assert [either.left for either in results if either.left] == []


def _search_down_for_customers(source: PowerbiSource, tables: list[Table]) -> None:
    _catalog(source, tables)
    catalog_search = source.metadata.es_search_from_fqn

    def es_search_from_fqn(entity_type, fqn_search_string, **kwargs):
        if "customers" in fqn_search_string.lower():
            raise RuntimeError("search is down")
        return catalog_search(entity_type, fqn_search_string, **kwargs)

    source.metadata.es_search_from_fqn = es_search_from_fqn


def test_a_failed_search_leaves_the_other_references_linked(source):
    _search_down_for_customers(source, [_table("mssql.sales.dbo.Orders"), _table("mssql.sales.dbo.Customers")])
    results = _model_results(source, [CUSTOMERS, *QUALIFIED])
    assert _linked(results) == ["mssql.sales.dbo.Orders"]
    assert ["search is down" in either.left.error for either in results if either.left] == [True]


def test_a_failed_dataflow_search_leaves_the_other_references_linked(source):
    _search_down_for_customers(source, [_table("mssql.sales.dbo.Orders"), _table("mssql.sales.dbo.Customers")])
    results = _dataflow_results(source, [CUSTOMERS, *QUALIFIED])
    assert _linked(results) == ["mssql.sales.dbo.Orders"]
    assert ["search is down" in either.left.error for either in results if either.left] == [True]


POSTGRES_ORDERS = PowerBiTable(
    name="Orders",
    source=[PowerBITableSource(expression='let Source = PostgreSQL.Database("pg", "db") in Source')],
    columns=[],
)


def test_a_table_whose_source_no_parser_reads_links_by_name_under_a_schema_prefix(source):
    _catalog(source, [_table("pg.db.public.Orders"), _table("mssql.sales.dbo.Orders")], prefixes=["pg.db.public"])
    assert _model_lineage(source, None, POSTGRES_ORDERS) == ["pg.db.public.Orders"]


def test_a_table_whose_source_no_parser_reads_links_to_nothing_without_a_schema_prefix(source):
    _catalog(source, [_table("pg.db.public.Orders")], prefixes=["pg"])
    assert _model_lineage(source, None, POSTGRES_ORDERS) == []


class _Unresolvable:
    """A service's resolution that cannot resolve the reference without more evidence (a BigQuery wildcard)."""

    def resolve(self, *_, **__):
        raise UnsupportedTableReferenceError("resolution incomplete")


def test_each_prefix_resolves_through_its_service_and_skips_what_it_cannot_resolve(source):
    _catalog(source, [_table("mssql.sales.dbo.Orders")], prefixes=["bq", "mssql"])
    services: list[str | None] = []
    shared = powerbi_metadata.get_table_binding_resolution

    def resolution(metadata, service):
        services.append(service)
        return _Unresolvable() if service == "bq" else shared(metadata, service)

    with patch.object(powerbi_metadata, "get_table_binding_resolution", side_effect=resolution):
        results = _model_results(source, QUALIFIED)
    assert services == ["bq", "mssql"]
    assert _linked(results) == ["mssql.sales.dbo.Orders"]
    assert [either.left for either in results if either.left] == []


def test_a_reference_is_searched_once_per_prefix_over_every_prefix_run(source):
    searches = _catalog(source, [_table("mssql.sales.dbo.Orders")], prefixes=["mssql", "pg", "other"])
    assert _model_lineage(source, QUALIFIED) == ["mssql.sales.dbo.Orders"]
    assert len(searches) == 3


def test_a_failed_search_is_reported_once_over_every_prefix_run(source):
    _catalog(source, [], prefixes=["mssql", "pg", "other"])

    def search_down(**_):
        raise RuntimeError("search is down")

    source.metadata.es_search_from_fqn = search_down
    results = _model_results(source, QUALIFIED)
    assert ["search is down" in either.left.error for either in results if either.left] == [True]


def test_a_quoted_prefix_part_holding_a_dot_is_one_part(source, caplog):
    _catalog(source, [_table('bq."google.com:proj".ds.Orders')], prefixes=['bq."google.com:proj".ds'])
    with caplog.at_level(logging.WARNING):
        assert _model_lineage(source, [{"table": "Orders"}]) == ['bq."google.com:proj".ds.Orders']
    assert "Skipping dbServicePrefixes entry" not in caplog.text


def test_the_database_of_a_reference_tells_two_tables_apart(source):
    _catalog(source, [_table("mssql.sales.dbo.Orders"), _table("mssql.hr.dbo.Orders")])
    assert _model_lineage(source, QUALIFIED) == ["mssql.sales.dbo.Orders"]
    assert _model_lineage(source, [{"schema": "dbo", "table": "Orders"}]) == []
    assert _model_lineage(source, [{"database": "hr", "schema": "dbo", "table": "Orders"}]) == ["mssql.hr.dbo.Orders"]


@pytest.mark.parametrize("prefix", ["mssql.hr", "mssql.sales.dbo.Customers"])
def test_a_prefix_whose_database_or_table_differs_links_to_nothing(source, prefix):
    _catalog(source, [_table("mssql.sales.dbo.Orders")], prefixes=[prefix])
    assert _model_lineage(source, QUALIFIED) == []


def test_a_prefix_matches_the_reference_ignoring_case(source):
    _catalog(source, [_table("mssql.sales.dbo.Orders")], prefixes=["mssql.SALES.DBO"])
    assert _model_lineage(source, QUALIFIED) == ["mssql.sales.dbo.Orders"]


def test_a_schema_another_prefix_gives_is_not_logged_as_unknown(source, caplog):
    _catalog(source, [_table("mssql.sales.dbo.Orders")], prefixes=["mssql", "mssql.sales.dbo"])
    with caplog.at_level(logging.INFO):
        assert _model_lineage(source, [{"database": "sales", "table": "Orders"}]) == ["mssql.sales.dbo.Orders"]
    assert "its schema is unknown" not in caplog.text


@pytest.mark.parametrize("malformed", ["pg.my.db.sch.extra", 'pg."db'])
def test_a_malformed_prefix_is_skipped_and_the_others_still_link(source, caplog, malformed):
    _catalog(source, [_table("mssql.sales.dbo.Orders")], prefixes=["mssql", malformed])
    with caplog.at_level(logging.WARNING):
        assert _model_lineage(source, QUALIFIED) == ["mssql.sales.dbo.Orders"]
    assert f"Skipping dbServicePrefixes entry [{malformed}]" in caplog.text
