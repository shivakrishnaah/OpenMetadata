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
A column edge from query or view lineage carries the SQL that computes its target column.

The SQL is parsed for real; only the table lookup is stubbed.
"""

import uuid
from unittest.mock import MagicMock, patch

import pytest

from metadata.generated.schema.entity.data.table import Table
from metadata.ingestion.lineage.models import Dialect
from metadata.ingestion.lineage.parser import LineageParser, column_functions_of
from metadata.ingestion.lineage.sql_lineage import get_lineage_by_query

SERVICE = "dbx"
SCHEMA_FQN = f"{SERVICE}.main.silver"


def _table(name: str, columns: list[str], schema_fqn: str = SCHEMA_FQN) -> Table:
    fqn = f"{schema_fqn}.{name}"
    return Table(
        id=uuid.uuid4(),
        name=name,
        fullyQualifiedName=fqn,
        columns=[{"name": column, "dataType": "STRING", "fullyQualifiedName": f"{fqn}.{column}"} for column in columns],
    )


MEMBER = _table("member", ["member_id", "gender", "birth_date", "is_current"])
ADDRESS = _table("member_address", ["member_id", "zip5"])
DIM_MEMBER = _table(
    "dim_member", ["member_key", "gender", "birth_date", "age_years", "address_count"], f"{SERVICE}.main.gold"
)
CTAS = """CREATE OR REPLACE TABLE gold.dim_member AS
WITH m AS (SELECT member_id, upper(trim(gender)) AS gender, birth_date FROM silver.member WHERE is_current = true),
     a AS (SELECT member_id, count(*) AS addr_count FROM silver.member_address GROUP BY member_id)
SELECT sha2(m.member_id, 256) AS member_key, m.gender, m.birth_date,
       floor(datediff(current_date(), m.birth_date) / 365.25) AS age_years, coalesce(a.addr_count, 0) AS address_count
FROM m LEFT JOIN a ON a.member_id = m.member_id"""


def _functions(query: str, tables: list[Table]) -> dict[tuple[str, str], str | None]:
    """Each column edge the query gives, as (from column, to column), with its function."""
    by_name = {table.name.root: table for table in tables}

    def lookup(table_name: str, **_) -> list[Table] | None:
        table = by_name.get(table_name.rsplit(".", maxsplit=1)[-1])
        return [table] if table else None

    with patch("metadata.ingestion.lineage.sql_lineage.get_table_entities_from_query", side_effect=lookup):
        results = list(
            get_lineage_by_query(
                MagicMock(),
                service_names=SERVICE,
                database_name="main",
                schema_name="silver",
                query=query,
                dialect=Dialect.DATABRICKS,
            )
        )
    edges = {}
    for result in results:
        assert result.right is not None, result.left
        for column_lineage in result.right.lineage_details.columnsLineage or []:
            for from_column in column_lineage.fromColumns:
                key = (from_column.root.rsplit(".", 2)[-2:][1], column_lineage.toColumn.root.rsplit(".", 1)[-1])
                edges[key] = column_lineage.function.root if column_lineage.function else None
    return edges


def test_a_computed_column_carries_the_sql_that_computes_it_from_the_tables():
    edges = _functions(CTAS, [MEMBER, ADDRESS, DIM_MEMBER])
    assert edges[("member_id", "member_key")] == "SHA2(member.member_id, 256)"
    assert edges[("birth_date", "age_years")] == "FLOOR(DATEDIFF(DAY, member.birth_date, CURRENT_DATE) / 365.25)"


def test_a_column_a_cte_computes_carries_what_the_cte_computes():
    edges = _functions(CTAS, [MEMBER, ADDRESS, DIM_MEMBER])
    # The final SELECT copies `gender` from the CTE; the CTE computes it.
    assert edges[("gender", "gender")] == "UPPER(TRIM(member.gender))"


def test_a_copied_column_has_no_function():
    edges = _functions(CTAS, [MEMBER, ADDRESS, DIM_MEMBER])
    assert edges[("birth_date", "birth_date")] is None


def test_a_view_column_carries_its_expression():
    lines = _table("lines", ["id", "price", "qty"])
    view = _table("v", ["id", "revenue"], f"{SERVICE}.main.gold")
    edges = _functions(
        "CREATE VIEW gold.v AS SELECT a.id, a.price * a.qty AS revenue FROM silver.lines a", [lines, view]
    )
    assert edges[("price", "revenue")] == "a.price * a.qty"
    assert edges[("qty", "revenue")] == "a.price * a.qty"
    assert edges[("id", "id")] is None


@pytest.mark.parametrize(
    ("query", "expected"),
    [
        pytest.param(CTAS, {"member_key", "gender", "age_years", "address_count"}, id="ctas"),
        pytest.param(
            "INSERT INTO gold.f (amount_usd, region) SELECT o.amount * fx.rate, upper(o.region) "
            "FROM silver.orders o JOIN silver.fx fx ON o.ccy = fx.ccy",
            {"amount_usd", "region"},
            id="insert with a column list",
        ),
        pytest.param("INSERT INTO t SELECT a + 1 AS x FROM s1 UNION ALL SELECT b FROM s2", set(), id="union"),
        pytest.param("SELECT a + 1 AS x FROM s1; SELECT b FROM s2", set(), id="two statements"),
        pytest.param("this is not sql", set(), id="unreadable"),
        pytest.param("SELECT upper(gender) AS gender FROM silver.member", set(), id="a select that only reads"),
    ],
)
def test_the_outputs_a_query_computes(query, expected):
    assert set(LineageParser(query, Dialect.DATABRICKS).column_functions) == expected


def test_an_aggregation_a_cte_computes_is_followed_into_the_cte():
    functions = LineageParser(CTAS, Dialect.DATABRICKS).column_functions
    assert functions["address_count"] == "COALESCE(COUNT(*), 0)"


def test_a_target_column_list_names_the_outputs_by_position():
    query = "INSERT INTO gold.f (amount_usd, region) SELECT o.amount * fx.rate, upper(o.region) FROM silver.orders o JOIN silver.fx fx ON o.ccy = fx.ccy"
    functions = LineageParser(query, Dialect.DATABRICKS).column_functions
    assert functions == {"amount_usd": "o.amount * fx.rate", "region": "UPPER(o.region)"}


def test_a_literal_is_kept_as_the_query_writes_it():
    query = "CREATE TABLE gold.g AS SELECT CASE WHEN gender = 'F' THEN 'Female' ELSE 'Other' END AS label FROM silver.member"
    assert LineageParser(query, Dialect.DATABRICKS).column_functions == {
        "label": "CASE WHEN member.gender = 'F' THEN 'Female' ELSE 'Other' END"
    }


def test_a_statement_that_writes_another_table_gives_none_for_the_target():
    query = "CREATE TABLE gold.other AS SELECT upper(gender) AS gender FROM silver.member"
    assert column_functions_of(query, Dialect.DATABRICKS, writes="main.gold.dim_member") == {}
    assert column_functions_of(query, Dialect.DATABRICKS, writes="main.gold.other") == {
        "gender": "UPPER(member.gender)"
    }
