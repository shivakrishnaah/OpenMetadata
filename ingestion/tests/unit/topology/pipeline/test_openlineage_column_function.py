"""
The column lineage function of an entry's OpenLineage input fields,
per ADR:2026-11-23-openlineage-input-transformations-are-the-column-lineage-function.
"""

import pytest

from metadata.ingestion.source.pipeline.openlineage.column_function import column_function


def field(name, *transformations):
    return {"field": name, "transformations": list(transformations)}


def described(description):
    return {"type": "DIRECT", "description": description}


@pytest.mark.parametrize(
    "input_fields, expected",
    [
        pytest.param(
            [field("amount", described("sum((amount * rate))"))], "sum((amount * rate))", id="one description"
        ),
        pytest.param(
            [field("amount", described("sum((amount * rate))")), field("rate", described("sum((amount * rate))"))],
            "sum((amount * rate))",
            id="inputs with the same description",
        ),
        pytest.param(
            [field("amount", described("sum((amount * rate))")), field("region", described("(region = 'EU')"))],
            "sum((amount * rate)); (region = 'EU')",
            id="different descriptions are all kept",
        ),
        pytest.param(
            [field("ccy", described("INNER JOIN ON (ccy = ccy)"), described("isnotnull(ccy)"))],
            "INNER JOIN ON (ccy = ccy); isnotnull(ccy)",
            id="an input's several transformations",
        ),
        pytest.param([field("amount", {"type": "DIRECT"})], None, id="a type without a description is not text"),
        pytest.param(
            [field("amount", {"description": ""}, None, "DIRECT"), field("rate", described("sum((amount * rate))"))],
            "sum((amount * rate))",
            id="empty and malformed entries are skipped",
        ),
        pytest.param([{"field": "amount", "transformations": 5}], None, id="transformations that are not a list"),
        pytest.param([{"field": "source_col"}], None, id="no transformations"),
    ],
)
def test_column_function(input_fields, expected):
    assert column_function(input_fields) == expected
