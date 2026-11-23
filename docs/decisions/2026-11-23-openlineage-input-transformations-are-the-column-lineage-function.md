# An OpenLineage input field's transformation descriptions are written to its column lineage entry's function

- **Status:** Accepted
- **Revisions:** v1 2026-11-23 (initial)
- **Deciders:** shivakrishnaah
- **Guard:** `OpenLineageColumnFunctionTest`;
  `OpenLineageMapperTest#mapRunEvent_withInputTransformations_writesEachTablesEntryFromItsOwnInputs`;
  `test_openlineage_column_function.py`;
  `test_openlineage.py#test_get_column_lineage_writes_each_entry_from_its_own_inputs`;
  `OpenLineageLineageResolutionIT#inputTransformations_areWrittenToTheColumnLineageFunction`
- **Related:** #38485, #35862

## Context

Version 1-2-0 of the OpenLineage column lineage facet reports how an input contributes to an
output column as a `transformations` list on each input field (`type`, `subtype`, `description`,
`masking`), in place of the output field's `transformationDescription`. The REST endpoint did not
model the list and the ingestion source did not read it, so an expression a producer sent never
reached `ColumnLineage.function`. OpenMetadata's other writers of `function` put the expression
alone there: Tableau's calculated-field formula, SAP HANA's calculation-view formula, and
OpenLineage's own `transformationDescription`.

## Decision

- An entry's `function` is the distinct `description` texts of its input fields' transformations,
  in the order the facet lists them, joined with `; `. `type`, `subtype` and `masking` are not
  written: `function` holds an expression, as it does for every other writer.
- Column lineage entries keep the shape they had: one per output column and input table (on the
  ingestion source, one per run of consecutive inputs from a table, as before). Only `function` is
  added, so an entry already stored keeps its key and the sink's merge updates its `function`.
- When no input of an entry has a description, the REST endpoint keeps the output field's
  `transformationDescription`, as it did before; the ingestion source, which never read that field,
  writes no `function`. `transformations` and its items require nothing.

## Consequences

- An entry from a producer that sends no descriptions has no `function`. Spark 1.55.0 sends the
  `type` and `subtype` of each transformation with an empty `description`, and by default reports
  join, filter and grouping columns in the facet's dataset-level `dataset` list, which is not read.
- The text describes the input fields as the event reports them. Lineage validation drops a
  `fromColumns` entry the table does not hold but keeps `function`, so the text can come from an
  input OpenMetadata has not ingested yet.
- A typed transformation on `ColumnLineage`, as #35862 proposes, would carry `type` and `subtype`;
  that change would supersede this record.
