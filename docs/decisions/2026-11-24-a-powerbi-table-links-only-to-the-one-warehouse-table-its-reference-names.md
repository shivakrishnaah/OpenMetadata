# A Power BI table links only to the one warehouse table its reference names

- **Status:** Accepted
- **Revisions:** v1 2026-11-24 (initial)
- **Deciders:** shivakrishnaah
- **Guard:** `ingestion/tests/unit/topology/dashboard/test_powerbi_source_table_match.py`
- **Related:** REVIEW.md "Lineage does not guess" and "One item does not fail the batch";
  `2026-11-12-sigma-lineage-searches-every-database-service-without-prefixes`;
  `2026-11-10-fivetran-table-lineage-requires-a-unique-entity`; Hex's `_resolve_table`

## Context

The Power BI connector found the warehouse table behind a data model table, or behind a dataflow
entity, with a wildcard search on `<service>.<database>.<schema>.<table>`, where every part it did
not know was `*`, and it linked the first hit. When no warehouse table was parsed from a table's
source, it searched for the Power BI table's own name in every service. So:

- a calculated or entered-data table named `Date` linked to any warehouse table called `date`;
- a table held by two services (the same warehouse ingested twice), or by two databases of one
  service (a Snowflake native query whose database is a parameter), linked to whichever the search
  returned first.

A wrong lineage edge sends impact analysis and root-cause tracing to a table the report never reads.
Hex and Sigma already resolve a reference to one table across the `dbServicePrefixes`, through the
shared table binding resolution.

## Decision

- Both lookups, data model tables and dataflow entities, resolve a reference through
  `_resolve_source_table`, the rule Hex follows (and Sigma, which takes no schema from a prefix):
  across every `dbServicePrefixes` entry (every database service when none is set), with the shared
  `TableBindingResolution` `SEARCH` lookup. When a prefix names the service, that service's own
  resolution applies (BigQuery's sharded tables); without one, the default resolution does.
- A reference is resolved only when its schema is known, from the source expression or from a
  prefix. SQL that does not qualify its tables (`FROM Orders`) resolves to none unless a prefix gives
  the schema: its default schema is a session setting the connector cannot see.
- A table whose source no parser reads a reference from is looked up by its Power BI name: a push
  dataset, a connector the parsers do not read such as `PostgreSQL.Database`, a DAX or entered-data
  table, and a source a parser recognises but cannot read (a stored procedure call, a query the SQL
  parser gives up on). Without a schema that lookup is a reference without a schema, so it needs a
  prefix that gives one.
- Exactly one table must match across all the prefixes. When several do, none is linked, and a
  warning names them and asks for `dbServicePrefixes` to choose one.
- The lineage stage runs once per prefix; a resolved table is linked only in the run of the prefix it
  was found under, so it gets one edge.
- Each reference is resolved on its own: a failed search is reported as an error for that reference
  in each prefix run that searches it, and the table's or dataflow's other references are still
  linked. A failure is never cached, so the next lookup of that reference searches again: a failed
  read is not "no table". A name holding a double quote, which `fqn.quote_name` rejects, is skipped,
  as before.
- A prefix is split as an FQN, so a quoted part holding a dot (`bq."google.com:proj".ds`) is one
  part; an entry that is not `service[.database[.schema[.table]]]` is skipped with a warning.

## Consequences

- A table looked up by its Power BI name, or read by unqualified SQL, links only when a
  `dbServicePrefixes` entry gives its schema. The name lookup is the "bare name" REVIEW.md rules out
  only in form: the match is the one table of that name in a schema the user named as where Power BI
  reads from. Its cost is that a table whose source names another object links to the table of its
  own name in that schema: a calculated or entered-data table (`Date`), or one read by a stored
  procedure (`EXEC dbo.GetOrderSummary`).
- With the default resolution a wildcard reference (BigQuery's `events_*`) links nothing, where it
  used to link one arbitrary member. Under a prefix that names a BigQuery service, BigQuery's
  resolution resolves it to its table family when that is the only match; a date-constrained one
  (`events_2024*`) it refuses, and the reference is skipped.
- With Use Admin APIs off, every table the API returns comes from the push datasets API with no
  source, so each is looked up by its name and links only under a `service.database.schema` prefix.
  Tables read from `.pbit` files carry their sources and are parsed.
- A search that fails in one prefix's run and succeeds in a later one finds a table that belongs to
  a prefix whose run has passed; the error is reported, and the edge is written by the next
  ingestion.
- A table that several services or databases hold gives no Power BI lineage until
  `dbServicePrefixes` narrows it to one, where it used to give an arbitrary one.
- A table whose first reference does not match a prefix still links its other references; before,
  the first mismatch ended the table's lookup.
- An ambiguous match is logged as a warning, and a reference no prefix gives a schema at info.
- Edges written before this change stay. `overrideLineage` replaces a data model's edges only on a
  run that writes a new edge to it, so a data model whose table now resolves to none keeps its old
  edge until it is deleted.
