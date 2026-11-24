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
  `_resolve_source_table`, the rule Hex and Sigma follow: across every `dbServicePrefixes` entry (every
  database service when none is set), with the shared `TableBindingResolution` `SEARCH` lookup. When
  a prefix names the service, that service's own resolution applies (BigQuery's sharded tables).
- A reference is resolved only when its schema is known, from the source expression or from a
  prefix. SQL that does not qualify its tables (`FROM Orders`) resolves to none: its default schema
  is a session setting the connector cannot see.
- A table whose source no parser reads a reference from (a push dataset, a connector the parsers do
  not read such as `PostgreSQL.Database`, a DAX or entered-data table) is looked up by its Power BI
  name. Without a schema that lookup is a reference without a schema, so it needs a prefix that gives
  one.
- Exactly one table must match across all the prefixes. When several do, none is linked, and a
  warning names them and asks for `dbServicePrefixes` to choose one.
- The lineage stage runs once per prefix; a resolved table is linked only in the run of the prefix it
  was found under, so it gets one edge.
- Each reference is resolved on its own: a failed search is reported as an error for that reference,
  once, in the prefix run that searched, and the table's or dataflow's other references are still
  linked. A name the FQN grammar rejects (a quote inside a quoted identifier) is skipped, as before.
- A prefix is split as an FQN, so a quoted part holding a dot (`bq."google.com:proj".ds`) is one
  part; an entry that is not `service[.database[.schema[.table]]]` is skipped with a warning.

## Consequences

- A table looked up by its Power BI name, or read by unqualified SQL, links only when a
  `dbServicePrefixes` entry gives its schema. The name lookup is the "bare name" REVIEW.md rules out
  only in form: the match is the one table of that name in a schema the user named as where Power BI
  reads from. Its cost is that a calculated or entered-data table named like a table in that schema
  (`Date`) links to it.
- A wildcard reference (BigQuery's `events_2024*`) links nothing; it used to link one arbitrary
  member.
- A table that several services or databases hold gives no Power BI lineage until
  `dbServicePrefixes` narrows it to one, where it used to give an arbitrary one.
- A table whose first reference does not match a prefix still links its other references; before,
  the first mismatch ended the table's lookup.
- An ambiguous match is logged as a warning, and a reference no prefix gives a schema at info.
- Edges written before this change stay. `overrideLineage` replaces a data model's edges only on a
  run that writes a new edge to it, so a data model whose table now resolves to none keeps its old
  edge until it is deleted.
