# A Power BI service with a .pbit files source and no credentials is read from the files alone

- **Status:** Accepted
- **Revisions:** v1 2026-11-24 (initial)
- **Deciders:** shivakrishnaah
- **Guard:** `ingestion/tests/unit/topology/dashboard/test_powerbi_pbit_files_only.py`
- **Related:** #35746

## Context

The Power BI connector read every entity from the Power BI REST API, so `clientId`, `clientSecret`
and `tenantId` were required. `pbitFilesSource` only added table -> data model lineage to the data
models the API returned. An organisation that will not grant a service principal access to its
tenant can still export its reports as Power BI templates (.pbit). A template's `DataModelSchema`
holds what the API's dataset holds: the tables, their columns and source expressions, the measures,
and the model's expressions.

## Decision

- `clientId`, `clientSecret` and `tenantId` are not required by the schema. When `pbitFilesSource`
  is set and none of the three is, the service is read from the files alone. When any of them is
  set, the API is read, and a missing one fails at the first token request with an error naming all
  three. This departs from the schema standard's rule that a field the connection cannot work
  without is required: "required unless `pbitFilesSource` is set" has no form that the Python, Java
  and UI generators all read.
- In that mode each .pbit becomes the dataset the API would return. A file is named by its path
  under the source (for a bucket source, the bucket followed by the object key), without the
  extension, and the files are read in that order. Its dataset id is
  the first `RemoteArtifacts.DatasetId` of its `Connections` entry. A file that names none gets
  `uuid5(PBIT_DATASET_NAMESPACE, <path>)`, where `PBIT_DATASET_NAMESPACE` is
  `uuid5(NAMESPACE_URL, "https://open-metadata.org/powerbi/pbit")`. The dataset's name is the
  file's name without the extension. Files that name the same dataset are one dataset: the first
  file read wins and the others are logged.
- Each read extracts its files, after downloading them for a bucket source, in a directory of its
  own inside `pbitFilesExtractDir` (the system temporary directory when a local source sets none), so
  neither a file removed from the source nor one another run left there is read.
- The datasets are held in one workspace, `pbit-files`, which has no name. Their data models get no
  `sourceUrl` and no `project`.
- What the run did not read is never confirmed as gone:
  - a source that cannot be read fails the data model listing;
  - a file that cannot be read fails it as well, and the stale data model pass is skipped. For a
    bucket source the files are the objects listed and downloaded, so one lost before it is read
    counts as unread;
  - the dashboard listing is never confirmed, since dashboards exist only in the Power BI service.
- The connection test's `CheckAccess` reads the files, and passes with a caveat when they hold no
  data model or some cannot be read. `GetDashboards` passes with a caveat that dashboards are not
  read.

## Consequences

- No dashboards, reports, charts, owners or dataflows are ingested, and nothing links back to the
  Power BI service.
- A file that names its published dataset gives the data model the id API ingestion would give it,
  so adding credentials later updates the same entity. A never-published file's data model changes
  id when the file is renamed or moved.
- Dashboards ingested earlier with credentials stay as they are when the credentials are removed.
