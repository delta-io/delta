# Invisible Columns

**Associated Github issue for discussions: https://github.com/delta-io/delta/issues/7731**

## Motivation

Some systems need to store data that belongs to each row without exposing it through the normal
table schema. Examples include ingestion metadata, stable identifiers, and data used by table
maintenance. Making these fields visible changes star expansion, positional writes, schema
inspection, and downstream schema propagation. Storing them outside data files makes them hard to
preserve when rows are rewritten.

## Feature summary

This RFC proposes a writer-only table feature named `invisibleColumns`. An invisible column is a
data column that MAY be stored in Delta data files and explicitly read by an engine that
understands Invisible Columns. It is not part of the table's visible schema and SHOULD be excluded
from default query output.

The visible schema remains in `metaData.schemaString`. The complete invisible schema is stored in
`metaData.invisibleSchemaString` in the same `metaData` action. Together, the two schemas describe
the table's data columns.

- Table feature name: `invisibleColumns`.
- Readers and writers, or writers only: Writer-only. See [Compatibility](#compatibility).
- Minimum protocol versions: Reader Version 1 and Writer Version 7. Invisible Columns do not
  require a reader-version increase; other table features may require a higher reader version.
- Required features: None. Other features used by the table retain their own prerequisites.
- Features it can't be used with: See
  [Constraints and interactions with other features](#constraints-and-interactions-with-other-features).
- Supported vs enabled: Supported when `invisibleColumns` is listed in `writerFeatures`, and
  active when the invisible schema contains at least one invisible field. There is no separate
  enabling table property. See [Enablement](#enablement).
- Table properties: None.

## Compatibility

The feature does not change the default behavior of readers that do not understand Invisible
Columns. Those readers continue to use `metaData.schemaString` and ignore unrequested fields in
data files.

| Case | Outcome |
|-|-|
| Old reader, table with this feature | Reads the visible schema normally and ignores the unknown metadata field and unrequested data fields, under [Protocol Evolution](#protocol-evolution). It cannot expose invisible columns through this feature. |
| Old writer, table with this feature | Rejected by the existing protocol-version or writer-feature checks, even if the invisible schema is empty. |
| New reader, table without this feature | Reads normally. An absent invisible schema is treated as an empty struct. |
| New writer, table without this feature | Writes normally. It must establish feature support before or in the commit that first writes a non-empty invisible schema. |
| Existing table that enables the feature (mix of old and new files) | Existing columns can change visibility without rewriting their data. Newly added invisible columns may be missing from existing files; the normal missing-column rules apply. |
| Checkpoint writer that doesn't know the feature | Rejected by the existing writer-feature requirements. Being allowed to read the table does not authorize writing its checkpoints. |

## Interactions with other features

| Feature | Interaction |
|-|-|
| Column mapping | See the Column mapping requirements under [Constraints and interactions with other features](#constraints-and-interactions-with-other-features). |
| Deletion vectors | Apply to rows, including their invisible-column values. Row positions, deletion-vector encoding, and statistics requirements are unchanged. |
| Checkpoints (classic, multi-part, V2, sidecars) | Existing [checkpoint requirements](#checkpoints-1) preserve the entire `metaData` action, including `invisibleSchemaString`, in all checkpoint formats. In V2 checkpoints, metadata remains in the top-level checkpoint, not a sidecar. See the Checkpoint Schema edit under [Appendix changes](#appendix-changes). |
| Log compaction | The reconciled `metaData` action retains both schema strings under the existing [log compaction](#log-compaction-files) rules. No new action or compaction rule is introduced. |
| Metadata cleanup and VACUUM | No new retention rules or files. Both schemas are retained as table metadata under the existing log-retention rules; invisible-column values remain in data files subject to normal file-retention rules. |
| Version checksum (`.crc`) | The existing `metadata` field contains the table metadata, including `invisibleSchemaString`. No new checksum field or metric is introduced. |
| Action reconciliation | The latest `metaData` action determines both schemas. The schema strings are not reconciled independently across commits. |
| Row tracking | Existing row-ID and row-commit-version assignment, preservation, and materialization rules are unchanged. Materialized-column names must remain distinct from names in the complete data schema, including physical names when Column Mapping is enabled. |
| Change data feed | Existing change-data-file and `add`/`remove` fallback rules apply. Invisible columns follow the same change-data requirements as visible data columns; their default visibility is unchanged. |
| Domain metadata | No new metadata domain or dependency on `domainMetadata`. Other features' domains continue to follow their existing rules. |
| In-commit timestamps | No change to commit timestamps, `commitInfo` placement, or time-travel rules. The schemas come from the metadata of the requested snapshot. |
| Type widening | See the Type widening requirements under [Constraints and interactions with other features](#constraints-and-interactions-with-other-features). |
| Generated columns, CHECK constraints, invariants, default columns, identity columns | See [Constraints and interactions with other features](#constraints-and-interactions-with-other-features). Column invariants apply to invisible columns under the same feature-support and enforcement requirements as visible columns. |
| Clustering and partitioning | See the Clustering and Partitioning requirements under [Constraints and interactions with other features](#constraints-and-interactions-with-other-features). |
| Statistics and data skipping | Invisible columns use the normal per-column statistics formats and column-resolution rules, including physical names with Column Mapping. Existing requirements for statistics, including those for clustering and deletion vectors, still apply. Readers use statistics for the columns they read; no new statistics format is introduced. |
| IcebergCompatV1/V2/V3, Iceberg writer compat | Invisible columns are prohibited when IcebergCompatV1/V2/V3 is enabled. This also excludes use with the proposed [IcebergWriterCompatV1](iceberg-writer-compat-v1.md), which requires IcebergCompatV2 to be enabled. |
| Catalog-managed tables | Existing catalog commit, publication, and maintenance requirements apply to updates containing either schema. This feature introduces no new catalog operation. |
| Collations, variant, timestamp without timezone, other types | No new data-type encoding or comparison rules. Hiding a column does not bypass its type's feature prerequisites or reader and writer requirements. |

## Concurrency

Both schema strings belong to the same `metaData` action. Concurrent changes to either schema
are table-metadata changes and are subject to the existing serializable-write requirements;
there is no separate invisible-schema action or conflict domain. A visibility transition updates
both schemas in one metadata action.

With Column Mapping, columns added to either schema share the existing column-ID namespace and
`delta.columnMapping.maxColumnId`. Writers cannot allocate IDs independently for the two schemas.

## Alternatives considered

- **Annotate invisible fields in `schemaString`.** Readers unaware of the annotation would still
  expose those fields as ordinary columns. Keeping them out of `schemaString` preserves the
  intended visible schema for readers that do not understand this writer-only feature.
- **Store the invisible schema in Domain Metadata.** This separates the two schemas across
  action types. Storing them in the same `metaData` action keeps their update and reconciliation
  together.

## Open questions

None.

--------

> ***Add the following section after [Type Widening](#type-widening)***

# Invisible Columns

Invisible Columns allow data columns to be present in data files while remaining absent from the
visible schema. They SHOULD be excluded from default reader output.

## Enablement

`invisibleColumns` is a writer-only feature and MUST NOT appear in `readerFeatures`. A table
**supports** Invisible Columns when it uses Writer Version 7 and its `writerFeatures` contain
`invisibleColumns`.

Invisible Columns are **active** in a snapshot when the table supports the feature and the
schema in `metaData.invisibleSchemaString` contains at least one invisible field as defined below.

A writer MAY enable the feature and populate `metaData.invisibleSchemaString` in the same commit.
A commit that writes a non-empty invisible schema MUST establish or preserve feature support.

## Schema

> ***In [Change Metadata](#change-metadata), replace the `schemaString` row and add the
> `invisibleSchemaString` row as follows***

Field Name | Data Type | Description | optional/required
-|-|-|-
`schemaString` | [Schema Struct](#schema-serialization-format) | Visible schema of the table, encoded as a JSON string. | required
`invisibleSchemaString` | [Schema Struct](#schema-serialization-format) | Invisible schema of the table, encoded as a JSON string. See [Invisible Columns](#invisible-columns). | optional

### Invisible Columns Metadata

The `metaData.invisibleSchemaString` field contains the complete invisible schema for the snapshot
as a JSON string. It uses the same [Schema Serialization Format](#schema-serialization-format) as
`metaData.schemaString`.

The valid states are:

- `invisibleSchemaString` is absent.
- The value of `invisibleSchemaString` is a JSON-encoded struct, possibly representing an empty
  struct.

An absent `invisibleSchemaString`, or a string encoding an empty struct, means that the
snapshot has no invisible columns.
`null` is not a valid value for `invisibleSchemaString`.

### Complete data schema

For a snapshot:

- The **visible schema** is the struct in `metaData.schemaString`.
- The **invisible schema** is the struct in `metaData.invisibleSchemaString`, or an empty struct
  when the field is absent.
- The **complete data schema** recursively merges the visible and invisible schemas.

Invisible columns MAY occur at any nesting level, including within structs, arrays, and maps.
The invisible schema MUST include the ancestor containers needed to describe a nested invisible
field's [field path](#field-path). A field in the complete data schema is visible if its path is
present in the visible schema; otherwise it is invisible. A shared ancestor remains visible;
its presence in the invisible schema only provides the structure needed for its invisible fields.
An invisible ancestor and all its descendants are absent from the visible schema.

The merge combines struct fields at matching field paths, retaining fields present in only one
schema. Shared structs are merged recursively; arrays and maps recursively merge their element,
key, and value types. Shared definitions MUST agree except for the nested struct fields contributed
by each schema. Their nullability and metadata, including column-mapping metadata, MUST match.
A shared ancestor represents one column in the complete data schema, not two distinct columns.
The complete data schema MUST satisfy the column-name uniqueness requirements of the
[Schema Serialization Format](#schema-serialization-format).

An invisible column is a data column. Except for the visibility behavior and the
[constraints below](#constraints-and-interactions-with-other-features), it has the same Delta
Protocol semantics and guarantees as a visible column. Writers that support Invisible Columns,
and readers that expose them, MUST apply every requirement that inspects, validates, reads, or
writes a data column to an invisible column in the same way as to a visible column.

## Reader Requirements for Invisible Columns

Invisible Columns impose no requirements on readers that do not understand the feature. Such
readers ignore the unrecognized `metaData.invisibleSchemaString` field under
[Protocol Evolution](#protocol-evolution) and continue to expose and read the visible schema from
`metaData.schemaString`.

A reader that chooses to expose invisible columns SHOULD:

- Use `metaData.schemaString` as the default table schema.
- Exclude invisible fields from default schema inspection and implicit column selection, including
  star expansion.

When reading invisible columns, a reader MUST:

- Resolve an explicitly requested invisible field against the invisible schema.
- Read invisible columns using their schema definitions and the normal Delta column-resolution
  rules.

These rules apply independently to each table version. Time travel therefore uses the visible and
invisible schemas from the requested snapshot.

A reader that requests invisible columns MUST reject the read if `metaData.invisibleSchemaString`
is malformed or the decoded schemas violate the [schema requirements](#schema). Readers that
request only visible columns MAY ignore `metaData.invisibleSchemaString`, including malformed
values. Unrecognized fields remain subject to [Protocol Evolution](#protocol-evolution).

## Writer Requirements for Invisible Columns

A writer that supports Invisible Columns MUST apply all Delta Protocol writer requirements to the
complete data schema. Values for invisible columns are validated and written under the same
protocol rules as values for visible columns.

A writer that emits a `metaData` action MUST preserve both schemas except for intentional schema
changes. Moving a column between the visible and invisible schemas MUST update both fields in the
same `metaData` action. Changing only a column's visibility does not require rewriting data files.
When Column Mapping is enabled, the column's physical name and ID MUST be preserved.

When rewriting an existing row, a writer MUST preserve its invisible-column values unless the
write explicitly changes them.

These requirements apply to table creation, appends, row updates and deletions, data-file
rewrites and maintenance, and operations that copy or restore table snapshots. Writers MUST also
satisfy the table's reader requirements, as required by [Table Features](#table-features).

The existing metadata-preservation rules for [checkpoints](#checkpoints-1) and
[log compaction](#log-compaction-files) apply to both schema strings. Invisible Columns introduce
no additional metadata-cleanup or data-file-retention requirements.

### Constraints and interactions with other features

- **Column mapping.** When [Column Mapping](#column-mapping) is enabled, invisible columns are
  assigned physical names and column IDs under the same requirements as visible columns.
  Column IDs MUST be unique across the complete data schema, and the existing physical field path
  uniqueness requirements apply across both schemas.
  The table's `delta.columnMapping.maxColumnId` tracks the maximum ID assigned in either schema.
- **CHECK constraints.** CHECK constraints involving invisible columns have the same support,
  validation, and enforcement requirements as those involving visible columns.
- **Partitioning.** An invisible column MUST NOT be a partition column.
- **Clustering.** An invisible column MAY be a clustering column, subject to the same
  [Clustered Table](#clustered-table) requirements as a visible column.
- **Identity columns.** An invisible column MUST NOT be an identity column.
- **Generated columns and column defaults.** An invisible column MUST NOT be a generated column,
  and MUST NOT carry a column default.
- **Type widening.** Invisible columns support [Type Widening](#type-widening) under the same
  protocol requirements as visible columns.
- **IcebergCompat.** When any of `icebergCompatV1`, `icebergCompatV2`, or `icebergCompatV3` is
  enabled, a writer MUST reject a schema containing invisible columns.

## Removing the feature

To remove `invisibleColumns` from the table's `writerFeatures`, a writer MUST ensure that every
invisible field has been moved into the visible schema and the invisible schema is empty.
These schema changes MUST be committed before or together with the protocol change.

After removal, writers that do not support Invisible Columns MAY write to the table, provided
they support the table's remaining protocol requirements.

## Examples

For an invisible `long` column named `source_sequence_number`, the `metaData` action contains the
following field. These schematic excerpts expand the JSON string in `invisibleSchemaString` for
readability and use ellipses to omit other metadata fields. They are not literal JSON log records.

```text
{
  "metaData": {
    ...,
    "invisibleSchemaString": {
      "type": "struct",
      "fields": [
        {
          "name": "source_sequence_number",
          "type": "long",
          "nullable": true,
          "metadata": {}
        }
      ]
    },
    ...
  }
}
```

An empty invisible schema is shown below and is equivalent to omitting the
`invisibleSchemaString` field:

```text
{
  "metaData": {
    ...,
    "invisibleSchemaString": {
      "type": "struct",
      "fields": []
    },
    ...
  }
}
```

For example, `profile.name` in the visible schema and `profile.internal_id` in the invisible schema
share the ancestor `profile`. The complete data schema contains one `profile` struct with both
fields. `profile` remains visible; only `profile.internal_id` is invisible.

The following two actions form a complete initial commit, `_delta_log/00000000000000000000.json`,
for an empty, unpartitioned table with a visible `id` column and an invisible
`source_sequence_number` column. The actions are formatted across multiple lines for readability;
in the log, each action occupies one line. Both schema fields are strings containing JSON.

The `protocol` action:

```json
{
  "protocol": {
    "minReaderVersion": 1,
    "minWriterVersion": 7,
    "writerFeatures": ["invisibleColumns"]
  }
}
```

The `metaData` action:

```json
{
  "metaData": {
    "id": "b6e26669-89be-435c-a87e-5fa015131b7a",
    "format": {
      "provider": "parquet",
      "options": {}
    },
    "schemaString":
      "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}}]}",
    "invisibleSchemaString":
      "{\"type\":\"struct\",\"fields\":[{\"name\":\"source_sequence_number\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}}]}",
    "partitionColumns": [],
    "configuration": {}
  }
}
```

## Changes to existing sections

The current `schemaString` row in [Change Metadata](#change-metadata) is:

Field Name | Data Type | Description | optional/required
-|-|-|-
schemaString|[Schema Struct](#Schema-Serialization-Format)| Schema of the table | required

The replacement row and the new `invisibleSchemaString` row are specified in [Schema](#schema).

The current requirements in
[Consistency Between Table Metadata and Data Files](#consistency-between-table-metadata-and-data-files)
are:

> - Any data file column that exists in the table schema MUST have the same type (except as allowed by the [Type Widening](#type-widening) table feature, if enabled).
> - Values for all partition columns present in the schema MUST be present for all files in the table.
> - Columns present in the schema of the table MAY be missing from data files. Readers SHOULD fill these missing columns in with `null`.

> ***Replace the requirements in
> [Consistency Between Table Metadata and Data Files](#consistency-between-table-metadata-and-data-files)
> with the following***

- Any data file column that exists in the complete data schema MUST have the same
  type as the corresponding schema column, except as allowed by the
  [Type Widening](#type-widening) table feature, if enabled.
- Values for all partition columns present in the schema MUST be present for all files in the
  table.
- Columns in the complete data schema MAY be missing from data files. Readers SHOULD treat
  the value of a missing column as `null`.

## Appendix changes

> ***Add the following row to
> [Valid Feature Names in Table Features](#valid-feature-names-in-table-features)***

Feature | Name | Readers or Writers?
-|-|-
[Invisible Columns](#invisible-columns) | `invisibleColumns` | Writers only

> ***In [Checkpoint Schema](#checkpoint-schema), add `invisibleSchemaString` after `schemaString`
> in the `metaData` struct of the example***

```text
|-- metaData: struct
|    ...
|    |-- schemaString: string
|    |-- invisibleSchemaString: string
|    ...
```

No changes to [Table Properties](#table-properties) are needed because this feature introduces
no table properties.

> ***Add the following entries after Type Widening in the table of contents***

```markdown
- [Invisible Columns](#invisible-columns)
  - [Enablement](#enablement-1)
  - [Schema](#schema)
    - [Invisible Columns Metadata](#invisible-columns-metadata)
    - [Complete data schema](#complete-data-schema)
  - [Reader Requirements for Invisible Columns](#reader-requirements-for-invisible-columns)
  - [Writer Requirements for Invisible Columns](#writer-requirements-for-invisible-columns)
    - [Constraints and interactions with other features](#constraints-and-interactions-with-other-features)
  - [Removing the feature](#removing-the-feature)
  - [Examples](#examples)
```
