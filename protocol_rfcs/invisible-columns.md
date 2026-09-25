# Invisible Columns

**Associated Github issue for discussions: https://github.com/delta-io/delta/issues/7731**

## Overview

This RFC proposes a writer-only table feature named `invisibleColumns`. An invisible column is a
data column that may be stored in Delta data files and explicitly read by an engine that
understands Invisible Columns. It is not part of the table's visible schema and should be excluded
from default query output.

The visible schema remains in `metaData.schemaString`. The complete invisible schema is stored in
`metaData.invisibleSchemaString` in the same `metaData` action. Together, the two schemas describe
the table's data columns.

The feature does not change the default behavior of readers that do not understand Invisible
Columns. Those readers continue to use `metaData.schemaString` and ignore unrequested fields in
data files.

## Motivation

Some systems need to store data that belongs to each row without exposing it through the normal
table schema. Examples include ingestion metadata, stable identifiers, and data used by table
maintenance. Making these fields visible changes star expansion, positional writes, schema
inspection, and downstream schema propagation. Storing them outside data files makes them hard to
preserve when rows are rewritten.

--------

> ***In [Change Metadata](#change-metadata), replace the `schemaString` row and add the
> `invisibleSchemaString` row as follows***

Field Name | Data Type | Description | optional/required
-|-|-|-
`schemaString` | [Schema Struct](#schema-serialization-format) | Visible schema of the table, encoded as a JSON string. | required
`invisibleSchemaString` | [Schema Struct](#schema-serialization-format) | Invisible schema of the table, encoded as a JSON string. See [Invisible Columns](#invisible-columns). | optional

> ***Add the following section after [Type Widening](#type-widening)***

# Invisible Columns

Invisible Columns allow data columns to be present in data files while remaining absent from the
visible schema. They should be excluded from default reader output. Invisible columns cannot be
partition columns.

## Enablement

`invisibleColumns` is a writer-only feature and must not appear in `readerFeatures`. A table
**supports** Invisible Columns when it uses Writer Version 7 and its `writerFeatures` contain
`invisibleColumns`.

Invisible Columns are **active** in a snapshot when the table supports the feature and the
schema in `metaData.invisibleSchemaString` contains at least one field.

A writer may enable the feature and populate `metaData.invisibleSchemaString` in the same commit.
A commit that writes a non-empty invisible schema must establish or preserve feature support.

## Invisible Columns Metadata

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

For example, the schema encoded by `invisibleSchemaString` for an invisible `long` column named
`source_sequence_number` is:

```json
{
  "type": "struct",
  "fields": [
    {
      "name": "source_sequence_number",
      "type": "long",
      "nullable": true,
      "metadata": {}
    }
  ]
}
```

Encoding the following empty schema in `invisibleSchemaString` is equivalent to omitting the
field:

```json
{
  "type": "struct",
  "fields": []
}
```

## Schema

For a snapshot with active Invisible Columns:

- The **visible schema** is the struct in `metaData.schemaString`.
- The **invisible schema** is the struct in `metaData.invisibleSchemaString`.
- The **complete data schema** combines the visible and invisible schemas.

An invisible column is a data column. Except for the visibility behavior defined in this section,
it has the same Delta Protocol semantics and guarantees as a visible column. Writers that support
Invisible Columns, and readers that expose them, must apply every requirement that inspects,
validates, reads, or writes a data column to an invisible column in the same way as to a visible
column. For example, CHECK constraints involving invisible columns have the same support,
validation, and enforcement requirements as those involving visible columns.

When [Column Mapping](#column-mapping) is enabled, invisible columns are assigned physical names
and column IDs under the same requirements as visible columns.

## Reader Requirements

Invisible Columns impose no requirements on readers that do not understand the feature. Such
readers ignore the unrecognized `metaData.invisibleSchemaString` field under
[Protocol Evolution](#protocol-evolution) and continue to expose and read the visible schema from
`metaData.schemaString`.

A reader that chooses to expose invisible columns should:

- Use `metaData.schemaString` as the default table schema.
- Exclude invisible fields from default schema inspection and implicit column selection, including
  star expansion.

When reading invisible columns, a reader must:

- Resolve an explicitly requested invisible field against the invisible schema.
- Read invisible columns using their schema definitions and the normal Delta column-resolution
  rules.

These rules apply independently to each table version. Time travel therefore uses the visible and
invisible schemas from the requested snapshot.

## Writer Requirements

A writer that supports Invisible Columns must apply all Delta Protocol writer requirements to the
complete data schema. Values for invisible columns are validated and written under the same
protocol rules as values for visible columns.

A writer that emits a `metaData` action MUST preserve both schemas except for intentional schema
changes. Moving a column between the visible and invisible schemas must update both fields in the
same `metaData` action. Changing only a column's visibility does not require rewriting data files.
When Column Mapping is enabled, the column's physical name and ID must be preserved.

When rewriting an existing row, a writer MUST preserve its invisible-column values unless the
write explicitly changes them.

## Feature Removal

To remove `invisibleColumns` from the table's `writerFeatures`, a writer MUST ensure that every
invisible field has been moved into the visible schema and the invisible schema is empty.
These schema changes must be committed before or together with the protocol change.

After removal, writers that do not support Invisible Columns may write to the table, provided
they support the table's remaining protocol requirements.

> ***Replace the requirements in
> [Consistency Between Table Metadata and Data Files](#consistency-between-table-metadata-and-data-files)
> with the following***

- Except where explicitly permitted elsewhere in this protocol, a column in a data file MUST be
  present in `metaData.schemaString` or, when Invisible Columns are active, in the invisible
  schema.
- Values for all partition columns present in the schema MUST be present for all files in the
  table.
- Columns in the visible or invisible schema MAY be missing from data files. Readers SHOULD treat
  the value of a missing column as `null`.

> ***Add the following row to
> [Valid Feature Names in Table Features](#valid-feature-names-in-table-features)***

Feature | Name | Readers or Writers?
-|-|-
[Invisible Columns](#invisible-columns) | `invisibleColumns` | Writers only
