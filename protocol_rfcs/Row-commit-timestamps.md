# Row Commit Timestamps

- [Motivation](#motivation)
- [Compatibility & Interaction with other features](#compatibility--interaction-with-other-features)
- [Concurrency](#concurrency)
- [Alternatives considered](#alternatives-considered)
- [Open questions](#open-questions)
- [Examples](#examples)
- [Proposed protocol text](#row-commit-timestamps-1)
- [Changes to existing sections](#changes-to-existing-sections)
- [Appendix changes](#appendix-changes)

**Associated GitHub issue for discussions: https://github.com/delta-io/delta/issues/8237**

## Motivation

Row Tracking assigns a Row Commit Version to each row in the table, corresponding to the latest version in which the row was modified. Getting the timestamp requires a potentially costly lookup over all commit files as this is the only place where timestamps are currently stored or it is impossible due to an exceeded log retention.
Row Commit Timestamps provide timestamps of the last insert or update operation of a row. This proposal defines row commit timestamps for Delta. A similar extension is currently being developed for [Iceberg](https://github.com/apache/iceberg/pull/18526) as well.

## Feature summary

A row commit timestamp shows the time, in milliseconds since the Unix epoch, of the last insert or update operation for each row. It has a similar lifecycle as a row commit version: New or updated rows inherit the file's default timestamp, while rows copied over into a new file without changes retain their previous timestamp through a materialized value that takes precedence over the new file's default.  
An update that writes a new row version assigns a new Row Commit Timestamp. A plain copy of a row does not change its row commit timestamp.

## Compatibility & Interaction with other features

* It requires Writer Version 7. This feature does not impose an additional Reader Version requirement.
* Writers may enable Row Commit Timestamps on an existing table after completing the required backfill and satisfying the enablement invariants for all existing rows.
* Writers that do not support `rowCommitTimestamp` must reject writes to tables supporting the feature.
* Readers do not need to support Row Commit Timestamps to read ordinary table columns, provided they support the table's other reader requirements.
* Tables without the feature remain unaffected.
* Row Tracking must be enabled.
* In-Commit Timestamps must be enabled. Their values are used as Row Commit Timestamps.
* Checkpoints and log cleanup operations must retain the existing values.

## Concurrency

The timestamp is the final `inCommitTimestamp` of a successful commit.

## Alternatives considered

Current way: Use only row commit versions and extract timestamps from the commit files. Reading the commit history may involve scanning many commit files in the case of rows spanning a wide range of versions. Furthermore, the timestamp lookup is impossible if the relevant commit files have been removed by log cleanup.

## Open questions

None.

## Examples

### Example: preserving timestamps across updates and compaction

Assume a table with Row Tracking, In-Commit Timestamps and Row Commit Timestamps enabled. Versions before version 10 are omitted.

| Version | Operation | `inCommitTimestamp` | UTC instant |
| --- | --- | ---: | --- |
| 10 | Insert rows 1 and 2 | 1794301200000 | 2026-11-10T09:00:00Z |
| 11 | Update row 1 and copy row 2 unchanged | 1794387600000 | 2026-11-11T09:00:00Z |
| 12 | Compact both rows using OPTIMIZE | 1794474000000 | 2026-11-12T09:00:00Z |

**Version 10:** Both rows are inserted into a new file. Its `defaultRowCommitVersion` is 10 and its `defaultRowCommitTimestamp` is `1794301200000`. Neither row needs a materialized timestamp because both inherit the file default.

**Version 11:** UPDATE writes the changed row 1 and the unchanged row 2 into a new file. Row 1 inherits the new file default. Row 2's previous timestamp is materialized to preserve it.

**Version 12:** OPTIMIZE rewrites both rows as part of compaction. Both timestamps are materialized and preserved.

| Version | Row | Materialized Row Commit Timestamp | Resolved Row Commit Timestamp |
| --- | --- | --- | --- |
| 10 | 1, inserted | absent | 2026-11-10T09:00:00Z |
| 10 | 2, inserted | absent | 2026-11-10T09:00:00Z |
| 11 | 1, updated | `null` | 2026-11-11T09:00:00Z |
| 11 | 2, copied unchanged | 2026-11-10T09:00:00Z | 2026-11-10T09:00:00Z |
| 12 | 1, copied unchanged | 2026-11-11T09:00:00Z | 2026-11-11T09:00:00Z |
| 12 | 2, copied unchanged | 2026-11-10T09:00:00Z | 2026-11-10T09:00:00Z |

In version 12, the new file's default timestamp is `1794474000000`, representing 2026-11-12T09:00:00Z, but neither row inherits it. Their stable Row Commit Versions remain 11 and 10, respectively.

After checkpointing, removing the original JSON commit files must not change the resolved timestamps of these retained rows.

--------

<!-- Proposed additions to PROTOCOL.md follow. -->

> ***Insert the following Row Commit Timestamps subsection immediately after [Row Commit Versions](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#row-commit-versions), within Row Tracking and before Reader Requirements for Row Tracking.***

## Row Commit Timestamps

Row Commit Timestamps record the timestamp at which a row was last modified. More precisely, a Row Commit Timestamp is the final `inCommitTimestamp` of the successful commit that inserted or updated the row. This helps retain the precise timestamps for write operations, as reading a Row Commit Timestamp must not require the originating `commitInfo` to remain available. 

Row Commit Timestamps introduce `rowCommitTimestamp`, a writer table feature that extends Row Tracking with another row tracking metadata field `row_commit_timestamp`. It is directly equivalent to each row's stable `row_commit_version` (backfilled values might differ). Therefore, both Row Tracking and In-Commit Timestamps have to be enabled.

When a row is inserted or updated, it is assigned the commit timestamp of the log entry containing the `add` entry with the new row.
  When a row is updated so that a new stable row commit version is set, the Row Commit Timestamp for this row is updated as well.
  When a row is copied, the Row Commit Timestamp for this row is preserved.
  When a row is restored (i.e. the table is restored to an earlier version), its Row Commit Timestamp is restored as well.

### Enablement

When Row Commit Timestamps are enabled:

- The table must be on Writer Version 7.
- The feature `rowCommitTimestamp` must exist in the table `protocol`'s `writerFeatures` and must not be listed in `readerFeatures`.
- The features rowTracking, domainMetadata and inCommitTimestamp MUST  
exist in writerFeatures.
- The table properties delta.enableRowCommitTimestamps,
delta.enableRowTracking and delta.enableInCommitTimestamps MUST be
set to true.
- The table property delta.rowTrackingSuspended MUST be absent or set  
to false.
- The materialized Row Commit Timestamp column name MUST be configured through  
delta.rowTracking.materializedRowCommitTimestampColumnName.
- The `defaultRowCommitTimestamp` field must be set for all active `add` actions in the version of the table in which `delta.enableRowCommitTimestamps` is set to `true`.
- If this field is missing from any active `add` action, writers must first commit replacement `add` actions that set it. This can be done in the enablement commit or in an earlier commit.
- The materialized Row Commit Timestamp column name must satisfy the same uniqueness requirements as the materialized Row Commit Version column name.

Enabling the feature on an existing table is supported. Writers must initialize the timestamps for existing rows as follows:

1. If a row already has a stable Row Commit Version and the corresponding `inCommitTimestamp` is available, writers must use that timestamp.
2. If a row already has a stable Row Commit Version but the corresponding `inCommitTimestamp` is not available, writers must use the earliest available `inCommitTimestamp` to backfill the timestamp values. If none is available rule 3 applies.
3. If Row Tracking and Row Commit Timestamps are enabled together or no historical `inCommitTimestamp` is available, writers must use the final `inCommitTimestamp` of the commit that initializes the row's timestamp metadata.

Timestamps assigned under rules 2 and 3 represent upper bounds on the historical commit time, rather than exact timestamps of the last insert or update. Rows with different stable Row Commit Versions may receive the same timestamp.


### Schema

> In Add File and Remove File, add the following row after defaultRowCommitVersion in both the add and remove action tables.

Field Name | Data Type | Description | optional/required
-|-|-|-
defaultRowCommitTimestamp | Long | Default Row Commit Timestamp for this file, in milliseconds since the Unix epoch | optional

Row Commit Timestamps are stored in two ways:

- **Default generated Row Commit Timestamps** use the `defaultRowCommitTimestamp` field in `add` and `remove` actions.
Default generated Row Commit Timestamps require little storage overhead but are reassigned every time a row is updated or moved to a different file (for instance when a row is contained in a file that is compacted by OPTIMIZE).
- **Materialized Row Commit Timestamps** are stored in a column in the data files.
This column is hidden from readers and writers, i.e. it is not part of the `schemaString` in the table's `metaData`.
Instead, the name of this column can be found in the value for the `delta.rowTracking.materializedRowCommitTimestampColumnName` key in the `configuration` of the table's `metaData` action.
This column may contain `null` values meaning that the corresponding row has no materialized Row Commit Timestamp. This column may be omitted if all its values are `null` in the file.
Materialized Row Commit Timestamps provide a mechanism for writers to preserve Row Commit Timestamps for rows that are copied.

The `defaultRowCommitTimestamp` field stores milliseconds since the Unix epoch as a Long, matching `inCommitTimestamp`.

Materialized Row Commit Timestamps use the Delta `timestamp` type defined in [Primitive Types](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#primitive-types) and its existing [Delta-to-Parquet type mapping](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#delta-data-type-to-parquet-type-mappings).  
Readers must convert `defaultRowCommitTimestamp` from epoch milliseconds into the same logical timestamp representation as the materialized value before returning the resolved Row Commit Timestamp. Row Commit Timestamps represent UTC instants.  

> Append the following requirements to Reader Requirements for Row Tracking.

## Reader Requirements for Row Commit Timestamps

When Row Commit Timestamps are enabled and requested, readers must reconstruct them as follows:

1. Readers must use the materialized Row Commit Timestamps if the column determined by `delta.rowTracking.materializedRowCommitTimestampColumnName` is present in the data file and the column contains a non-`null` value for a row.
2. Otherwise, readers must use the default generated Row Commit Timestamps of the `add` or `remove` action containing the row in all other cases.

I.e., readers must use the `defaultRowCommitTimestamp` of the `add` or `remove` action for the file containing the row.

> Append the following requirements to Writer Requirements for Row Tracking.

## Writer Requirements for Row Commit Timestamps

The writer requirements for Row Commit Versions apply correspondingly to Row Commit Timestamps when `rowCommitTimestamp` is present in the protocol with the timestamp-specific rules below. When Row Commit Timestamps are enabled, writers must satisfy the following additional requirements:

- Writers must set defaultRowCommitTimestamp in add actions for newly written data files to the successful commit's final inCommitTimestamp. If a retry changes that timestamp, writers must update these defaults accordingly.
- Writers must preserve the previously committed defaultRowCommitTimestamp in recommitted and checkpointed add actions and corresponding remove actions.
- Writers must ensure every written row has a resolvable Row Commit Timestamp, either from a non-null materialized value or from the file's `defaultRowCommitTimestamp`.
  - Writers must preserve the Row Commit Timestamps of rows that are copied (but not updated) using materialized Row Commit Timestamps.
    - The preserved Row Commit Timestamp must equal the resolved Commit Timestamp of the same logical row before it was copied.
    - Materialized Row Commit Timestamps must be written to the column determined by `delta.rowTracking.materializedRowCommitTimestampColumnName` in the `configuration` of the table's `metaData` action.
    For newly inserted or updated rows, the materialized column must be absent or set to `NULL`, making these rows inherit the file default.
  - Writers must preserve the stable Row Commit Version together with the Row Commit Timestamp for rows copied without an update.
  - Writers performing RESTORE must restore the historical file defaults and materialized Row Commit Timestamps rather than assign the restore commit's timestamp.

## Changes to existing sections

### Reader Requirements for Row Tracking

> In Reader Requirements for Row Tracking, replace the existing sentence that excludes Row IDs and Row Commit Versions from `cdc` action files with the following sentence.

- Readers cannot read Row IDs, Row Commit Versions and Row Commit Timestamps while reading change data files from `cdc` actions.

### Checkpoint Schema

> In Checkpoint Schema, add `defaultRowCommitTimestamp: long` to the `add` and `remove` structs. Include the field in the Column Mapping example as well.

## Appendix changes

### Valid Feature Names in Table Features

> In Valid Feature Names in Table Features, add the following row after Row Tracking.

| Feature                                         | Name                  | Readers or Writers? |
| ----------------------------------------------- | --------------------- | ------------------- |
| [Row Commit Timestamps](#row-commit-timestamps) | `rowCommitTimestamp` | Writers only |
