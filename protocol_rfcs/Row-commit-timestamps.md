# Row Commit Timestamps

- [Motivation](#motivation)
- [Feature summary](#feature-summary)
- [Compatibility](#compatibility)
- [Interactions with other features](#interactions-with-other-features)
- [Concurrency](#concurrency)
- [Alternatives considered](#alternatives-considered)
- [Open questions](#open-questions)
- [Proposed protocol text](#row-commit-timestamps-1)
- [Changes to existing sections](#changes-to-existing-sections)
- [Appendix changes](#appendix-changes)

**Associated GitHub issue for discussions: https://github.com/delta-io/delta/issues/8237**

## Motivation

Row Commit Timestamps provide timestamps of the last insert or update operation of a row. Currently, getting the timestamp of a row write can be impossible once the log retention period has expired or may require a costly lookup across commit versions. This proposal defines row commit timestamps for Delta. A similar extension is currently being developed for [Iceberg](https://github.com/apache/iceberg/pull/18526) as well.

## Feature summary

A row commit timestamp shows the time, in milliseconds since the Unix epoch, of the last insert or update operation for each row. It has a similar lifecycle as a row commit version: New or updated rows inherit the file's default timestamp, while rows copied over into a new file without changes retain their previous timestamp through a materialized value that takes precedence over the new file's default.  
An update that writes a new row version assigns a new Row Commit Timestamp, even if the user-visible values remain unchanged. A plain copy of a row does not change its row commit timestamp.

## Compatibility

It requires writer version 7 and reader version 3.  
Writers may enable Row Commit Timestamps on an existing table after completing the required backfill and satisfying the enablement invariants for all existing rows. Clients that do not support the new reader-writer table feature must reject tables using it. Tables without the feature remain unaffected.

## Interactions with other features

It requires Row Tracking to be enabled. The values from In-Commit Timestamps are used.
Furthermore, checkpoints and log cleanup operations must retain the existing values.

## Concurrency

The timestamp is the final `inCommitTimestamp` of a successful commit.

## Alternatives considered

Current way: Save row commit versions only and extract timestamps from there.

## Open questions

- Is millisecond precision sufficient?
- What value should be backfilled for rows whose Row Commit Version cannot be resolved to a timestamp (e.g., because the log retention period has expired)?
- How should the feature be disabled, and what are the consequences?

--------

<!-- Proposed additions to PROTOCOL.md follow. -->

> ***Insert the following Row Commit Timestamps subsection immediately after [Row Commit Versions](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#row-commit-versions), within Row Tracking and before Reader Requirements for Row Tracking.***

## Row Commit Timestamps

Row Commit Timestamps provide timestamps of the last write operation of a row. More precisely, a Row Commit Timestamp is the final `inCommitTimestamp` of the successful commit that inserted or updated the row. This helps retain the precise timestamps for write operations, as reading a Row Commit Timestamp must not require the originating `commitInfo` to remain available. 

This is an extension to Row Tracking in which the `row_commit_timestamp` metadata field corresponds to each row's stable `row_commit_version`. Therefore, both Row Tracking and In-Commit Timestamps have to be enabled.

When a row is inserted or updated, it is assigned the commit timestamp of the log entry containing the `add` entry with the new row.
  When a row is updated so that a new stable row commit version is set, the Row Commit Timestamp for this row is updated as well.
  When a row is copied, the Row Commit Timestamp for this row is preserved.
  When a row is restored (i.e. the table is restored to an earlier version), its Row Commit Timestamp is restored as well.

### Enablement

When Row Commit Timestamps are enabled:

- The table must be on Writer Version 7 and Reader Version 3.
- The feature `rowCommitTimestamp` must exist in the table `protocol`'s `writerFeatures` and `readerFeatures`.
- The features rowTracking, domainMetadata and inCommitTimestamp MUST  
exist in writerFeatures.
- The table properties delta.enableRowCommitTimestamps,
delta.enableRowTracking and delta.enableInCommitTimestamps MUST be
set to true.
- The table property delta.rowTrackingSuspended MUST be absent or set  
to false.
- The materialized Row Commit Timestamp column name MUST be configured through  
delta.rowTracking.materializedRowCommitTimestampColumnName.

Enabling the feature on an existing table is supported.

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

A reader must use the materialized Row Commit Timestamp. If the materialized timestamp is null or the materialized Row Commit Timestamp column is absent, the file default Row Commit Timestamp must be used. 

When Row Commit Timestamps are enabled and requested, readers must reconstruct them as follows:

1. Readers must use the materialized Row Commit Timestamps if the column determined by `delta.rowTracking.materializedRowCommitTimestampColumnName` is present in the data file and the column contains a non-`null` value for a row.
2. Otherwise, readers must use the default generated Row Commit Timestamps of the `add` or `remove` action containing the row in all other cases.

I.e., readers must use the `defaultRowCommitTimestamp` of the `add` or `remove` action for the file containing the row.

> Append the following requirements to Writer Requirements for Row Tracking.

## Writer Requirements for Row Commit Timestamps

When Row Commit Timestamps are enabled, writers must satisfy the following additional requirements:

- Writers must set defaultRowCommitTimestamp in add actions for newly written data files to the successful commit's final inCommitTimestamp. If a retry changes that timestamp, writers must update these defaults accordingly.
- Writers must preserve the previously committed defaultRowCommitTimestamp in recommitted and checkpointed add actions and corresponding remove actions.
- Writers must assign Row Commit Timestamps to all rows.
  - Writers must preserve the Row Commit Timestamps of rows that are copied (but not updated) using materialized Row Commit Timestamps.
    - The preserved Row Commit Timestamp must equal the resolved Commit Timestamp of the same logical row before it was copied.
    - Materialized Row Commit Timestamps must be written to the column determined by `delta.rowTracking.materializedRowCommitTimestampColumnName` in the `configuration` of the table's `metaData` action.
    For newly inserted or updated rows, the materialized column must be absent or set to `NULL`, making these rows inherit the file default.
  - Writers must preserve the stable Row Commit Version together with the Row Commit Timestamp for rows copied without an update.
  - Writers performing RESTORE must restore the historical file defaults and materialized Row Commit Timestamps rather than assign the restore commit's timestamp.

## Removing the feature

TBD

## Examples

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
| [Row Commit Timestamps](#row-commit-timestamps) | `rowCommitTimestamp` | Readers and Writers |
