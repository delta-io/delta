---
description: Access the list of available Delta table properties.
---

# Delta table properties reference

<!-- NOTE: Patterned after the format in https://spark.apache.org/docs/latest/configuration.html#available-properties -->

<Delta> reserves Delta table properties starting with `delta.`. These properties may have specific meanings, and affect behaviors when these
properties are set. Available Delta table properties include:

+-------------------------------------------------------------------------------------------+
| Property                                                                                  |
+===========================================================================================+
| `delta.appendOnly`                                                                        |
|                                                                                           |
| `true` for this Delta table to be append-only.                                            |
| If append-only, existing records cannot be deleted, and                                   |
| existing values cannot be updated.                                                        |
|                                                                                           |
| See [_](/delta-batch.md#table-properties).                                                |
|                                                                                           |
| Data type: `Boolean`                                                                      |
|                                                                                           |
| Default:   `false`                                                                        |
+-------------------------------------------------------------------------------------------+
| `delta.checkpoint.writeStatsAsJson`                                                       |
|                                                                                           |
| `true` for <Delta> to write file statistics in                                            |
| checkpoints in JSON format for the `stats` column.                                        |
|                                                                                           |
| Data type: `Boolean`                                                                      |
|                                                                                           |
| Default:   `true`                                                                         |
+-------------------------------------------------------------------------------------------+
| `delta.checkpoint.writeStatsAsStruct`                                                     |
|                                                                                           |
| `true` for <Delta> to write file statistics to checkpoints                                |
| in struct format for the `stats_parsed` column and to write                               |
| partition values as a struct for `partitionValues_parsed`.                                |
|                                                                                           |
| Data type: `Boolean`                                                                      |
|                                                                                           |
| Default:   (none)                                                                         |
+-------------------------------------------------------------------------------------------+
| `delta.compatibility.symlinkFormatManifest.enabled`                                       |
|                                                                                           |
| `true` for <Delta> to configure the Delta table so that all write operations on the       |
| table automatically update the manifests.                                                 |
|                                                                                           |
| See [_](/presto-integration.md#step-3-update-manifests).                                  |
|                                                                                           |
| Data type: `Boolean`                                                                      |
|                                                                                           |
| Default:   `false`                                                                        |
+-------------------------------------------------------------------------------------------+
| `delta.dataSkippingNumIndexedCols`                                                        |
|                                                                                           |
| The number of columns for <Delta> to collect statistics                                   |
| about for data skipping. A value of `-1` means to collect                                 |
| statistics for all columns. Updating this property does not                               |
| automatically collect statistics again; instead, it                                       |
| redefines the statistics schema of the Delta table. For example,                          |
| it changes the behavior of future statistics collection                                   |
| (such as during appends and optimizations) as well as                                     |
| data skipping (such as ignoring column statistics beyond                                  |
| this number, even when such statistics exist).                                            |
|                                                                                           |
| See [_](/optimizations.md#data-skipping).                                                 |
|                                                                                           |
| Data type: `Int`                                                                          |
|                                                                                           |
| Default:   `32`                                                                           |
+-------------------------------------------------------------------------------------------+
| `delta.deletedFileRetentionDuration`                                                      |
|                                                                                           |
| The shortest duration for <Delta> to keep logically deleted                               |
| data files before deleting them physically. This is to                                    |
| prevent failures in stale readers after compactions or                                    |
| partition overwrites.                                                                     |
|                                                                                           |
| This value should be large enough to ensure that:                                         |
|                                                                                           |
| - It is larger than the longest possible duration of a                                    |
|   job if you run `VACUUM` when there are concurrent readers                               |
|   or writers accessing the Delta table.                                                   |
| - If you run a streaming query that reads from the table,                                 |
|   that the query does not stop for longer than this value.                                |
|   Otherwise, the query may not be able to restart, as it                                  |
|   must still read old files.                                                              |
|                                                                                           |
| See [_](/delta-batch.md#data-retention).                                                  |
|                                                                                           |
| Data type: `CalendarInterval`                                                             |
|                                                                                           |
| Default:   `interval 1 week`                                                              |
+-------------------------------------------------------------------------------------------+
| `delta.enableChangeDataFeed`                                                              |
|                                                                                           |
| `true` to enable change data feed.                                                        |
|                                                                                           |
| See [_](/delta-change-data-feed.md#enable-change-data-feed).                              |
|                                                                                           |
| Data type: `Boolean`                                                                      |
|                                                                                           |
| Default:   `false`                                                                        |
+-------------------------------------------------------------------------------------------+
| `delta.logRetentionDuration`                                                              |
|                                                                                           |
| How long the history for a Delta table is kept.                                           |
|                                                                                           |
| Each time a checkpoint is written, <Delta> automatically cleans up log entries older      |
| than the retention interval. If you set this property to a large enough value, many       |
| log entries are retained. This should not impact performance as operations against the    |
| log are constant time. Operations on history are parallel but will become more expensive  |
| as the log size increases.                                                                |
|                                                                                           |
| See [_](/delta-batch.md#data-retention).                                                  |
|                                                                                           |
| Data type: `CalendarInterval`                                                             |
|                                                                                           |
| Default:   `interval 30 days`                                                             |
+-------------------------------------------------------------------------------------------+
| `delta.minReaderVersion`                                                                  |
|                                                                                           |
| The minimum required protocol reader version for a reader that allows to read from this   |
| Delta table.                                                                              |
|                                                                                           |
| See [_](/versioning.md).                                                                  |
|                                                                                           |
| Data type: `Int`                                                                          |
|                                                                                           |
| Default:   `1`                                                                            |
+-------------------------------------------------------------------------------------------+
| `delta.minWriterVersion`                                                                  |
|                                                                                           |
| The minimum required protocol writer version for a writer that allows to write to this    |
| Delta table.                                                                              |
|                                                                                           |
| See [_](/versioning.md).                                                                  |
|                                                                                           |
| Data type: `Int`                                                                          |
|                                                                                           |
| Default:   `2`                                                                            |
+-------------------------------------------------------------------------------------------+
| `delta.setTransactionRetentionDuration`                                                   |
|                                                                                           |
| The shortest duration within which new snapshots will retain transaction identifiers      |
| (for example, `SetTransaction`s). When a new snapshot sees a transaction identifier older |
| than or equal to the duration specified by this property, the snapshot considers it       |
| expired and ignores it. The `SetTransaction` identifier is used when making the writes    |
| idempotent. See [_](delta-streaming.md#idempotent-table-writes-in-foreachbatch)           |
| for details.                                                                              |
|                                                                                           |
| Data type: `CalendarInterval`                                                             |
|                                                                                           |
| Default:   (none)                                                                         |
+-------------------------------------------------------------------------------------------+
| `delta.checkpointPolicy`                                                                  |
|                                                                                           |
| `classic` for classic <Delta> checkpoints. `v2` for v2 checkpoints.                       |
| See                                                                                       |
| [V2 Checkpoint Spec](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#v2-spec)   |   
| for details.                                                                              |
|                                                                                           |
| See [_](/versioning.md) for details around compatibility.                                 |
|                                                                                           |
| Data type: `String`                                                                       |
|                                                                                           |
| Default: `classic`                                                                        |
+-------------------------------------------------------------------------------------------+

.. <Delta> replace:: Delta Lake
.. <AS> replace:: Apache Spark