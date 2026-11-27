# Record-Level Index

**Associated Github issue for discussions: https://github.com/delta-io/delta/issues/7435**

This RFC proposes a new **writer** table feature called **Record-Level Index** (`recordLevelIndex`). When enabled, the writer maintains a persistent index that maps a row's *index key* to that row's current *physical locator* (data file + physical row position). Row-level DML — `MERGE`, `UPDATE`, `DELETE`, and point lookups — can then locate matching target rows through an index probe instead of joining the source against every candidate file that survives data skipping. This substantially reduces the read and write amplification of continuous, CDC-style upsert workloads.

The feature is a *writer* feature: query results are identical whether or not a reader understands it, so readers are unaffected and no reader-feature break is introduced. It builds on and requires [Row Tracking](#row-tracking), [Domain Metadata](#domain-metadata), and (recommended) [Deletion Vectors](#deletion-vectors).

> During the experimental phase, this feature is exposed under the temporary name `recordLevelIndex-dev`. The `-dev` suffix will be removed once this RFC is accepted and a well-tested production implementation exists. See the [RFC process](https://github.com/delta-io/delta/tree/master/protocol_rfcs).

For further discussions about this protocol change, please refer to the Github issue - https://github.com/delta-io/delta/issues/7435 (see #7435).

--------

# Record-Level Index
> ***New Section after the [Row Tracking](#row-tracking) section***

The Record-Level Index writer feature maintains, as part of the table's committed state, an index that maps an *index key* to a *physical locator* for the row currently identified by that key. Writers use the index to resolve which data files (and which rows within them) are affected by a row-level operation without scanning or joining against the full set of data-skipping candidates.

The index is an *optimization and targeting* structure. It never changes the logical contents of the table: a reader that does not implement this feature returns exactly the same rows. A writer that implements the feature must keep the index consistent with the table state it commits, but is always free to fall back to a full scan (for example, to (re)build the index) and produce identical results.

Record-Level Index is defined to be **supported** or **enabled** on a table as follows:
- When the feature `recordLevelIndex` exists in the table `protocol`'s `writerFeatures`, then we say that Record-Level Index is **supported**. In this situation the index may or may not be present.
- When additionally the table property `delta.enableRecordLevelIndex` is set to `true`, then we say that Record-Level Index is **enabled**. In this situation, writers that support the feature must maintain the index on every commit that changes data, and the index can be relied upon to cover all active rows.

Enablement:
- The table must be on Writer Version 7.
- The features `recordLevelIndex`, [`rowTracking`](#row-tracking), and [`domainMetadata`](#domain-metadata) must all exist in the table `protocol`'s `writerFeatures`. The [`deletionVectors`](#deletion-vectors) reader/writer feature is recommended but not required.
- The table property `delta.enableRowTracking` must be set to `true` (Record-Level Index derives stable row identity from Row Tracking).
- The table property `delta.enableRecordLevelIndex` must be set to `true`.

## Index Key Modes

The index key is determined by the table property `delta.recordLevelIndex.keyMode`, which must be one of:

- `rowId` (default): the index key is the **stable Row ID** of a row, as defined by [Row Tracking](#row-ids). This mode requires no user configuration and accelerates operations that identify rows by their Delta-assigned identity (e.g. re-ingesting CDC keyed on a previously materialized Row ID, deduplication, and self-referential upserts).
- `userKey`: the index key is derived from one or more user-designated **record-key columns** listed in the table property `delta.recordLevelIndex.keyColumns` (a JSON array of top-level column [field paths](#field-path); physical field paths when [Column Mapping](#column-mapping) is enabled). The index key is the 128-bit hash defined in [Index Key Encoding](#index-key-encoding) computed over the record-key columns. This mode accelerates `MERGE`/`UPDATE`/`DELETE` whose match condition is an equality on the record key.

The key mode and, in `userKey` mode, the key columns are fixed at enablement time and must not change while the feature is enabled. Changing them requires disabling the feature (which clears the index state, see [Disabling](#disabling-record-level-index)) and re-enabling with the new configuration.

## Index Key Encoding

For `userKey` mode, the index key is a 128-bit value produced as follows:
1. Encode each record-key column value using the table's [Partition Value Serialization](#partition-value-serialization) rules to obtain a canonical UTF-8 string, using the literal string `__DELTA_NULL__` for SQL `NULL` values.
2. Concatenate the encoded values in the order given by `delta.recordLevelIndex.keyColumns`, separated by the byte `0x00`.
3. Compute the 128-bit xxHash128 (seed `0`) of the resulting byte sequence.

For `rowId` mode, the index key is the stable Row ID (a `long`) zero-extended to 128 bits.

Writers must reject enabling `userKey` mode when the record-key columns do not form a unique key under the intended semantics only if the implementation depends on uniqueness; otherwise, when multiple live rows share the same key, the index must map that key to the set of all matching locators (see [Writer Requirements](#writer-requirements-for-record-level-index)).

## Physical Locator

A *physical locator* identifies a live row by the file that stores it and the row's physical position within that file:

Field Name | Data Type | Description
-|-|-
fileId | String | Stable identifier of the data file. In `rowId` mode the file is identified indirectly through the row's `baseRowId`; in `userKey` mode `fileId` is the URI-encoded `path` of the `add` action (or its content hash when the implementation chooses content addressing). See [Locator Stability](#locator-stability).
rowPosition | Long | Physical (0-based) index of the row within the data file, identical to the index used to reconstruct default generated Row IDs from `baseRowId`.

A locator is *stale* when the row it references has been superseded (updated, deleted, moved by `OPTIMIZE`, or invalidated by a [Deletion Vector](#deletion-vectors)). Stale locators are corrected by the newest index entry for the same key and eventually removed by index maintenance; they never cause incorrect results because writers must validate a probed locator against the current snapshot before acting on it.

## Index Storage

Index state is stored in **index files** located in the `_delta_log/_record_index/` directory of the table. Index files are immutable and content-addressed; their names are `<uuid>.parquet`. Each index file stores a set of `(indexKeyHigh: long, indexKeyLow: long, fileId: string, rowPosition: long, rowCommitVersion: long)` records sorted by `(indexKeyHigh, indexKeyLow)`, where `rowCommitVersion` is the [Row Commit Version](#row-commit-versions) at which the entry was produced and is used to break ties in favor of the newest entry.

The set of index files that make up the current index, together with the index configuration, is tracked in a system-controlled [metadata domain](#domain-metadata) named `delta.recordLevelIndex`. The `configuration` string of this `domainMetadata` action is a JSON object with the following schema:

Field Name | Data Type | Description
-|-|-
layoutVersion | Int | Version of the index on-disk layout. This RFC defines layout version `1`.
keyMode | String | Either `rowId` or `userKey`; must match `delta.recordLevelIndex.keyMode`.
keyColumns | Array[String] | The record-key field paths for `userKey` mode; empty in `rowId` mode.
enablementVersion | Long | The table version at which `delta.enableRecordLevelIndex` was set to `true` and the index became complete.
indexFiles | Array[IndexFileRef] | The manifest of index files that constitute the current index (see below).

Each `IndexFileRef` is a JSON object:

Field Name | Data Type | Description
-|-|-
path | String | The index file name within `_delta_log/_record_index/`.
sizeInBytes | Long | Size of the index file in bytes.
numEntries | Long | Number of index entries in the file.
minRowCommitVersion | Long | The smallest `rowCommitVersion` among entries in the file. Used to prune superseded index files.

Because the manifest is stored in a single `domainMetadata` action, [Action Reconciliation](#action-reconciliation) applies to it directly: the latest `delta.recordLevelIndex` domain seen during log replay wins, and two concurrent commits that both touch the index conflict on this domain (see [Domain Metadata](#domain-metadata)). This gives the index the same optimistic-concurrency guarantees as Row Tracking's high-water mark.

## Writer Requirements for Record-Level Index

When Record-Level Index is supported but not enabled, writers must preserve the `delta.recordLevelIndex` domain and any existing index files but are not required to maintain them.

When Record-Level Index is **enabled** (`delta.enableRecordLevelIndex` is `true`), then for every commit that has `dataChange = true`:
1. Writers must produce an updated index manifest that reflects the post-commit snapshot: every live row must be discoverable through the index at a locator that resolves to that row in the committed version.
2. Writers must commit the updated `delta.recordLevelIndex` `domainMetadata` action **in the same commit** as the data changes it describes. Any new index files referenced by the manifest must be written to `_delta_log/_record_index/` before the commit and must not be referenced until the commit succeeds.
3. Writers must set `rowCommitVersion` of each new index entry to the [Row Commit Version](#row-commit-versions) assigned to the corresponding row in this commit. When two entries share the same key, the entry with the greater `rowCommitVersion` supersedes the other; on a tie the entry produced later in the same commit supersedes.
4. Writers may append incremental index files rather than rewriting the whole index. Writers should periodically compact index files (removing superseded and orphaned entries) and prune index files whose entries are all superseded, updating the manifest accordingly. Index compaction is a metadata-only operation and must be committed with `dataChange = false`.
5. When resolving a row-level operation, a writer that uses the index must **validate** each probed locator against the current snapshot (the referenced file is still an active `add`, and the row is not invalidated by a Deletion Vector) before treating it as a match. If validation fails, the writer must fall back to normal file resolution for that key so that results are always correct even if the index is stale.
6. Writers must not delete index files that are still referenced by any `delta.recordLevelIndex` manifest reachable by time travel within the table's [tombstone/VACUUM retention window](#add-file-and-remove-file); such deletion is the responsibility of `VACUUM` (see [VACUUM Protocol Check](#vacuum-protocol-check)).

Concurrency: because the index manifest lives in the `delta.recordLevelIndex` domain, two overlapping transactions that both modify the index conflict per the [Domain Metadata](#domain-metadata) rules and one must abort/retry. Writers that only append data but choose not to maintain the index in a given commit must not set `delta.enableRecordLevelIndex` to `true` for that commit; while the feature is enabled, all data-changing commits must maintain the index.

### Enabling Record-Level Index

A writer enables the feature by setting `delta.enableRecordLevelIndex` to `true` in the `configuration` of the table's `metaData`. This is only allowed when:
- The features `recordLevelIndex`, `rowTracking`, and `domainMetadata` are present in `writerFeatures` (added in this or an earlier version), and `delta.enableRowTracking` is `true`.
- `delta.recordLevelIndex.keyMode` is set, and in `userKey` mode `delta.recordLevelIndex.keyColumns` references existing columns.
- The index has been built to cover all active rows: the writer must, in the enabling commit or in an earlier commit, write index files and a `delta.recordLevelIndex` manifest whose `enablementVersion` equals the version at which the index became complete. Building the index requires a full scan that reads the current `baseRowId`/row positions (and record-key columns for `userKey` mode) of all active `add` files.

### Disabling Record-Level Index

A writer disables the feature by setting `delta.enableRecordLevelIndex` to `false`. On disablement the writer should mark the `delta.recordLevelIndex` domain removed (tombstone) so the manifest is dropped from the snapshot per [Action Reconciliation](#action-reconciliation); the orphaned index files are removed by `VACUUM`. The `recordLevelIndex` feature name remains in `writerFeatures` (features are never removed from `protocol`).

## Reader Requirements for Record-Level Index

None. Record-Level Index is a writer feature. Readers do not need to understand it and must continue to reconcile the unknown `delta.recordLevelIndex` system domain per the [Domain Metadata](#domain-metadata) reader requirements (support-domain-metadata readers preserve it; others ignore it). Query results do not depend on the index.

## Locator Stability

Operations that move rows between files — most notably `OPTIMIZE` and any file-rewriting maintenance — invalidate the locators of the rows they move. Because such operations already rewrite `add`/`remove` actions and (with Row Tracking) preserve stable Row IDs via `baseRowId`/materialized Row IDs, the writer performing the rewrite must, in the same commit, emit index entries that re-point the affected keys to their new locators (at the new file and row position), with `rowCommitVersion` unchanged for copied rows so that these maintenance entries do not incorrectly supersede genuine updates that happen concurrently (such a concurrent update conflicts on the `delta.recordLevelIndex` domain and forces a retry). This mirrors how Row Tracking preserves identity across `OPTIMIZE`.

## Compatibility, VACUUM, and Checkpoints

- **VACUUM.** Index files under `_delta_log/_record_index/` that are not referenced by any `delta.recordLevelIndex` manifest reachable within the retention window are removable by `VACUUM`. Because writers may place index files under the table's log directory, tables that enable Record-Level Index should also support [VACUUM Protocol Check](#vacuum-protocol-check) so that older VACUUM implementations cannot delete live index files; implementations are encouraged to require `vacuumProtocolCheck` alongside `recordLevelIndex`.
- **Checkpoints.** The index manifest is carried by the `delta.recordLevelIndex` `domainMetadata` action, which is already included in [checkpoints](#action-reconciliation) like any other domain; no new checkpoint action is introduced.
- **Existing readers/writers.** Writers that do not support `recordLevelIndex` will refuse to write the table once the feature is in `writerFeatures` (per [Table Features](#table-features)), preventing them from silently invalidating the index. Readers are unaffected.

## Table Property Summary

Property | Values | Description
-|-|-
delta.enableRecordLevelIndex | `true`/`false` | Enables index maintenance. Requires `delta.enableRowTracking = true`.
delta.recordLevelIndex.keyMode | `rowId` (default) / `userKey` | Selects the index key.
delta.recordLevelIndex.keyColumns | JSON array of field paths | Record-key columns for `userKey` mode; must be absent or empty in `rowId` mode.

## Valid Feature Names in Table Features
> ***Change to existing section***

Add the following row to the table of valid writer feature names:

Feature | Name | Readers or Writers?
-|-|-
Record-Level Index | `recordLevelIndex` | Writers only
