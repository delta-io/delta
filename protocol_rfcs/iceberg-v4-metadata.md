# Iceberg V4 Adaptive Metadata Tree
**Associated Github issue for discussions: https://github.com/delta-io/delta/issues/6640**

This RFC introduces a new reader-writer table feature `adaptiveMetadata` that enables Delta tables to adopt the [Apache Iceberg™ V4 adaptive metadata tree](https://s.apache.org/iceberg-single-file-commit) as their native content metadata format.

## Terminology

This RFC adopts the *adaptive metadata tree* from the Iceberg V4 spec. The following terms are used throughout:

- **content metadata**: metadata about data files and their statistics. e.g. the `add` and `remove` actions.
- **non-content metadata**: all other table state. e.g. the `protocol`, `metaData`, `domainMetadata`, and `txn` (SetTransaction) actions.
- **`adaptiveMetadata`**: the Delta table feature defined by this RFC.
- **metadata tree**: the data structure the feature adopts: a root manifest referencing leaf manifests, as defined by Iceberg's [single-file commit design](https://s.apache.org/iceberg-single-file-commit).
- **manifest**: a Parquet file of content entries (a root manifest or a leaf manifest).

## Motivation

Current Delta checkpoints rewrite the entire table state on every checkpoint, regardless of how much actually changed. This makes metadata costs proportional to table size rather than operation size, and prevents caching since checkpoint files change completely between versions.

Delta also stores all of a commit's changes directly in the commit JSON file, regardless of how large the commit is. Compared to a columnar manifest, JSON compresses roughly an order of magnitude worse than Parquet, cannot be read or written in parallel, and causes high memory pressure at both read and write time.

The `adaptiveMetadata` feature addresses these limitations by adopting a two-level tree structure where:
- A **root manifest** serves as the entry point, containing references to
  leaf manifests as well as inline data file entries
- **Leaf manifests** contain data file entries and remain stable across operations
- **Manifest Deletion Vectors (MDVs)** mark rows in leaf manifests as deleted without rewriting
  them

This design enables:
- Small tables to store all file entries inline in the root manifest (no leaf manifests needed)
- Small appends to write only to the Delta log, or at most a new root manifest
- Commits that delete files can update MDVs instead of rewriting manifests
- Metadata changes proportional to operation size
- Stable leaf manifests that can be cached effectively

--------

# Changes to existing sections

### Add File

> ***Change to [existing section](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#add-file-and-remove-file)***

<ins>When the `adaptiveMetadata` table feature is enabled, the `add` action supports a `backReference` field:</ins>

| Field Name | Data Type | Description |
| - | - | - |
| <ins>backReference</ins> | <ins>Struct</ins> | <ins>Reference to the existing entry in the metadata tree that this add supersedes (e.g., stats backfill, DV update). Null when the file has no entry in the tree. Contains `manifest` (String) and `pos` (Long). See [Backreferences](#backreferences).</ins> |

### Remove File

> ***Change to [existing section](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#remove-file)***

<ins>When the `adaptiveMetadata` table feature is enabled, the `remove` action requires a `backReference` field, sets `extendedFileMetadata` to true, and has a null `deletionTimestamp`:</ins>

| Field Name | Data Type | Description |
| - | - | - |
| <ins>deletionTimestamp</ins> | <ins>Long</ins> | <ins>Must be null. Metadata cleanup uses tree reachability instead of timestamp-based expiration.</ins> |
| <ins>extendedFileMetadata</ins> | <ins>Boolean</ins> | <ins>Must be true. `partitionValues`, `size`, and `tags` are always present on the `remove`.</ins> |
| <ins>backReference</ins> | <ins>Struct</ins> | <ins>Required reference to the file's location in the metadata tree. Contains `manifest` (String) and `pos` (Long). See [Backreferences](#backreferences).</ins> |
| <ins>stats</ins> | <ins>String</ins> | <ins>Must be present. Statistics of the removed file, with `numRecords` required at minimum; column statistics are included when recorded for the file. Copied from the matching `add.stats`, or converted from the file's tree entry (`record_count`, `content_stats`).</ins> |

<ins>`remove` actions are transient. During log replay a `remove` cancels the matching `add` (or, via its `backReference`, marks the corresponding tree entry deleted) and is then discarded. Removes are **not** retained as tombstones in checkpoints or in the reconstructed table state. There is no timestamp-based tombstone expiration; physical file cleanup is driven by tree reachability (see [Metadata Cleanup](#metadata-cleanup)).</ins>

<ins>Because removes carry the removed file's stats, filtered log replay can apply the same data skipping to removes as to adds: a `remove` whose stats do not match the scan filter can only cancel entries that the filter also eliminates, so both can be dropped without tracking. This keeps reconciliation state bounded when replaying a high-churn log tail.</ins>


### Last Checkpoint File

> ***Change to [existing section](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#last-checkpoint-file)***

<ins>The `_last_checkpoint` file remains a non-authoritative hint, as defined in the existing protocol. Readers discover whether a table supports `adaptiveMetadata` from the `protocol` action in the log or checkpoint, not from `_last_checkpoint`.</ins>

<ins>When the `adaptiveMetadata` table feature is enabled and a manifest commit has been written, the `_last_checkpoint` file may embed the [`checkpoint` action](#checkpoint-action) for the latest checkpoint version:</ins>

| Field Name | Data Type | Description |
| - | - | - |
| <ins>checkpoint</ins> | <ins>Struct</ins> | <ins>The `checkpoint` action for the latest manifest commit. Schema defined in [Checkpoint Action](#checkpoint-action).</ins> |

<ins>When the embedded `checkpoint` action is present, readers can use `contentRoot.path` to begin prefetching the root manifest immediately, and the action provides the complete table state at `checkpointMetadata.version`. If the hint is absent, stale, or does not contain a `checkpoint` action, readers fall back to log replay to locate the latest `checkpoint` action.</ins>

### Checkpoints

> ***Change to [existing section](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#checkpoints)***

<ins>When the `adaptiveMetadata` table feature is enabled, checkpoint information may be embedded directly in Delta log entries via the `checkpoint` action, rather than stored in separate checkpoint files. See [Checkpoint Action](#checkpoint-action) for details.</ins>

<ins>The `checkpoint` action is self-contained: it embeds the content root reference along with all non-content metadata (`protocol`, `metaData`, `domainMetadata`, `txn`, and `sidecar` entries). Together with the referenced manifest tree, a single `checkpoint` action represents the complete table state up to `checkpointMetadata.version`. Log commits after that version must still be replayed.</ins>

<ins>Once `adaptiveMetadata` is enabled, writers must not produce classic (single-file, multi-part) or V2 checkpoints; checkpoint state is instead carried by the `checkpoint` action, including in [standalone checkpoints](#standalone-checkpoint).</ins>

<ins>Table-level aggregates (file count, total size, row count) are derived from the root manifest's aggregated metrics, not from a checkpoint that enumerates every file. Because checkpoints retain no remove tombstones, the count of removes is not part of checkpoint state, and multi-part checkpoint sizing based on that count does not apply.</ins>

### Action Reconciliation

> ***Change to [existing section](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#action-reconciliation)***

<ins>When the `adaptiveMetadata` table feature is enabled, `remove` actions are not carried through reconciliation. A `remove` is applied during replay to cancel its matching `add` (or to mark the referenced tree entry deleted) and is then dropped. The reconstructed table state and any checkpoint produced from it contain only live entries; they do not retain removes. Scans never consumed tombstones, and tree-reachability cleanup replaces the VACUUM use of tombstones, so removes have no remaining role in reconciled state.</ins>

### Deletion Vectors

> ***Change to [existing section](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#deletion-vectors)***

<ins>When the `adaptiveMetadata` table feature is enabled, inline deletion vectors (storage type `i`) are forbidden. The content entry represents deletion vectors as file references (`location`, `offset`, `size_in_bytes`, `cardinality`) with no field for inline bytes. Storage types `u` (UUID-relative) and `p` (absolute path) are supported since both resolve to file paths. When writing manifest entries, writers must resolve the `u` encoding to a relative path (e.g., `data/deletion_vector_<uuid>.bin`) for the DV `location` field. Existing inline DVs must be converted to file-based DVs before or during feature enablement.</ins>

### Metadata Cleanup

> ***Change to [existing section](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#metadata-cleanup)***

<ins>When the `adaptiveMetadata` table feature is enabled, cleanup (both metadata cleanup and VACUUM) is **reachability-based**: a file is retained if it is reachable from live table state, and only unreachable files past their retention period may be deleted. This applies uniformly to data files, deletion vectors, manifests, and sidecars. Cleanup must not use directory layout or filename patterns to decide what is deletable — these files are referenced by explicit paths and may reside in any location.</ins>

<ins>The reachable set is computed by walking references from retained versions:</ins>

<ins>1. **Manifests**: Starting from the content root referenced by each retained `checkpoint` action, follow tree references to each retained root manifest and every leaf manifest it references. A leaf manifest may be referenced by multiple root manifests across versions; it remains reachable as long as any retained root references it.</ins>

<ins>2. **Data files and deletion vectors**: Any data file or deletion vector referenced by a reachable manifest entry, or by an `add` action in a retained log commit, is reachable.</ins>

<ins>3. **Sidecars**: Any sidecar referenced by a retained `checkpoint` action is reachable.</ins>

<ins>Files not in the reachable set may be deleted once past the retention period. Reachability is derived from the live tree, not from `remove` tombstones, so no tombstone tracking is required (see [Remove File](#remove-file)).</ins>

--------

> ***Add a new section at the [Table Features](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#table-features) section***

# Adaptive Metadata

The `adaptiveMetadata` table feature enables Delta tables to store table state using an adaptive metadata tree structure. When enabled, commits can choose between writing changes to the Delta log (*log commits*) or producing a metadata tree with an embedded checkpoint action (*manifest commits*).

## Table Feature Enablement

The `adaptiveMetadata` table feature is supported when:
- The table is on Reader Version 3 and Writer Version 7.
- The feature `adaptiveMetadata` exists in the table `protocol`'s `readerFeatures` and `writerFeatures`.

Required table features that must also be enabled:
- `columnMapping` (`id` mode): Stable column identification across schema evolution
- `rowTracking`: Manifest entries natively carry row-tracking fields (`first_row_id`, `sequence_number`); see [Row Tracking Compatibility](#row-tracking-compatibility)
- `domainMetadata`: Storing feature-specific metadata
- `deletionVectors`: Deletes are represented as deletion vectors, not by rewriting files
- `inCommitTimestamp`: Reliable timestamp-based metadata cleanup and time travel

## Storage Layout

When `adaptiveMetadata` is enabled, the table storage layout includes a `metadata/` directory alongside the standard `_delta_log/`:

```
table/
├── _delta_log/
│   ├── 00000000000000000042.json     # Commit with embedded checkpoint action
│   ├── 00000000000000000043.json     # Log commit (small changes only)
│   ├── _sidecars/
│   │   └── txn-v42.parquet           # Transaction identifiers sidecar
│   └── _last_checkpoint
├── metadata/                         # metadata tree
│   ├── a3d1f7e2-v42.parquet          # Root manifest (UUID + version)
│   ├── 7c2e8f1a-m0.parquet           # Leaf manifest
│   └── e9f4a2b1-m0.parquet           # Leaf manifest
└── ... data files ...
```

Filenames and directory layout shown above are illustrative. The protocol does not prescribe manifest naming conventions or file locations: manifests, data files, and deletion vectors are referenced by explicit paths (relative to the table root, or absolute URIs) and may reside anywhere the writer places them, including outside the directories shown. Readers locate files via those path references, not by directory or filename pattern, and cleanup determines what to delete by reachability (see [Metadata Cleanup](#metadata-cleanup)), not by layout.

The `metadata/` directory is the default and recommended location for manifests. Manifest paths in the protocol are stored as **relative paths from the table root** (e.g., `metadata/root-v42.parquet`). A path is relative if it does not contain a URI scheme. Relative paths are resolved by joining the table location and the relative path with a `/` separator (`table_location + "/" + relative_path`). Absolute paths (with a URI scheme) are used as-is. Relative paths must not start with `/` and table locations must not end with `/` to avoid duplicate separators. This follows the [Iceberg V4 relative paths specification](https://iceberg.apache.org/spec/#paths-in-metadata).

The `_delta_log/_sidecars/` directory stores auxiliary data that is too large to embed in the checkpoint action.

## Checkpoint Action

When a manifest commit occurs, the Delta log entry contains a self-contained `checkpoint` action whose value is an array of the actions needed to reconstruct the full table state — the content root reference plus all non-content metadata:

```json
{
  "checkpoint": [
    { "checkpointMetadata": { "version": 42 } },
    { "contentRoot": { "path": "metadata/a3d1f7e2-v42.parquet", "sizeInBytes": 1024, "version": 42 } },
    { "protocol": { "minReaderVersion": 3, "minWriterVersion": 7, "readerFeatures": ["columnMapping", "deletionVectors", "adaptiveMetadata"], "writerFeatures": ["columnMapping", "deletionVectors", "domainMetadata", "rowTracking", "adaptiveMetadata"] } },
    { "metaData": { "id": "af23c9d7-fff1-4a5a-a2c8-55c59bd782aa", "name": "my_table", "schemaString": "{...}", "partitionColumns": [], "configuration": {}, "createdTime": 1234567890000 } },
    { "domainMetadata": { "domain": "delta.rowTracking", "configuration": "{\"rowIdHighWaterMark\": 1000000}", "removed": false } },
    { "txn": { "appId": "streaming-app-1", "version": 100, "lastUpdated": 1234567890000 } },
    { "sidecar": { "type": "txn", "path": "txn-v42.parquet", "sizeInBytes": 2048, "modificationTime": 1234567890000, "tags": {} } }
  ]
}
```

The array wrapper is used only when the checkpoint is embedded inline in a commit's Delta log entry. In a [standalone checkpoint](#standalone-checkpoint), these same actions are stored as separate rows (one action per row) with no enclosing `checkpoint` array — the checkpoint file itself is the grouping.

### Checkpoint Action Contents

The `checkpoint` action is an array of action entries. Each entry is one of:

| Action | Description |
|--------|-------------|
| `checkpointMetadata` | Contains `version`: the table version up to which the checkpoint is complete. May be less than or equal to the commit version (e.g., commit v100 may checkpoint v50). Checkpoint versions must be strictly monotonically increasing across all checkpoint actions in the log. |
| `contentRoot` | Reference to the root manifest: `path` (relative to the table root, or an absolute URI), `sizeInBytes`, and `version` — the table version the root reflects. `version` must be `<= checkpointMetadata.version`; the two are equal in a manifest commit, and less in a standalone checkpoint (the gap is covered by inline file actions). |
| `protocol` | The Protocol action at this checkpoint version. |
| `metaData` | The Metadata action at this checkpoint version. |
| `domainMetadata` | A DomainMetadata action. System domains (keys prefixed with `delta.`) must appear here; user domains may appear here or in a sidecar. |
| `txn` | A SetTransaction action (`appId`, `version`, `lastUpdated`). May appear here or in a sidecar. |
| `sidecar` | A sidecar reference for spilled user domain metadata or txns: `type` (`txn` or `domainMetadata`), `path`, `sizeInBytes`, `modificationTime`, `tags`. File-level metadata is stored in the content tree, not in sidecars. |

System domain metadata (domains prefixed with `delta.`) must always appear as `domainMetadata` entries in the checkpoint action; it is small and required for correctness. User domain metadata and `txn`s may appear as entries, in sidecars, or split across both.

For both user domain metadata and `txns`, inline and sidecar storage may coexist: a small inline set and one or more sidecars can be present together. The only constraint is per-key uniqueness — a given `appId` (or `domain`) must appear in exactly one place, whether inline or in a single sidecar, never in more than one. Each sidecar carries a `type` field (`txn` or `domainMetadata`) identifying which kind it holds, and a kind may be split across multiple sidecars of the same `type`. The complete set of transaction identifiers (or user domain metadata) at `checkpointMetadata.version` is the union of the inline entries and all sidecars of the corresponding type.

## Backreferences

When `adaptiveMetadata` is enabled, `remove` and `add` actions carry a `backReference` field that identifies where the file's existing entry is located in the metadata tree. A backreference is non-null when the file has a live entry in a manifest, and null when the file has no manifest entry (it exists only in the Delta log).

A backreference is meaningful only relative to the tree it was computed from, identified by that tree's `contentRoot.version`. A commit's backreferences are valid only if they target the current `contentRoot.version`; if a concurrent manifest commit has advanced the tree (e.g., compaction moved entries between manifests), they are stale and must be recomputed against the new tree before the commit can proceed (see [Conflict Resolution](#conflict-resolution)).

Backreferences enable efficient [Manifest Deletion Vector (MDV)](#manifest-deletion-vectors-mdvs) creation during manifest commits: writers can directly construct MDVs from backreferences without scanning leaf manifests. The engine must propagate (manifest, position) metadata through the planning pipeline so it is available at commit time.

### Remove with Backreference

```json
{
  "remove": {
    "path": "data/part-00001.parquet",
    "dataChange": true,
    "deletionVector": {
      "storageType": "u",
      "pathOrInlineDv": "ab^-aqEH",
      "offset": 4,
      "sizeInBytes": 40,
      "cardinality": 6
    },
    "backReference": {
      "manifest": "metadata/leaf-m1.parquet",
      "pos": 17
    }
  }
}
```

### Add with Backreference (Re-add)

When an `add` supersedes an existing manifest entry (e.g., `OPTIMIZE` backfilling stats on a file), the backreference points to the old entry:

```json
{
  "add": {
    "path": "data/part-00001.parquet",
    "size": 1234567,
    "stats": "{\"numRecords\":1000,\"minValues\":{\"id\":1},\"maxValues\":{\"id\":1000}}",
    "backReference": {
      "manifest": "metadata/leaf-m1.parquet",
      "pos": 17
    }
  }
}
```

### Backreference Fields

| Field | Type | Description |
|-------|------|-------------|
| `manifest` | String | Path to the leaf manifest containing this file, relative to the table root (e.g., `metadata/leaf-m1.parquet`) |
| `pos` | Long | Row position (0-indexed) of the file entry within the manifest |

## Content Entry Schema

Both root and leaf manifests use a single unified entry schema, following the [Iceberg V4 content entry design](https://github.com/apache/iceberg/pull/15049). Certain fields are only applicable to specific `content_type` values (noted below). Manifests are Parquet files. Writers must set Parquet `field_id` metadata on all fields so that readers can resolve them by field ID, and **readers must resolve fields by field ID, not by name.**

Leaf manifests contain entries for data files only (`content_type` = DATA). They cannot reference other manifests: this enforces a two-level tree hierarchy (root -> leaves only).

The root manifest contains entries of the following types:

| `content_type` Value | Description |
|----------------------|-------------|
| `DATA` (0) | Inline data file entry (for small appends), may include `deletion_vector` |
| `DATA_MANIFEST` (3) | Reference to a leaf manifest containing data file entries |

**Note:** Deletion vectors are combined with data file entries via the `deletion_vector` field, not stored as separate `POSITION_DELETES` entries. Equality deletes (`EQUALITY_DELETES`, content_type 2) are not supported.

### Entry Fields

| Field ID | Field Name | Delta Type | Required | Applicable To | Description |
|----------|------------|------------|----------|---------------|-------------|
| 147 | `tracking` | Struct ([Tracking](#tracking)) | Required | All | Tracking information for this entry |
| 134 | `content_type` | Int | Required | All | 0=DATA, 3=DATA_MANIFEST |
| 100 | `location` | String | Required | All | Path relative to table root (e.g., `metadata/leaf-m1.parquet` or `data/part-00001.parquet`). May be absolute URI. |
| 101 | `file_format` | String | Required | All | File format name. Delta only supports `parquet`. |
| 102 | `partition` | Struct | Required | DATA | Partition data tuple. Struct fields are keyed by partition `field-id` (`1000 + i` for the i-th partition column). |
| 103 | `record_count` | Long | Required | All | Number of records in the file |
| 104 | `file_size_in_bytes` | Long | Required | All | Total file size in bytes |
| 141 | `spec_id` | Int | Optional | All | Partition spec ID used for this entry |
| 146 | `content_stats` | Struct ([Content Stats](#content-stats)) | Optional | All | Per-column statistics |
| 140 | `sort_order_id` | Int | Optional | DATA | Sort order ID for this file |
| 148 | `deletion_vector` | Struct ([Deletion Vector](#deletion-vector)) | Optional | DATA | Deletion vector for the data file |
| 150 | `manifest_info` | Struct ([Manifest Info](#manifest-info)) | Optional | DATA_MANIFEST | Manifest-level summary and MDV |
| 132 | `split_offsets` | Array\<Long\> (element ID 133) | Optional | DATA | Row group split offsets |

### Tracking

| Field ID | Field Name | Delta Type | Required | Description |
|----------|------------|------------|----------|-------------|
| 0 | `status` | Int | Required | 0=EXISTING, 1=ADDED, 2=DELETED, 3=REPLACED |
| 1 | `snapshot_id` | Long | Optional | Snapshot ID where entry was added or deleted. Inherited when null. |
| 3 | `sequence_number` | Long | Optional | Data sequence number. Inherited when null and status=ADDED. |
| 4 | `file_sequence_number` | Long | Optional | File sequence number (when file was physically added). Inherited when null and status=ADDED. |
| 5 | `dv_snapshot_id` | Long | Optional | Snapshot ID where DV was added. Null when no DV. |
| 142 | `first_row_id` | Long | Optional | Starting row ID for this file (DATA) or manifest (DATA_MANIFEST) |
| 6 | `deleted_positions` | Binary | Optional | Bitmap of positions deleted in this commit (DATA_MANIFEST only, for CDF) |
| 7 | `replaced_positions` | Binary | Optional | Bitmap of positions replaced in this commit (DATA_MANIFEST only, for CDF) |

**Status values:** `status` is required and always materialized (never null). Each entry has exactly one status.

Only `EXISTING` and `ADDED` entries are live (visible to scans). `DELETED` and `REPLACED` entries are not live and exist only for change data feed (CDF) reconstruction. A file path has at most one live entry in the tree at any time.

- `EXISTING` (0): Unchanged from previous commit. Live.
- `ADDED` (1): New in this commit. Live.
- `DELETED` (2): Removed in this commit. Not live.
- `REPLACED` (3): Superseded (e.g., DV added/changed). Not live. A
  new DATA entry with updated metadata exists elsewhere in the tree as the live entry for this file path.

Non-live entries are transient: they are dropped in the next manifest commit that rewrites the affected manifest (root or leaf).

**Inheritance:** Inheritance applies only to leaf manifest entries inheriting from the root. Following the [Iceberg specification](https://iceberg.apache.org/spec/#sequence-number-inheritance):

1. A writer creates a leaf manifest with `ADDED` entries. Tracking
   fields like `sequence_number` are null because the commit version is not yet assigned.
2. The writer creates the root manifest with a `DATA_MANIFEST` entry
   referencing the leaf. The `DATA_MANIFEST` entry has explicit (non-null) tracking values assigned at commit time.
3. A reader encounters a null tracking field in a leaf entry and
   inherits the value from the `DATA_MANIFEST` entry in the root that references that leaf.

The fields that support inheritance:

- **`snapshot_id`**: Inherited when null.
- **`sequence_number`**: Inherited when null and status is `ADDED`.
- **`file_sequence_number`**: Inherited when null and status is `ADDED`.
- **`first_row_id`**: Inherited when null.
- **`dv_snapshot_id`**: Not inherited. Null means no DV is present.

Root manifest entries must always have explicit (non-null) tracking values since there is nothing above them to inherit from.

Leaf manifests are immutable. Entries stay `ADDED` with null tracking fields forever. The `DATA_MANIFEST` entry in the root tracks the manifest's lifecycle (`ADDED` -> `EXISTING` across commits), but individual entries in the leaf do not change. This allows writers to create a leaf manifest once and reuse it across commit retries. Only the root manifest needs to be updated on retry.

After manifest compaction, inherited values must be materialized in the new leaf manifest with `status=EXISTING`, since compacted entries originate from different commits and cannot share a single inherited value from the root.

### Deletion Vector

Tracks where a DV blob can be read. Only for `content_type` = DATA entries.

| Field ID | Field Name | Delta Type | Required | Description |
|----------|------------|------------|----------|-------------|
| 155 | `location` | String | Required | Path to file containing the DV |
| 144 | `offset` | Long | Required | Offset in the file where DV content starts |
| 145 | `size_in_bytes` | Long | Required | Length of DV content in the file |
| 156 | `cardinality` | Long | Required | Number of set bits (deleted rows) in the vector |

### Manifest Info

Summary information for `content_type` = DATA_MANIFEST entries. Includes file/row counts and the Manifest Deletion Vector (MDV).

| Field ID | Field Name | Delta Type | Required | Description |
|----------|------------|------------|----------|-------------|
| 504 | `added_files_count` | Int | Required | Number of files added |
| 505 | `existing_files_count` | Int | Required | Number of existing files |
| 506 | `deleted_files_count` | Int | Required | Number of deleted files |
| 520 | `replaced_files_count` | Int | Required | Number of replaced files |
| 512 | `added_rows_count` | Long | Required | Number of rows in added files |
| 513 | `existing_rows_count` | Long | Required | Number of rows in existing files |
| 514 | `deleted_rows_count` | Long | Required | Number of rows in deleted files |
| 521 | `replaced_rows_count` | Long | Required | Number of rows in replaced files |
| 516 | `min_sequence_number` | Long | Required | Minimum sequence number of files in this manifest |
| 522 | `dv` | Binary | Optional | MDV bitmap marking deleted positions in leaf manifest. Must be non-null if and only if `dv_cardinality` is non-null. |
| 523 | `dv_cardinality` | Long | Optional | Number of entries marked as deleted in the MDV. Must be non-null if and only if `dv` is non-null. |

### Manifest Deletion Vectors (MDVs)

MDVs track which entries in a leaf manifest have been deleted or replaced. The `dv` field in `manifest_info` contains a Roaring bitmap of all invalidated positions (cumulative, used for reading).  The `deleted_positions` and `replaced_positions` fields in `tracking` represent what changed in the current commit only (used for CDF):

- **`manifest_info.dv`**: Cumulative set of all invalidated positions. Grows
  monotonically. Used by readers to skip entries.
- **`tracking.deleted_positions`**: Positions newly deleted in this
  commit (for CDF).
- **`tracking.replaced_positions`**: Positions newly replaced in this
  commit (for CDF).

A position cannot be set in both `deleted_positions` and `replaced_positions` simultaneously.

When a `DATA_MANIFEST` entry has a non-null MDV, its `content_stats` are stale: aggregate bounds and counts still include values from invalidated entries. Writers must set `tight_bounds = false` on the manifest-level stats when an MDV is present. After compaction clears the MDV, stats are recomputed from live entries only and `tight_bounds` can be true again.

### Content Stats

The `content_stats` field (ID 146) uses Iceberg V4's typed column statistics structure, as defined in the [Iceberg V4 specification](https://iceberg.apache.org/spec/#content-stats).

The `content_stats` struct is optional on the entry, and each per-column stats struct within it is also optional. Writers are not required to produce stats for every column.

Only leaf (primitive) columns get stats structs. Complex types (struct, list, map) do not. The stats structs are flat fields in `content_stats`, not nested under parent types. Field IDs are deterministically calculated from table field IDs:

| Table Field ID | Stats Struct Field ID | Calculation |
|----------------|----------------------|-------------|
| 0 | 10000 | 10_000 + 200 × 0 |
| 1 | 10200 | 10_000 + 200 × 1 |
| 2 | 10400 | 10_000 + 200 × 2 |
| 100 | 30000 | 10_000 + 200 × 100 |

Each per-column stats struct contains:

| Offset | Field Name | Delta Type | Included For | Description |
|--------|------------|------------|--------------|-------------|
| +1 | `lower_bound` | (typed) | all primitives | Lower bound, type matches the table column type |
| +2 | `upper_bound` | (typed) | all primitives | Upper bound, type matches the table column type |
| +3 | `tight_bounds` | Boolean | all primitives | When true, bounds are exact min/max |
| +4 | `value_count` | Long | all | Number of values (including nulls and NaN) |
| +5 | `null_value_count` | Long | optional fields | Number of null values |
| +6 | `nan_value_count` | Long | float, double | Number of NaN values |
| +7 | `avg_value_size_in_bytes` | Int | string, binary | Avg uncompressed value size in bytes |

Implementations are not required to write a stats struct for every table field. If any field is missing from the struct, readers must assume it is unknown.

**Aggregation rules** (for manifest-level stats in root manifest `DATA_MANIFEST` entries, computed over constituent leaf entries):

If any child entry is missing a stats field, the aggregate for that field must be null. Aggregation is only valid when all children have the field present.

- `value_count`, `null_value_count`, `nan_value_count`: sum
- `lower_bound`: min of non-null child values
- `upper_bound`: max of non-null child values
- `tight_bounds`: AND (false if any constituent is false)
- `avg_value_size_in_bytes`: must be null at the manifest level

### Stats Reconciliation

Delta `add` actions carry statistics as a JSON `stats` string (`numRecords`, `nullCount`, `minValues`, `maxValues`, `tightBounds`), keyed by physical column name. Manifest entries carry the same information as `content_stats`, keyed by column-mapping field ID and stored in each field's type. A live file's stats exist as `add.stats` while it is only in the Delta log, and as `content_stats` once it is folded into the tree — never both. Readers must treat the two representations as equivalent for data skipping. `remove` actions carry a copy of the removed file's stats so that log replay can prune them (see [Remove File](#remove-file)).

When folding a log `add` into a manifest, writers convert `add.stats` to `content_stats` as follows. Field IDs are resolved via `columnMapping` (a required dependent feature), and `minValues`/`maxValues` are converted from their JSON representation to the field's typed value.

| Delta `add.stats` | V4 `content_stats` |
|-------------------|--------------------|
| `numRecords` | entry `record_count` (field 103) |
| `nullCount[col]` | `null_value_count` |
| `minValues[col]` | `lower_bound` |
| `maxValues[col]` | `upper_bound` |
| `tightBounds` | `tight_bounds` |

`value_count` and `nan_value_count` have no Delta source and are left unpopulated (readers treat them as unknown). `tightBounds` carries Delta's wide-bounds-under-deletion-vectors semantics to `tight_bounds`, consistent with manifest-level stats being `tight_bounds = false` when an MDV is present.

## Snapshot ID Generation

The `snapshot_id` field in tracking identifies when content was added or modified. Writers must generate a unique long value for each manifest commit.

## Row Tracking Compatibility

Delta's row tracking fields map to Iceberg V4 tracking as follows:

| Delta Field | Iceberg Tracking Field | Field ID |
|-------------|-------------------|----------|
| `baseRowId` | `first_row_id` | 142 |
| `defaultRowCommitVersion` | `sequence_number`, `file_sequence_number` | 3, 4 |
| `rowIdHighWaterMark` | `next-row-id` | - |

The `rowIdHighWaterMark` in the `delta.rowTracking` domain metadata remains the authoritative row ID allocator. It is the highest assigned row ID, while Iceberg's `next-row-id` table metadata field is the next unassigned one, so `next-row-id` resolves to `rowIdHighWaterMark + 1`.

When `adaptiveMetadata` is enabled, Iceberg's `sequence_number` (data sequence number) and `file_sequence_number` are both set to the Delta commit version of the `add` action that introduced the file, and always resolve to the same value. In Iceberg these can diverge: the data sequence number records the relative age of a file's content and is used to decide which delete files apply to a data file, so a rewritten file (e.g., compaction) can keep an older data sequence number than the commit that physically wrote it. Delta has no such notion because it binds deletion vectors directly to their data file rather than resolving delete application by sequence number, so there is never a reason for the two to differ. `file_sequence_number` is required by Iceberg's inheritance model but Delta does not read it back.

For ADDED entries in leaf manifests, both are null and inherited from the `DATA_MANIFEST` entry in the root (see [Inheritance](#inheritance)). For EXISTING entries (e.g., after compaction), both are materialized.

On compaction, the output file is a new physical file at the compaction commit version, so all three file-level fields (`defaultRowCommitVersion`, `sequence_number`, `file_sequence_number`) are the compaction version. The original per-row values are preserved by materializing them into the row tracking columns of the output Parquet file (see [Materialized Row Tracking Columns](#materialized-row-tracking-columns)).

### Materialized Row Tracking Columns

Row tracking values that must survive file rewrites are stored in the materialized row ID and row commit version columns of the data files (see [Row Tracking](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#row-tracking)). The physical names of these columns are table-specific, so writers must set Parquet `field_id` metadata on them using the field IDs that the [Iceberg specification reserves](https://iceberg.apache.org/spec/#reserved-field-ids) for row lineage, and readers must resolve them by field ID, not by name:

| Materialized Column | Iceberg Row Lineage Field | Field ID |
|---------------------|---------------------------|----------|
| Row ID (`delta.rowTracking.materializedRowIdColumnName`) | `_row_id` | 2147483540 |
| Row commit version (`delta.rowTracking.materializedRowCommitVersionColumnName`) | `_last_updated_sequence_number` | 2147483539 |

A null (or absent) materialized value resolves to the same result under both protocols: the row ID is the entry's `first_row_id` plus the row's physical position in the file, and the last updated sequence number is the entry's data sequence number. Because `sequence_number` is the Delta commit version, materialized row commit versions are valid `_last_updated_sequence_number` values without conversion.

## Partition Specs

Iceberg uses **partition specs** to define how a table is partitioned. Each partition spec has a unique `spec_id` and describes the partition columns.

Delta defines partitioning via `partitionColumns` in the table metadata, supports only identity partitioning, and does not support partition evolution. When `adaptiveMetadata` is enabled, the `partition` struct schema is derived deterministically from `partitionColumns`: the i-th column (0-based) is assigned partition `field-id = 1000 + i` and typed as that column's type. Writers and readers derive the same assignment from `partitionColumns`.

## Partition Values

Partition values are stored in the `partition` struct (field ID 102) on each DATA entry. The struct has one field per partition column, keyed by `field-id = 1000 + i` and typed as the i-th partition column's type.

When producing manifest entries, writers convert Delta's `partitionValues` string map to the typed `partition` struct: each value is parsed into its column's type and placed at that column's `field-id`. When reading manifest entries back into Delta actions, readers extract each value by `field-id = 1000 + i` and map it back to the i-th partition column.

Because partition column values are materialized in the data files (see [Writer Requirements](#writer-requirements)), readers read them from the file like any other column; the `partition` struct is used only for partition pruning.

## Commit Types

Writers can produce two types of commits:

### Log Commit

A **log commit** writes changes directly to the Delta log JSON file without updating the metadata tree. This is suitable for:
- Small appends (few files added)
- Metadata-only changes (schema updates, table properties)

Log commits contain standard Delta actions (`add`, `remove`, `metadata`, etc.) without a `checkpoint` action.

### Manifest Commit

A **manifest commit** produces a new metadata tree and includes an embedded `checkpoint` action in the Delta log.

Writers should trigger manifest commits to limit the number of JSON log files and the volume of JSON bytes readers must consume. The specific thresholds are implementation-defined.

Manifest commits have the following characteristics:

1. **File actions may be logged**: A manifest commit may also
   write `add` and `remove` actions to the Delta log, in addition to updating the metadata tree. The `checkpointMetadata.version` may be less than the commit version (the tree covers up to `checkpointMetadata.version`; remaining changes are in the log). Checkpoint versions must be strictly monotonically increasing across all checkpoint actions in the log.

2. **Non-file actions must be logged**: A manifest commit must always
   write non-file actions (`metadata`, `protocol`, `txn`, `domainMetadata`, `commitInfo`) to the Delta log. These actions are not stored in the metadata tree.

3. **Incorporates preceding commits**: Manifest commits must
   incorporate all preceding log commits (since the last checkpoint) into the new metadata tree.

### Standalone Checkpoint

Independently of commits, any writer can produce a standalone checkpoint file that references an existing metadata tree without producing a new one.

A standalone checkpoint uses the V2 checkpoint naming scheme (`n.checkpoint.u.parquet`) and contains a `checkpoint` action. It must reference an existing tree via `contentRoot` and must not produce a new root manifest, so its `contentRoot.version` is that existing tree's version and is less than `checkpointMetadata.version`. File actions (`add`, `remove`) after `contentRoot.version` (up to `checkpointMetadata.version`) are included inline in the checkpoint file (not in sidecars). This keeps standalone checkpoints cheap: any writer can produce one without the cost of building a new tree. The inline file actions are bounded: if the volume of pending file actions were large, the writer would produce a manifest commit instead.

## Manifest Deletion Vectors (MDVs)

MDVs reduce the write amplification: instead of rewriting a leaf manifest to remove a file, the root manifest records an MDV that marks row positions in the leaf manifest as deleted.

MDVs are stored in `manifest_info.dv` on `DATA_MANIFEST` entries in the root manifest. See [Manifest Deletion Vectors](#manifest-deletion-vectors-mdvs) for field definitions and CDF bitmap semantics.

Example root manifest entry with MDV:

```
DATA_MANIFEST entry:
  content_type: 3 (DATA_MANIFEST)
  location: "metadata/leaf-m1.parquet"
  tracking:
    status: EXISTING
  manifest_info:
    dv: <bitmap marking positions 2, 17>
    dv_cardinality: 2
```

When reading a leaf manifest, readers must check the `DATA_MANIFEST` entry for a non-null `manifest_info.dv`. If present, deserialize the bitmap and skip entries at positions marked in it.

## Reader Requirements

When `adaptiveMetadata` is supported and active, readers must:

1. **Find the latest checkpoint**: Locate the `checkpoint` action with the greatest `checkpointMetadata.version` — across manifest commits, standalone checkpoints, and `_last_checkpoint` — and use its `contentRoot` as the metadata tree. Because `checkpointMetadata.version` is monotonic, a later manifest commit that builds a newer tree supersedes an earlier standalone checkpoint that references an older tree.

2. **Read the content root**: Parse the root manifest file referenced by
   `contentRoot.path`.

3. **Apply MDVs**: When reading leaf manifests, skip entries at positions
   marked in `manifest_info.dv`.

4. **Replay log commits**: Apply any log commits after `checkpointMetadata.version`
   to get the current table state.

5. **Handle sidecars**: If the checkpoint has `sidecar` entries, read auxiliary
   data from the referenced sidecar files.

## Writer Requirements

When `adaptiveMetadata` is supported and active, writers must:

1. **Track backreferences**: Writers must record the manifest location
   (manifest path and row position) for every file they read from the tree. For `remove` actions, the backreference identifies the entry being deleted. For `add` actions that supersede an existing entry (stats backfill, DV update), the backreference identifies the entry being replaced. See [Backreferences](#backreferences).

2. **Choose commit type**: Based on operation size and accumulated log entries:
   - Small operations -> log commit
   - Large operations or compaction threshold reached -> manifest commit

3. **Maintain two-level hierarchy**: The metadata tree must have at most two levels
   (root -> leaves). Writers must not create nested manifest references.

4. **Resolve concurrent commits**: When another commit lands while a manifest
   commit is in progress, resolve the conflict per [Conflict Resolution](#conflict-resolution).

5. **Generate MDVs from backreferences**: When creating a manifest commit,
   use backreferences from accumulated removes and re-adds to populate `manifest_info.dv` on affected `DATA_MANIFEST` entries. For change data feed support, also populate tracking bitmaps:
   - `manifest_info.dv`: accumulates all deleted/replaced positions (used
     for reading)
   - `tracking.deleted_positions`: positions deleted in this commit only
     (for removes) (used for CDF)
   - `tracking.replaced_positions`: positions replaced in this commit only
     (for re-adds) (used for CDF)
   - Add new DATA entries with updated info for re-added files

6. **Set sequence numbers**: When producing manifest entries, writers
   must set `sequence_number` (data sequence number) to the Delta commit version when the data was originally written, and `file_sequence_number` to the commit version that physically adds the file. For new files, both are the current commit version. For compacted files, `sequence_number` preserves the original version while `file_sequence_number` is the compaction version. See [Row Tracking Compatibility](#row-tracking-compatibility).

7. **Set field IDs on materialized row tracking columns**: When writing
   data files, writers must set Parquet `field_id` metadata on the materialized row ID and row commit version columns using the Iceberg reserved field IDs. See [Materialized Row Tracking Columns](#materialized-row-tracking-columns).

8. **Materialize partition columns**: Partition column values must be
   materialized when writing Parquet data files, written with the partition column's `field_id`. See [Partition Values](#partition-values).

9. **Populate partition and content_stats**: When producing manifest
   entries, writers must:
   - Convert Delta's `partitionValues` string map to the typed
     `partition` struct
   - Convert Delta's `stats` JSON to typed content_stats entries

### Manifest Commit Procedure

```
1. Read the previous checkpoint's metadata tree
2. Collect all log commits since previous checkpointMetadata.version
3. For removes and re-adds with backreferences:
   - Group by manifest path
   - Add positions to manifest_info.dv bitmap (accumulates all
     deletions/replacements)
   - For removes: add position to tracking.deleted_positions
     bitmap (this commit only)
   - For re-adds: add position to tracking.replaced_positions
     bitmap (this commit only)
   - Add new DATA entries with updated info for re-added files
4. For adds from preceding log commits (versions <=
   checkpointMetadata.version): set status=EXISTING with explicit
   snapshot_id and sequence numbers. These files are already
   part of the table state.
5. For adds from the current commit (if any): set status=ADDED.
   Sequence numbers may be null (inherited from root).
6. For both (4) and (5):
   a. Convert partitionValues to partition struct
   b. Convert Delta stats JSON to content_stats struct
   c. Small number of adds: inline in root manifest
   d. Large number of adds: create new leaf manifest
7. Determine which leaf manifests need compaction
8. Write new leaf manifests (if any)
9. Write new root manifest
10. Write checkpoint action to Delta log
```

The `status` field reflects what changed in the commit that produces the tree, not the original Delta log commit. Files from preceding log commits must be marked `EXISTING`. Only files added in the current commit are marked `ADDED`.

## Conflict Resolution

When concurrent commits occur, conflict resolution depends on the commit types involved:

| Winner Commit | Candidate Commit | Resolution |
|---------------|------------------|------------|
| Log commit | Log commit | Standard Delta conflict resolution (rebase add/remove actions) |
| Log commit | Manifest commit | Candidate incorporates winner's changes, advances `checkpointMetadata.version` |
| Manifest commit | Log commit | Candidate recomputes backreferences against winner's new tree |
| Manifest commit | Manifest commit | Candidate fails and must retry with fresh read of winner's tree |

### Log + Log Conflict

Standard Delta conflict resolution applies. Both commits modify the Delta log directly, and the losing commit rebases its actions.

### Log + Manifest Conflict

If a log commit wins while a manifest commit is in progress:
1. The manifest commit reads the winning log commit
2. Incorporates the winner's changes into the new tree
3. Sets `checkpointMetadata.version` to the version after the winner
4. Proceeds with the manifest commit

### Manifest + Log Conflict

If a manifest commit wins while a log commit is in progress:
1. The log commit's backreferences are stale (referencing
   old tree positions). Files may have moved between manifests due to compaction in the winning tree.
2. On retry, the log commit must re-read the new tree
3. Recomputes backreferences for all removes and re-adds
4. Retries as either a log commit or a manifest commit

### Manifest + Manifest Conflict

Both commits attempt to write new trees simultaneously:
1. One commit wins (via Delta log atomicity)
2. The losing commit fails entirely
3. On retry, the loser reads the winner's new tree and
   recomputes backreferences (files may have moved)
4. May retry as either a log commit or a manifest commit

## Manifest Compaction

Over time, MDVs can accumulate, degrading read performance. Writers should periodically compact manifests to maintain read efficiency.

*Implementation recommendation*: Compact when the ratio of invalidated entries to total entries in a leaf manifest exceeds ~10%, or when explicitly requested (e.g., OPTIMIZE).

### Compaction Procedure

```
1. Identify DATA_MANIFEST entries with non-null manifest_info.dv
2. For each such entry:
   a. Read the leaf manifest entries
   b. Apply manifest_info.dv bitmap to filter out stale entries
   c. Write new leaf manifest with only live entries (status=EXISTING)
3. In new root manifest:
   - Reference new compacted leaf manifests (without manifest_info.dv)
   - Old leaf manifests are no longer referenced
```

## Catalog-Managed Tables

*Note: The CC protocol extensions in this section are independent of the adaptive metadata tree and apply to catalog-managed tables generally. They are included here because the metadata tree benefits significantly from catalog-level checkpoint tracking and inline commits.*

For catalog-managed tables, the commit coordination protocol is extended to support the metadata tree.

### Design Goals

- Avoid serial I/O of Delta logs and root manifest during reads and writes
- Support low-latency commits for small changes
- Enable parallel prefetching of content metadata

### CC Protocol Extensions

Two extensions to the CC protocol enable efficient metadata tree handling:

1. **Catalog stores latest checkpoint**: The catalog stores and returns
   the latest checkpoint in its response, enabling readers to immediately begin prefetching the root manifest in parallel with log replay.

2. **Inline commits**: Small commit payloads can be sent inline with
   the Commit API call, avoiding the need to write a staged Delta file. The size threshold is implementation-defined.

### CC Protocol Details

The core reader and writer requirements are the same as for file-system-based tables (see [Reader Requirements](#reader-requirements) and [Writer Requirements](#writer-requirements)). The catalog extensions optimize the I/O path:

- **GetCommits response**: Returns the latest checkpoint along
  with commits, enabling readers to immediately prefetch the root manifest in parallel with log replay.
- **Commit request**: Writers include the checkpoint block for
  manifest commits, allowing the catalog to track checkpoint state without parsing Delta files.
- **Inline commits**: Small payloads (under ~100KB) can be sent
  inline with the Commit API call, avoiding a staged Delta file write.

## Feature Enablement

When `adaptiveMetadata` is added to `readerFeatures` and `writerFeatures`, the required dependent features must also be present. Existing checkpoints remain valid. The first manifest commit after enablement produces the initial metadata tree.

## Feature Removal

When `adaptiveMetadata` is removed from the protocol, a traditional checkpoint must be produced from the current metadata tree state so that the table can be read without adaptiveMetadata support. Manifest files that are no longer referenced may be cleaned up.

## Compatibility Notes

- **Existing readers**: Readers that do not support `adaptiveMetadata`
  will fail fast when encountering the feature in the protocol, as expected for reader features.

- **Iceberg interoperability**: This feature uses Iceberg V4's manifest
  format internally but does not enable Iceberg reader/writer access to the table. Iceberg interoperability requires the separate `icebergNativeV4` feature (not covered in this RFC).
