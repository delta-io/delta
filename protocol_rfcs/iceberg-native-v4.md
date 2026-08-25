# IcebergNativeV4
**Associated Github issue for discussions: https://github.com/delta-io/delta/issues/7373**

This protocol change introduces a compatibility flag, which ensures that a Delta table can be safely
read as an Apache Iceberg™ V4 format table, similar to
[IcebergCompatV1](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#iceberg-compatibility-v1),
[IcebergCompatV2](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#iceberg-compatibility-v2),
and [IcebergCompatV3](https://github.com/delta-io/delta/blob/master/protocol_rfcs/iceberg-compat-v3.md).

The [Adaptive Metadata](https://github.com/delta-io/delta/blob/master/protocol_rfcs/iceberg-v4-metadata.md)
feature (`adaptiveMetadata`) lets a commit either write `add` and `remove` actions to the Delta log
(a *log commit*) or fold them into the metadata tree (a *manifest commit*). An Iceberg engine reads
the tree but not the Delta log, so files that live only in a log commit are invisible to it.
`icebergNativeV4` forbids file actions in the log, making the tree the complete record of table
content.

Like the other Iceberg compatibility features, this feature does not implement or specify the
conversion itself. How an Iceberg engine discovers the tree, including any Iceberg `metadata.json`
root pointer or catalog handoff, is an implementation detail outside the scope of this protocol.

--------

# Changes to existing sections

### Add File and Remove File

> ***Change to [existing section](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#add-file-and-remove-file)***

<ins>When the `icebergNativeV4` table feature is enabled, `add` and `remove` actions must not be
written to the Delta log; the Iceberg V4 metadata tree carries all content metadata instead (see
[Iceberg Native V4](#iceberg-native-v4)). The actions themselves are unchanged from
[Adaptive Metadata](https://github.com/delta-io/delta/blob/master/protocol_rfcs/iceberg-v4-metadata.md).</ins>

### Add CDC File

> ***Change to [existing section](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#add-cdc-file)***

<ins>`cdc` actions must not be written to the Delta log while `icebergNativeV4` is enabled.
`changeDataFeed` is the only producer of `cdc` actions and is an
[incompatible feature](#incompatible-table-features) under `icebergNativeV4`, so no `cdc` actions
are produced. The Iceberg V4 content entry schema has no content type for change data files, so they
have no place in the tree either.</ins>

### Checkpoints

> ***Change to [existing section](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#checkpoints)***

<ins>Standalone checkpoints remain permitted, but carry no inline file actions, so their
`contentRoot.version` may lag `checkpointMetadata.version` only across versions that changed no
files. [Adaptive Metadata](https://github.com/delta-io/delta/blob/master/protocol_rfcs/iceberg-v4-metadata.md)
already requires that the `checkpoint` action carry checkpoint state and that classic and V2
checkpoints not be produced.</ins>

--------

> ***New Section after [Iceberg Compatibility V3](https://github.com/delta-io/delta/blob/master/protocol_rfcs/iceberg-compat-v3.md)***

# Iceberg Native V4

This table feature (`icebergNativeV4`) ensures that all of a Delta table's content metadata is
stored in an Apache Iceberg V4 adaptive metadata tree, so that the table can be read natively by an
Iceberg V4 engine. This table feature does not implement or specify that read path.

To support this feature:
- The feature `icebergNativeV4` must exist in the table protocol's `writerFeatures`.
- The feature `adaptiveMetadata` must exist in the table protocol's `readerFeatures` and `writerFeatures`. Its required features are transitively required here.

This table feature is enabled when the table property `delta.enableIcebergNativeV4` is set to
`true`.

> **NOTE:** Like IcebergCompatV3, and unlike IcebergCompatV1 and IcebergCompatV2, this feature does
> _NOT_ forbid supporting and enabling Deletion Vectors on the table. `adaptiveMetadata` requires
> them.

## Relationship to Iceberg Compatibility V1, V2, and V3

Each Iceberg compatibility feature targets one Iceberg format version: IcebergCompatV1 and
IcebergCompatV2 target Iceberg V2, IcebergCompatV3 targets Iceberg V3, and `icebergNativeV4` targets
Iceberg V4. At most one may be active at a time.

Writers must ensure that IcebergCompatV1, IcebergCompatV2, and IcebergCompatV3 are not active: for
each of `icebergCompatV1`, `icebergCompatV2`, and `icebergCompatV3`, either the table feature is
absent from the protocol or its table property (`delta.enableIcebergCompatV1`,
`delta.enableIcebergCompatV2`, `delta.enableIcebergCompatV3`) is not set to `true`.

Since IcebergCompatV3 must be inactive, its rules are instead **incorporated by reference**: every
requirement in
[Writer Requirements for IcebergCompatV3](https://github.com/delta-io/delta/blob/master/protocol_rfcs/iceberg-compat-v3.md#writer-requirements-for-icebergcompatv3)
applies as though restated here, with modifications. IcebergCompatV3 governs only Delta-to-Iceberg
conversion; because the tree here may also be written by an external Iceberg engine, a few
constraints beyond IcebergCompatV3 are added:

- The requirement that IcebergCompatV1 and IcebergCompatV2 be inactive is broadened to include IcebergCompatV3, as stated above.
- IcebergCompatV3 permits Column Mapping in `name` or `id` mode; here only `id` mode is permitted, because an external Iceberg writer resolves columns by field ID and does not honor Delta's physical column names.
- The type allow-list carries over **except `byte` and `short`, which are removed**: Iceberg has no 8- or 16-bit integer type, so an external Iceberg writer would store such a column as a 32-bit integer, and reading those values back as `byte` or `short` could overflow. The allowed types are therefore: [`integer`, `long`, `float`, `double`, `decimal`, `string`, `binary`, `boolean`, `timestamp`, `timestampNTZ`, `date`, `array`, `map`, `struct`, `variant`, `geometry`, `geography`].
- Any schema change to a `struct` used as a `map` key must be blocked (changes to the map value are allowed), matching Iceberg, which does not permit evolving map-key types.

All other IcebergCompatV3 requirements apply unchanged: Row
Tracking with the reserved materialized column field IDs, nested field identifiers for `ArrayType`
and `MapType`, materialized partition column values, int64 timestamps, the partition spec
replacement restriction, the Type Widening allow-list (its `byte` and `short` source rows are
unreachable once those types are disallowed), and literal-only column write defaults.

## Incompatible Table Features

Because the tree is the complete, authoritative record of table content and may be written by an
external Iceberg engine that does not honor Delta write-time semantics, any feature whose guarantee
depends on the Delta writer validating or deriving content at write time cannot be upheld. Writers
must ensure that each of the following is not active — for each feature, either its name is absent
from `writerFeatures` or its enabling table property is not set to `true`:

- `invariants` and `checkConstraints` — write-time validation of column values that an external Iceberg writer does not perform.
- `generatedColumns` and `identityColumns` — column values the Delta writer derives at write time (identity columns also maintain `delta.identity.highWaterMark`); an external Iceberg writer does not.
- `changeDataFeed` — requires emitting `cdc` files for row-level changes, which an externally produced manifest commit does not, so the change feed would be incomplete. (`cdc` actions are never represented in the tree; see [Add CDC File](#add-cdc-file).)
- `appendOnly` (`delta.appendOnly`) — a write constraint an external Iceberg writer does not read, so "no data removed" cannot be enforced.

`clustering` may remain enabled: it only influences physical layout, so an external writer that does
not maintain it still produces a correct tree. Row Tracking remains required (inherited from
IcebergCompatV3), with its values materialized in the tree entries. These restrictions may be
revisited if a future Iceberg spec version gains equivalent support (e.g. constraints,
generated/identity columns, or collations).

## Allowed Table Features

The list above blocks features known to be incompatible today, but it cannot anticipate features
defined in the future, some of which may also break the guarantee that the tree is a complete,
externally writable record. To stay forward-safe, writers must additionally verify that every
feature present in `writerFeatures` appears in the following allow-list. A feature not on this list
must not be present at all; enabling `icebergNativeV4` on a table that carries an unlisted feature
must be rejected until that feature's interaction with this one has been reviewed and the list
updated.

The allowed features are:

- `icebergNativeV4`, `adaptiveMetadata`, and every feature `adaptiveMetadata` transitively requires (including `columnMapping`, `deletionVectors`, and `rowTracking`).
- `domainMetadata`, `vacuumProtocolCheck`, `inCommitTimestamp`, `clustering`, `timestampNtz`, `typeWidening`, `variantType`, `variantShredding`, `geometry`, `geography`, `catalogManaged`, `checkpointProtection`. `catalogManaged` only coordinates how commits are published, and `checkpointProtection` only constrains checkpoint removal during history cleanup; neither affects the content tree.
- The features named under [Incompatible Table Features](#incompatible-table-features), which may be present only as inactive legacy carry-overs and only while the not-active checks in that section hold.
- The Iceberg compatibility features `icebergCompatV1`, `icebergCompatV2`, `icebergCompatV3`, and `icebergWriterCompatV1` (which itself requires `icebergCompatV2` be present), which — per [Relationship to Iceberg Compatibility V1, V2, and V3](#relationship-to-iceberg-compatibility-v1-v2-and-v3) — may be present only while inactive.

Because `icebergNativeV4` is a writer feature, this allow-list governs `writerFeatures`; a reader-only feature (one absent from `writerFeatures`) is out of its scope.

## Writer Requirements for IcebergNativeV4

When this feature is supported and enabled, writers must additionally:

- Write every commit that adds or removes files as a *manifest commit*, as defined by [Adaptive Metadata](https://github.com/delta-io/delta/blob/master/protocol_rfcs/iceberg-v4-metadata.md#commit-types). Equivalently: `add` and `remove` actions must never be written to the Delta log.
- Keep the tree complete as of `checkpointMetadata.version`. `adaptiveMetadata` allows `contentRoot.version` to lag and covers the gap with inline file actions; with no file actions in the log there is nothing to cover a gap with, so every version in the gap must have changed no files. A commit that adds or removes files must therefore set `contentRoot.version` equal to both `checkpointMetadata.version` and the commit version.
- Continue to write non-file actions (`metaData`, `protocol`, `txn`, `domainMetadata`, `commitInfo`) to the Delta log, as `adaptiveMetadata` already requires. The tree does not store them.
- Reject inline deletion vectors (storage type `i`) on every commit (`adaptiveMetadata` already forbids them).


### Enablement

This feature may be enabled at table creation, or on an existing table via `ALTER TABLE`. Enablement
must be rejected if any [incompatible feature](#incompatible-table-features) is active, or if any
feature outside the [allow-list](#allowed-table-features) is present; such features must be disabled
or removed first.

An existing table's log may contain historical `add` and `remove` actions. The first commit after
enablement must be a manifest commit that folds all pre-existing state into the tree, so the tree is
complete as of the enablement version. Until it succeeds, the table does not satisfy this feature's
guarantee and must not be presented to an Iceberg engine as a native V4 table.

Writers must not change `delta.enableIcebergNativeV4` from `true` to `false` except as part of
feature removal.

### History and Time Travel

This feature constrains only versions committed while it is active. Earlier commits may contain
`add` and `remove` actions and are replayed under the normal Delta rules, so a time-travel read of a
pre-enablement version, or a change-data-feed read spanning one, may still consume file actions from
the log. The native Iceberg read path applies from the enablement version onward.


## Feature Removal

To remove `icebergNativeV4`, set `delta.enableIcebergNativeV4` to `false` and drop
`icebergNativeV4` from the protocol's `writerFeatures`. No rewrite of table state is required: the
tree remains valid and readable by any client supporting `adaptiveMetadata`. Removing
`adaptiveMetadata` itself follows that feature's own removal rules, which require producing a
traditional checkpoint from the current tree.

## Open Questions

1. **Do we need to specify additional constraints on reading partition tuple** - Iceberg might have
multiple partition specs each with different columns.  Delta would have to read these empirically and
write them?  For this iteration we assume non-content metadata is always compatible with Delta, so
partition evolution does not currently apply.
