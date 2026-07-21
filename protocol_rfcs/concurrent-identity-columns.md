# Concurrent Identity Columns

**Associated Github issue for discussions: https://github.com/delta-io/delta/issues/XXXX**
<!-- Replace XXXX with the actual github issue number once the Protocol Change Request is filed. -->

## Overview

Delta already supports [Identity Columns](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#identity-columns):
a writer generates unique `start + k * step` values for a column and records the highest value it
emitted in the column's `delta.identity.highWaterMark` schema metadata, bumping that key in the same
commit that writes the rows. That serializes identity generation through the commit: a writer cannot
know which values are safe to assign until it has read the current mark, and two writers that read the
same mark would generate overlapping values. An identity-column table therefore admits only one
concurrent writer of new identity values, a poor fit for high-throughput ingestion.

This RFC proposes a new **writer-only** table feature, `concurrentIdentityColumns`, that removes the
high-water mark from the write path. An identity column is instead bound to a **monotonic sequence**
owned by the table's catalog. Writers *reserve* disjoint ranges of values from the sequence and assign
them locally, so many writers (and many tasks within a writer) can generate identity values in parallel
without coordinating through the Delta commit. Because ranges are disjoint by construction, uniqueness
no longer depends on winning the commit or on reading a shared mark.

The sequence lives in, and is allocated by, the same catalog that coordinates the table's commits, so
the feature is restricted to [Catalog-Managed Tables](https://github.com/delta-io/delta/blob/master/protocol_rfcs/accepted/catalog-managed.md).
This RFC does **not** standardize the sequence-allocation RPC (that is a catalog concern); it
standardizes only what is persisted in the Delta log, so that any writer that understands the feature,
talking to a compatible catalog, produces a table any reader can read with no changes.

## Motivation

Each writer, and each task within a write job, holds its own reserved range, so identity generation
scales with write parallelism instead of funneling through one high-water-mark update. The highest
generated value is tracked by the sequence service, so it is no longer rewritten into schema metadata
on every commit. The trade-off is a dependency on an external, catalog-hosted allocator and the extra
communication required to reserve ranges.

--------

<!-- Proposed additions to PROTOCOL.md follow. -->

> ***Add the following key to the column-metadata list in [Identity Columns](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#identity-columns), after `delta.identity.allowExplicitInsert`.***

When the `concurrentIdentityColumns` feature is supported, the `metadata` for an identity column MAY
additionally contain:

- `delta.identity.concurrent.sequenceId`: The identifier of the catalog-hosted sequence (a monotonic
  counter, see [Concurrent Identity Columns](#concurrent-identity-columns)) that currently allocates
  values for this column. This is a string type value. Its presence marks the column as
  **service-backed**: its values are allocated from the named sequence rather than derived from
  `delta.identity.highWaterMark`, and the two keys are mutually exclusive. It is an opaque pointer to
  the column's *current* sequence, not a stable column identifier: a writer must not change it during
  ordinary writes, it is replaced when the column is re-bound, and it is removed when the column leaves
  the service backend. See [Concurrent Identity Columns](#concurrent-identity-columns).

> ***New Section after the [Identity Columns](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#identity-columns) section***

## Concurrent Identity Columns

A **service-backed** identity column draws its values from a monotonic sequence hosted by the table's
catalog instead of from `delta.identity.highWaterMark`. An identity column is service-backed if and
only if its schema metadata contains `delta.identity.concurrent.sequenceId`.

To support this feature:
- Since this table feature depends on [Catalog-Managed Tables](https://github.com/delta-io/delta/blob/master/protocol_rfcs/accepted/catalog-managed.md),
  the table must be on Reader Version 3 and the feature `catalogManaged` must exist in the
  `protocol`'s `readerFeatures` and `writerFeatures`. The catalog owns and allocates the sequence, so
  the feature is undefined without one.
- The table must be on Writer Version 7.
- The feature `concurrentIdentityColumns` must exist in the table `protocol`'s `writerFeatures`.
- The feature `identityColumns` must exist in the table `protocol`'s `writerFeatures`. A
  service-backed column is still an [Identity Column](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#identity-columns)
  and must satisfy the identity-column requirements (`start`, `step`, `allowExplicitInsert`); only
  value generation and the highest-value bookkeeping are delegated to the sequence.

`concurrentIdentityColumns` is a table-level mode for identity generation, not a per-column opt-in: on
a table that supports the feature, **every** identity column must be service-backed. Uniformity keeps
the write path unambiguous, no writer has to reconcile two generation models. Consequently:
- A writer that adds the feature to a table must bind every existing identity column to a sequence in
  the same operation.
- A writer creating or altering an identity column on a feature-supporting table must make it
  service-backed.
- A writer must reject a table state in which the feature is supported but some identity column lacks
  a `sequenceId`, or in which a column carries both `sequenceId` and `delta.identity.highWaterMark`,
  rather than fall back to high-water-mark generation for that column.

### The sequence

A sequence is a catalog-hosted counter identified by `delta.identity.concurrent.sequenceId` and scoped
by `(table, sequenceId)`: the contract is that a persisted `sequenceId` resolves, via the table's
catalog, to the sequence the column's values are drawn from. Each identity column is bound to its own
sequence, so a table with several identity columns has several independent counters.

The sequence service hands out values `start + k * step` for strictly increasing, non-negative integers `k`
(`start` and `step` are the column's `delta.identity.start` and `delta.identity.step`). It tracks the
largest `k` it has allocated, not a numeric extreme, so a negative `step` works unchanged. Every
reservation must return a range disjoint from every range that sequence has ever returned, and must
advance the allocation frontier atomically and durably before returning it. Reservations are not
idempotent: retrying after a timeout or lost response is a new request that must allocate a new range.

Any allocation or transition that would produce a value outside the signed 64-bit `BIGINT` range must
fail rather than wrap.

**Gaps are acceptable.** The sequence guarantees only that a generated value is never reused, not that
generated values are contiguous. A range a writer reserves but does not fully use (it crashes, aborts,
or under-fills) leaves those values permanently unallocated, so a service-backed column's values may
contain holes, exactly as a classic identity column's may after a failed write. Writers must not return
or replay an unused range to close the hole.

### Writer Requirements for Concurrent Identity Columns

A writer to a table that supports `concurrentIdentityColumns` must:

- Obtain each service-backed column's values by **reserving disjoint ranges** from that column's
  sequence via the catalog, and assign only values drawn from ranges it has reserved. Writers must not
  generate a service-backed column's values from `delta.identity.highWaterMark` or from any local
  counter, and must never write or advance that key for such a column.
- Honor `delta.identity.allowExplicitInsert` exactly as for classic identity columns. Explicitly
  inserted values are not drawn from the sequence and the catalog does not account for them, just as
  they do not advance the high-water mark today, so when `allowExplicitInsert` is `true` a
  user-supplied value may collide with a generated one; deduplicating explicit inserts remains the
  user's responsibility.
- When creating a service-backed identity column, or converting a classic identity column to a
  service-backed one, bind the column to a sequence and persist its `sequenceId` in the column
  metadata in the same operation. During ordinary writes a writer must not change a column's
  `sequenceId`; it changes only when the binding is repaired or the feature is removed (see
  [Establishing, Repairing, and Removing a Sequence Binding](#establishing-repairing-and-removing-a-sequence-binding)).

### Establishing, Repairing, and Removing a Sequence Binding

Three operations establish, re-establish, or tear down a column's binding to a sequence. Each is a
table-metadata transaction that must conflict with every concurrent transaction on the table:
whichever commits first, the loser aborts and retries against the new table state, and a retried
writer must obtain new reservations rather than commit identity values generated from a binding that
is no longer current.

- **Establishing a binding.** In the version that adds `concurrentIdentityColumns` to the table's
  `writerFeatures`, a writer must, for every identity column, create a sequence seeded at the first
  value past the column's current `delta.identity.highWaterMark` (`highWaterMark + step`, or `start`
  when the column has never emitted a value) and persist its `sequenceId`. The writer trusts the mark
  and does not scan the data.
- **Repairing a binding.** A writer may re-bind a service-backed column to a **fresh** sequence seeded
  strictly past the column's current extreme in the data (max for ascending `step`, min for
  descending), persisting the new `sequenceId`. This scans the data, and repairs a sequence that
  drifted from it, e.g. after explicit inserts the sequence never saw. It does not convert a column
  between the classic and service backends. (delta-spark exposes this as
  `ALTER TABLE ... ALTER COLUMN c SYNC IDENTITY`.)
- **Removing the binding.** In the version that removes `concurrentIdentityColumns` from the table's
  `writerFeatures`, a writer must convert every service-backed column back to a classic identity
  column: rather than scan the data, it reserves a **single** value from the column's sequence, writes
  that value into `delta.identity.highWaterMark`, and removes
  `delta.identity.concurrent.sequenceId`. The reserved value is past everything the sequence has handed
  out, so the next classic value (`highWaterMark + step`) clears them all.

Because all three seed strictly past the existing extreme (or preserve it in the high-water mark), no
transition can cause a previously-written identity value to be regenerated.

### Reader Requirements for Concurrent Identity Columns

None beyond those already imposed by the required `catalogManaged` reader-writer feature. Identity
values are fully materialized into the data files by writers, exactly as for classic identity columns,
and `delta.identity.concurrent.sequenceId` is write-path bookkeeping.

### Compatibility with other Delta Features

| Feature | Interaction |
|-|-|
| [Identity Columns](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#identity-columns) | **Required** (`identityColumns` in `writerFeatures`). A service-backed column is an identity column whose value generation is delegated to a sequence. Every identity column on a feature-supporting table must be service-backed. |
| [Catalog-Managed Tables](https://github.com/delta-io/delta/blob/master/protocol_rfcs/accepted/catalog-managed.md) | **Required.** The catalog owns and allocates the sequence, so a table must not support `concurrentIdentityColumns` without `catalogManaged`. |
| Per-file statistics | Unchanged. Computed from the materialized values exactly as for any other column. |
| Time Travel / Change Data Feed | Unchanged. Identity values are materialized in the data, so historical versions and CDF read the values that were written, with no dependency on the current sequence state. |
| Column Mapping | Unchanged. The `sequenceId` binds to the column's logical identity, independent of physical name/id. |

## Valid Feature Names in Table Features

> ***Add the following row after In-Commit Timestamps in [Valid Feature Names in Table Features](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#valid-feature-names-in-table-features).***

Feature | Name | Readers or Writers?
-|-|-
[Concurrent Identity Columns](#concurrent-identity-columns) | `concurrentIdentityColumns` | Writers only

## Non-Goals

- **The sequence-allocation wire protocol.** How a writer creates a sequence and reserves ranges from
  the catalog (RPC surface, batching, buffering, retry policy) is a catalog concern. This RFC
  standardizes only what is persisted in the Delta log and the resulting value guarantees.
- **Lifecycle and garbage collection of sequences.** A sequence left unreferenced by a dropped column,
  table, or feature downgrade may be retired by the catalog; this RFC does not mandate a reclamation
  protocol. A `sequenceId` that no longer resolves simply means no further values can be generated for
  that column until it is repaired.
- **Cross-table or shared sequences.** A sequence is scoped to the table it is stamped on.
- **Support on path-based / non-catalog-managed tables.**

## Appendix: The Sequence Service (non-normative)

A catalog that backs this feature is expected to expose three operations on a sequence, keyed by
`(table, sequenceId)`. This sketch is **informative**: a concrete catalog is free to shape, batch, or
name these calls differently as long as it honors the contract above.

- **`createSequences`**: register a new sequence for a column, seeded as described in
  [Establishing, Repairing, and Removing a Sequence Binding](#establishing-repairing-and-removing-a-sequence-binding).
  Idempotent create-or-get, so a post-commit retry is safe.
- **`reserveRanges`**: reserve a contiguous range of `count` values from an existing sequence and return
  its inclusive bounds. Removing the binding uses the degenerate `count = 1` case to read back the next
  value as the classic high-water mark.
- **`dropSequences`**: retire a sequence the table no longer references. Best-effort: a failed drop only
  strands a sequence the table no longer points at, never leaves a live table pointing at a dropped
  sequence.
