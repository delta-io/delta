# Concurrent Identity Columns

**Associated Github issue for discussions: https://github.com/delta-io/delta/issues/XXXX**
<!-- Replace XXXX with the actual github issue number once the Protocol Change Request is filed. -->

## Overview

Delta already supports [Identity Columns](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#identity-columns):
a writer generates unique `start + k * step` values for a column and records the highest value
it emitted in the column's `delta.identity.highWaterMark` schema-metadata key, bumping that key in
the same commit that writes the rows.

That high-water-mark design forces identity generation to be **serialized through the commit**: a
writer cannot know which values are safe to assign until it has read the current high-water mark,
and two writers that read the same mark would generate overlapping values. In practice this means
identity-column tables admit only one concurrent writer of new identity values, and each writer
must win the metadata commit to advance the mark. This is a poor fit for high-throughput and
highly-concurrent ingestion.

This RFC proposes a new **writer-only** table feature, `concurrentIdentityColumns`, that removes the
high-water mark from the write path. Instead, an identity column is bound to a **monotonic sequence**
owned by an external allocation service (the table's catalog). Writers *reserve* disjoint ranges of
values from the sequence and assign them locally, so many writers (and many tasks within a writer)
can generate identity values in parallel without coordinating through the Delta commit. Because
ranges are disjoint by construction, uniqueness no longer depends on winning the commit or on reading
a shared mark.

The feature builds on [Catalog-Managed Tables](https://github.com/delta-io/delta/blob/master/protocol_rfcs/accepted/catalog-managed.md):
the sequence lives in, and is allocated by, the same catalog that coordinates the table's commits.
This RFC does **not** standardize the sequence-allocation RPC itself (that is a catalog concern); it
standardizes only what is persisted in the Delta log so that any writer that understands the feature,
talking to a compatible catalog, produces a correct table, and any reader can read it with no changes.

## Motivation

- **Concurrent writers.** Multiple writers can append to an identity-column table simultaneously,
  each holding its own reserved range, with no commit-level contention over the identity value.
- **Intra-writer parallelism.** A single write job's tasks each reserve their own range, so identity
  generation scales with the write's parallelism instead of funneling through one high-water-mark
  update.
- **No metadata write-amplification for the mark.** The highest generated value is tracked by the
  sequence service, not rewritten into schema metadata on every commit.

The trade-off is a dependency on an external, catalog-hosted allocator and the additional communication
between writers and the catalog required to reserve ranges. This is why the feature is restricted to
catalog-managed tables (see Compatibility below).

--------

<!-- Proposed additions to PROTOCOL.md follow. -->

> ***New keys in the [Identity Columns](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#identity-columns) column-metadata list***

When the `concurrentIdentityColumns` feature is supported, the `metadata` for an identity column MAY
additionally contain:

- `delta.identity.concurrent.sequenceId`: The identifier of the sequence that currently allocates
  values for this column. This is a string. Its presence marks the column as **service-backed**: its
  values are allocated from the named sequence rather than derived from
  `delta.identity.highWaterMark`. It is an opaque pointer to the *current* sequence, not a stable
  column identifier: a writer MUST NOT mutate it during ordinary writes, but it is **replaced** when
  the binding is re-established (SYNC IDENTITY repair rebinds the column to a freshly-seeded
  sequence) and **removed** when the column leaves the service backend (feature downgrade). See
  [SYNC IDENTITY and Feature Upgrade/Downgrade](#sync-identity-and-feature-upgradedowngrade).

The existing `delta.identity.start`, `delta.identity.step`, and `delta.identity.allowExplicitInsert`
keys retain their meaning and remain the source of truth for `start`/`step`/explicit-insert policy.
A service-backed column does not use `delta.identity.highWaterMark`; the sequence service is
authoritative for the highest allocated value, so writers neither read nor write that key for a
service-backed column.
`delta.identity.highWaterMark` and `delta.identity.concurrent.sequenceId` are mutually exclusive:
an identity column MUST NOT contain both metadata keys.

> ***New Section after the Identity Columns section***

## Concurrent Identity Columns

To support this feature:
 - The table must be on Reader Version 3 and Writer Version 7, and a feature name
   `concurrentIdentityColumns` must exist in the table `protocol`'s `writerFeatures`.
 - The feature name `identityColumns` must exist in the table `protocol`'s `writerFeatures`.
   Service-backed columns are still [Identity Columns](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#identity-columns)
   and carry `delta.identity.*` metadata, so a writer must understand `identityColumns` to enforce
   the identity-column rules (notably `delta.identity.allowExplicitInsert`).
 - The table must support [Catalog-Managed Tables](https://github.com/delta-io/delta/blob/master/protocol_rfcs/accepted/catalog-managed.md):
   the `catalogManaged` reader-writer feature must exist in both `readerFeatures` and
   `writerFeatures`. The sequence is owned and allocated by the table's catalog, so the feature is
   only valid on catalog-managed tables.

`concurrentIdentityColumns` is a superset relationship over [Identity Columns](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#identity-columns):
a service-backed column is still an identity column and MUST also satisfy the identity-column
requirements (`start`, `step`, `allowExplicitInsert`), except that value generation and the
highest-value bookkeeping are delegated to the sequence rather than the high-water mark.

### Column state

An identity column is **service-backed** if and only if its schema metadata contains
`delta.identity.concurrent.sequenceId`.

`concurrentIdentityColumns` is a **table-level mode** for identity generation, not a per-column
opt-in: on a table that supports the feature, **every** identity column MUST be service-backed
(carry a `sequenceId`). A table MUST NOT mix service-backed and classic high-water-mark identity
columns. Consequently:
 - A writer that adds the `concurrentIdentityColumns` feature to a table MUST bind every existing
   identity column to a sequence in the same operation.
 - A writer creating or altering an identity column on a feature-supporting table MUST make it
   service-backed.
 - A writer MUST reject a table state in which the feature is supported but some identity column
   lacks a `sequenceId` (a mixed state), rather than fall back to high-water-mark generation for
   that column.
 - A writer MUST reject an identity column that contains both `delta.identity.highWaterMark` and
   `delta.identity.concurrent.sequenceId`.

Requiring uniformity keeps a table's identity write-path semantics unambiguous: on a
feature-supporting table, all identity values come from sequences, so no writer has to reconcile two
generation models or advance a high-water mark for some columns and not others.

### The sequence

A sequence is a catalog-hosted counter identified by `delta.identity.concurrent.sequenceId`.
Today it is scoped by `(table, sequenceId)`: the catalog authorizes and keys allocations by the
table's catalog identity, so a given `sequenceId` is currently meaningful only in the context of the
table it is stamped on. (This scoping is a property of the catalog's allocation model and MAY evolve;
the Delta-log contract is only that a persisted `sequenceId` resolves, via the table's catalog, to a
sequence from which the column's values are drawn.)

A table MAY have **multiple sequences**: each identity column is bound to its own sequence and
carries its own `sequenceId`, and the `(table, sequenceId)` pairs are independent counters. This is
explicitly allowed, sequences are not shared across columns even within one table, so two identity
columns on the same table advance independently. (Sharing one sequence across tables is a separate
matter and is out of scope, see Non-Goals.)

The sequence hands out values `start + k * step` for strictly increasing,
non-negative integers `k` (`start` = the column's `delta.identity.start`, `step` = its
`delta.identity.step`). `step` may be negative, in which case the emitted values decrease in
magnitude order while `k` still advances monotonically; the sequence tracks the largest `k` it has
allocated, not a numeric minimum or maximum. The sequence service MUST atomically and durably advance
the allocation frontier for each reservation before returning it. For a given sequence, every successful
reservation request MUST return a newly allocated range that is disjoint from every range that sequence
has ever returned. Reservation requests are not idempotent: retrying after a timeout or lost response is
a new request that MUST allocate a new range, and the range returned by the original request remains allocated.

Allocation MUST detect when any value in a requested range falls outside the signed 64-bit `BIGINT`
range and fail the request rather than wrap. For example, allocating past `Long.MaxValue` results in
an error; arithmetic overflow MUST NOT produce a value at the opposite end of the range.

**Gaps are acceptable.** The sequence guarantees only that a generated value is never reused, not
that generated values are contiguous. A writer reserves values in advance, so a range it reserves
but does not fully use (it crashes, aborts, under-fills, or is speculatively over-provisioned) leaves
those values permanently unallocated. Writers MUST NOT return or replay an unused range to close the
hole; on retry a writer simply reserves a fresh range. A service-backed column's values may therefore
contain holes, exactly as a classic identity column's may after a failed write.

This RFC does not define the wire protocol for creating a sequence or reserving from it; those are
catalog APIs. It defines only that:
 - a `sequenceId` persisted in a column's metadata refers to a sequence the table's catalog can
   allocate from, and
 - values allocated from that sequence for that column follow `start + k * step` for non-negative
   integers `k`, and within a sequence no `k` is ever allocated more than once, so generated values
   from that sequence are never reused (but need not be contiguous, see gaps above).

### Writer Requirements

A writer to a table that supports `concurrentIdentityColumns` MUST:

 - For each service-backed identity column (one carrying `delta.identity.concurrent.sequenceId`),
   obtain identity values by **reserving disjoint ranges** from that column's sequence via the
   catalog, and assign only values drawn from ranges it has reserved. A reserved range is a set of
   values `start + k * step` that the catalog guarantees it has handed to no other reserver. Writers
   MUST NOT generate a service-backed column's values from `delta.identity.highWaterMark` or from any
   local counter.
 - Honor `delta.identity.allowExplicitInsert` exactly as for classic identity columns: when it is
   `false`, users MUST NOT supply their own values; when `true`, user-supplied values are allowed and
   the writer only generates values for rows that omit one. Explicitly-inserted values are not drawn
   from the sequence and the catalog does not account for them (identical to the classic case's
   relationship with the high-water mark), so when `allowExplicitInsert` is `true` a user-supplied
   value MAY collide with a generated one; deduplication of explicit inserts is the user's
   responsibility, exactly as for classic identity columns.
 - Never write or advance `delta.identity.highWaterMark` for a service-backed column; the sequence is
   authoritative for the values it has allocated.
 - When creating a service-backed identity column, or when converting a classic identity column to a
   service-backed one, bind the column to a sequence and persist its `sequenceId` in the column
   metadata in the same operation. During ordinary writes a writer MUST NOT change a column's
   `sequenceId`; it is only re-bound by SYNC IDENTITY or removed by downgrade (see
   [SYNC IDENTITY and Feature Upgrade/Downgrade](#sync-identity-and-feature-upgradedowngrade)).

### SYNC IDENTITY and Feature Upgrade/Downgrade

A column's binding to a sequence is established, re-established, or torn down by three operations.
In all of them the persisted `delta.identity.concurrent.sequenceId` reflects the column's *current*
sequence; it is not a stable identifier across these transitions.

Upgrade, SYNC IDENTITY, and downgrade are table-metadata transactions that MUST conflict with every
concurrent transaction on the table. Consequently, either the transition commits first, in which case
every transaction planned against the previous identity metadata MUST abort and retry from the new
metadata, or another transaction commits first, in which case the transition MUST retry against the
new table state. A retried writer MUST obtain new reservations using the current column metadata and
MUST NOT commit data files containing identity values generated from a sequence binding that is no
longer current. This conflict rule fences in-flight writers across a sequence rebind and makes the
committed table state used to seed a transition authoritative.

- **Upgrade (bind an identity column to the service):** adding `concurrentIdentityColumns` support
  to a table, e.g.
  `ALTER TABLE t SET TBLPROPERTIES ('delta.feature.concurrentIdentityColumns' = 'supported')`,
  or converting a classic identity column, MUST, for every identity column, create a sequence seeded
  from the column's current `delta.identity.highWaterMark` (the first lattice value past it,
  `highWaterMark + step`; `start` when the column has never emitted a value) and persist its
  `sequenceId` in the column metadata. Upgrade trusts the existing high-water mark and does **not**
  scan the data: the mark already tracks the extreme the column emitted, so seeding one step past it
  is enough to keep every already-written value unreachable. After upgrade the column no longer uses
  `delta.identity.highWaterMark`. If the first value past the high-water mark is outside the `BIGINT`
  range, the upgrade MUST fail rather than create a wrapping sequence.
- **SYNC IDENTITY (repair):** `ALTER TABLE ... ALTER COLUMN c SYNC IDENTITY` on a service-backed
  column re-binds the column to a **fresh** sequence seeded strictly past the current data extreme
  (max for ascending `step`, min for descending), and persists the new `sequenceId`. This is the
  repair path for a service that was reset or drifted from the data (e.g. after explicit inserts the
  service never saw). SYNC only ever advances the effective floor for future values; it never lowers
  it, and it does not convert a column between the classic and service backends. The fresh sequence has
  an allocation history independent of the sequence it replaces. If no `BIGINT` lattice value strictly
  past the data extreme exists, SYNC MUST fail.
- **Downgrade (remove the feature):** dropping `concurrentIdentityColumns`, e.g.
  `ALTER TABLE t DROP FEATURE concurrentIdentityColumns`, MUST convert every service-backed column
  back to a classic identity column. Rather than scan the data, the writer
  reserves a **single** value from the column's sequence (a `reserveRanges` request with `count = 1`)
  and writes that value into `delta.identity.highWaterMark`, then removes
  `delta.identity.concurrent.sequenceId`. The reserved value is the next value the service would
  have emitted, so it is past everything the service already handed out; the next classic value
  (`highWaterMark + step`) clears all of them. Deriving the mark this way needs no separate
  "read the current maximum" RPC, it uses the same `createSequences`/`reserveRanges` calls the write
  path already relies on. After downgrade the column is an ordinary high-water-mark identity column.
  If reserving the value needed to establish the classic high-water mark exceeds the `BIGINT` range,
  downgrade MUST fail rather than write a wrapped high-water mark.
  The now-unreferenced sequence MAY be retired by the catalog (best-effort; a stranded sequence is a
  catalog resource concern, not a table-correctness one, as noted in Non-Goals).

Because upgrade, SYNC, and downgrade all seed strictly past the existing data extreme (or preserve
it in the high-water mark), no transition can cause a previously-written identity value to be
regenerated.

### Reader Requirements

No requirements beyond those already imposed by the required `catalogManaged` reader-writer feature.
`concurrentIdentityColumns` itself is writer-only: identity values are fully materialized into the
data files by writers, exactly as for classic identity columns, and
`delta.identity.concurrent.sequenceId` is write-path bookkeeping. A reader that understands
`catalogManaged` but not `concurrentIdentityColumns` reads the materialized values correctly.

### Compatibility with other Delta Features

| Feature | Interaction |
|-|-|
| [Identity Columns](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#identity-columns) | **Required** (`identityColumns` in `writerFeatures`). `concurrentIdentityColumns` extends identity columns; a service-backed column is an identity column whose value generation is delegated to a sequence. On a feature-supporting table every identity column must be service-backed (no mixing with classic high-water-mark columns). |
| [Catalog-Managed Tables](https://github.com/delta-io/delta/blob/master/protocol_rfcs/accepted/catalog-managed.md) | **Required.** The sequence is owned and allocated by the table's catalog, so the feature is only valid on catalog-managed tables. A table MUST NOT support `concurrentIdentityColumns` without `catalogManaged`. |
| Per-file statistics | Unchanged. Statistics for a service-backed column are computed from the materialized values exactly as for any other column. |
| Time Travel / Change Data Feed | Unchanged. Identity values are materialized in the data, so historical versions and CDF read the values that were written, with no dependency on the current sequence state. |
| Column Mapping | Unchanged; the `sequenceId` binds to the column's logical identity, independent of physical name/id. |

## Non-Goals

The following are out of scope for this RFC:

- **The sequence-allocation wire protocol.** How a writer creates a sequence and reserves ranges
  from the catalog (the RPC surface, batching, and buffering strategy) is a catalog concern and is
  deliberately not standardized here. The driver is responsible for deciding whether and when to
  retry a failed or indeterminate reservation request; every retry is a new request and follows the
  non-idempotent reservation semantics above. This RFC standardizes only what is persisted in the Delta
  log (`delta.identity.concurrent.sequenceId`) and the resulting value guarantees.
- **Lifecycle and garbage collection of sequences.** When a service-backed column is dropped, or a
  table is dropped or replaced, the associated sequence may be retired by the catalog. This RFC does
  not mandate a reclamation protocol; a stranded sequence is a catalog-side resource concern, not a
  table-correctness one (a persisted `sequenceId` that no longer resolves simply means no further
  values can be generated for that column until it is repaired).
- **Cross-table or shared sequences.** A sequence is scoped to the table it is stamped on; sharing a
  sequence across tables is not defined.
- **Support on path-based / non-catalog-managed tables.** The feature requires a catalog to own the
  sequence and is undefined without one.

## Appendix: The Sequence Service (non-normative)

This section is **informative**. The Delta-log contract is only what the sections above specify (the
`delta.identity.concurrent.sequenceId` pointer and the resulting value guarantees); the RPC surface
itself is a catalog concern and is **not** standardized by this RFC (see Non-Goals). It is sketched
here to show how the persisted state is produced and consumed. A concrete catalog is free to shape,
batch, or name these calls differently as long as it honors the contract above.

A catalog that backs this feature is expected to expose three operations on a sequence, keyed by
`(table, sequenceId)`:

- **`createSequences`**: register a new sequence for a column, seeded from its `start`/`step` (and,
  on upgrade/SYNC, seeded strictly past the mark or data extreme as described above). Idempotent
  create-or-get: registering an already-existing `(table, sequenceId)` with matching parameters is a
  no-op, so a post-commit retry is safe.
- **`reserveRanges`**: reserve a contiguous range of `count` values from an existing sequence and
  return its bounds (`start`, `end`, inclusive). Writers reserve disjoint ranges and assign values
  locally, which is what lets many writers (and many tasks) generate identity values without
  coordinating through the Delta commit. An unused reserved range is not reclaimed (see
  [The sequence](#the-sequence), gaps are acceptable). Downgrade uses the degenerate `count = 1`
  case to read back the next value as the classic high-water mark.
- **`dropSequences`**: retire a sequence the table no longer references (feature downgrade, dropping a
  service-backed identity column, or a REPLACE that re-stamps fresh ids). Best-effort: a failed drop
  only strands a sequence the table no longer points at, never a live table pointing at a dropped
  sequence. (`SYNC` also leaves the superseded sequence unreferenced, but relies on the catalog's
  garbage collection rather than an explicit drop.)
