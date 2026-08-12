# Table Branching & Write-Audit-Publish

**Associated Github issue for discussions: https://github.com/delta-io/delta/issues/7439**

This RFC proposes a new **writer** table feature called **Table Branching** (`tableBranching`). It adds Git-like, **zero-copy branches** to a Delta table together with an atomic **Write-Audit-Publish (WAP)** workflow: a writer can fork a branch from a table version without copying data, write and validate ("audit") changes in isolation on the branch, and then **atomically publish** the branch into the main history — or discard it — so that readers of the main table never observe partial or unvalidated data.

The feature is a *writer* feature: readers of the main table only ever see the main commit chain and are unaffected. Reading a branch directly is an opt-in capability for writers/tools that implement the feature.

> During the experimental phase, this feature is exposed under the temporary name `tableBranching-dev`. The `-dev` suffix will be removed once this RFC is accepted and a well-tested production implementation exists. See the [RFC process](https://github.com/delta-io/delta/tree/master/protocol_rfcs).

For further discussions about this protocol change, please refer to the Github issue - https://github.com/delta-io/delta/issues/7439 (see #7439).

--------

# Table Branching
> ***New Section after the [Clustered Table](#clustered-table) section***

A **branch** is a named, isolated commit chain that forks from a specific *base version* of a table's main commit chain and accumulates its own commits independently. Branches are **zero-copy**: creating a branch copies no data files; the branch shares all files that exist at its base version and only writes new files for the data it changes. A branch can be **published** — atomically fast-forwarded into the main chain with ordinary conflict detection — or **discarded**.

This provides two capabilities as one primitive:
- **Write-Audit-Publish (WAP):** write to a branch, run data-quality checks against the branch's fully materialized snapshot (the exact result that would be published), then publish atomically so main-table readers only ever see validated data.
- **Zero-copy experimentation / CI:** cheaply branch a large table to test a migration or backfill, then promote or throw away the result without duplicating data.

Table Branching is defined to be **supported** on a table as follows:
- When the feature `tableBranching` exists in the table `protocol`'s `writerFeatures`, then we say that Table Branching is **supported** and the table may have branches.

Enablement:
- The table must be on Writer Version 7.
- The feature `tableBranching` must exist in the table `protocol`'s `writerFeatures`, and `domainMetadata` must exist in `writerFeatures`.
- Tables that support `tableBranching` should also support [`vacuumProtocolCheck`](#vacuum-protocol-check) so that older writers cannot delete data files that are still referenced by a live branch (see [Branches and VACUUM](#branches-and-vacuum)).

## Branch Storage Layout

The **main chain** is the table's ordinary `_delta_log` and is unchanged by this feature.

Each branch has its own commit chain stored under:

```
_delta_log/_branches/<branchId>/_delta_log/
```

where `<branchId>` is a UUID assigned at branch creation. A branch's `_delta_log` contains Delta files (and, optionally, checkpoints) using the same format as the main chain, with versions numbered **starting at the branch's `baseVersion` + 1**. Reconstructing a branch snapshot at version `v` is done by:
1. computing the main-chain snapshot at `baseVersion`, then
2. applying the branch's Delta files for versions `baseVersion + 1 … v` on top, using ordinary [Action Reconciliation](#action-reconciliation).

Data files written on a branch are stored under the table root (like any other data file) and referenced by relative path from the branch's `add` actions. Because both main and branch reference files by the same table-relative paths, files present at `baseVersion` are shared with zero copying.

## The `delta.tableBranching` Metadata Domain

The set of branches is tracked in a system-controlled [metadata domain](#domain-metadata) named `delta.tableBranching`, committed on the **main** chain. Its `configuration` is a JSON object:

Field Name | Data Type | Description
-|-|-
branches | Array[BranchRef] | The registry of all non-purged branches.

Each `BranchRef`:

Field Name | Data Type | Description
-|-|-
branchId | String | UUID identifying the branch and its `_delta_log/_branches/<branchId>/` directory.
name | String | Human-readable branch name. Must be unique among `open` branches.
baseVersion | Long | Main-chain version the branch forked from.
createdVersion | Long | Main-chain version at which the branch was registered.
status | String | One of `open`, `published`, or `discarded`.
publishedVersion | Long | (status `published` only) The main-chain version that the branch was published as.

Because the registry is a single `domainMetadata` action, [Action Reconciliation](#action-reconciliation) and the [Domain Metadata](#domain-metadata) conflict rules apply: two concurrent main-chain commits that both mutate the branch registry conflict and one must retry. This gives branch create/publish/discard the same optimistic-concurrency guarantees as other domain-backed features.

## Writer Requirements for Table Branching

### Create branch
To create a branch, a writer commits, on the **main** chain, a `domainMetadata` update to `delta.tableBranching` that adds a `BranchRef` with a fresh `branchId`, a unique `name`, `baseVersion` set to a main-chain version that still exists (its files have not been VACUUMed), `createdVersion` set to the committing version, and `status = open`. No data files are written or copied. The writer then initializes `_delta_log/_branches/<branchId>/_delta_log/` (empty; the branch's first snapshot equals the main snapshot at `baseVersion`).

### Write / Audit
Writers commit to a branch by appending Delta files to the branch's `_delta_log` using the normal commit rules (conflict resolution is scoped to the branch's own chain). All table features active at `baseVersion` remain active on the branch and their writer requirements continue to apply (e.g. Row Tracking must keep assigning IDs). Auditing is simply reading the branch snapshot; because the branch materializes the exact result that would be published, quality checks run against precisely that state.

### Publish
Publishing atomically merges an `open` branch into the main chain:
1. The writer computes the set of data/metadata changes represented by the branch relative to `baseVersion` (the branch's net `add`/`remove`/`metaData`/`domainMetadata`/… actions).
2. The writer commits those changes as a **single commit on the main chain** at main's current tip version `t + 1`, performing ordinary conflict detection against every main-chain commit in `(baseVersion, t]` (add/remove conflicts, [Row Tracking](#row-tracking) high-water-mark and domain conflicts, [metadata](#change-metadata) conflicts, etc.). If a conflict is detected, publish fails; the writer may rebase the branch onto a newer base and retry.
3. In the same publishing commit, the writer updates the `delta.tableBranching` registry to set the branch's `status = published` and `publishedVersion = t + 1`.

Publish is all-or-nothing: either the single main-chain commit (data changes + registry update) succeeds, making the branch's result visible atomically, or it fails and main is unchanged. Writers must not make a branch's changes visible on main by any means other than a publishing commit.

### Discard
To discard an `open` branch, a writer commits a `delta.tableBranching` registry update on the main chain setting the branch's `status = discarded`. Data files written exclusively on that branch (not referenced by main or any other live branch) become VACUUM-eligible. The branch's `_delta_log/_branches/<branchId>/` directory may be removed by metadata cleanup.

## Branches and VACUUM

A data file may be referenced by the main chain and/or one or more `open` branches. Before physically deleting a file, VACUUM must consider **all `open` branches** in the `delta.tableBranching` registry (and their retained history) in addition to the main chain, and must not delete a file that is still reachable from any of them within the retention window. Making `tableBranching` require/recommend [`vacuumProtocolCheck`](#vacuum-protocol-check) ensures older VACUUM implementations — which are unaware of branches — cannot delete files that a branch still needs. Similarly, a main-chain commit must not VACUUM away a `baseVersion` that an `open` branch still forks from.

## Reader Requirements for Table Branching

None for reading the **main** table: the main chain is a normal Delta log, and branch state lives in a separate directory plus a system domain that non-supporting readers ignore per [Domain Metadata](#domain-metadata) reader requirements. Reading a branch directly (for audit tooling) requires understanding the [branch storage layout](#branch-storage-layout) and is opt-in.

## Non-Goals

- **Long-lived divergent branches / general three-way merge.** This RFC specifies fast-forward publish with conflict detection (and manual rebase-and-retry on conflict), not automatic content-level merge resolution of conflicting row edits.
- **Cross-table branching.** A branch is scoped to a single table. Coordinated branching across tables can be layered on [Multi-Table Transactions](https://github.com/delta-io/delta/blob/master/protocol_rfcs/accepted/catalog-managed.md) in the future.
- **Branch-level access control.** Governance of who may create/publish branches is a catalog concern, out of scope for the storage protocol.

## Valid Feature Names in Table Features
> ***Change to existing section***

Add the following row to the table of valid writer feature names:

Feature | Name | Readers or Writers?
-|-|-
Table Branching | `tableBranching` | Writers only
