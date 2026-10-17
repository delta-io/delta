# Multi-Table Transactions

**Associated Github issue for discussions: https://github.com/delta-io/delta/issues/7437**

This RFC proposes a new **writer** table feature called **Multi-Table Transactions** (`multiTableTransaction`). It builds directly on [Catalog-Managed Tables](https://github.com/delta-io/delta/blob/master/protocol_rfcs/accepted/catalog-managed.md) (`catalogManaged`) and allows a single logical transaction to atomically commit changes across **multiple** Delta tables: every participating table advances to its new version together, or none of them do.

The [Catalog-Managed Tables RFC](https://github.com/delta-io/delta/blob/master/protocol_rfcs/accepted/catalog-managed.md) identifies this as an explicit motivation — making the catalog the source of truth for commits "opens a clear path to transactions that could span multiple tables … because filesystem-based commits (i.e. using PUT-if-absent) do not admit any way to coordinate with other entities." This RFC specifies the table-side contract that turns that path into a concrete feature.

The feature is a *writer* feature: a committed multi-table transaction looks like an ordinary commit on each participating table, so readers are unaffected and there is no reader-feature break.

> During the experimental phase, this feature is exposed under the temporary name `multiTableTransaction-dev`. The `-dev` suffix will be removed once this RFC is accepted and a well-tested production implementation exists. See the [RFC process](https://github.com/delta-io/delta/tree/master/protocol_rfcs).

For further discussions about this protocol change, please refer to the Github issue - https://github.com/delta-io/delta/issues/7437 (see #7437).

--------

# Multi-Table Transactions
> ***New Section after the [Catalog-Managed Tables](#catalog-managed-tables) section***

A **multi-table transaction** is a set of commits — one per participating table — that must become visible atomically: either every participant's commit is ratified and published, or none is. The feature composes the [Catalog-Managed Tables](#catalog-managed-tables) commit protocol (staged commits + catalog ratification) with a small amount of cross-table bookkeeping and an all-or-nothing ratification rule.

Multi-Table Transactions is defined to be **supported** on a table as follows:
- When the feature `multiTableTransaction` exists in the table `protocol`'s `writerFeatures`, then we say that Multi-Table Transactions is **supported**, and the table may participate in multi-table transactions.

Enablement:
- The table must be on Writer Version 7.
- The feature `catalogManaged` must exist in the table `protocol`'s `readerFeatures` and `writerFeatures` (Multi-Table Transactions requires catalog-managed commits; all of catalog-managed's own requirements apply).
- The feature `multiTableTransaction` must exist in the table `protocol`'s `writerFeatures`.

Every table that participates in a given multi-table transaction must support `multiTableTransaction`. A writer must not include a non–catalog-managed table, or a catalog-managed table that does not support this feature, as a participant.

## Terminology

- **Coordinating catalog.** The single [catalog](#terminology-catalogs) that manages ratification for all participating tables in a transaction. In v1 all participants must be managed by the same coordinating catalog (see [Non-Goals](#non-goals)).
- **Participant.** One (table, proposed version) pair taking part in the transaction. A transaction has two or more participants; a single-participant transaction is equivalent to an ordinary catalog-managed commit.
- **Transaction group.** The full set of participants, identified by a shared **transaction id**.

## The `transactionGroup` Action

A new action, `transactionGroup`, self-describes a staged commit's participation in a multi-table transaction. When present, it must appear in the [staged commit](#staged-commit) file of every participant of the transaction. Its schema:

Field Name | Data Type | Description | optional/required
-|-|-|-
transactionId | String | A globally unique identifier (e.g. a UUID) shared by all participants of the transaction. | required
participants | Array[Participant] | The complete participant set of the transaction (see below). Must be identical in every participant's staged commit. | required
coordinatingCatalog | String | Catalog-specific identifier of the [coordinating catalog](#terminology) responsible for group ratification. | required

Each `Participant` is a struct:

Field Name | Data Type | Description | optional/required
-|-|-|-
tableId | String | The coordinating catalog's stable identifier for the participating table. | required
proposedVersion | Long | The version this participant proposes to commit (the version embedded in its staged commit file name). | required

Because `participants` is identical across all staged commits of the transaction, any participant's staged commit is sufficient to discover the whole group — this is what makes recovery and orphan cleanup possible without a central log.

Example:
```json
{
  "transactionGroup": {
    "transactionId": "b6c03b3a-bca9-4d87-9d3b-dd9b0d95ee47",
    "participants": [
      { "tableId": "cat://sales/fact_orders",     "proposedVersion": 512 },
      { "tableId": "cat://sales/dim_customer",     "proposedVersion": 77 }
    ],
    "coordinatingCatalog": "unity://prod"
  }
}
```

## Commit Protocol for Multi-Table Transactions

A writer executes a multi-table transaction in three phases, reusing the catalog-managed [commit protocol](#commit-protocol):

### 1. Stage
For each participant, the writer resolves conflicts against that table's current version exactly as for an ordinary catalog-managed commit, determines the participant's `proposedVersion`, and writes a [staged commit](#staged-commit) file `_delta_log/_staged_commits/<proposedVersion>.<uuid>.json`. Each staged commit:
- must contain the transaction's `transactionGroup` action with the same `transactionId` and identical `participants` set;
- must contain a `commitInfo` action (as required by catalog-managed) whose `tags` include `delta.multiTableTransaction.transactionId` set to the transaction id, so the association is discoverable from provenance alone.

Staging produces no visible change to any table; a staged commit is not a [ratified commit](#ratified-commit) until the catalog ratifies it.

### 2. Group-ratify
The writer requests the [coordinating catalog](#terminology) to ratify the entire transaction group in a single atomic catalog operation. The catalog must ratify **all** participants' staged commits or **none**:
- The catalog must verify, for each participant, that `proposedVersion` is still the next version for that table (i.e. no other commit has been ratified at that version in the meantime). If any participant's proposed version is stale, group ratification fails and no participant is ratified.
- On success, all participants' staged commits become [ratified commits](#ratified-commit) simultaneously from the catalog's point of view. On failure, none do.

Group ratification is the sole atomicity boundary of the transaction. No filesystem-level cross-file atomicity is required, because — as under catalog-managed — the catalog, not the filesystem, is the source of truth for whether a commit attempt succeeded.

### 3. Publish
After successful group ratification, each participant's ratified staged commit is [published](#publishing-commits) to its table's `_delta_log` as a normal Delta file, exactly as for single-table catalog-managed commits. Publishing is an idempotent, best-effort operation that may be performed by the writer or the catalog and may lag ratification; readers that consult the catalog observe the ratified versions regardless of publish state.

## Isolation and Conflict Handling

- Each participant enforces its own optimistic concurrency control at its `proposedVersion`. If any participant would conflict (its proposed version is no longer the next version, or it loses a Delta-level conflict such as a row-tracking or domain-metadata conflict), the whole transaction fails; the writer must re-stage the failed participant(s) at fresh versions and retry group ratification, or abort.
- The transaction provides **all-or-nothing visibility**: because group ratification is atomic in the catalog, no reader can observe a state in which some but not all participants have advanced.
- The catalog must not ratify two overlapping transaction groups that share a participant table at the same proposed version; standard catalog-managed single-version ratification already guarantees at most one ratified commit per (table, version), and the group-ratify operation must uphold this for every participant.

## Recovery and Cleanup

- **Abandoned transactions.** If a writer stages some participants and then fails before group ratification, the staged commit files are orphans. They are never visible (they were never ratified) and are removed by the same mechanism catalog-managed already defines for un-ratified staged commits during [Metadata Cleanup](#metadata-cleanup) / VACUUM. The `transactionGroup` action lets a cleanup process confirm that a group was never ratified before removing its staged commits.
- **Partial publish.** If group ratification succeeds but publishing is interrupted, any client can complete publishing using the catalog's ratified-version information; the `transactionGroup` participant set identifies all tables that must be published. This is a continuation of catalog-managed's existing publish-recovery behavior and changes no visibility.

## Reader Requirements for Multi-Table Transactions

None. Multi-Table Transactions is a writer feature. A ratified, published transaction is indistinguishable from ordinary catalog-managed commits on each table, so readers need no changes. Readers that encounter a `transactionGroup` action in a commit they read (for provenance/history purposes) may ignore it.

## Non-Goals

- **Cross-catalog transactions.** v1 requires a single coordinating catalog for all participants. Two-phase commit across independent catalogs is a future extension.
- **Non-table catalog updates** in the same transaction (the second half of catalog-managed advantage #2) are a natural extension but are not specified here.
- **Long-running / interactive transactions and cross-table read locks.** Isolation is provided at commit time through per-table OCC plus atomic group ratification; this RFC does not introduce table-spanning read locks or serializable interactive sessions.

## Valid Feature Names in Table Features
> ***Change to existing section***

Add the following row to the table of valid writer feature names:

Feature | Name | Readers or Writers?
-|-|-
Multi-Table Transactions | `multiTableTransaction` | Writers only
