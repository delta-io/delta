# Managed Commits
**Associated Github issue for discussions: https://github.com/delta-io/delta/issues/2598**

This RFC proposes a new table feature `managedCommits` which changes the way Delta Lake manages commits.

Today’s Delta commit protocol relies on the filesystem to provide commit atomicity. This feature request is to allow delta tables which gets commit atomicity using an external commit-owner and not
the filesystem (s3, abfs etc). This allows us to deal with various limitations of delta:

1. No reliable way for the table's owner to participate in commits.
    - The table's owner (such as a catalog) cannot reliably stay in sync with the table state, nor reject commit attempts it wouldn’t like, because it doesn’t even know about writes until they are already durable (and visible to readers).
    - No clear path to transactions that could span multiple tables and/or involve catalog updates, because filesystem commits cannot be made conditionally or atomically.
2. No way to tie commit ownership to a table.
    - In general, Delta tables have no way to advertise that they are managed by catalog or LogStore X (at endpoint Y)
    - There is no central entity that needs to be contacted in order to commit to the table. So if the underlying file system is missing _putIfAbsent_ semantics, then there is no way to ensure that a commit is atomic, which could lead
      to lost writes when concurrent writers are writing to the table.

--------

> ***Change to existing section***

### Delta Log Entries
Delta files are stored as JSON in a directory at the root of the table named `_delta_log`, and together with checkpoints make up the log of all changes that have occurred to a table. They are the unit of atomicity for a table.

**Note:** If [managed commits](#managed-commits) table feature is enabled on the table, recently committed delta files may reside in the `_delta_log/_commits` directory. Delta clients have to contact
the corresponding commit-owner of the table in order to find the information about the [un-backfilled commits](#commit-backfills).

The delta files in `_delta_log` directory are named using the next available version number, zero-padded to 20 digits.

For example:

```
./_delta_log/00000000000000000000.json
```

The delta files in the `_delta_log/_commits` directory on the other hand have a UUID embedded into them and follow the pattern <version>.<uuid>.json where the version corresponds to the next attempt version zero-padded to 20 digits.

For example:

```
./_delta_log/_commits/00000000000000000000.3a0d65cd-4056-49b8-937b-95f9e3ee90e5.json
./_delta_log/_commits/00000000000000000001.7d17ac10-5cc3-401b-bd1a-9c82dd2ea032.json
./_delta_log/_commits/00000000000000000001.016ae953-37a9-438e-8683-9a9a4a79a395.json
./_delta_log/_commits/00000000000000000002.3ae45b72-24e1-865a-a211-34987ae02f2a.json
```

The delta files in `_delta_log/_commits` directory may not be committed and should not be trusted. The [commit-owner](#commit-owner) is the source of truth
for the information around which one of these are valid. Refer to [managed commits](#managed-commits) for more details.

Delta files use new-line delimited JSON format, where every action is stored as a single line JSON document.
A delta file, corresponding to version `n`, contains an atomic set of [_actions_](#Actions) that should be applied to the previous table state, corresponding to `n-1`, in order to construct the `n`th snapshot of the table.
An action changes one aspect of the table's state, for example, adding or removing a file.

> ***New Section***

# Managed Commits

With this feature enabled:
- The file system remains the source of truth for the content of a (proposed) commit.
- The [commit-owner](#commit-owner) becomes the source of truth for whether a given commit succeeded.

The following is a high-level overview of how commits work in a table with managed-commits enabled:

1. Delta client passes the actions that need to be committed to the [commit-owner](#commit-owner).
2. The [commit-owner](#commit-owner) abstracts the commit process and defines the atomicity protocol for
   commits to that table. It writes the actions in a [delta file](#delta-log-entries) and atomically makes
   this file part of the table. Refer to [commit protocol](#commit protocol) section for details around how
   the commit-owner performs commits.
3. In case of no conflict, the [commit-owner](#commit-owner) responds with success to the delta client.
4. Delta clients could contact the commit-owner to get the information about the commits on the delta table.

Essentially the [managed-commits](#managed-commits) table feature defines the overall [commit protocol](#commit-protocol) (e.g. atomicity requirements, backfills, etc), and the
commit-owner is responsible to implement that protocol.

## Commit Owner

A commit-owner is an external entity which manages the commits on a delta table. It could be backed by a database, a file system, or any other storage system. Each commit-owner has its own spec around how they should be contacted and how they do a commit.

## Commit Files

A commit file is a [delta file](#delta-log-entries) that contains the actions that are committed / need to be committed.

There are two types of commit files:
1. **Un-backfilled commit files**: These reside in the `_delta_log/_commits` directory.
    - The filename must follow the pattern: `<version>.<uuid>.json`. Here the `uuid` is a random UUID that is generated for each commit and `version` is the version `v` which is being committed, zero-padded to 20 digits.
    - The commit-owner must track these files until they are backfilled.
    - Mere existence of these files does not mean that the file is a _valid_ commit. It might correspond to a failed or in-progress commit.
      The commit-owner is the source of truth around which un-backfilled commits are valid.

2. **Backfilled commit files**: These reside in the `_delta_log` directory.
    - The filename must follow the pattern: `<version>.json`. Here the `version` is the version `v` which is being committed, zero-padded to 20 digits.
    - The existence of a `<version>.json` file proves that the corresponding version `v` is committed, even for managed-commit tables. Filesystem based Delta clients can use filesystem listing operations to directly discover such commits.

Without [managed-commits](#managed-commits), a delta client must always write backfilled commit files directly, relying on filesystem atomicity
to prevent lost writes when multiple writers attempt to commit the same version at the same time.

With [managed-commits](#managed-commits), the delta client asks the [commit-owner](#commit-owner) to commit the version `v` and the commit-owner
decides whether to write the backfilled/un-backfilled commit file based on the [managed commit protocol](#commit-protocol).

## Commit Owner API

When managed commits are enabled, a `commit-owner` manages commits to the table on behalf of the Delta client. A commit-owner always has a client-side component (which the Delta client interacts with directly). It may also
involve a server-side component (which the client-side component would be responsible to communicate with). The Delta client is responsible to define the client-side API that commit-owners should target, and commit-owners
are responsible to define the commit atomicity and backfill protocols which the commit-owner client should implement.

At a high level, the `commit-owner` needs to provide:
- API to atomically commit a version `x` with given set of `actions`. This is explained in detail in the [commit protocol](#commit-protocol) section.
- API to retrieve information about the recent commits on the table. This is explained in detail in the [getting un-backfilled commits from commit-owner](#getting-un-backfilled-commits-from-commit-owner) section.

### Commit Protocol

When a `commit-owner` receives a request to commit version `v`, it must first verify that the previous version `v-1` already exists, and that version `v` does not yet exist. It then has following choices to publish the commit:
1. Write the actions to an 'un-backfilled' [commit file](#commit-files) in the `_delta_log/_commits` directory, and **atomically** record that the new file now corresponds to version `v`.
2. Atomically write a backfilled [commit file](#commit-files) in the `_delta_log` directory. Note that the commit will be considered to have succeeded as soon as the file becomes visible to
   other clients in the filesystem, regardless of when or whether the originating client receives a response.
    - A commit-owner must not write a backfilled commit until the previous commit has been backfilled.

The commit-owner must track the un-backfilled commits until they are [backfilled](#commit-backfills).

### Getting Un-backfilled Commits from Commit Owner

Even after a commit succeeds, Delta clients can only discover the commit through filesystem operations if the commit is [backfilled](#backfills). If the commit is not backfilled, then delta implementations
have no way to determine which file in `_delta_log/_commits` directory corresponds to the actual commit `v`.

The commit-owner is responsible to implement an API (defined by the Delta client) that Delta clients can use to retrieve information about un-backfilled commits maintained
by the commit-owner. Delta clients who are unaware of the commit-owner (or unwilling to talk to it), will fall back to working only with backfilled commits and may encounter stale reads.

## Sample Commit Owner API

The following is an example of a possible commit-owner API which some Java-based Delta client might require commit-owner implementations to target:

```java

interface CommitStore {
    /**
     * Commits the given set of `actions` to the given commit `version`.
     *
     * @param version The version we want to commit.
     * @param actions Actions that need to be committed.
     *
     * returns CommitResponse which has details around the new committed delta file.
     */
    def commit(
        version: Long,
        actions: Iterator[String]): CommitResponse

    /**
     * API to get the un-backfilled commits for the table represented by the given `tablePath` after
     * the given `startVersion`. The returned commits are contiguous and in ascending version order.
     * Note that the first version returned by this API may not be equal to the `startVersion`. This
     * happens when few versions starting from `startVersion` are already backfilled and so
     * CommitStore may have stopped tracking them.
     *
     * @return a list of `Commit` which are tracked by commit-owner.
     *
     */
    def getCommits(startVersion: Long): Seq[Commit]

    /**
     * API to ask the commit-owner to backfill all commits <= given `version`.
     */
    def backfillToVersion(version: Long): Unit
}
```

## Commit Backfills
Backfilling is the process of copying the un-backfilled commits i.e. `_delta_log/_commits/<version>.<uuid>.json` to `_delta_log/<version>.json`.
With the help of backfilling, the [delta files](#delta-log-entries) are visible even to the filesystem based Delta clients that do not
understand `managed-commits`. Backfill also allows the commit-owner to reduce the number of commits it must track internally.

Backfill must be sequential. In other words, a commit-owner must ensure that backfill of commit `v-1` is complete before initiating backfill of commit 'v'.

`commit-owner`s are encouraged to backfill the commits frequently. This has several advantages:
1. Filesystem-based Delta implementations may only understand backfilled commits, and frequent backfill allows them to access the most recent table snapshots.
2. Frequent backfilling minimizes the chance of data loss in case the `commit-owner` loses its state due to some failover/issue.
3. Some maintenance operations (such as checkpoints, log compaction, and metadata cleanup) can be performed only on the backfilled part of the table. Refer to the [Maintenance operations on managed-commit tables](#maintenance-operations-on-managed-commit-tables) section for more details.

The commit-owner may also expose an API to backfill the commits. This will allow clients to ask the commit-owner to backfill the commits if needed in order to do some maintenance operations.

## Converting an existing filesystem based table to managed-commit table
In order for a commit-owner to successfully take over an existing filesystem-based Delta table, the following invariants must hold:
- The commit-owner must agree to take ownership of the table, by ratifying a proposed commit that would install it. This essentially follows the normal commit protocol, except…
- The commit-owner and client must both recognize that the ownership change only officially takes effect when the ownership-change is successfully backfilled. Unlike the backfill of a normal commit, this ownership-change backfill must
  be atomic because it is also a filesystem-based commit that potentially races with other filesystem-based commit attempts.

Assuming the client follows the commit-owner’s protocol for ownership changes, the commit-owner MUST NOT refuse ownership after the backfill succeeds. Otherwise, the table would become permanently unusable, because the advertised commit-owner refuses
to ratify the very commits that would repair the table by removing that commit-owner.

Thus, the commit-owner and client effectively perform a two-phase commit, where the commit-owner persists its commitment to own the table, and the actual commit point is the PUT-if-absent.
Notifying the commit-owner that backfill has completed becomes a post-commit cleanup operation. If the put-if-absent fails (because somebody else gets there first), the commit-owner forgets
about the proposed ownership change.

Once the backfill succeeds, clients will start contacting the commit-owner for any further commits. Meanwhile, any clients who were already attempting filesystem-based commits will encounter
a physical conflict, see the protocol change, and either abort the commit or route it to the new owner.

## Creating a new managed-commit table

Conceptually, creating a new managed-commit table is very similar to proposing an ownership change of an existing filesystem-based table that happens to not yet contain any commits. This means that, until commit 0
has been backfilled, there is a risk of multiple clients racing to create the same table with different commit-owners (or to create a filesystem-based table).

To avoid such races, Commit-owners are encouraged to use a put-if-absent API (if available) to write the backfilled commit directly (i.e. `_delta_log/00000000000000000000.json`).
If such put-if-absent is not available, then it is the responsibility of commit-owners to take whatever measures they deem appropriate to avoid or respond to such races.

## Converting a managed-commit table to filesystem table

In order to convert a managed-commit table to a filesystem-based table, the delta client needs to initiate a commit which tries to remove the commit-owner information
from [change-metadata](#change-metadata) and also removes the table feature from the [protocol](#protocol-evolution) action. The commit-owner is not required to give
up ownership, and may reject the request. If it chooses to honor such a request, it has to ensure the following:

1. It must ensure that all prior commit files are backfilled.
2. It must not accept any new commits on the table.
3. Write the commit which removes the ownership.
    - Either the commit-owner writes the backfilled commit file directly.
    - Or it writes an unbackfilled commit and ensures that it is backfilled reliably. Until the backfill is done, table will be in unusable state:
        - the filesystem based delta clients won't be able to write to such table as they still believe that table has managed-commit enabled.
        - the managed-commit aware delta clients won't be able to write to such table as the commit-owner won't accept any new
          commits. In such a scenario, they could backfill required commit themselves (preferably using PUT-if-absent) to unblock themselves.

## Reading managed-commit tables

With `managed-commits` enabled, a table could have some part of table already backfilled and some part of the table yet-to-be-backfilled.
The precise information about what are the valid un-backfilled commits is maintained by the commit-owner.

E.g.
```
_delta_log/00000000000000000000.json
_delta_log/00000000000000000001.json
_delta_log/00000000000000000002.json
_delta_log/00000000000000000002.checkpoint.parquet
_delta_log/00000000000000000003.json
_delta_log/00000000000000000003.00000000000000000005.compacted.json
_delta_log/00000000000000000004.json
_delta_log/00000000000000000005.json
_delta_log/00000000000000000006.json
_delta_log/00000000000000000007.json
_delta_log/_commits/00000000000000000006.3a0d65cd-4056-49b8-937b-95f9e3ee90e5.json
_delta_log/_commits/00000000000000000007.016ae953-37a9-438e-8683-9a9a4a79a395.json
_delta_log/_commits/00000000000000000008.7d17ac10-5cc3-401b-bd1a-9c82dd2ea032.json
_delta_log/_commits/00000000000000000008.b91807ba-fe18-488c-a15e-c4807dbd2174.json
_delta_log/_commits/00000000000000000009.41bf693a-f5b9-4478-9434-af7475d5a9f0.json
_delta_log/_commits/00000000000000000010.0f707846-cd18-4e01-b40e-84ee0ae987b0.json
_delta_log/_commits/00000000000000000010.7a980438-cb67-4b89-82d2-86f73239b6d6.json
```

Let say the commit-owner is tracking:
```
{
  6  -> "00000000000000000006.3a0d65cd-4056-49b8-937b-95f9e3ee90e5.json",
  7  -> "00000000000000000007.016ae953-37a9-438e-8683-9a9a4a79a395.json",
  8  -> "00000000000000000008.7d17ac10-5cc3-401b-bd1a-9c82dd2ea032.json",
  9  -> "00000000000000000009.41bf693a-f5b9-4478-9434-af7475d5a9f0.json"
}
```

Delta clients have two choices to read such tables:
1. A non-managed-commit aware delta implementation can read such table by listing the `_delta_log` directory and reading the delta/checkpoint/log-compaction files.
   They cannot use the un-backfilled commits in `_delta_log/_commits` directory as they are not aware of the external commit-owner. Because of this, they may see a stale snapshot
   of the table if the recent commits are not backfilled.
    - In the above example, such delta implementation will see version 7 as the latest snapshot.
2. A client that understands managed commits can supplement the file listing result by contacting the commit-owner for information about un-backfilled commits, in order to get the most
   recent snapshot of the table.
    - In the above example, a delta implementation could get information about versions 0 through 7 from _delta_log directory and get information about un-backfilled commits (v8, v9) from the commit-owner.

## Maintenance operations on managed-commit tables

Maintenance operations such as [checkpoints](#checkpoints-1), [log compaction](#log-compaction-files), [metadata cleanup](#metadata-cleanup) can be performed only on the backfilled part of a managed-commit table.

- A [checkpoint](#checkpoints-1) for a version `v` can be created only if the commit `v` is already backfilled i.e. `_delta_log/<v>.json` exists.
- A [log compaction](#log-compaction-files) can be done only on the backfilled part of the table. The log compaction file `<x>.<y>.compacted.json` can be created only if commit `y` is already backfilled i.e. `_delta_log/<y>.json` exists.
- A [Metadata cleanup](#metadata-cleanup) must always preserve the newest k >= 1 backfilled commits. In other words, [metadata cleanup](#metadata-cleanup) must always choose a backfilled `cutOffCheckpoint`.
    - This allows the table to remain discoverable by the filesystem-based delta readers even after metadata cleanup deletes everything before commit version `cutOffCheckpoint`.

## Managed Commit Enablement

The managed-commit feature is supported and active when:
- The table must be on Writer Version 7.
- The table has a `protocol` action with `writerFeatures` containing the feature `managedCommits`.
- The table has a metadata property `delta.managedCommitOwnerName` in the [change-metadata](#change-metadata)'s configuration.
- The table may have a metadata property `delta.managedCommitOwnerConf` in the [change-metadata](#change-metadata)'s configuration. The value of this property is a json-coded string-to-string map.
    - A commit-owner can store additional information (e.g. configuration information such as service endpoints) in this field, for use by the commit-owner client (it is opaque to the Delta client).

Note that a table is in invalid state if the change-metadata contains the `delta.managedCommitOwnerName` property but the table does not have the `managedCommits` feature in the `protocol` action or vice versa.

E.g.
```json
{
  "metaData":{
    "id":"af23c9d7-fff1-4a5a-a2c8-55c59bd782aa",
    "format":{"provider":"parquet","options":{}},
    "schemaString":"...",
    "partitionColumns":[],
    "configuration":{
      "appendOnly": "true",
      "delta.managedCommitOwnerName": "commit-owner-1",
      "delta.managedCommitOwnerConf": "{\"batchSize\":\"10\",\"endpoint\":\"http://sample-url.com/commit\"}"
    }
  }
}
```

## Writer Requirements for Managed Commits

When supported and active:
- Require that `inCommitTimestamp` table feature must be supported and active.
- Writer must do commits via the commit-owner's commit protocol and it must not write the backfilled commit files directly.
- Writer must not write checkpoints or log compaction files for the commits that are not backfilled.
- Metadata cleanup must always preserve the newest k >= 1 backfilled commits.

## Reader Requirements for Managed Commits
Managed commits is a writer feature. So it doesn't put any restrictions on the reader.

- Filesystem-based delta readers which do not understand [managed commits](#managed-commits) may only
  be able to read the backfilled commits. They may see a stale snapshot of the table if the recent commits are not backfilled.

- The [managed commits](#managed-commits) aware delta readers could additionally contact the commit-owner to
  get the information about the recent un-backfilled commits. This allows them to get the most recent snapshot of the table.
