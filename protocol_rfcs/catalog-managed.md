# Catalog-Managed Tables
**Associated Github issue for discussions: https://github.com/delta-io/delta/issues/4381**

This RFC proposes a new reader-writer table feature `catalogManaged` which changes the way Delta Lake
discovers and accesses tables.

Today’s Delta protocol relies entirely on the filesystem for read-time discovery as well as
write-time commit atomicity. This feature request is to allow catalog-managed Delta tables whose
discovery and commits go through the table's managing catalog instead of going directly to the
filesystem (s3, abfs, etc). In particular, the catalog becomes the source of truth about whether a
given commit attempt succeeded or not, instead of relying exclusively on filesystem PUT-if-absent
primitives.

Making the catalog the source of truth for commits to a table brings several important advantages:

1. Allows the catalog to broker all commits to the tables it manages, and to reject filesystem-based
   commits that would bypass the catalog. Otherwise, the catalog cannot reliably stay in sync with
   the table state, nor can it reject invalid commits, because it doesn’t even know about writes
   until they are already durable and visible to readers. For instance, a catalog might want to block
   low-privilege writers from modifying table metadata (e.g. schema, table features, or table
   properties) while still allowing normal reads and writes. Similarly, if a column is referenced by
   a foreign key, the catalog might want to prevent dropping its NOT NULL constraint.

2. Opens a clear path to transactions that could span multiple tables and/or involve non-table
   catalog updates. Otherwise, the catalog cannot participate in commit at all, because
   filesystem-based commits (i.e. using PUT-if-absent) do not admit any way to coordinate with other
   entities.

3. Allows the catalog to facilitate efficient writes of the table, e.g. by directly hosting the
   content of small commits instead of forcing clients to write them to cloud storage first. Otherwise,
   the catalog is not a source of truth, and at best it can only mirror stale copies of table state.

4. Allows the catalog to facilitate efficient reads of the table. Examples include vending storage
   credentials, as well as serving up the content of small commits and/or table state such as [version
   checksum file](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#version-checksum-file), so
   that clients do not have to read those files from cloud storage.

5. Allows the catalog to be the authoritative source of the latest table version, no longer
   requiring Delta clients to LIST the `_delta_log` to discover it. This saves time and can also
   allow implementations of Delta on file systems where LIST is not ordered, such as S3 Express One
   Zone.

6. Allows the catalog to trigger followup actions based on a commit, such as VACUUMing, data layout
   optimizations, automatic UniForm conversions, or triggering arbitrary listeners such as downstream
   ETL or streaming pipelines.

--------

# Changes to existing sections

### Delta Log Entries

> ***Change to [existing section](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#delta-log-entries)***

<ins>Delta Log Entries, also known as Delta files</ins>, are JSON files stored in the `_delta_log`
directory at the root of the table. Together with checkpoints, they make up the log of all changes
that have occurred to a table. Delta files are the unit of atomicity for a table, and are named
using the next available version number, zero-padded to 20 digits.

For example:

```
./_delta_log/00000000000000000000.json
```

<ins>Delta files use newline-delimited JSON format, where every action is stored as a single-line
JSON document. A Delta file, corresponding to version `v`, contains an atomic set of
[_actions_](#Actions) that should be applied to the previous table state corresponding to version
`v-1`, in order to construct the `v`th snapshot of the table. An action changes one aspect of the
table's state, for example, adding or removing a file.</ins>

<ins>**Note:** If the [`catalogManaged` table feature](#catalog-managed-tables) is enabled on the table,
recently [ratified commits](#ratified-commit) may not yet be published to the `_delta_log` directory as normal Delta
files - they may be stored directly by the catalog or reside in the `_delta_log/_staged_commits`
directory. Delta clients must contact the table's managing catalog in order to find the information
about these [ratified, potentially-unpublished commits](#publishing-commits).</ins>

<ins>The `_delta_log/_staged_commits` directory is the staging area for [staged](#staged-commit)
commits. Delta files in this directory have a UUID embedded into them and follow the pattern
`<version>.<uuid>.json`, where the version corresponds to the proposed commit version, zero-padded
to 20 digits.</ins>

<ins>For example:</ins>

```
./_delta_log/_staged_commits/00000000000000000000.3a0d65cd-4056-49b8-937b-95f9e3ee90e5.json
./_delta_log/_staged_commits/00000000000000000001.7d17ac10-5cc3-401b-bd1a-9c82dd2ea032.json
./_delta_log/_staged_commits/00000000000000000001.016ae953-37a9-438e-8683-9a9a4a79a395.json
./_delta_log/_staged_commits/00000000000000000002.3ae45b72-24e1-865a-a211-34987ae02f2a.json
```

<ins>NOTE: The (proposed) version number of a staged commit is authoritative - file
`00000000000000000100.<uuid>.json` always corresponds to a commit attempt for version 100. Besides
simplifying implementations, it also acknowledges the fact that commit files cannot safely be reused
for multiple commit attempts. For example, resolving conflicts in a table with [row
tracking](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#row-tracking) enabled requires
rewriting all file actions to update their `baseRowId` field.</ins>

<ins>The [catalog](#terminology-catalogs) is the source of truth about which staged commit files in
the `_delta_log/_staged_commits` directory correspond to ratified versions, and Delta clients should
not attempt to directly interpret the contents of that directory. Refer to
[catalog-managed tables](#catalog-managed-tables) for more details.</ins>

~~Delta files use new-line delimited JSON format, where every action is stored as a single line JSON
document. A delta file, `n.json`, contains an atomic set of [_actions_](#actions) that should be
applied to the previous table state, `n-1.json`, in order to construct the `n`th snapshot of the
table. An action changes one aspect of the table's state, for example, adding or removing a file.~~ 

### Commit Provenance Information

> ***Change to [existing section](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#commit-provenance-information)***

<ins>When the `catalogManaged` table feature is enabled, the `commitInfo` action must have a field
`txnId` that stores a unique transaction identifier string.</ins>

### Metadata Cleanup

> ***Change to [existing section](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#metadata-cleanup)***

2. Identify the newest checkpoint that is not newer than the `cutOffCommit`. A checkpoint at the
   `cutOffCommit` is ideal, but an older one will do. Let's call it `cutOffCheckpoint`. We need to
   preserve the `cutOffCheckpoint` and all <ins>published</ins> commits after it, because we need
   them to enable time travel for commits between `cutOffCheckpoint` and the next available
   checkpoint.
    - <ins>If no `cutOffCheckpoint` can be found, do not proceed with metadata cleanup as there is
      nothing to cleanup.</ins>
3. Delete all [delta log entries](#delta-log-entries), [checkpoint files](#checkpoints), <ins>and
   [version checksum files](#version-checksum-file)</ins> before the `cutOffCheckpoint` checkpoint. Also delete all the [log compaction files](#log-compaction-files)
   having startVersion <= `cutOffCheckpoint`'s version.
    - <ins>Also delete all the [staged commit files](#staged-commit) having version <=
      `cutOffCheckpoint`'s version from the `_delta_log/_staged_commits` directory.</ins>

--------

> ***The next set of sections will be added to the existing spec just before [Iceberg Compatibility V1](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#iceberg-compatibility-v1) section***

# Catalog-managed tables

With this feature enabled, the [catalog](#terminology-catalogs) that manages the table becomes the
source of truth for whether a given commit attempt succeeded.

The table feature defines the parts of the [commit protocol](#commit-protocol) that directly impact
the Delta table (e.g. atomicity requirements, publishing, etc). The Delta client and catalog
together are responsible for implementing the Delta-specific aspects of commit as defined by this
spec, but are otherwise free to define their own APIs and protocols for communication with each
other.

**NOTE**: Filesystem-based access to catalog-managed tables is not supported. Delta clients are
expected to discover and access catalog-managed tables through the managing catalog, not by direct
listing in the filesystem. This feature is primarily designed to warn filesystem-based readers that
might attempt to access a catalog-managed table's storage location without going through the catalog
first, and to block filesystem-based writers who could otherwise corrupt both the table and the
catalog by failing to commit through the catalog.

Before we can go into details of this protocol feature, we must first align our terminology.

## Terminology: Commits

A commit is a set of [actions](#actions) that transform a Delta table from version `v - 1` to `v`.
It contains the same kind of content as is stored in a [Delta file](#delta-log-entries).

A commit may be stored in the file system as a Delta file - either _published_ or _staged_ - or
stored _inline_ in the managing catalog, using whatever format the catalog prefers.

There are several types of commits:

1. **Proposed commit**:  A commit that a Delta client has proposed for the next version of the
   table. It could be _staged_ or _inline_. It will either become _ratified_ or be rejected.

2. <a name="staged-commit">**Staged commit**</a>: A commit that is written to disk at
   `_delta_log/_staged_commits/<v>.<uuid>.json`. It has the same content and format as a published
   Delta file.
    - Here, the `uuid` is a random UUID that is generated for each commit and `v` is the version
      which is proposed to be committed, zero-padded to 20 digits.
    - The mere existence of a staged commit does not mean that the file has been ratified or even
      proposed. It might correspond to a failed or in-progress commit attempt.
    - The catalog is the source of truth around which staged commits are ratified.
    - The catalog stores only the location, not the content, of a staged (and ratified) commit.

3. <a name="inline-commit">**Inline commit**</a>: A proposed commit that is not written to disk but
   rather has its content sent to the catalog for the catalog to store directly.

4. <a name="ratified-commit">**Ratified commit**</a>: A proposed commit that a catalog has
   determined has won the commit at the desired version of the table.
    - The catalog must store ratified commits (that is, the staged commit's location or the inline
      commit's content) until they are published to the `_delta_log` directory.
    - A ratified commit may or may not yet be published.
    - A ratified commit may or may not even be stored by the catalog at all - the catalog may
      have just atomically published it to the filesystem directly, relying on PUT-if-absent
      primitives to facilitate the ratification and publication all in one step.

5. <a name="published-commit">**Published commit**</a>: A ratified commit that has been copied into
   the `_delta_log` as a normal Delta file, i.e. `_delta_log/<v>.json`.
    - Here, the `v` is the version which is being committed, zero-padded to 20 digits.
    - The existence of a `<v>.json` file proves that the corresponding version `v` is ratified,
      regardless of whether the table is catalog-managed or filesystem-based. The catalog is allowed
      to return information about published commits, but Delta clients can also use filesystem
      listing operations to directly discover them.
    - Published commits do not need to be stored by the catalog.

## Terminology: Delta Client

This is the component that implements support for reading and writing Delta tables, and implements
the logic required by the `catalogManaged` table feature. Among other things, it
- triggers the filesystem listing, if needed, to discover published commits
- generates the commit content (the set of [actions](#actions))
- works together with the query engine to trigger the commit process and invoke the client-side
  catalog component with the commit content

The Delta client is also responsible for defining the client-side API that catalogs should target.
That is, there must be _some_ API that the [catalog client](#catalog-client) can use to communicate
to the Delta client the subset of catalog-managed information that the Delta client cares about.
This protocol feature is concerned with what information Delta cares about, but leaves to Delta
clients the design of the API they use to obtain that information from catalog clients.

## Terminology: Catalogs

1. **Catalog**: A catalog is an entity which manages a Delta table, including its creation, writes,
   reads, and eventual deletion.
    - It could be backed by a database, a filesystem, or any other persistence mechanism.
    - Each catalog has its own spec around how catalog clients should interact with them, and how
      they perform a commit.

2. <a name="catalog-client">**Catalog Client**</a>: The catalog always has a client-side component
   which the Delta client interacts with directly. This client-side component has two primary
   responsibilities:
    - implement any client-side catalog-specific logic (such as staging or
      [publishing](#publishing-commits) commits)
    - communicate with the Catalog Server, if any

3. **Catalog Server**: The catalog may also involve a server-side component which the client-side
   component would be responsible to communicate with.
    - This server is responsible for coordinating commits and potentially persisting table metadata
      and enforcing authorization policies.
    - Not all catalogs require a server; some may be entirely client-side, e.g. filesystem-backed
      catalogs, or they may make use of a generic database server and implement all of the catalog's
      business logic client-side.

**NOTE**: This specification outlines the responsibilities and actions that catalogs must implement.
This spec does its best not to assume any specific catalog _implementation_, though it does call out
likely client-side and server-side responsibilities. Nonetheless, what a given catalog does
client-side or server-side is up to each catalog implementation to decide for itself.

## Catalog Responsibilities

When the `catalogManaged` table feature is enabled, a catalog performs commits to the table on behalf
of the Delta client.

As stated above, the Delta spec does not mandate any particular client-server design or API for
catalogs that manage Delta tables. However, the catalog does need to provide certain capabilities
for reading and writing Delta tables:

- Atomically commit a version `v` with a given set of `actions`. This is explained in detail in the
  [commit protocol](#commit-protocol) section.
- Retrieve information about recent ratified commits and the latest ratified version on the table.
  This is explained in detail in the [Getting Ratified Commits from the Catalog](#getting-ratified-commits-from-the-catalog) section.
- Though not required, it is encouraged that catalogs also return the latest table-level metadata,
  such as the latest Protocol and Metadata actions, for the table. This can provide significant
  performance advantages to conforming Delta clients, who may forgo log replay and instead trust
  the information provided by the catalog during query planning.

## Reading Catalog-managed Tables

A catalog-managed table can have a mix of (a) published and (b) ratified but non-published commits.
The catalog is the source of truth for ratified commits. Also recall that ratified commits can be
[staged commits](#staged-commit) that are persisted to the `_delta_log/_staged_commits` directory,
or [inline commits](#inline-commit) whose content the catalog stores directly.

For example, suppose the `_delta_log` directory contains the following files:

```
00000000000000000000.json
00000000000000000001.json
00000000000000000002.checkpoint.parquet
00000000000000000002.json
00000000000000000003.00000000000000000005.compacted.json
00000000000000000003.json
00000000000000000004.json
00000000000000000005.json
00000000000000000006.json
00000000000000000007.json
_staged_commits/00000000000000000007.016ae953-37a9-438e-8683-9a9a4a79a395.json // ratified and published
_staged_commits/00000000000000000008.7d17ac10-5cc3-401b-bd1a-9c82dd2ea032.json // ratified
_staged_commits/00000000000000000008.b91807ba-fe18-488c-a15e-c4807dbd2174.json // rejected
_staged_commits/00000000000000000010.0f707846-cd18-4e01-b40e-84ee0ae987b0.json // not yet ratified
_staged_commits/00000000000000000010.7a980438-cb67-4b89-82d2-86f73239b6d6.json // partial file
```

Further, suppose the catalog stores the following ratified commits:
```
{
  7  -> "00000000000000000007.016ae953-37a9-438e-8683-9a9a4a79a395.json",
  8  -> "00000000000000000008.7d17ac10-5cc3-401b-bd1a-9c82dd2ea032.json",
  9  -> <inline commit: content stored by the catalog directly>
}
```

Some things to note are:
- the catalog isn't aware that commit 7 was already published - perhaps the response from the
  filesystem was dropped
- commit 9 is an inline commit
- neither of the two staged commits for version 10 have been ratified

To read such tables, Delta clients must first contact the catalog to get the ratified commits. This
informs the Delta client of commits [7, 9] as well as the latest ratified version, 9.

If this information is insufficient to construct a complete snapshot of the table, Delta clients
must LIST the `_delta_log` directory to get information about the published commits. For commits
that are both returned by the catalog and already published, Delta clients must treat the catalog's
version as authoritative and read the commit returned by the catalog. Additionally, Delta clients
must ignore any files with versions greater than the latest ratified commit version returned by the
catalog.

Combining these two sets of files and commits enables Delta clients to generate a snapshot at the
latest version of the table.

**NOTE**: This spec prescribes the _minimum_ required interactions between Delta clients and
catalogs for commits. Catalogs may very well expose APIs and work with Delta clients to be
informed of other non-commit [file types](#file-types), such as checkpoint, log
compaction, and version checksum files. This would allow catalogs to return additional
information to Delta clients during query and scan planning, potentially allowing Delta
clients to avoid LISTing the filesystem altogether.

## Commit Protocol

To start, Delta Clients send the desired actions to be committed to the client-side component of the
catalog.

This component then has several options for proposing, ratifying, and publishing the commit,
detailed below.

- Option 1: Write the actions (likely client-side) to a [staged commit file](#staged-commit) in the
  `_delta_log/_staged_commits` directory and then ratify the staged commit (likely server-side) by
  atomically recording (in persistent storage of some kind) that the file corresponds to version `v`.
- Option 2: Treat this as an [inline commit](#inline-commit) (i.e. likely that the client-side
  component sends the contents to the server-side component) and atomically record (in persistent
  storage of some kind) the content of the commit as version `v` of the table.
- Option 3: Catalog implementations that use PUT-if-absent (client- or server-side) can ratify and
  publish all-in-one by atomically writing a [published commit file](#published-commit)
  in the `_delta_log` directory. Note that this commit will be considered to have succeeded as soon
  as the file becomes visible in the filesystem, regardless of when or whether the catalog is made
  aware of the successful publish. The catalog does not need to store these files.

A catalog must not ratify version `v` until it has ratified version `v - 1`, and it must ratify
version `v` at most once.

The catalog must store both flavors of ratified commits (staged or inline) and make them available
to readers until they are [published](#publishing-commits).

For performance reasons, Delta clients are encouraged to establish an API contract where the catalog
provides the latest ratified commit information whenever a commit fails due to version conflict.

## Getting Ratified Commits from the Catalog

Even after a commit is ratified, it is not discoverable through filesystem operations until it is
[published](#publishing-commits).

The catalog-client is responsible to implement an API (defined by the Delta client) that Delta clients can
use to retrieve the latest ratified commit version (authoritative), as well as the set of ratified
commits the catalog is still storing for the table. If some commits needed to complete the snapshot
are not stored by the catalog, as they are already published, Delta clients can issue a filesystem
LIST operation to retrieve them.

Delta clients must establish an API contract where the catalog provides ratified commit information
as part of the standard table resolution process performed at query planning time.

## Publishing Commits

Publishing is the process of copying the ratified commit with version `<v>` to
`_delta_log/<v>.json`. The ratified commit may be a staged commit located in
`_delta_log/_staged_commits/<v>.<uuid>.json`, or it may be an inline commit whose content the
catalog stores itself. Because the content of a ratified commit is immutable, it does not matter
whether the client-side, server-side, or both catalog components initiate publishing.

Implementations are strongly encouraged to publish commits promptly. This reduces the number of
commits the catalog needs to store internally (and serve up to readers).

Commits must be published _in order_. That is, version `v - 1` must be published _before_ version
`v`.

**NOTE**: Because commit publishing can happen at any time after the commit succeeds, the file
modification timestamp of the published file will not accurately reflect the original commit time.
For this reason, catalog-managed tables must use [in-commit-timestamps](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#in-commit-timestamps)
to ensure stability of time travel reads. Refer to [Writer Requirements for Catalog-managed Tables](#writer-requirements-for-catalog-managed-tables)
section for more details.

## Maintenance Operations on Catalog-managed Tables

[Checkpoints](#checkpoints-1) and [Log Compaction Files](#log-compaction-files) can only be created
for versions that are already published in the `_delta_log`. In other words, in order to checkpoint
version `v` or produce a log compaction file for commit range `x <= v <= y`, `_delta_log/<v>.json`
must exist.

Notably, the [Version Checksum File](#version-checksum-file) for version `v` _can_ be created in the
`_delta_log` even if the commit for version `v` is not published.

Delta clients must also honor any _additional_ maintenance policies defined by the managing catalog.
Those policies apply to the maintenance operations listed above (checkpoints, log compaction files,
and version checksum files) and to other operations like [Metadata Cleanup](#metadata-cleanup) and
VACUUM. Consequently, Delta clients must consult the catalog before running any maintenance job.

## Creating and Dropping Catalog-managed Tables

The catalog and query engine ultimately dictate how to create and drop catalog-managed tables.

As one example, table creation often works in three phases:

1. An initial catalog operation to obtain a unique storage location which serves as an unnamed
   "staging" table
2. A table operation that physically initializes a new `catalogManaged`-enabled table at the staging
   location.
3. A final catalog operation that registers the new table with its intended name.

Delta clients would primarily be involved with the second step, but an implementation could choose
to combine the second and third steps so that a single catalog call registers the table as part of
the table's first commit.

As another example, dropping a table can be as simple as removing its name from the catalog (a "soft
delete"), followed at some later point by a "hard delete" that physically purges the data. The Delta
client would not be involved at all in this process, because no commits are made to the table.

## Catalog-managed Table Enablement

The `catalogManaged` table feature is supported and active when:
- The table is on Reader Version 3 and Writer Version 7.
- The table has a `protocol` action with `readerFeatures` and `writerFeatures` both containing the
  feature `catalogManaged`.

## Writer Requirements for Catalog-managed tables

When supported and active:

- Writers must discover and access the table using catalog calls, which happens _before_ the table's
  protocol is known. See [Table Discovery](#table-discovery) for more details.
- The [in-commit-timestamps](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#in-commit-timestamps)
  table feature must be supported and active.
- The `commitInfo` action must also contain a field `txnId` that stores a unique transaction
  identifier string
- Writers must follow the catalog's [commit protocol](#commit-protocol) and must not perform
  ordinary filesystem-based commits against the table.
- Writers must follow the catalog's [maintenance operation protocol](#maintenance-operations-on-catalog-managed-tables)

## Reader Requirements for Catalog-managed tables

When supported and active:

- Readers must discover the table using catalog calls, which happens before the table's protocol
  is known. See [Table Discovery](#table-discovery) for more details.
- Readers must contact the catalog for information about unpublished ratified commits.
- Readers must follow the rules described in the [Reading Catalog-managed Tables](#reading-catalog-managed-tables)
  section above. Notably
  - If the catalog said `v` is the latest version, clients must ignore any later versions that may
    have been published
  - When the catalog returns a ratified commit for version `v`, readers must use that
    catalog-supplied commit and ignore any published Delta file for version `v` that might also be
    present.

## Table Discovery

The requirements above state that readers and writers must discover and access the table using
catalog calls, which occurs _before_ the table's protocol is known. This raises an important
question: how can a client discover a `catalogManaged` Delta table without first knowing that it
_is_, in fact, `catalogManaged` (according to the protocol)?

To solve this, first note that, in practice, catalog-integrated engines already ask the catalog to
resolve a table name to its storage location during the name resolution step. This protocol
therefore encourages that the same name resolution step also indicate whether the table is
catalog-managed. Surfacing this at the very moment the catalog returns the path imposes no extra
round-trips, yet it lets the client decide — early and unambiguously — whether to follow the
`catalogManaged` read and write rules.

## Sample Catalog Client API

The following is an example of a possible API which a Java-based Delta client might require catalog
implementations to target:

```java

interface CatalogManagedTable {
    /**
     * Commits the given set of `actions` to the given commit `version`.
     *
     * @param version The version we want to commit.
     * @param actions Actions that need to be committed.
     *
     * @return CommitResponse which has details around the new committed delta file.
     */
    def commit(
        version: Long,
        actions: Iterator[String]): CommitResponse

    /**
     * Retrieves a (possibly empty) suffix of ratified commits in the range [startVersion,
     * endVersion] for this table.
     * 
     * Some of these ratified commits may already have been published. Some of them may be staged,
     * in which case the staged commit file path is returned; others may be inline, in which case
     * the inline commit content is returned.
     * 
     * The returned commits are sorted in ascending version number and are contiguous.
     *
     * If neither start nor end version is specified, the catalog will return all available ratified
     * commits (possibly empty, if all commits have been published).
     *
     * In all cases, the response also includes the table's latest ratified commit version.
     *
     * @return GetCommitsResponse which contains an ordered list of ratified commits
     *         stored by the catalog, as well as table's latest commit version.
     */
    def getRatifiedCommits(
        startVersion: Option[Long],
        endVersion: Option[Long]): GetCommitsResponse
}
```

Note that the above is only one example of a possible Catalog Client API. It is also _NOT_ a catalog
API (no table discovery, ACL, create/drop, etc). The Delta protocol is agnostic to API details, and
the API surface Delta clients define should only cover the specific catalog capabilities that Delta
client needs to correctly read and write catalog-managed tables.
