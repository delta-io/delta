# In-Commit Timestamps

This RFC proposes a new Writer table feature called In-Commit Timestamps. When enabled, commit metadata includes a monotonically increasing timestamp that allows for reliable TIMESTAMP AS OF time travel even if filesystem operations change a commit file's modification timestamp.

**For further discussions about this protocol change, please refer to the Github issue - https://github.com/delta-io/delta/issues/2532**

--------


### Commit Provenance Information
> ***Change to existing section***

A delta file can optionally contain additional provenance information about what higher-level operation was being performed as well as who executed it.

Implementations are free to store any valid JSON [object literal](https://www.w3schools.com/js/js_json_objects.asp) as the `commitInfo` action <ins>unless some table feature (e.g. [In-Commit Timestamps](#in-commit-timestamps)) imposes additional requirements on the data</ins>.

<ins>When In-Commit Timestamp are enabled, writers are required to include a commitInfo action with every commit, which must include the `inCommitTimestamp` field.</ins>

#### Reader Requirements for AddCDCFile
> ***Change to existing section***

...
3. Change data readers should return the following extra columns:

Field Name | Data Type | Description
-|-|-
_commit_version|`Long`| The table version containing the change. This can be derived from the name of the Delta log file that contains actions.
_commit_timestamp|`Timestamp`| The timestamp associated when the commit was created. ~~This can be derived from the file modification time of the Delta log file that contains actions.~~ <ins>Depending on whether [In-Commit Timestamps](#in-commit-timestamps) are enabled, this is derived from either the `inCommitTimestamp` field of the `commitInfo` action of the version's Delta log, or from the Delta log's file modification time.</ins>

# In-Commit Timestamps
> ***New Section after the [Clustered Table](#clustered-table) section***

The In-Commit Timestamps writer feature strongly associates a monotonically increasing timestamp with each commit by storing it in the commit's metadata.

Enablement:
- The table must be on Writer Version 7.
- The feature `inCommitTimestamps` must exist in the table `protocol`'s `writerFeatures`.
- The table property `delta.enableInCommitTimestamps` must be set to `true`.

## Writer Requirements for In-Commit Timestamps

When In-Commit Timestamps is enabled, then:
1. Writers must write the `commitInfo` (see [Commit Provenance Information](#commit-provenance-information)) action in the commit.
2. The `commitInfo` action must be the first action in the commit.
3. The `commitInfo` action must include a field named `inCommitTimestamp`, of type `long` (see [Primitive Types](#primitive-types)), which represents the time (in milliseconds since the Unix epoch) when the commit is considered to have succeeded. It is the larger of two values:
   - The time, in milliseconds since the Unix epoch, at which the writer attempted the commit
   - One millisecond later than the previous commit's `inCommitTimestamp`
4. If the table has commits from a period when this feature was not enabled, provenance information around when this feature was enabled must be tracked in table properties:
   - The property `delta.inCommitTimestampEnablementVersion` must be used to track the version of the table when this feature was enabled.
   - The property `delta.inCommitTimestampEnablementTimestamp` must be the same as the `inCommitTimestamp` of the commit when this feature was enabled.
5. The `inCommitTimestamp` of the commit that enables this feature must be greater than the file modification time of the immediately preceding commit.

## Recommendations for Readers of Tables with In-Commit Timestamps

For tables with In-Commit timestamps enabled, readers should use the `inCommitTimestamp` as the commit timestamp for operations like time travel and [`DESCRIBE HISTORY`](https://docs.delta.io/latest/delta-utility.html#retrieve-delta-table-history).
If a table has commits from a period before In-Commit timestamps were enabled, the table properties `delta.inCommitTimestampEnablementVersion` and `delta.inCommitTimestampEnablementTimestamp` would be set and can be used to identify commits that don't have `inCommitTimestamp`.
To correctly determine the commit timestamp for these tables, readers can use the following rules:
1. For commits with version >= `delta.inCommitTimestampEnablementVersion`, readers should use the `inCommitTimestamp` field of the `commitInfo` action.
2. For commits with version < `delta.inCommitTimestampEnablementVersion`, readers should use the file modification timestamp.

Furthermore, when attempting timestamp-based time travel where table state must be fetched as of `timestamp X`, readers should use the following rules:
1. If `timestamp X` >= `delta.inCommitTimestampEnablementTimestamp`, only table versions >= `delta.inCommitTimestampEnablementVersion` should be considered for the query.
2. Otherwise, only table versions less than `delta.inCommitTimestampEnablementVersion` should be considered for the query.
