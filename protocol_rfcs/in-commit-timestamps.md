# In-Commit Timestamps

This RFC proposes a new Writer feature called In-Commit Timestamps which strongly associates a monotonically increasing timestamp with each commit by storing it in the commit's metadata. By storing the timestamp inside the commit, we get a more reliable commit timestamp which is robust against operations that can inadvertantly change the file modification timestamp. This makes the operations that rely on commit timestamps (e.g. Time Travel queries) more reliable.

**For further discussions about this protocol change, please refer to the Github issue - https://github.com/delta-io/delta/issues/2532**

--------


> ***Change to existing section***
### Commit Provenance Information
A delta file can optionally contain additional provenance information about what higher-level operation was being performed as well as who executed it.

Implementations are free to store any valid JSON-formatted data via the `commitInfo` action ~~.~~ **as long as any table feature (see [In-Commit Timestamps](#in-commit-timestamps)) does not impose additional requirements on the data.**

When In-Commit Timestamp are enabled, writers are required to include a commitInfo action with every commit, which must include the `inCommitTimestamp` field.

> ***New Section***
# In-Commit Timestamps

The In-Commit Timestamps writer feature strongly associates a monotonically increasing timestamp with each commit by storing it in the commit's metadata.

Enablement:
- The table must be on Writer Version 7.
- The feature `inCommitTimestamps` must exist in the table `protocol`'s `writerFeatures`.
- The table property `delta.enableInCommitTimestamps` must be set to `true`.

## Writer Requirements for In-Commit Timestamps

When In-Commit Timestamps is enabled, then:
1. Writers must write the `commitInfo` (see [Commit Provenance Information](#commit-provenance-information)) action in the commit.
2. The `commitInfo` action must be the first action in the commits.
3. The `commitInfo` action must include a field named `inCommitTimestamp`, of type `timestamp` (see [Primitive Types](#primitive-types)), which represents the UTC time when the commit is considered to have succeeded. It is the larger of two values:
   - The UTC wall clock time at which the writer attempted the commit
   - One millisecond later than the previous commit's `inCommitTimestamp`
4. If the table has commits from a period when this feature was not enabled, provenance information around when this feature was enabled must be tracked in table properties:
   - The property `delta.inCommitTimestampEnablementVersion` must be used to track the version of the table when this feature was enabled.
   - The property `delta.inCommitTimestampEnablementTimestamp` must be the same as the `inCommitTimestamp` of the commit when this feature was enabled.
5. The `inCommitTimestamp` of the commit that enables this feature must be greater than the file modification time of the immediately preceding commit.

## Recommendations for Readers of Tables with In-Commit Timestamps

For tables with In-Commit timestamps enabled, readers should use the `inCommitTimestamp` as the commit timestamp for operations like time travel.
If a table has commits from a period before In-Commit timestamps were enabled, the table property `delta.inCommitTimestampEnablementVersion` would be set and can be used to identify commits that don't have `inCommitTimestamp`.
To correctly determine the commit timestamp for these tables, readers can use the following rules:
1. For commits with version >= `delta.inCommitTimestampEnablementVersion`, readers should use the `inCommitTimestamp` field of the `commitInfo` action.
2. For commits with version < `delta.inCommitTimestampEnablementVersion`, readers should use the file modification timestamp.


