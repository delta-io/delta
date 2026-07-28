# Checkpoint Protection

This RFC introduces a new Writer feature named `checkpointProtection`. When the feature is present in the protocol, no checkpoint removal/creation before `delta.requireCheckpointProtectionBeforeVersion` is allowed during metadata cleanup, unless everything is cleaned up in one go.

The motivation is to improve the drop feature functionality. Today, dropping a feature requires truncating the history of a Delta table at the version boundary where the feature is removed from the protocol. This is necessary because the Delta protocol only safely supports table protocols that are monotonically increasing with table versions. And because it is unsafe to truncate the history of a Delta table while transactions are running, dropping a feature requires a 24-hour wait time to avoid corrupting the table.

We can improve this process by setting up the table's history (including checkpoints) in such a way that older readers will be able to handle it correctly, i.e., to read correctly at versions for which they support the read protocol, and to reject reading of versions for which they do not support all features. The `checkpointProtection` feature is needed to ensure that this very specific setup of the history _stays in place_ until the feature removal is cleaned up from the retained version history.

A key component of this solution is a special set of protected checkpoints at the DROP FEATURE boundary that are guaranteed to persist until all history is truncated up to the checkpoints in one go. These checkpoints act as barriers that hide unsupported commit
records behind them. By "hiding", we mean that older readers will not need to replay those commits that they _don't_ support in order to reconstruct the table state at a later version that they _do_ support. With the `checkpointProtection`, we can guarantee these checkpoints will persist until history is truncated.

Furthermore, with the new drop feature method, it is no longer guaranteed that protocols are monotonically increasing. This means that clients that validate against the latest protocol can no longer assume that they can also operate on earlier versions correctly. In particular, writers are allowed to create checkpoints at earlier versions, but if they do this without checking the protocol at that specific version, and then they may write corrupted checkpoints for table versions for which they do not support the protocol. The `checkpointProtection` table feature also protects against these cases by requiring writers to check the protocol versions at historical table versions before creating a new checkpoint.

With these changes, we can drop table features without needing to truncate history. More importantly, they simplify the drop feature user journey by requiring a single execution of the DROP FEATURE command.

**For further discussions about this protocol change, please refer to the Github issue - https://github.com/delta-io/delta/issues/4152**

--------

> ***Add a new section at the [Table Features](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#table-features) section***
# Checkpoint Protection

The `checkpointProtection` is a Writer feature that protects checkpoints before the version indicated by table property `delta.requireCheckpointProtectionBeforeVersion`, and that forbids writers from creating checkpoints before that version unless they confirm that they support the table protocol at that version.

Enablement:
- The table must be at least on Writer Version 7 and Reader Version 1.
- The feature `checkpointProtection` must exist in the table `protocol`'s `writerFeatures`.

## Writer Requirements for Checkpoint Protection

For tables with `checkpointProtection` supported in the protocol:

a) Writers must not clean up any checkpoints for table versions before the version given by table property `delta.requireCheckpointProtectionBeforeVersion`.

b) Writers must not create new checkpoints for table versions before the version given by table property `delta.requireCheckpointProtectionBeforeVersion` unless they support all of the features in the table protocol at that version.

c) Writers must not clean up version history for table versions for which they do not support the protocol. A writer is allowed to clean up a range of versions if it supports all table features for every version that is being cleaned up. If a writer does not support the protocol for some of the versions that are being cleaned up, then the cleanup is allowed if and only if the cleanup includes _all_ table versions before the version given by  `delta.requireCheckpointProtectionBeforeVersion`. In this case, a single cleanup operation should truncate the history up to that boundary version in one go as opposed to several cleanup operations truncating in chunks.

d) In version history cleanup, writers must remove commits _before_ removing the associated checkpoints, so that requirement (a) is satisfied even during the cleanup.

## Recommendations for Readers of Tables with Checkpoint Protection feature

For tables with `checkpointProtection` supported in the protocol, readers do not need to understand or change anything new; they just need to acknowledge the feature exists.