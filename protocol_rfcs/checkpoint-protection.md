# Checkpoint Protection

This RFC introduces a new Writer feature named `checkpointProtection`. When the feature is present in the protocol, no checkpoint removal/creation before that version is allowed during metadata cleanup unless everything is cleaned up in one go.

The motivation is to improve the drop feature functionality. Today, dropping a feature requires the execution of the DROP FEATURE command twice with a 24 hour waiting time in between. In addition, it also results in the truncation of the history of the Delta table to the last 24 hours.

We can improve this process by introducing `checkpointProtection`, which allows us to set up the table's history (including checkpoints) in such a way that older readers will be able to handle it correctly until we atomically delete it.

A key component of this solution is a special set of protected checkpoints at the DROP FEATURE boundary that are guaranteed to persist until all history is truncated up to the checkpoints in one go. These checkpoints act as barriers that hide unsupported log records behind them. With the `checkpointProtection`, we can guarantee these checkpoints will persist until history is truncated.

Furthermore, with the new drop feature method, validating against the latest protocol is no longer sufficient. Therefore, creating checkpoints to historical versions can lead to corruption if the writer does not support the target protocol. The `checkpointProtection` also protects against these cases by disallowing checkpoint creation before `requireCheckpointProtectionBeforeVersion`.

With these changes, we can drop table features in a single command without needing to truncate history. More importantly, they simplify the drop feature user journey by requiring a single execution of the DROP FEATURE command.

**For further discussions about this protocol change, please refer to the Github issue - https://github.com/delta-io/delta/issues/4152**

--------


> ***New Section***
# Checkpoint Protection

The `checkpointProtection` is a Writer feature that allows writers to clean up metadata iff metadata can be cleaned up to the `requireCheckpointProtectionBeforeVersion` table property in one go.

Enablement:
- The table must be on Writer Version 7 and Reader Version 1.
- The feature `checkpointProtection` must exist in the table `protocol`'s `writerFeatures`.

## Writer Requirements for Checkpoint Protection

For tables with `checkpointProtection` supported in the protocol, writers need to check `requireCheckpointProtectionBeforeVersion` before cleaning up metadata. Metadata clean up can proceed iff metadata can be cleaned up to the `requireCheckpointProtectionBeforeVersion` table property in one go. This means that a single cleanup operation should truncate up to `requireCheckpointProtectionBeforeVersion` as opposed to several cleanup operations truncating in chunks.

There are two exceptions to this rule. If any of the two holds, the rule above can be ignored:

a) The writer does not create any checkpoints during history cleanup and does not erase any checkpoints after the truncation version.

b) The writer verifies it supports all protocols between `[cleanup start version, min(checkpoint creation version, requireCheckpointProtectionBeforeVersion)]`.

The `checkpointProtection` feature can only be removed if history is truncated up to at least the `requireCheckpointProtectionBeforeVersion`.

## Recommendations for Readers of Tables with Checkpoint Protection feature

For tables with `checkpointProtection` supported in the protocol, readers do not need to understand or change anything new; they just need to acknowledge the feature exists.