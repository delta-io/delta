---
description: Learn how to drop table features in <Delta> to downgrade reader and writer protocol requirements and resolve compatibility issues.
oprhan: 1
---

# Drop Delta table features

.. note:: This feature is available in <Delta> 3.0.0 and above.

<Delta> provides limited support for dropping table features. To drop a table feature, the following must occur:

- Disable table properties that use the table feature.
- Remove all traces of the table feature from the data files backing the table.
- Remove transaction entries that use the table feature from the transaction log.
- Downgrade the table protocol.

Where supported, you should only use this functionality to support compatibility with earlier <Delta> versions, Delta Sharing, or other <Delta> reader or writer clients.

## How can I drop a Delta table feature?

To remove a Delta table feature, you run an `ALTER TABLE <table-name> DROP FEATURE <feature-name> [TRUNCATE HISTORY]` command. You must use <Delta> 3.0.0 or above.

## What Delta table features can be dropped?

You can drop the following Delta table features:

- `deletionVectors`. See [_](delta-deletion-vectors.md).
- `typeWidening-preview`. See [_](delta-type-widening.md). Type widening is available in preview in <Delta> 3.2.0 and above.
- `v2Checkpoint`. See [V2 Checkpoint Spec](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#v2-spec). Drop support for V2 Checkpoints is available in <Delta> 3.1.0 and above.
- `coordinatedCommits-preview`. See [_](delta-coordinated-commits.md). Coordinated Commits is available in preview in <Delta> 4.0.0 Preview.

You cannot drop other [Delta table features](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#valid-feature-names-in-table-features).

## How are Delta table features dropped?

Because Delta table features represent reader and writer protocols, they must be completely absent from the transaction log for full removal. Dropping a feature occurs in two stages and requires time to elapse before completion. The specifics of feature removal vary by feature, but the following section provides a general overview.

### Prepare to drop a table feature

During the first stage, the user prepares to drop the table feature. The following describes what happens during this stage:

#. The user runs the `DROP FEATURE` command.
#. Table properties that specifically enable a table feature have values set to disable the feature.
#. Table properties that control behaviors associated with the dropped feature have options set to default values before the feature was introduced.
#. As necessary, data and metadata files are rewritten respecting the updated table properties.
#. The command finishes running and returns an error message informing the user they must wait 24 hours to proceed with feature removal.

After first disabling a feature, you can continue writing to the target table before completing the protocol downgrade, but cannot use the table feature you are removing.

.. note:: If you leave the table in this state, operations against the table do not use the table feature, but the protocol still supports the table feature. Until you complete the final downgrade step, the table is not readable by Delta clients that do not understand the table feature.

### Downgrade the protocol and drop a table feature

To drop the table feature, you must remove all transaction history associated with the feature and downgrade the protocol.

#. After at least 24 hours have passed, the user executes the `DROP FEATURE` command again with the `TRUNCATE HISTORY` clause.
#. The client confirms that no transactions in the specified retention threshold use the table feature, then truncates the table history to that treshold.
#. The protocol is downgraded, dropping the table feature.
#. If the table features that are present in the table can be represented by a legacy protocol version, the `minReaderVersion` and `minWriterVersion` for the table are downgraded to the lowest version that supports exactly all remaining features in use by the Delta table.

.. important:: Running `ALTER TABLE <table-name> DROP FEATURE <feature-name> TRUNCATE HISTORY` removes all transaction log data older than 24 hours. After dropping a Delta table feature, you do not have access to table history or time travel.

See [_](versioning.md).

.. include:: /shared/replacements.md
