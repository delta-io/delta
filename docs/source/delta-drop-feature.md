---
description: Learn how to drop table features in <Delta> to downgrade reader and writer protocol requirements and resolve compatibility issues.
oprhan: 1
---

# Drop Delta table features

This article describes how to drop Delta Lake table features and downgrade protocol versions.

.. note:: This feature is available in <Delta> 4.0.0 and above. A legacy implementation of the feature is available since <Delta> 3.0.0. Not all Delta table features can be dropped. See [What Delta table features can be dropped?](#what-delta-table-features-can-be-dropped)

You should only use this functionality to support compatibility with earlier <Delta> versions, Delta Sharing, or other <Delta> reader or writer clients.

## How can I drop a Delta table feature?

To remove a Delta table feature, you run an `ALTER TABLE <table-name> DROP FEATURE <feature-name>` command.

## What Delta table features can be dropped?

You can drop the following Delta table features:

- `deletionVectors`. See [_](delta-deletion-vectors.md).
- `typeWidening-preview`. See [_](delta-type-widening.md). Type widening is available in preview in <Delta> 3.2.0 and above.
- `typeWidening`. See [_](delta-type-widening.md). Type widening is available in <Delta> 4.0.0 and above.
- `v2Checkpoint`. See [V2 Checkpoint Spec](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#v2-spec). Drop support for V2 Checkpoints is available in <Delta> 3.1.0 and above.
- `columnMapping`. See [_](delta-column-mapping.md). Drop support for column mapping is available in <Delta> 3.3.0 and above.
- `vacuumProtocolCheck`. See [Vacuum Protocol Check Spec](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#vacuum-protocol-check). Drop support for vacuum protocol check is available in <Delta> 3.3.0 and above.
- `checkConstraints`. See [_](delta-constraints.md). Drop support for check constraints is available in <Delta> 3.3.0 and above.
- `inCommitTimestamp`. See [_](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#in-commit-timestamps). Drop support for In-Commit Timestamp is available in <Delta> 3.3.0 and above.
- `checkpointProtection`. See [Checkpoint Protection Spec](https://github.com/delta-io/delta/blob/master/protocol_rfcs/checkpoint-protection.md). Drop support for checkpoint protection is available in <Delta> 4.0.0 and above.

You cannot drop other [Delta table features](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#valid-feature-names-in-table-features).

## What happens when a table feature is dropped?

When you drop a table feature, Delta Lake atomically commits changes to the table to accomplish the following:

- Disable table properties that use the table feature.
- Rewrite data files as necessary to remove all traces of the table feature from the data files backing the table in the current version.
- Create a set of protected checkpoints that allow reader clients to interpret table history correctly.
- Add the writer table feature checkpointProtection to the table protocol.
- Downgrade the table protocol to the lowest reader and writer versions that support all remaining table features.

## What is the checkpointProtection table feature?

When you drop a feature, Delta Lake rewrites data and metadata in the table's history as protected checkpoints to respect the protocol downgrade. After the downgrade, the table should always be readable by more reader clients. This is because the protocol for the table now reflects that support for the dropped feature is no longer required to read the table. The protected checkpoints and the checkpointProtection feature accomplish the following:

- Reader clients that understand the dropped table feature can access all available table history.
- Reader clients that do not support the dropped table feature only need to read the table history starting from the protocol downgrade version.
- Writer clients do not rewrite checkpoints prior to the protocol downgrade.
- Table maintenance operations respect requirements set by `checkpointProtection`, which mark protocol downgrade checkpoints as protected.
- While you can only drop one table feature with each DROP FEATURE command, a table can have multiple protected checkpoints and dropped features in its table history.

The table feature `checkpointProtection` should not block read-only access from Delta Lake clients. To fully downgrade the table and remove the `checkpointProtection` table feature, you must use TRUNCATE HISTORY. The recommendation is to only using this pattern if you need to write to tables with external Delta clients that do not support checkpointProtection.


## Fully downgrade table protocols for legacy clients

If integrations with external Delta Lake clients require writes that don't support the checkpointProtection table feature, you must use TRUNCATE HISTORY to fully remove all traces of the disabled table features and fully downgrade the table protocol.

It is recommended to always test the default behavior for DROP FEATURE before proceeding with TRUNCATE HISTORY. Running TRUNCATE HISTORY removes all table history greater than 24 hours.

Full table downgrade occurs in two steps that must occur at least 24 hours apart.

### Step 1: Prepare to drop a table feature

During the first stage, the user prepares to drop the table feature. The following describes what happens during this stage:

#. You run the `ALTER TABLE <table-name> DROP FEATURE <feature-name> TRUNCATE HISTORY` command.
#. Table properties that specifically enable a table feature have values set to disable the feature.
#. Table properties that control behaviors associated with the dropped feature have options set to default values before the feature was introduced.
#. As necessary, data and metadata files are rewritten respecting the updated table properties.
#. The command finishes running and returns an error message informing the user they must wait 24 hours to proceed with feature removal.

After first disabling a feature, you can continue writing to the target table before completing the protocol downgrade, but you cannot use the table feature you are removing.

.. note:: If you leave the table in this state, operations against the table do not use the table feature, but the protocol still supports the table feature. Until you complete the final downgrade step, the table is not readable by Delta clients that do not understand the table feature.

### Step 2: Downgrade the protocol and drop a table feature

To fully remove all transaction history associated with the feature and downgrade the protocol:

#. After at least 24 hours have passed, you run the `ALTER TABLE <table-name> DROP FEATURE <feature-name> TRUNCATE HISTORY` command.
#. The client confirms that no transactions in the specified retention threshold use the table feature, then truncates the table history to that threshold.
#. The protocol is downgraded, dropping the table feature.
#. If the table features that are present in the table can be represented by a legacy protocol version, the `minReaderVersion` and `minWriterVersion` for the table are downgraded to the lowest version that supports exactly all remaining features in use by the Delta table.


.. important:: Running `ALTER TABLE <table-name> DROP FEATURE <feature-name> TRUNCATE HISTORY` removes all transaction log data older than 24 hours. After dropping a Delta table feature, you do not have access to table history or time travel.

See [_](versioning.md).

.. <Delta> replace:: Delta Lake
.. <AS> replace:: Apache Spark
