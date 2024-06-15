---
description: Learn how Delta table protocols are versioned.
orphan: 1
---

# How does <Delta> manage feature compatibility?

Many <Delta> optimizations require enabling <Delta> features on a table. <Delta> features are always backwards compatible, so tables written by a lower <Delta> version can always be read and written by a higher <Delta> version. Enabling some features breaks forward compatibility with workloads running in a lower <Delta> version. For features that break forward compatibility, you must update all workloads that reference the upgraded tables to use a compliant <Delta> version.

## What <Delta> features require client upgrades?

The following <Delta> features break forward compatibility. Features are enabled on a table-by-table basis.

.. csv-table::
   :header: "Feature", "Requires <Delta> version or later", "Documentation"

   `CHECK` constraints,[Delta Lake 0.8.0](https://github.com/delta-io/delta/releases/tag/v0.8.0),[_](/delta-constraints.md#check-constraint)
   Generated columns,[Delta Lake 1.0.0](https://github.com/delta-io/delta/releases/tag/v1.0.0),[_](/delta-batch.md#use-generated-columns)
   Column mapping,[Delta Lake 1.2.0](https://github.com/delta-io/delta/releases/tag/v1.2.0),[_](/delta-column-mapping.md)
   Change data feed,[Delta Lake 2.0.0](https://github.com/delta-io/delta/releases/tag/v2.0.0),[_](/delta-change-data-feed.md)
   Deletion vectors,[Delta Lake 2.3.0](https://github.com/delta-io/delta/releases/tag/v2.3.0),[_](/delta-deletion-vectors.md)
   Table features,[Delta Lake 2.3.0](https://github.com/delta-io/delta/releases/tag/v2.3.0),[_](#table-features)
   Timestamp without Timezone,[Delta Lake 2.4.0](https://github.com/delta-io/delta/releases/tag/v2.4.0),[TimestampNTZType](https://spark.apache.org/docs/latest/sql-ref-datatypes.html)
   Iceberg Compatibility V1, [Delta Lake 3.0.0](https://github.com/delta-io/delta/releases/tag/v3.0.0),[IcebergCompatV1](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#iceberg-compatibility-v1)
   Iceberg Compatibility V2, [Delta Lake 3.1.0](https://github.com/delta-io/delta/releases/tag/v3.1.0),[IcebergCompatV2](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#iceberg-compatibility-v2)
   V2 Checkpoints, [Delta Lake 3.0.0](https://github.com/delta-io/delta/releases/tag/v3.0.0),[V2 Checkpoint Spec](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#v2-spec)
   Domain metadata, [Delta Lake 3.0.0](https://github.com/delta-io/delta/releases/tag/v3.0.0),[Domain Metadata Spec](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#domain-metadata)
   Clustering, [Delta Lake 3.1.0](https://github.com/delta-io/delta/releases/tag/v3.1.0),[_](/delta-clustering.md)
   Type widening (Preview),[Delta Lake 3.2.0](https://github.com/delta-io/delta/releases/tag/v3.2.0),[_](/delta-type-widening.md)
   Coordinated Commits (Preview),[Delta Lake 4.0.0 Preview](https://github.com/delta-io/delta/releases/tag/v4.0.0rc1), [_](/delta-coordinated-commits.md)
   Variant Type (Preview), [Delta Lake 4.0.0 Preview](https://github.com/delta-io/delta/releases/tag/v4.0.0rc1),[Variant Type](https://github.com/delta-io/delta/blob/master/protocol_rfcs/variant-type.md)

<a id="table-protocol"></a>

## What is a table protocol specification?

Every Delta table has a protocol specification which indicates the set of features that the table supports. The protocol specification is used by applications that read or write the table to determine if they can handle all the features that the table supports. If an application does not know how to handle a feature that is listed as supported in the protocol of a table, then that application is not be able to read or write that table.

The protocol specification is separated into two components: the *read protocol* and the *write protocol*.

### Read protocol

The read protocol lists all features that a table supports and that an application must understand in order to read the table correctly. Upgrading the read protocol of a table requires that all reader applications support the added features.

.. important:: 
  All applications that write to a Delta table must be able to construct a snapshot of the table. As such, workloads that write to Delta tables must respect both reader and writer protocol requirements.

  If you encounter a protocol that is unsupported by a workload on <Delta>, you must upgrade to a higher <Delta> implementation with more comprehensive support.

### Write protocol

The write protocol lists all features that a table supports and that an application must understand in order to write to the table correctly. Upgrading the write protocol of a table requires that all writer applications support the added features. It does not affect read-only applications, unless the read protocol is also upgraded.

## Which protocols must be upgraded?

Some features require upgrading both the read protocol and the write protocol. Other features only require upgrading the write protocol. 

As an example, support for `CHECK` constraints is a write protocol feature: only writing applications need to know about `CHECK` constraints and enforce them. 

In contrast, column mapping requires upgrading both the read and write protocols. Because the data is stored differently in the table, reader applications must understand column mapping so they can read the data correctly.

For more on upgrading, see [_](#upgrade).

<a id="table-features"></a>

## What are table features?

In <Delta> 2.3.0 and above, <Delta> table features introduce granular flags specifying which features are supported by a given table. Table features are the successor to protocol versions and are designed with the goal of improved flexibility for clients that read and write <Delta>. See [_](#table-version).

.. note:: Table features have protocol version requirements. See [_](#protocol-table).

A Delta table feature is a marker that indicates that the table supports a particular feature. Every feature is either a write protocol feature (meaning it only upgrades the write protocol) or a read/write protocol feature (meaning both read and write protocols are upgraded to enable the feature).

To learn more about supported table features in <Delta>, see the [Delta Lake protocol](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#valid-feature-names-in-table-features).

## Do table features change how <Delta> features are enabled?

If you only interact with Delta tables through <Delta>, you can continue to track support for <Delta> features using minimum <Delta> requirements. If you read and write from Delta tables using other systems, you might need to consider how table features impact compatibility, because there is a risk that the system could not understand the upgraded protocol versions.

<a id="table-version"></a>

## What is a protocol version?

A protocol version is a protocol number that indicates a particular grouping of table features. In <Delta> 2.3.0 and below, you cannot enable table features individually. Protocol versions bundle a group of features.

Delta tables specify a separate protocol version for read protocol and write protocol. The transaction log for a Delta table contains protocol versioning information that supports <Delta> evolution.

The protocol versions bundle all features from previous protocols. See [_](#protocol-table).

.. note:: Starting with writer version 7 and reader version 3, <Delta> has introduced the concept of table features. Using table features, you can now choose to only enable those features that are supported by other clients in your data ecosystem. See [_](#table-features).

<a id="protocol-table"></a>

## Features by protocol version

The following table shows minimum protocol versions required for <Delta> features.

.. csv-table::
   :header: "Feature", "`minWriterVersion`", "`minReaderVersion`", "Documentation"

   Basic functionality,2,1,[_](/index.md)
   `CHECK` constraints,3,1,[_](/delta-constraints.md#check-constraint)
   Change data feed,4,1,[_](/delta-change-data-feed.md)
   Generated columns,4,1,[_](/delta-batch.md#use-generated-columns)
   Column mapping,5,2,[_](/delta-column-mapping.md)
   Table features read,7,1,[_](#table-features)
   Table features write,7,3,[_](#table-features)
   Deletion vectors,7,3,[_](/delta-deletion-vectors.md)
   Timestamp without Timezone,7,3,[TimestampNTZType](https://spark.apache.org/docs/latest/sql-ref-datatypes.html)
   Iceberg Compatibility V1,7,2,[IcebergCompatV1](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#iceberg-compatibility-v1)
   V2 Checkpoints,7,3,[V2 Checkpoint Spec](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#v2-spec)
   Vacuum Protocol Check,7,3,[Vacuum Protocol Check Spec](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#vacuum-protocol-check)
   Type widening (Preview),7,3,[_](/delta-type-widening.md)
   Coordinated Commits (Preview),7,3,[_](/delta-coordinated-commits.md)
   Variant Type (Preview),7,3,[Variant Type](https://github.com/delta-io/delta/blob/master/protocol_rfcs/variant-type.md)

<a id="upgrade"></a>

## Upgrading protocol versions

You can choose to manually update a table to a newer protocol version. We recommend using the lowest protocol versions that support the <Delta> features required for your table. Upgrading the writer protocol might cause less disruption than upgrading the reader protocol since systems and workloads using older <Delta> versions can still read from tables, even if they do not support the updated writer protocol.

.. warning::
  Protocol version upgrades are irreversible, and upgrading the protocol version might break the existing <Delta> table readers, writers, or both. We recommend you upgrade specific tables only when needed, such as to opt-in to new features in <Delta>. You should also check to make sure that all of your current and future production tools support <Delta> tables with the new protocol version.


To upgrade a table to a newer protocol version, use the `DeltaTable.upgradeTableProtocol` method:

.. code-language-tabs::

  ```sql
  -- Upgrades the reader protocol version to 1 and the writer protocol version to 3.
  ALTER TABLE <table_identifier> SET TBLPROPERTIES('delta.minReaderVersion' = '1', 'delta.minWriterVersion' = '3')
  ```

  ```python
  from delta.tables import DeltaTable
  delta = DeltaTable.forPath(spark, "path_to_table") # or DeltaTable.forName
  delta.upgradeTableProtocol(1, 3) # upgrades to readerVersion=1, writerVersion=3
  ```

  ```scala
  import io.delta.tables.DeltaTable
  val delta = DeltaTable.forPath(spark, "path_to_table") // or DeltaTable.forName
  delta.upgradeTableProtocol(1, 3) // Upgrades to readerVersion=1, writerVersion=3.
  ```

.. include:: /shared/replacements.md
