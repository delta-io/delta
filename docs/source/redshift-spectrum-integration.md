---
description: Learn how to set up an integration to enable you to read Delta tables from <Redshift>.
---

# <Redshift> to <Delta> integration

.. admonition::  Experimental
    :class: preview

    This is an experimental integration. Use with caution.


A Delta table can be read by <Redshift> using a _manifest file_, which is a text file containing the list of data files to read for querying a Delta table. This article describes how to set up a <Redshift> to <Delta> integration using manifest files and query Delta tables.

## Set up a <Redshift> to <Delta> integration and query Delta tables

You set up a <Redshift> to <Delta> integration using the following steps.

.. contents::
  :local:
  :depth: 1

### Step 1: Generate manifests of a Delta table using <AS>

Run the `generate` operation on a Delta table at location `<path-to-delta-table>`:

.. code-language-tabs::

  ```sql
  GENERATE symlink_format_manifest FOR TABLE delta.`<path-to-delta-table>`
  ```

  ```scala
  val deltaTable = DeltaTable.forPath(<path-to-delta-table>)
  deltaTable.generate("symlink_format_manifest")
  ```

  ```java
  DeltaTable deltaTable = DeltaTable.forPath(<path-to-delta-table>);
  deltaTable.generate("symlink_format_manifest");
  ```

  ```python
  deltaTable = DeltaTable.forPath(<path-to-delta-table>)
  deltaTable.generate("symlink_format_manifest")
  ```

See [Generate a manifest file](delta-utility.md#delta-generate) for details.

The `generate` operation generates manifest files at `<path-to-delta-table>/_symlink_format_manifest/`. In other words, the files in this directory contain the names of the data files (that is, Parquet files) that should be read for reading a snapshot of the Delta table.

.. note::
  We recommend that you define the Delta table in a location that <Redshift> can read directly.

### Step 2: Configure <Redshift> to read the generated manifests

Run the following commands in your <Redshift> environment.

#. Define a new external table in <Redshift> using the format `SymlinkTextInputFormat` and the manifest location `<path-to-delta-table>/_symlink_format_manifest/`.

   ```sql
   CREATE EXTERNAL TABLE mytable ([(col_name1 col_datatype1, ...)])
   [PARTITIONED BY (col_name2 col_datatype2, ...)]
   ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
   STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat'
   OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
   LOCATION '<path-to-delta-table>/_symlink_format_manifest/'  -- location of the generated manifest
   ```

   `SymlinkTextInputFormat` configures <Redshift> to compute file splits for `mytable` by reading the manifest file instead of using a directory listing to find data files. Replace `mytable` with the name of the external table and `<path-to-delta-table>` with the absolute path to the Delta table.

   .. important::

       - `mytable` must be the same schema and have the same partitions as the Delta table.
       - The set of `PARTITIONED BY` columns must be distinct from the set of non-partitioned columns. Furthermore, you cannot specify partitioned columns with `AS <select-statement>`.


- You cannot use this table definition in <AS>; it can be used only by <Redshift>.

#. If the Delta table is partitioned, you must add the partitions explicitly to the <Redshift> table. This is needed because the manifest of a partitioned table is itself partitioned in the same directory structure as the table.

   - For every partition in the table, run the following in <Redshift>, either directly in <Redshift>, or using the AWS CLI or [Data API](https://docs.aws.amazon.com/redshift/latest/mgmt/data-api.html):

     ```sql
     ALTER TABLE mytable.redshiftdeltatable ADD IF NOT EXISTS PARTITION (col_name=col_value) LOCATION '<path-to-delta-table>/_symlink_format_manifest/col_name=col_value'
     ```

This steps will provide you with a [consistent](#data-consistency) view of the Delta table.

### Step 3: Update manifests

When data in a Delta table is updated, you must regenerate the manifests using either of the following approaches:

- **Update explicitly**: After all the data updates, you can run the `generate` operation to update the manifests.
- **Update automatically**: You can configure a Delta table so that all write operations on the table automatically update the manifests. To enable this automatic mode, set the corresponding table property using the following SQL command.

  ```sql
  ALTER TABLE delta.`<path-to-delta-table>` SET TBLPROPERTIES(delta.compatibility.symlinkFormatManifest.enabled=true)
  ```
  To disable this automatic mode, set this property to `false`.

  .. note::
     After enabling automatic mode on a partitioned table, each write operation updates only manifests corresponding to the partitions that operation wrote to. This incremental update ensures that the overhead of manifest generation is low for write operations. However, this also means that if the manifests in other partitions are stale, enabling automatic mode will not automatically fix it. Therefore, you should explicitly run `GENERATE` to update manifests for the entire table immediately after enabling automatic mode.

Whether to update automatically or explicitly depends on the concurrent nature of write operations on the Delta table and the desired data consistency. For example, if automatic mode is enabled, concurrent write operations lead to concurrent overwrites to the manifest files. With such unordered writes, the manifest files are not guaranteed to point to the latest version of the table after the write operations complete. Hence, if concurrent writes are expected and you want to avoid stale manifests, you should consider explicitly updating the manifest after the expected write operations have completed.

In addition, if your table is partitioned, then you must add any new partitions or remove deleted partitions by following the same process as described in the preceding step.

## Limitations

The <Redshift> integration has known limitations in its behavior.

### Data consistency

Whenever <Delta> generates updated manifests, it atomically overwrites existing manifest files. Therefore, <Redshift> will always see a consistent view of the data files; it will see all of the old version files or all of the new version files. However, the granularity of the consistency guarantees depends on whether or not the table is partitioned.

- **Unpartitioned tables**: All the files names are written in one manifest file which is updated atomically. In this case <Redshift> will see full table snapshot consistency.

- **Partitioned tables**: A manifest file is partitioned in the same Hive-partitioning-style directory structure as the original Delta table. This means that each partition is updated atomically, and <Redshift> will see a consistent view of each partition but not a consistent view across partitions. Furthermore, since all manifests of all partitions cannot be updated together, concurrent attempts to generate manifests can lead to different partitions having manifests of different versions. While this consistency guarantee under data change is weaker than that of reading Delta tables with Spark, it is still stronger than formats like Parquet as they do not provide partition-level consistency.

Depending on what storage system you are using for Delta tables, it is possible to get incorrect results when <Redshift> concurrently queries the manifest while the manifest files are being rewritten. In file system implementations that lack atomic file overwrites, a manifest file may be momentarily unavailable. Hence, use manifests with caution if their updates are likely to coincide with queries from <Redshift>.


### Performance

This is an experimental integration and its performance and scalability characteristics have not yet been tested.

### Schema evolution

<Delta> supports schema evolution and queries on a Delta table automatically use the latest schema regardless of the schema defined in the table in the Hive metastore. However, <Redshift> uses the schema defined in its table definition, and will not query with the updated schema until the table definition is updated to the new schema.


.. <Redshift> replace:: Redshift Spectrum

.. include:: /shared/replacements.md
