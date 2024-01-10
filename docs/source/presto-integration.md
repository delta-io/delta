---
description: Learn how to set up an integration to enable you to read Delta tables from <PrestoAnd>.
---


# <PrestoAnd> to <Delta> integration using manifests

.. important::

   Presto, Trino and Athena all have native support for Delta Lake. Support is as follows:

   - Presto [version 0.269](https://prestodb.io/docs/0.269/release/release-0.269.html#delta-lake-connector-changes) and above natively supports reading the <Delta> tables. For details on using the native <Delta> connector, see [Delta Lake Connector - Presto](https://prestodb.io/docs/current/connector/deltalake.html). For Presto versions lower than [0.269](https://prestodb.io/docs/0.269/release/release-0.269.html#delta-lake-connector-changes), you can use the manifest-based approach in this article.

   - Trino [version 373](https://trino.io/docs/current/release/release-373.html) and above natively supports reading and writing the <Delta> tables. For details on using the native <Delta> connector, see [Delta Lake Connector - Trino](https://trino.io/docs/current/connector/delta-lake.html). For Trino versions lower than [version 373](https://trino.io/docs/current/release/release-373.html), you can use the manifest-based approach detailed in this article.

   - Athena [version 3](https://docs.aws.amazon.com/athena/latest/ug/engine-versions-reference-0003.html) and above  natively supports reading <Delta> tables. For details on using the native <Delta> connector, see [Querying Delta Lake tables](https://docs.aws.amazon.com/athena/latest/ug/delta-lake-tables.html). For Athena versions lower than [version 3](https://docs.aws.amazon.com/athena/latest/ug/engine-versions-reference-0003.html), you can use the manifest-based approach detailed in this article.


<PrestoAnd> support reading from external tables using a _manifest file_, which is a text file containing the list of data files to read for querying a table. When an external table is defined in the Hive metastore using manifest files, <PrestoAnd> can use the list of files in the manifest rather than finding the files by directory listing. This article describes how to set up a <PrestoAnd> to <Delta> integration using manifest files and query Delta tables.

## Set up the <PrestoOr> to <Delta> integration and query Delta tables

You set up a <PrestoOr> to <Delta> integration using the following steps.

### Step 1: Generate manifests of a Delta table using <AS>

Using Spark [configured](quick-start.md#set-up-as-with-delta) with <Delta>, run any of the following commands on a Delta table at location `<path-to-delta-table>`:

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

The `generate` command generates manifest files at `<path-to-delta-table>/_symlink_format_manifest/`. In other words, the files in this directory will contain the names of the data files (that is, Parquet files) that should be read for reading a snapshot of the Delta table.

### Step 2: Configure <PrestoOr> to read the generated manifests

#. Define a new table in the Hive metastore connected to <PrestoOr> using the format `SymlinkTextInputFormat` and the manifest location `<path-to-delta-table>/_symlink_format_manifest/`.

   ```sql
   CREATE EXTERNAL TABLE mytable ([(col_name1 col_datatype1, ...)])
   [PARTITIONED BY (col_name2 col_datatype2, ...)]
   ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
   STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat'
   OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
   LOCATION '<path-to-delta-table>/_symlink_format_manifest/'  -- location of the generated manifest
   ```

   `SymlinkTextInputFormat` configures <PrestoOr> to compute file splits for `mytable` by reading the manifest file instead of using a directory listing to find data files. Replace `mytable` with the name of the external table and `<path-to-delta-table>` with the absolute path to the Delta table.


   .. important::

    - `mytable` must be the same schema and have the same partitions as the Delta table.
    - The set of `PARTITIONED BY` columns must be distinct from the set of non-partitioned columns. Furthermore, you cannot specify partitioned columns with `AS <select-statement>`.


- You cannot use this table definition in <AS>; it can be used only by <PrestoAnd>.

   The tool you use to run the command depends on whether <AS> and <PrestoOr> use the same Hive metastore.

   - **Same metastore**: If both <AS> and <PrestoOr> use the same Hive metastore, you can define the table using <AS>.

- **Different metastores**: If <AS> and <PrestoOr> use different metastores, you must define the table using other tools.
  - Athena: You can define the external table in Athena.
  - Presto: Presto does not support the syntax `CREATE EXTERNAL TABLE ... STORED AS ...`, so you must use another tool (for example, Spark or Hive) connected to the same metastore as Presto to create the table.


#. If the Delta table is partitioned, run `MSCK REPAIR TABLE mytable` after generating the manifests to force the metastore (connected to <PrestoOr>) to discover the partitions. This is needed because the manifest of a partitioned table is itself partitioned in the same directory structure as the table. Run this command using _the same tool_ used to create the table. Furthermore, you should run this command:
   - **After every manifest generation**: New partitions are likely to be visible immediately after the manifest files have been updated. However, doing this too frequently can cause high load for the Hive metastore.
   - **As frequently as new partitions are expected**: For example, if a table is partitioned by date, then you can run repair once after every midnight, after the new partition has been created in the table and its corresponding manifest files have been generated.

### Step 3: Update manifests

When the data in a Delta table is updated you must regenerate the manifests using either of the following approaches:

- **Update explicitly**: After all the data updates, you can run the `generate` operation to update the manifests.
- **Update automatically**: You can configure a Delta table so that all write operations on the table automatically update the manifests. To enable this automatic mode, set the corresponding table property using the following SQL command.

  ```sql
  ALTER TABLE delta.`<path-to-delta-table>` SET TBLPROPERTIES(delta.compatibility.symlinkFormatManifest.enabled=true)
  ```

  To disable this automatic mode, set this property to `false`. In addition, for partitioned tables, you have to run `MSCK REPAIR` to ensure the metastore connected to <PrestoOr> to update partitions.

  .. note::
    After enabling automatic mode on a partitioned table, each write operation updates only manifests corresponding to the partitions that operation wrote to. This incremental update ensures that the overhead of manifest generation is low for write operations. However, this also means that if the manifests in other partitions are stale, enabling automatic mode will not automatically fix it. Therefore, it is recommended that you explicitly run `GENERATE` to update manifests for the entire table immediately after enabling automatic mode.

Whether to update automatically or explicitly depends on the concurrent nature of write operations on the Delta table and the desired data consistency. For example, if automatic mode is enabled, concurrent write operations lead to concurrent overwrites to the manifest files. With such unordered writes, the manifest files are not guaranteed to point to the latest version of the table after the write operations complete. Hence, if concurrent writes are expected and you want to avoid stale manifests, you should consider explicitly updating the manifest after the expected write operations have completed.

## Limitations

The <PrestoAnd> integration has known limitations in its behavior.

### Data consistency

Whenever <Delta> generates updated manifests, it atomically overwrites existing manifest files. Therefore, <PrestoAnd> will always see a consistent view of the data files; it will see all of the old version files or all of the new version files. However, the granularity of the consistency guarantees depends on whether or not the table is partitioned.

- **Unpartitioned tables**: All the files names are written in one manifest file which is updated atomically. In this case <PrestoAnd> will see full table snapshot consistency.

- **Partitioned tables**: A manifest file is partitioned in the same Hive-partitioning-style directory structure as the original Delta table. This means that each partition is updated atomically, and <PrestoOr> will see a consistent view of each partition but not a consistent view across partitions. Furthermore, since all manifests of all partitions cannot be updated together, concurrent attempts to generate manifests can lead to different partitions having manifests of different versions. While this consistency guarantee under data change is weaker than that of reading Delta tables with Spark, it is still stronger than formats like Parquet as they do not provide partition-level consistency.

Depending on what storage system you are using for Delta tables, it is possible to get incorrect results when <PrestoOr> concurrently queries the manifest while the manifest files are being rewritten. In file system implementations that lack atomic file overwrites, a manifest file may be momentarily unavailable. Hence, use manifests with caution if their updates are likely to coincide with queries from <PrestoOr>.

### Performance

Very large numbers of files can hurt the performance of <PrestoAnd>. Hence it is recommended that you [compact the files](best-practices.md#delta-compact-files) of the table before generating the manifests. The number of files should not exceed 1000 (for the entire unpartitioned table or for each partition in a partitioned table).

### Schema evolution

<Delta> supports schema evolution and queries on a Delta table automatically use the latest schema regardless of the schema defined in the table in the Hive metastore. However, <PrestoOr> uses the schema defined in the Hive metastore and will not query with the updated schema until the table used by <PrestoOr> is redefined to have the updated schema.



### Encrypted tables

Athena does not support reading manifests from [CSE-KMS](https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-emrfs-encryption-cse.html) encrypted tables. See the AWS documentation for the latest information.


.. <PrestoOr> replace:: Presto, Trino, or Athena
.. <PrestoAnd> replace:: Presto, Trino, and Athena
.. include:: /shared/replacements.md
