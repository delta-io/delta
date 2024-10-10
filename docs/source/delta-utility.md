---
description: Learn about <Delta> utility commands.
---

# Table utility commands

Delta tables support a number of utility commands.


For many <Delta> operations, you enable integration with Apache Spark DataSourceV2 and Catalog APIs (since 3.0) by setting configurations when you create a new `SparkSession`. See [_](delta-batch.md#sql-support).

.. contents:: In this article:
  :local:
  :depth: 2

<a id="delta-vacuum"></a>

## Remove files no longer referenced by a Delta table

You can remove files no longer referenced by a Delta table and are older than the retention
threshold by running the `vacuum` command  on the table. `vacuum` is not triggered automatically. The
default retention threshold for the files is 7 days. To change this behavior, see [_](delta-batch.md#data-retention).

.. important::
    - `vacuum` removes all files from directories not managed by Delta Lake, ignoring directories beginning with `_`. If you are storing additional metadata like Structured Streaming checkpoints within a Delta table directory, use a directory name such as `_checkpoints`.
    - `vacuum` deletes only data files, not log files. Log files are deleted automatically and asynchronously after checkpoint operations. The default retention period of log files is 30 days, configurable through the `delta.logRetentionDuration` property which you set with the `ALTER TABLE SET TBLPROPERTIES` SQL method. See [_](delta-batch.md#table-properties).
    - The ability to [time travel](delta-batch.md#deltatimetravel) back to a version older than the retention period is lost after running `vacuum`.

.. code-language-tabs::

  .. lang:: sql

    ```sql
    VACUUM eventsTable   -- vacuum files not required by versions older than the default retention period

    VACUUM '/data/events' -- vacuum files in path-based table

    VACUUM delta.`/data/events/`

    VACUUM delta.`/data/events/` RETAIN 100 HOURS  -- vacuum files not required by versions more than 100 hours old

    VACUUM eventsTable DRY RUN    -- do dry run to get the list of files to be deleted

    VACUUM eventsTable USING INVENTORY inventoryTable  —- vacuum files based on a provided reservoir of files as a delta table

    VACUUM eventsTable USING INVENTORY (select * from inventoryTable)  —- vacuum files based on a provided reservoir of files as spark SQL query
    ```

    See [_](delta-batch.md#sql-support) for the steps to enable support for SQL commands.

  .. lang:: python

    ```python
    from delta.tables import *

    deltaTable = DeltaTable.forPath(spark, pathToTable)  # path-based tables, or
    deltaTable = DeltaTable.forName(spark, tableName)    # Hive metastore-based tables

    deltaTable.vacuum()        # vacuum files not required by versions older than the default retention period

    deltaTable.vacuum(100)     # vacuum files not required by versions more than 100 hours old
    ```

  .. lang:: scala

    ```scala
    import io.delta.tables._

    val deltaTable = DeltaTable.forPath(spark, pathToTable)

    deltaTable.vacuum()        // vacuum files not required by versions older than the default retention period

    deltaTable.vacuum(100)     // vacuum files not required by versions more than 100 hours old
    ```

  .. lang:: java

      ```java
      import io.delta.tables.*;
      import org.apache.spark.sql.functions;

      DeltaTable deltaTable = DeltaTable.forPath(spark, pathToTable);

      deltaTable.vacuum();        // vacuum files not required by versions older than the default retention period

      deltaTable.vacuum(100);     // vacuum files not required by versions more than 100 hours old
      ```

.. note::
  When using `VACUUM`, to configure Spark to delete files in parallel (based on the number of shuffle partitions) set the session configuration `"spark.databricks.delta.vacuum.parallelDelete.enabled"` to `"true"` .

See the [_](delta-apidoc.md) for Scala, Java, and Python syntax details.

.. include:: /shared/delta-retention-warning.md

<Delta> has a safety check to prevent you from running a dangerous `VACUUM`
command. If you are certain that there are no operations being performed on
this table that take longer than the retention interval you plan to specify,
you can turn off this safety check by setting the Spark configuration property
`spark.databricks.delta.retentionDurationCheck.enabled` to `false`.

#### Inventory Table

An inventory table contains a list of file paths together with their size, type (directory or not), and the last modification time. When an INVENTORY option is provided, VACUUM will consider the files listed there instead of  doing the full listing of the table directory, which can be time consuming for very large tables. The inventory table can be specified as a delta table or a spark SQL query that gives the expected table schema. The schema should be as follows:

| Column Name      | Type    | Description                             |
| -----------------| ------- | --------------------------------------- |
| path             | string  | fully qualified uri                     |
| length           | integer | size in bytes                           |
| isDir            | boolean | boolean indicating if it is a directory |
| modificationTime | integer | file update time in milliseconds        |

<a id="delta-history"></a>

## Retrieve Delta table history

You can retrieve information on the operations, user, timestamp, and so on for each write to a Delta table
by running the `history` command. The operations are returned in reverse chronological order. By default table history is retained for 30 days.

.. code-language-tabs::

  .. lang:: sql

    ```sql
    DESCRIBE HISTORY '/data/events/'          -- get the full history of the table

    DESCRIBE HISTORY delta.`/data/events/`

    DESCRIBE HISTORY '/data/events/' LIMIT 1  -- get the last operation only

    DESCRIBE HISTORY eventsTable
    ```


    See [_](delta-batch.md#sql-support) for the steps to enable support for SQL commands in <AS>.

  .. lang:: python

    ```python
    from delta.tables import *

    deltaTable = DeltaTable.forPath(spark, pathToTable)

    fullHistoryDF = deltaTable.history()    # get the full history of the table

    lastOperationDF = deltaTable.history(1) # get the last operation
    ```

  .. lang:: scala

    ```scala
    import io.delta.tables._

    val deltaTable = DeltaTable.forPath(spark, pathToTable)

    val fullHistoryDF = deltaTable.history()    // get the full history of the table

    val lastOperationDF = deltaTable.history(1) // get the last operation
    ```

  .. lang:: java

    ```java
    import io.delta.tables.*;

    DeltaTable deltaTable = DeltaTable.forPath(spark, pathToTable);

    DataFrame fullHistoryDF = deltaTable.history();       // get the full history of the table

    DataFrame lastOperationDF = deltaTable.history(1);    // fetch the last operation on the DeltaTable
    ```

    See the [_](delta-apidoc.md) for Scala/Java/Python syntax details.

### History schema

The output of the `history` operation has the following columns.

| Column | Type | Description |
| --- | --- | ---|
| version | long | Table version generated by the operation. |
| timestamp | timestamp | When this version was committed. |
| userId | string | ID of the user that ran the operation. |
| userName | string | Name of the user that ran the operation. |
| operation | string | Name of the operation. |
| operationParameters | map | Parameters of the operation (for example, predicates.) |
| job | struct | Details of the job that ran the operation.  |
| notebook | struct | Details of notebook from which the operation was run. |
| clusterId | string | ID of the cluster on which the operation ran. |
| readVersion | long | Version of the table that was read to perform the write operation. |
| isolationLevel | string | Isolation level used for this operation. |
| isBlindAppend | boolean | Whether this operation appended data. |
| operationMetrics | map | Metrics of the operation (for example, number of rows and files modified.) |
| userMetadata | string | User-defined commit metadata if it was specified |

```
+-------+-------------------+------+--------+---------+--------------------+----+--------+---------+-----------+--------------+-------------+--------------------+
|version|          timestamp|userId|userName|operation| operationParameters| job|notebook|clusterId|readVersion|isolationLevel|isBlindAppend|    operationMetrics|
+-------+-------------------+------+--------+---------+--------------------+----+--------+---------+-----------+--------------+-------------+--------------------+
|      5|2019-07-29 14:07:47|  null|    null|   DELETE|[predicate -> ["(...|null|    null|     null|          4|  Serializable|        false|[numTotalRows -> ...|
|      4|2019-07-29 14:07:41|  null|    null|   UPDATE|[predicate -> (id...|null|    null|     null|          3|  Serializable|        false|[numTotalRows -> ...|
|      3|2019-07-29 14:07:29|  null|    null|   DELETE|[predicate -> ["(...|null|    null|     null|          2|  Serializable|        false|[numTotalRows -> ...|
|      2|2019-07-29 14:06:56|  null|    null|   UPDATE|[predicate -> (id...|null|    null|     null|          1|  Serializable|        false|[numTotalRows -> ...|
|      1|2019-07-29 14:04:31|  null|    null|   DELETE|[predicate -> ["(...|null|    null|     null|          0|  Serializable|        false|[numTotalRows -> ...|
|      0|2019-07-29 14:01:40|  null|    null|    WRITE|[mode -> ErrorIfE...|null|    null|     null|       null|  Serializable|         true|[numFiles -> 2, n...|
+-------+-------------------+------+--------+---------+--------------------+----+--------+---------+-----------+--------------+-------------+--------------------+
```

.. note::
  - Some of the columns may be nulls because the corresponding information may not be available in your environment.
  - Columns added in the future will always be added after the last column.

<a id="delta-history-metrics"></a>

### Operation metrics keys

The `history` operation returns a collection of operations metrics in the `operationMetrics` column map.


The following table lists the map key definitions by operation.

.. csv-table::
  :header: "Operation", "Metric name", "Description"

  "WRITE, CREATE TABLE AS SELECT, REPLACE TABLE AS SELECT, COPY INTO",,
  ,"numFiles", Number of files written.
  ,"numOutputBytes", Size in bytes of the written contents.
  ,"numOutputRows", Number of rows written.
  STREAMING UPDATE,,
  ,"numAddedFiles", Number of files added.
  ,"numRemovedFiles", Number of files removed.
  ,"numOutputRows", Number of rows written.
  ,"numOutputBytes", Size of write in bytes.
  DELETE,,
  ,"numAddedFiles", Number of files added. Not provided when partitions of the table are deleted.
  ,"numRemovedFiles", Number of files removed.
  ,"numDeletedRows", Number of rows removed. Not provided when partitions of the table are deleted.
  ,"numCopiedRows", Number of rows copied in the process of deleting files.
  ,"executionTimeMs", Time taken to execute the entire operation.
  ,"scanTimeMs", Time taken to scan the files for matches.
  ,"rewriteTimeMs", Time taken to rewrite the matched files.
  TRUNCATE,,
  ,"numRemovedFiles", Number of files removed.
  ,"executionTimeMs", Time taken to execute the entire operation.
  MERGE,,
  ,"numSourceRows", Number of rows in the source DataFrame.
  ,"numTargetRowsInserted", Number of rows inserted into the target table.
  ,"numTargetRowsUpdated", Number of rows updated in the target table.
  ,"numTargetRowsDeleted", Number of rows deleted in the target table.
  ,"numTargetRowsCopied", Number of target rows copied.
  ,"numOutputRows", Total number of rows written out.
  ,"numTargetFilesAdded", Number of files added to the sink(target).
  ,"numTargetFilesRemoved", Number of files removed from the sink(target).
  ,"executionTimeMs", Time taken to execute the entire operation.
  ,"scanTimeMs", Time taken to scan the files for matches.
  ,"rewriteTimeMs", Time taken to rewrite the matched files.
  UPDATE,,
  ,"numAddedFiles", Number of files added.
  ,"numRemovedFiles", Number of files removed.
  ,"numUpdatedRows", Number of rows updated.
  ,"numCopiedRows", Number of rows just copied over in the process of updating files.
  ,"executionTimeMs", Time taken to execute the entire operation.
  ,"scanTimeMs", Time taken to scan the files for matches.
  ,"rewriteTimeMs", Time taken to rewrite the matched files.
  FSCK,"numRemovedFiles", Number of files removed.
  CONVERT,"numConvertedFiles", Number of Parquet files that have been converted.
  OPTIMIZE,,
  ,"numAddedFiles", Number of files added.
  ,"numRemovedFiles", Number of files optimized.
  ,"numAddedBytes", Number of bytes added after the table was optimized.
  ,"numRemovedBytes", Number of bytes removed.
  ,"minFileSize", Size of the smallest file after the table was optimized.
  ,"p25FileSize", Size of the 25th percentile file after the table was optimized.
  ,"p50FileSize", Median file size after the table was optimized.
  ,"p75FileSize", Size of the 75th percentile file after the table was optimized.
  ,"maxFileSize", Size of the largest file after the table was optimized.
  VACUUM,,
  ,numDeletedFiles, Number of deleted files.
  ,numVacuumedDirectories, Number of vacuumed directories.
  ,numFilesToDelete, Number of files to delete.


.. csv-table::
  :header: "Operation", "Metric name", "Description"

  RESTORE,,
  ,"tableSizeAfterRestore", Table size in bytes after restore.
  ,"numOfFilesAfterRestore", Number of files in the table after restore.
  ,"numRemovedFiles", Number of files removed by the restore operation.
  ,"numRestoredFiles", Number of files that were added as a result of the restore.
  ,"removedFilesSize", Size in bytes of files removed by the restore.
  ,"restoredFilesSize", Size in bytes of files added by the restore.

<a id="delta-detail"></a>

## Retrieve Delta table details

You can retrieve detailed information about a Delta table (for example, number of files, data size) using `DESCRIBE DETAIL`.

.. code-language-tabs::

  .. lang:: sql

    ```sql
    DESCRIBE DETAIL '/data/events/'

    DESCRIBE DETAIL eventsTable
    ```


    See [_](delta-batch.md#sql-support) for the steps to enable support for SQL commands in <AS>.

  .. lang:: python

    ```python
    from delta.tables import *

    deltaTable = DeltaTable.forPath(spark, pathToTable)

    detailDF = deltaTable.detail()
    ```

  .. lang:: scala

    ```scala
    import io.delta.tables._

    val deltaTable = DeltaTable.forPath(spark, pathToTable)

    val detailDF = deltaTable.detail()
    ```

  .. lang:: java

    ```java
    import io.delta.tables.*;

    DeltaTable deltaTable = DeltaTable.forPath(spark, pathToTable);

    DataFrame detailDF = deltaTable.detail();
    ```

    See the [_](delta-apidoc.md) for Scala/Java/Python syntax details.

### Detail schema

The output of this operation has only one row with the following schema.

| Column | Type | Description |
| --- | --- | --- |
| format | string | Format of the table, that is, `delta`. |
| id | string | Unique ID of the table. |
| name | string | Name of the table as defined in the metastore. |
| description | string | Description of the table. |
| location | string | Location of the table. |
| createdAt | timestamp | When the table was created. |
| lastModified | timestamp | When the table was last modified.  |
| partitionColumns | array of strings | Names of the partition columns if the table is partitioned. |
| numFiles | long | Number of the files in the latest version of the table. |
| sizeInBytes | int | The size of the latest snapshot of the table in bytes. |
| properties | string-string map | All the properties set for this table. |
| minReaderVersion | int | Minimum version of readers (according to the log protocol) that can read the table. |
| minWriterVersion | int | Minimum version of writers (according to the log protocol) that can write to the table. |


```text
+------+--------------------+------------------+-----------+--------------------+--------------------+-------------------+----------------+--------+-----------+----------+----------------+----------------+
|format|                  id|              name|description|            location|           createdAt|       lastModified|partitionColumns|numFiles|sizeInBytes|properties|minReaderVersion|minWriterVersion|
+------+--------------------+------------------+-----------+--------------------+--------------------+-------------------+----------------+--------+-----------+----------+----------------+----------------+
| delta|d31f82d2-a69f-42e...|default.deltatable|       null|file:/Users/tuor/...|2020-06-05 12:20:...|2020-06-05 12:20:20|              []|      10|      12345|        []|               1|               2|
+------+--------------------+------------------+-----------+--------------------+--------------------+-------------------+----------------+--------+-----------+----------+----------------+----------------+
```

<a id="delta-generate"></a>

## Generate a manifest file

You can a generate manifest file for a Delta table that can be used by other processing engines (that is, other than <AS>) to read the Delta table. For example, to generate a manifest file that can be used by <PrestoAnd> to read a Delta table, you run the following:

.. code-language-tabs::

  .. lang:: sql

    ```sql
    GENERATE symlink_format_manifest FOR TABLE delta.`<path-to-delta-table>`
    ```

    See [_](delta-batch.md#sql-support) for the steps to enable support for SQL commands in <AS>.

  .. lang:: python

    ```python
    deltaTable = DeltaTable.forPath(<path-to-delta-table>)
    deltaTable.generate("symlink_format_manifest")
    ```

  .. lang:: scala

    ```scala
    val deltaTable = DeltaTable.forPath(<path-to-delta-table>)
    deltaTable.generate("symlink_format_manifest")
    ```

  .. lang:: java

    ```java
    DeltaTable deltaTable = DeltaTable.forPath(<path-to-delta-table>);
    deltaTable.generate("symlink_format_manifest");
    ```

<a id="convert-to-delta"></a>

## Convert a Parquet table to a Delta table

Convert a Parquet table to a Delta table in-place. This command lists all the files in the directory, creates a <Delta> transaction log that tracks these files, and automatically infers the data schema by reading the footers of all Parquet files. If your data is partitioned, you must specify the schema of the partition columns as a DDL-formatted string (that is, `<column-name1> <type>, <column-name2> <type>, ...`).

By default, this command will collect per-file statistics (e.g. minimum and maximum values for each column). These statistics will be used at query time to provide faster queries. You can disable this statistics collection in the SQL API using `NO STATISTICS`.

.. note:: If a Parquet table was created by Structured Streaming, the listing of files can be avoided by using the `_spark_metadata` sub-directory as the source of truth for files contained in the table setting the SQL configuration `spark.databricks.delta.convert.useMetadataLog` to `true`.

.. code-language-tabs::

  .. lang:: sql

    ```sql
    -- Convert unpartitioned Parquet table at path '<path-to-table>'
    CONVERT TO DELTA parquet.`<path-to-table>`

    -- Convert unpartitioned Parquet table and disable statistics collection
    CONVERT TO DELTA parquet.`<path-to-table>` NO STATISTICS

    -- Convert partitioned Parquet table at path '<path-to-table>' and partitioned by integer columns named 'part' and 'part2'
    CONVERT TO DELTA parquet.`<path-to-table>` PARTITIONED BY (part int, part2 int)

    -- Convert partitioned Parquet table and disable statistics collection
    CONVERT TO DELTA parquet.`<path-to-table>` NO STATISTICS PARTITIONED BY (part int, part2 int)
    ```

    See [_](delta-batch.md#sql-support) for the steps to enable support for SQL commands in <AS>.

  .. lang:: python

    ```python
    from delta.tables import *

    # Convert unpartitioned Parquet table at path '<path-to-table>'
    deltaTable = DeltaTable.convertToDelta(spark, "parquet.`<path-to-table>`")

    # Convert partitioned parquet table at path '<path-to-table>' and partitioned by integer column named 'part'
    partitionedDeltaTable = DeltaTable.convertToDelta(spark, "parquet.`<path-to-table>`", "part int")
    ```

  .. lang:: scala

    ```scala
    import io.delta.tables._

    // Convert unpartitioned Parquet table at path '<path-to-table>'
    val deltaTable = DeltaTable.convertToDelta(spark, "parquet.`<path-to-table>`")

    // Convert partitioned Parquet table at path '<path-to-table>' and partitioned by integer columns named 'part' and 'part2'
    val partitionedDeltaTable = DeltaTable.convertToDelta(spark, "parquet.`<path-to-table>`", "part int, part2 int")
    ```

  .. lang:: java

    ```java
    import io.delta.tables.*;

    // Convert unpartitioned Parquet table at path '<path-to-table>'
    DeltaTable deltaTable = DeltaTable.convertToDelta(spark, "parquet.`<path-to-table>`");

    // Convert partitioned Parquet table at path '<path-to-table>' and partitioned by integer columns named 'part' and 'part2'
    DeltaTable deltaTable = DeltaTable.convertToDelta(spark, "parquet.`<path-to-table>`", "part int, part2 int");
    ```

.. note:: Any file not tracked by <Delta> is invisible and can be deleted when you run `vacuum`. You should avoid updating or appending data files during the conversion process. After the table is converted, make sure all writes go through <Delta>.

<a id="convert-iceberg-to-delta"></a>

## Convert an Iceberg table to a Delta table

.. note::

  It is available from Delta Lake 2.3 and above.

You can convert an Iceberg table to a Delta table in place if the underlying file format of the Iceberg table is Parquet. Similar to a conversion from a Parquet table, the conversion is in-place and there won't be any data copy or data rewrite. The original Iceberg table and the converted Delta table have separate history, so modifying the Delta table should not affect the Iceberg table as long as the source data Parquet files are not touched or deleted.

The following command creates a <Delta> transaction log based on the Iceberg table's native file manifest, schema and partitioning information. The converter also collects column stats during the conversion, unless `NO STATISTICS` is specified.

```sql
-- Convert the Iceberg table in the path <path-to-table>.
CONVERT TO DELTA iceberg.`<path-to-table>`

-- Convert the Iceberg table in the path <path-to-table> without collecting statistics.
CONVERT TO DELTA iceberg.`<path-to-table>` NO STATISTICS
```

.. important:: 

  An additional jar `delta-iceberg` is needed to use the converter. For example, `bin/spark-sql --packages io.delta:delta-spark_2.12:3.0.0,io.delta:delta-iceberg_2.12:3.0.0:...`.

  `delta-iceberg` is currently not available for the <Delta> 2.4.0 release since `iceberg-spark-runtime` does not support Spark 3.4 yet. It is available for <Delta> 2.3.0.

.. note::

  - Converting Iceberg metastore tables is not supported.

  - Converting Iceberg tables that have experienced [partition evolution](https://iceberg.apache.org/docs/latest/evolution/#partition-evolution) is not supported.

  - Converting Iceberg merge-on-read tables that have experienced updates, deletions, or merges is not supported.

## Convert a Delta table to a Parquet table

You can easily convert a Delta table back to a Parquet table using the following steps:

1. If you have performed <Delta> operations that can change the data files (for example, `delete` or `merge`), run [vacuum](#delta-vacuum) with retention of 0 hours to delete all data files that do not belong to the latest version of the table.
#. Delete the `_delta_log` directory in the table directory.

<a id="restore-delta-table"></a>

## Restore a Delta table to an earlier state

You can restore a Delta table to its earlier state by using the `RESTORE` command. A Delta table internally maintains historic versions of the table that enable it to be restored to an earlier state.
A version corresponding to the earlier state or a timestamp of when the earlier state was created are supported as options by the `RESTORE` command.

.. important::
  - You can restore an already restored table.

  - Restoring a table to an older version where the data files were deleted manually or by `vacuum` will fail. Restoring to this version partially is still possible if `spark.sql.files.ignoreMissingFiles` is set to `true`.

  - The timestamp format for restoring to an earlier state is `yyyy-MM-dd HH:mm:ss`. Providing only a date(`yyyy-MM-dd`) string is also supported.

.. code-language-tabs::

  ```sql
  RESTORE TABLE db.target_table TO VERSION AS OF <version>
  RESTORE TABLE delta.`/data/target/` TO TIMESTAMP AS OF <timestamp>
  ```

  ```python
  from delta.tables import *

  deltaTable = DeltaTable.forPath(spark, <path-to-table>)  # path-based tables, or
  deltaTable = DeltaTable.forName(spark, <table-name>)    # Hive metastore-based tables

  deltaTable.restoreToVersion(0) # restore table to oldest version

  deltaTable.restoreToTimestamp('2019-02-14') # restore to a specific timestamp
  ```

  ```scala
  import io.delta.tables._

  val deltaTable = DeltaTable.forPath(spark, <path-to-table>)
  val deltaTable = DeltaTable.forName(spark, <table-name>)

  deltaTable.restoreToVersion(0) // restore table to oldest version

  deltaTable.restoreToTimestamp("2019-02-14") // restore to a specific timestamp
  ```

  ```java
  import io.delta.tables.*;

  DeltaTable deltaTable = DeltaTable.forPath(spark, <path-to-table>);
  DeltaTable deltaTable = DeltaTable.forName(spark, <table-name>);

  deltaTable.restoreToVersion(0) // restore table to oldest version

  deltaTable.restoreToTimestamp("2019-02-14") // restore to a specific timestamp
  ```

.. important::

   Restore is considered a data-changing operation. <Delta> log entries added by the `RESTORE` command contain [dataChange](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#add-file-and-remove-file) set to true. If there is a downstream application, such as a [Structured streaming](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html) job that processes the updates to a <Delta> table, the data change log entries added by the restore operation are considered as new data updates, and processing them may result in duplicate data.

   For example:

   .. csv-table::
      :header: "Table version","Operation","Delta log updates","Records in data change log updates"

      "0","INSERT","AddFile(/path/to/file-1, dataChange = true)","(name = Viktor, age = 29, (name = George, age = 55)"
      "1","INSERT","AddFile(/path/to/file-2, dataChange = true)","(name = George, age = 39)"
      "2","OPTIMIZE","AddFile(/path/to/file-3, dataChange = false), RemoveFile(/path/to/file-1), RemoveFile(/path/to/file-2)","(No records as Optimize compaction does not change the data in the table)"
      "3","RESTORE(version=1)","RemoveFile(/path/to/file-3), AddFile(/path/to/file-1, dataChange = true), AddFile(/path/to/file-2, dataChange = true)","(name = Viktor, age = 29), (name = George, age = 55), (name = George, age = 39)"

   In the preceding example, the `RESTORE` command results in updates that were already seen when reading the Delta table version 0 and 1. If a streaming query was reading this table, then these files will be considered as newly added data and will be processed again.

### Restore metrics

`RESTORE` reports the following metrics as a single row DataFrame once the operation is complete:

- `table_size_after_restore`: The size of the table after restoring.
- `num_of_files_after_restore`: The number of files in the table after restoring.
- `num_removed_files`: Number of files removed (logically deleted) from the table.
- `num_restored_files`: Number of files restored due to rolling back.
- `removed_files_size`: Total size in bytes of the files that are removed from the table.
- `restored_files_size`: Total size in bytes of the files that are restored.

  ![Restore metrics example](/_static/images/delta/restore-metrics.png)

<a id="clone-delta-table"></a>

.. contents:: In this section:
    :local:
    :depth: 1

## Shallow clone a Delta table

.. note::

  It is available from Delta Lake 2.3 and above.

You can create a shallow copy of an existing Delta table at a specific version using the `shallow clone` command.

Any changes made to shallow clones affect only the clones themselves and not the source table, as long as they don't touch the source data Parquet files.

The metadata that is cloned includes: schema, partitioning information, invariants, nullability. For shallow clones, stream metadata is not cloned. Metadata not cloned are the table description and [user-defined commit metadata](/delta-batch.md#set-user-defined-commit-metadata).

.. important::
  - Shallow clones reference data files in the source directory. If you run `vacuum` on the source table, clients will no longer be able to read the referenced data files and a `FileNotFoundException` will be thrown. In this case, running clone with `replace` over the shallow clone will repair the clone.

  - If a target already has a non-Delta table at that path, cloning with `replace` to that target will create a Delta log. Then, you can clean up any existing data by running `vacuum`.

  - If a Delta table exists in the target path, a new commit is created that includes the new metadata and new data from the source table. In the case of `replace`, the target table needs to be emptied first to avoid data duplication.

  - Cloning a table is not the same as `Create Table As Select` or `CTAS`. A shallow clone takes the metadata of the source table. Cloning also has simpler syntax: you don't need to specify partitioning, format, invariants, nullability and so on as they are taken from the source table.

  - A cloned table has an independent history from its source table. Time travel queries on a cloned table will not work with the same inputs as they work on its source table. For example, if the source table was at version 100 and we are creating a new table by cloning it, the new table will have version 0, and therefore we could not run time travel queries on the new table such as `SELECT * FROM tbl AS OF VERSION 99`.

```sql
CREATE TABLE delta.`/data/target/` SHALLOW CLONE delta.`/data/source/` -- Create a shallow clone of /data/source at /data/target

CREATE OR REPLACE TABLE db.target_table SHALLOW CLONE db.source_table -- Replace the target. target needs to be emptied

CREATE TABLE IF NOT EXISTS delta.`/data/target/` SHALLOW CLONE db.source_table -- No-op if the target table exists

CREATE TABLE db.target_table SHALLOW CLONE delta.`/data/source`

CREATE TABLE db.target_table SHALLOW CLONE delta.`/data/source` VERSION AS OF version

CREATE TABLE db.target_table SHALLOW CLONE delta.`/data/source` TIMESTAMP AS OF timestamp_expression -- timestamp can be like “2019-01-01” or like date_sub(current_date(), 1)
```

### Clone metrics

`CLONE` reports the following metrics as a single row DataFrame once the operation is complete:

- `source_table_size`: Size of the source table that's being cloned in bytes.
- `source_num_of_files`: The number of files in the source table.

#### Cloud provider permissions

If you have created a shallow clone, any user that reads the shallow clone needs permission to read the files in the original table, since the data files remain in the source table's directory where we cloned from. To make changes to the clone, users will need write access to the clone's directory.

### Clone use cases

#### Machine learning flow reproduction

When doing machine learning, you may want to archive a certain version of a table on which you trained an ML model. Future models can be tested using this archived data set.

```sql
-- Trained model on version 15 of Delta table
CREATE TABLE delta.`/model/dataset` SHALLOW CLONE entire_dataset VERSION AS OF 15
```

#### Short-term experiments on a production table

To test a workflow on a production table without corrupting the table, you can easily create a shallow clone. This allows you to run arbitrary workflows on the cloned table that contains all the production data but does not affect any production workloads.

```sql
-- Perform shallow clone
CREATE OR REPLACE TABLE my_test SHALLOW CLONE my_prod_table;

UPDATE my_test WHERE user_id is null SET invalid=true;
-- Run a bunch of validations. Once happy:

-- This should leverage the update information in the clone to prune to only
-- changed files in the clone if possible
MERGE INTO my_prod_table
USING my_test
ON my_test.user_id <=> my_prod_table.user_id
WHEN MATCHED AND my_test.user_id is null THEN UPDATE *;

DROP TABLE my_test;
```

#### Table property overrides

Table property overrides are particularly useful for:

- Annotating tables with owner or user information when sharing data with different business units.
- Archiving Delta tables and time travel is required. You can specify the log retention period independently for the archive table. For example:

```sql
CREATE OR REPLACE TABLE archive.my_table SHALLOW CLONE prod.my_table
TBLPROPERTIES (
  delta.logRetentionDuration = '3650 days',
  delta.deletedFileRetentionDuration = '3650 days'
)
LOCATION 'xx://archive/my_table'
```

## Clone Parquet or Iceberg table to Delta

.. note:: It is available from Delta Lake 2.3 and above.

Shallow clone for Parquet and Iceberg combines functionality used to clone Delta tables and convert tables to Delta Lake. You can use shallow clone functionality to convert data from Parquet or Iceberg data sources to managed or external Delta tables with the same basic syntax.

`replace` has the same limitation as Delta shallow clone, the target table must be emptied before applying replace.

```sql
CREATE OR REPLACE TABLE <target_table_name> SHALLOW CLONE parquet.`/path/to/data`;

CREATE OR REPLACE TABLE <target_table_name> SHALLOW CLONE iceberg.`/path/to/data`;
```

.. <PrestoAnd> replace:: Presto and Athena

.. <Delta> replace:: Delta Lake
.. <AS> replace:: Apache Spark