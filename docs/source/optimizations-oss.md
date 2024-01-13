---
description: Learn about the optimizations available with Delta Lake.
orphan: 1
---

# Optimizations

Delta Lake provides optimizations that accelerate data lake operations.

## Optimize performance with file management

To improve query speed, Delta Lake supports the ability to optimize the layout of data in storage. There are various ways to optimize the layout.

<a id="delta-optimize"></a>

### Compaction (bin-packing)

.. note:: This feature is available in <Delta> 1.2.0 and above.

Delta Lake can improve the speed of read queries from a table by coalescing small files into larger ones.

.. code-language-tabs::

  .. lang:: sql

    ```sql
    OPTIMIZE '/path/to/delta/table' -- Optimizes the path-based Delta Lake table

    OPTIMIZE delta_table_name;

    OPTIMIZE delta.`/path/to/delta/table`;

    -- If you have a large amount of data and only want to optimize a subset of it, you can specify an optional partition predicate using `WHERE`:
    OPTIMIZE delta_table_name WHERE date >= '2017-01-01'
    ```

  .. lang:: python

     ```python
     from delta.tables import *

     deltaTable = DeltaTable.forPath(spark, pathToTable)  # For path-based tables
     # For Hive metastore-based tables: deltaTable = DeltaTable.forName(spark, tableName)

     deltaTable.optimize().executeCompaction()

     # If you have a large amount of data and only want to optimize a subset of it, you can specify an optional partition predicate using `where`
     deltaTable.optimize().where("date='2021-11-18'").executeCompaction()
     ```

  .. lang:: scala

     ```scala
     import io.delta.tables._

     val deltaTable = DeltaTable.forPath(spark, pathToTable)  // For path-based tables
     // For Hive metastore-based tables: val deltaTable = DeltaTable.forName(spark, tableName)

     deltaTable.optimize().executeCompaction()

     // If you have a large amount of data and only want to optimize a subset of it, you can specify an optional partition predicate using `where`
     deltaTable.optimize().where("date='2021-11-18'").executeCompaction()
     ```

For Scala, Java, and Python API syntax details, see the [_](delta-apidoc.md).\

.. note::
  - Bin-packing optimization is _idempotent_, meaning that if it is run twice on the same dataset, the second run has no effect.
  - Bin-packing aims to produce evenly-balanced data files with respect to their size on disk, but not necessarily number of tuples per file. However, the two measures are most often correlated.
  - Python and Scala APIs for executing `OPTIMIZE` operation are available from <Delta> 2.0 and above.
  - Set Spark session configuration `spark.databricks.delta.optimize.repartition.enabled=true` to use `repartition(1)` instead of `coalesce(1)` for better performance when compacting many small files.

Readers of Delta tables use snapshot isolation, which means that they are not interrupted when `OPTIMIZE` removes unnecessary files from the transaction log. `OPTIMIZE` makes no data related changes to the table, so a read before and after an `OPTIMIZE` has the same results. Performing `OPTIMIZE` on a table that is a streaming source does not affect any current or future streams that treat this table as a source. `OPTIMIZE` returns the file statistics (min, max, total, and so on) for the files removed and the files added by the operation. Optimize stats also contains the number of batches, and partitions optimized.

## Data skipping

.. note:: This feature is available in <Delta> 1.2.0 and above.

Data skipping information is collected automatically when you write data into a <Delta> table. <Delta> takes advantage of this information (minimum and maximum values for each column) at query time to provide faster queries. You do not need to configure data skipping; the feature is activated whenever applicable. However, its effectiveness depends on the layout of your data. For best results, apply [Z-Ordering](#z-ordering-multi-dimensional-clustering).

Collecting statistics on a column containing long values such as `string` or `binary` is an expensive operation. To avoid collecting statistics on such columns you can configure the [table property](/delta-batch.md#table-properties) `delta.dataSkippingNumIndexedCols`. This property indicates the position index of a column in the table's schema. All columns with a position index less than the `delta.dataSkippingNumIndexedCols` property will have statistics collected. For the purposes of collecting statistics, each field within a nested column is considered as an individual column. To avoid collecting statistics on columns containing long values, either set the `delta.dataSkippingNumIndexedCols` property so that the long value columns are after this index in the table's schema, or move columns containing long strings to an index position greater than the `delta.dataSkippingNumIndexedCols` property by using [ALTER TABLE ALTER COLUMN](https://spark.apache.org/docs/latest/sql-ref-syntax-ddl-alter-table.html#alter-or-change-column).

## Z-Ordering (multi-dimensional clustering)

.. note:: This feature is available in <Delta> 2.0.0 and above.

Z-Ordering is a [technique](https://en.wikipedia.org/wiki/Z-order_curve) to colocate related information in the same set of files. This co-locality is automatically used by <Delta> in data-skipping algorithms. This behavior dramatically reduces the amount of data that <Delta> on <AS> needs to read. To Z-Order data, you specify the columns to order on in the `ZORDER BY` clause:

.. code-language-tabs::

  .. lang:: sql

    ```sql
    OPTIMIZE events ZORDER BY (eventType)

    -- If you have a large amount of data and only want to optimize a subset of it, you can specify an optional partition predicate by using "where".
    OPTIMIZE events WHERE date = '2021-11-18' ZORDER BY (eventType)
    ```

  .. lang:: python

     ```python
     from delta.tables import *

     deltaTable = DeltaTable.forPath(spark, pathToTable)  # path-based table
     # For Hive metastore-based tables: deltaTable = DeltaTable.forName(spark, tableName)

     deltaTable.optimize().executeZOrderBy(eventType)

     # If you have a large amount of data and only want to optimize a subset of it, you can specify an optional partition predicate using `where`
     deltaTable.optimize().where("date='2021-11-18'").executeZOrderBy(eventType)
     ```

  .. lang:: scala

     ```scala
     import io.delta.tables._

     val deltaTable = DeltaTable.forPath(spark, pathToTable)  // path-based table
     // For Hive metastore-based tables: val deltaTable = DeltaTable.forName(spark, tableName)

     deltaTable.optimize().executeZOrderBy(eventType)

     // If you have a large amount of data and only want to optimize a subset of it, you can specify an optional partition predicate by using "where".
     deltaTable.optimize().where("date='2021-11-18'").executeZOrderBy(eventType)
     ```

For Scala, Java, and Python API syntax details, see the [_](delta-apidoc.md)

If you expect a column to be commonly used in query predicates and if that column has high cardinality (that is, a large number of distinct values), then use `ZORDER BY`.

You can specify multiple columns for `ZORDER BY` as a comma-separated list. However, the effectiveness of the locality drops with each extra column. Z-Ordering on columns that do not have statistics collected on them would be ineffective and a waste of resources. This is because data skipping requires column-local stats such as min, max, and count. You can configure statistics collection on certain columns by reordering columns in the schema, or you can increase the number of columns to collect statistics on. See [_](#data-skipping).

.. note::

  - Z-Ordering is _not idempotent_. Everytime the Z-Ordering is executed it will try to create a new clustering of data in all files (new and existing files that were part of previous Z-Ordering) in a partition.
  - Z-Ordering aims to produce evenly-balanced data files with respect to the number of tuples, but not necessarily data size on disk. The two measures are most often correlated, but there can be situations when that is not the case, leading to skew in optimize task times.

    For example, if you `ZORDER BY` _date_ and your most recent records are all much wider (for example longer arrays or string values) than the ones in the past, it is expected that the `OPTIMIZE` job's task durations will be skewed, as well as the resulting file sizes. This is, however, only a problem for the `OPTIMIZE` command itself; it should not have any negative impact on subsequent queries.

## Multi-part checkpointing

.. note:: This feature is available in <Delta> 2.0.0 and above. This feature is in experimental support mode.

<Delta> table periodically and automatically compacts all the incremental updates to the Delta log into a Parquet file. This "checkpointing" allows read queries to quickly reconstruct the current state of the table (that is, which files to process, what is the current schema) without reading too many files having incremental updates.

<Delta> protocol allows [splitting the checkpoint](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#checkpoints) into multiple Parquet files. This parallelizes and speeds up writing the checkpoint. In <Delta>, by default each checkpoint is written as a single Parquet file. To to use this feature, set the SQL configuration `spark.databricks.delta.checkpoint.partSize=<n>`, where `n` is the limit of number of actions (such as `AddFile`) at which <Delta> on <AS>  will start parallelizing the checkpoint and attempt to write a maximum of this many actions per checkpoint file.

.. note:: This feature requires no reader side configuration changes. The existing reader already supports reading a checkpoint with multiple files.



## Log compactions

.. note:: This feature is available in <Delta> 3.0.0 and above.

<Delta> protocol allows new log compaction files with the format `<x>.<y>.compact.json`. These files contain the aggregated actions for commit range `[x, y]`. Log compactions reduce the need for frequent checkpoints and minimize the latency spikes caused by them.

The read support for the log compaction files is available in <Delta> 3.0.0 and above. It is enabled by default and can be disabled using the SQL conf `spark.databricks.delta.deltaLog.minorCompaction.useForReads=<value>` where `value` can be `true/false`. The write support for the log compaction will be added in a future version of Delta.


## Optimized Write

.. note:: This feature is available in <Delta> 3.1.0 and above.

Optimized writes improve file size as data is written and benefit subsequent reads on the table.

Optimized writes are most effective for partitioned tables, as they reduce the number of small files written to each partition. Writing fewer large files is more efficient than writing many small files, but you might still see an increase in write latency because data is shuffled before being written.

The following image demonstrates how optimized writes works:

![Optimized writes](/_static/images/delta/optimized-writes.png)

.. note:: You might have code that runs coalesce(n) or repartition(n) just before you write out your data to control the number of files written. Optimized writes eliminates the need to use this pattern.

The optimized write feature is **disabled** by default. It can be enabled at the table, SQL session, and/or DataFrameWriter level using the following settings (in order of precedence from low to high):

* The `delta.autoOptimize.optimizeWrite` table property (default=None);
* The `spark.databricks.delta.optimizeWrite.enabled` SQL configuration (default=None);
* The DataFrameWriter option `optimizeWrite` (default=None).

Besides the above, the following advanced SQL configurations can be used to further fine-tune the number and size of files written:

* `spark.databricks.delta.optimizeWrite.binSize` (default=512MiB), which controls the target in-memory size of each output file;
* `spark.databricks.delta.optimizeWrite.numShuffleBlocks` (default=50,000,000), which controls "maximum number of shuffle blocks to target";
* `spark.databricks.delta.optimizeWrite.maxShufflePartitions` (default=2,000), which controls "max number of output buckets (reducers) that can be used by optimized writes".


.. include:: /shared/replacements.md
