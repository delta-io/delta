---
description: Learn best practices when using <Delta>.
---

# <Title>



.. <Title> replace:: Best practices

This article describes best practices when using <Delta>.

## Choose the right partition column

You can partition a Delta table by a column. The most commonly used partition column is `date`.
Follow these two rules of thumb for deciding on what column to partition by:

* If the cardinality of a column will be very high, do not use that column for partitioning. For example, if you partition by a column `userId` and if there can be 1M distinct user IDs, then that is a bad partitioning strategy.
* Amount of data in each partition: You can partition by a column if you expect data in that partition to be at least 1 GB.

<a id="delta-compact-files"></a>

## Compact files

If you continuously write data to a Delta table, it will over time accumulate a large number of files, especially if you add data in small batches. This can have an adverse effect on the efficiency of table reads, and it can also affect the performance of your file system. Ideally, a large number of small files should be rewritten into a smaller number of larger files on a regular basis. This is known as compaction.

You can compact a table by repartitioning it to smaller number of files. In addition, you can specify the option `dataChange` to be `false` indicates that the operation does not change the data, only rearranges the data layout. This would ensure that other concurrent operations are minimally affected due to this compaction operation.

For example, you can compact a table into 16 files:

.. code-language-tabs::

  ```scala
  val path = "..."
  val numFiles = 16

  spark.read
   .format("delta")
   .load(path)
   .repartition(numFiles)
   .write
   .option("dataChange", "false")
   .format("delta")
   .mode("overwrite")
   .save(path)
  ```

  ```python
  path = "..."
  numFiles = 16

  (spark.read
   .format("delta")
   .load(path)
   .repartition(numFiles)
   .write
   .option("dataChange", "false")
   .format("delta")
   .mode("overwrite")
   .save(path))
  ```

If your table is partitioned and you want to repartition just one partition based on a predicate, you can read only the partition using `where` and write back to that using `replaceWhere`:

.. code-language-tabs::

  ```scala
  val path = "..."
  val partition = "year = '2019'"
  val numFilesPerPartition = 16

  spark.read
   .format("delta")
   .load(path)
   .where(partition)
   .repartition(numFilesPerPartition)
   .write
   .option("dataChange", "false")
   .format("delta")
   .mode("overwrite")
   .option("replaceWhere", partition)
   .save(path)
  ```

  ```python
  path = "..."
  partition = "year = '2019'"
  numFilesPerPartition = 16

  (spark.read
   .format("delta")
   .load(path)
   .where(partition)
   .repartition(numFilesPerPartition)
   .write
   .option("dataChange", "false")
   .format("delta")
   .mode("overwrite")
   .option("replaceWhere", partition)
   .save(path))
  ```

.. warning:: Using `dataChange = false` on an operation that changes data can corrupt the data in the table.

.. note::
  This operation does not remove the old files. To remove them, run the [VACUUM](delta-utility.md#delta-vacuum) command.

<a id="delta-replace-table"></a>

## Replace the content or schema of a table

Sometimes you may want to replace a Delta table. For example:

- You discover the data in the table is incorrect and want to replace the content.
- You want to rewrite the whole table to do incompatible schema changes (such as changing column types).

While you can delete the entire directory of a Delta table and create a new table on the same path, it's *not recommended* because:

- Deleting a directory is not efficient. A directory containing very large files can take hours or even days to delete.
- You lose all of content in the deleted files; it's hard to recover if you delete the wrong table.
- The directory deletion is not atomic. While you are deleting the table a concurrent query reading the table can fail or see a partial table.

If you don't need to change the table schema, you can [delete](delta-update.md#delete-from-a-table) data from a Delta table and insert your new data, or [update](delta-update.md#update-a-table) the table to fix the incorrect values.

If you want to change the table schema, you can replace the whole table atomically. For example:

.. code-language-tabs::

  .. lang:: python

    ```python
    dataframe.write \
      .format("delta") \
      .mode("overwrite") \
      .option("overwriteSchema", "true") \
      .partitionBy(<your-partition-columns>) \
      .saveAsTable("<your-table>") # Managed table
    dataframe.write \
      .format("delta") \
      .mode("overwrite") \
      .option("overwriteSchema", "true") \
      .option("path", "<your-table-path>") \
      .partitionBy(<your-partition-columns>) \
      .saveAsTable("<your-table>") # External table
    ```

  .. lang:: sql

    ```sql
    REPLACE TABLE <your-table> USING DELTA PARTITIONED BY (<your-partition-columns>) AS SELECT ... -- Managed table
    REPLACE TABLE <your-table> USING DELTA PARTITIONED BY (<your-partition-columns>) LOCATION "<your-table-path>" AS SELECT ... -- External table
    ```

  .. lang:: scala

    ```scala
    dataframe.write
      .format("delta")
      .mode("overwrite")
      .option("overwriteSchema", "true")
      .partitionBy(<your-partition-columns>)
      .saveAsTable("<your-table>") // Managed table
    dataframe.write
      .format("delta")
      .mode("overwrite")
      .option("overwriteSchema", "true")
      .option("path", "<your-table-path>")
      .partitionBy(<your-partition-columns>)
      .saveAsTable("<your-table>") // External table
    ```

There are multiple benefits with this approach:

- Overwriting a table is much faster because it doesn't need to list the directory recursively or delete any files.
- The old version of the table still exists. If you delete the wrong table you can easily retrieve the old data using [Time Travel](delta-batch.md#query-an-older-snapshot-of-a-table-time-travel).
- It's an atomic operation. Concurrent queries can still read the table while you are deleting the table.
- Because of <Delta> ACID transaction guarantees, if overwriting the table fails, the table will be in its previous state.

In addition, if you want to delete old files to save storage cost after overwriting the table, you can use [VACUUM](delta-utility.md#delta-vacuum) to delete them. It's optimized for file deletion and usually faster than deleting the entire directory.

## Spark caching

You should not use [Spark caching](optimizations/delta-cache.md#delta-and-rdd-cache-comparison) for the following reasons:

- You lose any data skipping that can come from additional filters added on top of the cached `DataFrame`.

- The data that gets cached may not be updated if the table is accessed using a different identifier (for example, you do `spark.table(x).cache()` but then write to the table using `spark.write.save(/some/path)`.

.. include:: /shared/replacements.md
