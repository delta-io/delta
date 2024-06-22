---
description: Learn how to migrate existing workloads to <Delta>.
---

# Migration guide

## Migrate workloads to <Delta>

When you migrate workloads to <Delta>, you should be aware of the following simplifications and differences compared with the data sources provided by <AS> and Apache Hive.

<Delta> handles the following operations automatically, which you should never perform manually:

- **Add and remove partitions**: <Delta> automatically tracks the set of partitions present in a table and updates the list as data is added or removed.  As a result, there is no need to run `ALTER TABLE [ADD|DROP] PARTITION` or `MSCK`.

- **Load a single partition**: As an optimization, you may sometimes directly load the partition of data you are interested in. For example, `spark.read.format("parquet").load("/data/date=2017-01-01")`.  This is unnecessary with <Delta>, since it can quickly read the list of files from the transaction log to find the relevant ones. If you are interested in a single partition, specify it using a `WHERE` clause. For example, `spark.read.delta("/data").where("date = '2017-01-01'")`. For large tables with many files in the partition, this can be much faster than loading a single partition (with direct partition path, or with `WHERE`) from a Parquet table because listing the files in the directory is often slower than reading the list of files from the transaction log.

When you port an existing application to <Delta>, you should avoid the following operations, which bypass the transaction log:

- **Manually modify data**:  <Delta> uses the transaction log to atomically commit changes to the table. Because the log is the source of truth, files that are written out but not added to the transaction log are not read by Spark. Similarly, even if you manually delete a file, a pointer to the file is still present in the transaction log. Instead of manually modifying files stored in a Delta table, always use the commands that are described in this guide.

- **External readers**: Directly reading the data stored in <Delta>. For information on how to read Delta tables, see [_](integrations.md).

### Example

Suppose you have Parquet data stored in a directory named `/data-pipeline`, and you want to create a Delta table named `events`.

The [first example](#save-as-delta-table) shows how to:

- Read the Parquet data from its original location, `/data-pipeline`, into a DataFrame.
- Save the DataFrame's contents in Delta format in a separate location, `/tmp/delta/data-pipeline/`.
-  Create the `events` table based on that separate location, `/tmp/delta/data-pipeline/`.

The [second example](#convert-to-delta-table) shows how to use `CONVERT TO TABLE` to convert data from Parquet to Delta format without changing its original location, `/data-pipeline/`.

#### Save as Delta table

1. Read the Parquet data into a DataFrame and then save the DataFrame's contents to a new directory in `delta` format:

   ```python
   data = spark.read.format("parquet").load("/data-pipeline")
   data.write.format("delta").save("/tmp/delta/data-pipeline/")
   ```

#. Create a Delta table named `events` that refers to the files in the new directory:

   ```python
   spark.sql("CREATE TABLE events USING DELTA LOCATION '/tmp/delta/data-pipeline/'")
   ```

#### Convert to Delta table


You have two options for converting a Parquet table to a Delta table:

- Convert files to <Delta> format and then create a Delta table:

  ```sql
  CONVERT TO DELTA parquet.`/data-pipeline/`
  CREATE TABLE events USING DELTA LOCATION '/data-pipeline/'
  ```

- Create a Parquet table and then convert it to a Delta table:

  ```sql
  CREATE TABLE events USING PARQUET OPTIONS (path '/data-pipeline/')
  CONVERT TO DELTA events
  ```

For details, see [_](delta-utility.md#convert-a-parquet-table-to-a-delta-table).


## Migrate <Delta> workloads to newer versions

This section discusses any changes that may be required in the user code when migrating from older to newer versions of <Delta>.

### Below <Delta> 3.0 to <Delta> 3.0 or above

Please note that the Delta Lake on Spark Maven artifact has been renamed from `delta-core` (before 3.0) to `delta-spark` (3.0 and above).

### <Delta> 2.1.1 or below to <Delta> 2.2 or above

<Delta> 2.2 collects statistics by default when converting a parquet table to a <Delta> table (e.g. using the `CONVERT TO DELTA` command). To opt out of statistics collection and revert to the 2.1.1 or below default behavior, use the `NO STATISTICS` SQL API (e.g. ``CONVERT TO DELTA parquet.`<path-to-table>` NO STATISTICS``)

### <Delta> 1.2.1, 2.0.0, or 2.1.0 to <Delta> 2.0.1, 2.1.1 or above

<Delta> 1.2.1, 2.0.0 and 2.1.0 have a bug in their DynamoDB-based S3 multi-cluster configuration implementations where an incorrect timestamp value was written to DynamoDB. This caused [DynamoDB's TTL](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/TTL.html) feature to cleanup completed items before it was safe to do so. This has been fixed in <Delta> versions 2.0.1 and 2.1.1, and the TTL attribute has been renamed from `commitTime` to `expireTime`.

If you *already* have TTL enabled on your DynamoDB table using the old attribute, you need to disable TTL for that attribute and then enable it for the new one. You may need to wait an hour between these two operations, as TTL settings changes may take some time to propagate. See the DynamoDB docs [here](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/time-to-live-ttl-before-you-start.html). If you don't do this, DyanmoDB's TTL feature will not remove any new and expired entries. There is no risk of data loss.

```bash
# Disable TTL on old attribute
aws dynamodb update-time-to-live \
  --region <region> \
  --table-name <table-name> \
  --time-to-live-specification "Enabled=false, AttributeName=commitTime"

# Enable TTL on new attribute
aws dynamodb update-time-to-live \
  --region <region> \
  --table-name <table-name> \
  --time-to-live-specification "Enabled=true, AttributeName=expireTime"
```

### <Delta> 2.0 or below to <Delta> 2.1 or above

When calling `CONVERT TO DELTA` on a catalog table <Delta> 2.1 infers the data schema from the catalog. In version 2.0 and below, <Delta> infers the data schema from the data. This means in Delta 2.1 data columns that are not defined in the original catalog table will not be present in the converted Delta table. This behavior can be disabled by setting the Spark session configuration `spark.databricks.delta.convert.useCatalogSchema=false`.

### <Delta> 1.2 or below to <Delta> 2.0 or above

<Delta> 2.0.0 introduced a behavior change for [DROP CONSTRAINT](delta-constraints.md#check-constraint). In version 1.2 and below, no error was thrown when trying to drop a non-existent constraint. In version 2.0.0 and above, the behavior is changed to throw a constraint not exists error. To avoid the error, use `IF EXISTS` construct (for example, `ALTER TABLE events DROP CONSTRAINT IF EXISTS constraint_name`). There is no change in behavior in dropping an existing constraint.

<Delta> 2.0.0 introduced support for [Dynamic Partition Overwrites](delta-batch.md#overwrite). In version 1.2 and below, enabling dynamic partition overwrite mode in either the Spark session configuration or a `DataFrameWriter` option was a no-op, and writes in `overwrite` mode replaced all existing data in every partition of the table. In version 2.0.0 and above, when dynamic partition overwrite mode is enabled, <Delta> replaces all existing data in each logical partition for which the write will commit new data.

### <Delta> 1.1 or below to <Delta> 1.2 or above

The [LogStore](https://docs.delta.io/latest/api/java/index.html) related code is extracted out from the `delta-core` Maven module into a new module `delta-storage` as part of the issue [#951](https://github.com/delta-io/delta/issues/951) for better code manageability. This results in an additional JAR `delta-storage-<version>.jar` dependency for `delta-core`. By default, the additional JAR is downloaded as part of the `delta-core-<version>_<scala-version>.jar` dependency. In clusters where there is *no internet connectivity*, `delta-storage-<version>.jar` cannot  be downloaded. It is advised to download the `delta-storage-<version>.jar` manually and place it in the Java classpath.

### <Delta> 1.0 or below to <Delta> 1.1 or above

If the name of a partition column in a Delta table contains invalid characters (` ,;{}()\n\t=`), you cannot read it in <Delta> 1.1 and above, due to [SPARK-36271](https://issues.apache.org/jira/browse/SPARK-36271). However, this should be rare as you cannot create such tables by using <Delta> 0.6 and above. If you still have such legacy tables, you can overwrite your tables with new valid column names by using <Delta> 1.0 and below before upgrading <Delta> to 1.1 and above, such as the following:

.. code-language-tabs::

    ```python
    spark.read \
      .format("delta") \
      .load("/the/delta/table/path") \
      .withColumnRenamed("column name", "column-name") \
      .write \
      .format("delta") \
      .mode("overwrite") \
      .option("overwriteSchema", "true") \
      .save("/the/delta/table/path")
    ```

    ```scala
    spark.read
      .format("delta")
      .load("/the/delta/table/path")
      .withColumnRenamed("column name", "column-name")
      .write
      .format("delta")
      .mode("overwrite")
      .option("overwriteSchema", "true")
      .save("/the/delta/table/path")
    ```

### <Delta> 0.6 or below to <Delta> 0.7 or above

If you are using `DeltaTable` APIs in Scala, Java, or Python to [update](delta-update.md) or [run utility operations](delta-utility.md) on them, then you may have to add the following configurations when creating the `SparkSession` used to perform those operations.

.. code-language-tabs::

    ```python
    from pyspark.sql import SparkSession

    spark = SparkSession \
      .builder \
      .appName("...") \
      .master("...") \
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
      .getOrCreate()
    ```

    ```scala
    import org.apache.spark.sql.SparkSession

    val spark = SparkSession
      .builder()
      .appName("...")
      .master("...")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()
    ```

    ```java
    import org.apache.spark.sql.SparkSession;

    SparkSession spark = SparkSession
      .builder()
      .appName("...")
      .master("...")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate();
    ```

Alternatively, you can add additional configurations when submitting you Spark application using `spark-submit` or when starting `spark-shell`/`pyspark` by specifying them as command line parameters.

```bash
spark-submit --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"  ...
```

```bash
pyspark --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"  ...
```

.. include:: /shared/replacements.md
