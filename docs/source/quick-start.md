---
description: Learn how to get started quickly with <Delta>.
---

# Quickstart

This guide helps you quickly explore the main features of <Delta>. It provides code snippets that show how to read from and write to Delta tables from interactive, batch, and streaming queries.

.. contents:: In this article:
  :local:
  :depth: 2

## Set up <AS> with <Delta>

Follow these instructions to set up <Delta> with Spark. You can run the steps in this guide on your local machine in the following two ways:

#. Run interactively: Start the Spark shell (Scala or Python) with <Delta> and run the code snippets interactively in the shell.

#. Run as a project: Set up a Maven or SBT project (Scala or Java) with <Delta>, copy the code snippets into a source file, and run the project. Alternatively, you can use the [examples provided in the Github repository](https://github.com/delta-io/delta/tree/master/examples).

.. important:: For all of the following instructions, make sure to install the correct version of Spark or PySpark that is compatible with <Delta> `3.2.0`. See the [release compatibility matrix](releases.md) for details.

### Prerequisite: set up Java

As mentioned in the official <AS> installation instructions [here](https://spark.apache.org/docs/latest/index.html#downloading), make sure you have a valid Java version installed (8, 11, or 17) and that Java is configured correctly on your system using either the system `PATH` or `JAVA_HOME` environmental variable.

Windows users should follow the instructions in this [blog](https://phoenixnap.com/kb/install-spark-on-windows-10), making sure to use the correct version of <AS> that is compatible with <Delta> `3.2.0`.

### Set up interactive shell

To use <Delta> interactively within the Spark SQL, Scala, or Python shell, you need a local installation of <AS>. Depending on whether you want to use SQL, Python, or Scala, you can set up either the SQL, PySpark, or Spark shell, respectively.

#### Spark SQL Shell

Download the [compatible version](releases.md) of <AS> by following instructions from [Downloading Spark](https://spark.apache.org/downloads.html), either using `pip` or by downloading and extracting the archive and running `spark-sql` in the extracted directory.

```bash
bin/spark-sql --packages io.delta:delta-spark_2.12:3.2.0 --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
```

#### PySpark Shell

#. Install the PySpark version that is [compatible](releases.md) with the <Delta> version by running the following:

   ```bash
   pip install pyspark==<compatible-spark-version>
   ```

#. Run PySpark with the <Delta> package and additional configurations:

   ```bash
   pyspark --packages io.delta:delta-spark_2.12:3.2.0 --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
   ```

#### Spark Scala Shell

Download the [compatible version](releases.md) of <AS> by following instructions from [Downloading Spark](https://spark.apache.org/downloads.html), either using `pip` or by downloading and extracting the archive and running `spark-shell` in the extracted directory.

```bash
bin/spark-shell --packages io.delta:delta-spark_2.12:3.2.0 --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
```

### Set up project

If you want to build a project using Delta Lake binaries from Maven Central Repository, you can use the following Maven coordinates.

#### Maven

You include <Delta> in your Maven project by adding it as a dependency in your POM file. <Delta> compiled with Scala 2.12.

```xml
<dependency>
  <groupId>io.delta</groupId>
  <artifactId>delta-spark_2.12</artifactId>
  <version>3.2.0</version>
</dependency>
```

#### SBT

You include <Delta> in your SBT project by adding the following line to your `build.sbt` file:

```scala
libraryDependencies += "io.delta" %% "delta-spark" % "3.2.0"
```

#### Python

To set up a Python project (for example, for unit testing), you can install <Delta> using `pip install delta-spark==3.2.0` and then configure the SparkSession with the `configure_spark_with_delta_pip()` utility function in <Delta>.

```python
import pyspark
from delta import *

builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()
```

## Create a table

To create a Delta table, write a DataFrame out in the `delta` format. You can use existing Spark SQL code and change the format from `parquet`, `csv`, `json`, and so on, to `delta`.

.. code-language-tabs::

  ```SQL
  CREATE TABLE delta.`/tmp/delta-table` USING DELTA AS SELECT col1 as id FROM VALUES 0,1,2,3,4;
  ```

  ```python
  data = spark.range(0, 5)
  data.write.format("delta").save("/tmp/delta-table")
  ```

  ```scala
  val data = spark.range(0, 5)
  data.write.format("delta").save("/tmp/delta-table")
  ```

  ```java
  import org.apache.spark.sql.SparkSession;
  import org.apache.spark.sql.Dataset;
  import org.apache.spark.sql.Row;

  SparkSession spark = ...   // create SparkSession

  Dataset<Row> data = spark.range(0, 5);
  data.write().format("delta").save("/tmp/delta-table");
  ```

These operations create a new Delta table using the schema that was _inferred_ from your DataFrame. For the full set of options available when you create a new Delta table, see [_](delta-batch.md#ddlcreatetable) and [_](delta-batch.md#deltadataframewrites).


.. note::
    This quickstart uses local paths for Delta table locations. For configuring HDFS or cloud storage for Delta tables, see [_](delta-storage.md).

## Read data

You read data in your Delta table by specifying the path to the files: `"/tmp/delta-table"`:

.. code-language-tabs::

  ```SQL
  SELECT * FROM delta.`/tmp/delta-table`;
  ```

  ```python
  df = spark.read.format("delta").load("/tmp/delta-table")
  df.show()
  ```

  ```scala
  val df = spark.read.format("delta").load("/tmp/delta-table")
  df.show()
  ```

  ```java
  Dataset<Row> df = spark.read().format("delta").load("/tmp/delta-table");
  df.show();
  ```

## Update table data

<Delta> supports several operations to modify tables using standard DataFrame APIs. This example runs a batch job to overwrite the data in the table:

### Overwrite

.. code-language-tabs::

  ```SQL
  INSERT OVERWRITE delta.`/tmp/delta-table` SELECT col1 as id FROM VALUES 5,6,7,8,9;
  ```

  ```python
  data = spark.range(5, 10)
  data.write.format("delta").mode("overwrite").save("/tmp/delta-table")
  ```

  ```scala
  val data = spark.range(5, 10)
  data.write.format("delta").mode("overwrite").save("/tmp/delta-table")
  df.show()
  ```

  ```java
  Dataset<Row> data = spark.range(5, 10);
  data.write().format("delta").mode("overwrite").save("/tmp/delta-table");
  ```

If you read this table again, you should see only the values `5-9` you have added because you overwrote the previous data.

### Conditional update without overwrite

<Delta> provides programmatic APIs to conditional update, delete, and merge (upsert) data into tables.
Here are a few examples.

.. code-language-tabs::

  ```SQL
  -- Update every even value by adding 100 to it
  UPDATE delta.`/tmp/delta-table` SET id = id + 100 WHERE id % 2 == 0;

  -- Delete very even value
  DELETE FROM delta.`/tmp/delta-table` WHERE id % 2 == 0;

  -- Upsert (merge) new data
  CREATE TEMP VIEW newData AS SELECT col1 AS id FROM VALUES 1,3,5,7,9,11,13,15,17,19;

  MERGE INTO delta.`/tmp/delta-table` AS oldData
  USING newData
  ON oldData.id = newData.id
  WHEN MATCHED
    THEN UPDATE SET id = newData.id
  WHEN NOT MATCHED
    THEN INSERT (id) VALUES (newData.id);

  SELECT * FROM delta.`/tmp/delta-table`;
  ```

  ```python
  from delta.tables import *
  from pyspark.sql.functions import *

  deltaTable = DeltaTable.forPath(spark, "/tmp/delta-table")

  # Update every even value by adding 100 to it
  deltaTable.update(
    condition = expr("id % 2 == 0"),
    set = { "id": expr("id + 100") })

  # Delete every even value
  deltaTable.delete(condition = expr("id % 2 == 0"))

  # Upsert (merge) new data
  newData = spark.range(0, 20)

  deltaTable.alias("oldData") \
    .merge(
      newData.alias("newData"),
      "oldData.id = newData.id") \
    .whenMatchedUpdate(set = { "id": col("newData.id") }) \
    .whenNotMatchedInsert(values = { "id": col("newData.id") }) \
    .execute()

  deltaTable.toDF().show()
  ```

  ```scala
  import io.delta.tables._
  import org.apache.spark.sql.functions._

  val deltaTable = DeltaTable.forPath("/tmp/delta-table")

  // Update every even value by adding 100 to it
  deltaTable.update(
    condition = expr("id % 2 == 0"),
    set = Map("id" -> expr("id + 100")))

  // Delete every even value
  deltaTable.delete(condition = expr("id % 2 == 0"))

  // Upsert (merge) new data
  val newData = spark.range(0, 20).toDF

  deltaTable.as("oldData")
    .merge(
      newData.as("newData"),
      "oldData.id = newData.id")
    .whenMatched
    .update(Map("id" -> col("newData.id")))
    .whenNotMatched
    .insert(Map("id" -> col("newData.id")))
    .execute()

  deltaTable.toDF.show()
  ```

  ```java
  import io.delta.tables.*;
  import org.apache.spark.sql.functions;
  import java.util.HashMap;

  DeltaTable deltaTable = DeltaTable.forPath("/tmp/delta-table");

  // Update every even value by adding 100 to it
  deltaTable.update(
    functions.expr("id % 2 == 0"),
    new HashMap<String, Column>() {{
      put("id", functions.expr("id + 100"));
    }}
  );

  // Delete every even value
  deltaTable.delete(condition = functions.expr("id % 2 == 0"));

  // Upsert (merge) new data
  Dataset<Row> newData = spark.range(0, 20).toDF();

  deltaTable.as("oldData")
    .merge(
      newData.as("newData"),
      "oldData.id = newData.id")
    .whenMatched()
    .update(
      new HashMap<String, Column>() {{
        put("id", functions.col("newData.id"));
      }})
    .whenNotMatched()
    .insertExpr(
      new HashMap<String, Column>() {{
        put("id", functions.col("newData.id"));
      }})
    .execute();

  deltaTable.toDF().show();
  ```

You should see that some of the existing rows have been updated and new rows have been inserted.

For more information on these operations, see [_](delta-update.md).

## Read older versions of data using time travel

You can query previous snapshots of your Delta table by using time travel. If you want to access the data that you overwrote, you can query a snapshot of the table before you overwrote the first set of data using the `versionAsOf` option.

.. code-language-tabs::

  ```SQL
  SELECT * FROM delta.`/tmp/delta-table` VERSION AS OF 0;
  ```


  ```python
  df = spark.read.format("delta").option("versionAsOf", 0).load("/tmp/delta-table")
  df.show()
  ```

  ```scala
  val df = spark.read.format("delta").option("versionAsOf", 0).load("/tmp/delta-table")
  df.show()
  ```

  ```java
  Dataset<Row> df = spark.read().format("delta").option("versionAsOf", 0).load("/tmp/delta-table");
  df.show();
  ```

You should see the first set of data, from before you overwrote it. Time travel takes advantage of the power of the <Delta> transaction log to access data that is no longer in the table. Removing the version 0 option (or specifying version 1) would let you see the newer data again. For more information, see [_](delta-batch.md#deltatimetravel).

## Write a stream of data to a table

You can also write to a Delta table using Structured Streaming. The <Delta> transaction log guarantees exactly-once processing, even when there are other streams or batch queries running concurrently against the table. By default, streams run in append mode, which adds new records to the table:

.. code-language-tabs::

  ```python
  streamingDf = spark.readStream.format("rate").load()
  stream = streamingDf.selectExpr("value as id").writeStream.format("delta").option("checkpointLocation", "/tmp/checkpoint").start("/tmp/delta-table")
  ```

  ```scala
  val streamingDf = spark.readStream.format("rate").load()
  val stream = streamingDf.select($"value" as "id").writeStream.format("delta").option("checkpointLocation", "/tmp/checkpoint").start("/tmp/delta-table")
  ```

  ```java
  import org.apache.spark.sql.streaming.StreamingQuery;

  Dataset<Row> streamingDf = spark.readStream().format("rate").load();
  StreamingQuery stream = streamingDf.selectExpr("value as id").writeStream().format("delta").option("checkpointLocation", "/tmp/checkpoint").start("/tmp/delta-table");
  ```

While the stream is running, you can read the table using the earlier commands.

.. note::
    If you're running this in a shell, you may see the streaming task progress, which make it hard to type commands in that shell. It may be useful to start another shell in a new terminal for querying the table.

You can stop the stream by running `stream.stop()` in the same terminal that started the stream.

For more information about <Delta> integration with Structured Streaming, see [_](delta-streaming.md). See also the [Structured Streaming Programming Guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html) on the <AS> website.

## Read a stream of changes from a table

While the stream is writing to the Delta table, you can also read from that table as streaming source. For example, you can start another streaming query that prints all the changes made to the Delta table. You can specify which version Structured Streaming should start from by providing the `startingVersion` or `startingTimestamp` option to get changes from that point onwards. See [Structured Streaming](delta-streaming.md#specify-initial-position) for details.

.. code-language-tabs::

  ```python
  stream2 = spark.readStream.format("delta").load("/tmp/delta-table").writeStream.format("console").start()
  ```

  ```scala
  val stream2 = spark.readStream.format("delta").load("/tmp/delta-table").writeStream.format("console").start()
  ```

  ```java
  StreamingQuery stream2 = spark.readStream().format("delta").load("/tmp/delta-table").writeStream().format("console").start();
  ```

.. include:: /shared/replacements.md
