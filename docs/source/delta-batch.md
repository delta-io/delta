---
description: Learn how to perform batch reads and writes on Delta tables.
---

# Table batch reads and writes

<Delta> supports most of the options provided by <AS> DataFrame read and write APIs for performing batch reads and writes on tables.


For many <Delta> operations on tables, you enable integration with <AS> DataSourceV2 and Catalog APIs (since 3.0) by setting configurations when you create a new `SparkSession`. See [_](#sql-support).


.. contents:: In this article:
  :local:
  :depth: 2

<a id="ddlcreatetable"></a>

## Create a table

<Delta> supports creating two types of tables---tables defined in the metastore and tables defined by path.

To work with metastore-defined tables, you must enable integration with <AS> DataSourceV2 and Catalog APIs by setting configurations when you create a new `SparkSession`. See [_](#sql-support).

You can create tables in the following ways.

- **SQL DDL commands**: You can use standard SQL DDL commands supported in <AS> (for example, `CREATE TABLE` and `REPLACE TABLE`) to create Delta tables.

  .. code-language-tabs::

    ```sql
    CREATE TABLE IF NOT EXISTS default.people10m (
      id INT,
      firstName STRING,
      middleName STRING,
      lastName STRING,
      gender STRING,
      birthDate TIMESTAMP,
      ssn STRING,
      salary INT
    ) USING DELTA

    CREATE OR REPLACE TABLE default.people10m (
      id INT,
      firstName STRING,
      middleName STRING,
      lastName STRING,
      gender STRING,
      birthDate TIMESTAMP,
      ssn STRING,
      salary INT
    ) USING DELTA
    ```


SQL also supports creating a table at a path, without creating an entry in the Hive metastore.

  .. code-language-tabs::

    ```sql
    -- Create or replace table with path
    CREATE OR REPLACE TABLE delta.`/tmp/delta/people10m` (
      id INT,
      firstName STRING,
      middleName STRING,
      lastName STRING,
      gender STRING,
      birthDate TIMESTAMP,
      ssn STRING,
      salary INT
    ) USING DELTA
    ```

- **`DataFrameWriter` API**: If you want to simultaneously create a table and insert data into it from Spark DataFrames or Datasets, you can use the Spark `DataFrameWriter` ([Scala or Java](https://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/DataFrameWriter.html) and [Python](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/io.html)).

  .. code-language-tabs::

    ```python
    # Create table in the metastore using DataFrame's schema and write data to it
    df.write.format("delta").saveAsTable("default.people10m")

    # Create or replace partitioned table with path using DataFrame's schema and write/overwrite data to it
    df.write.format("delta").mode("overwrite").save("/tmp/delta/people10m")
    ```

    ```scala
    // Create table in the metastore using DataFrame's schema and write data to it
    df.write.format("delta").saveAsTable("default.people10m")

    // Create table with path using DataFrame's schema and write data to it
    df.write.format("delta").mode("overwrite").save("/tmp/delta/people10m")
    ```


You can also create Delta tables using the Spark `DataFrameWriterV2` API.

- **`DeltaTableBuilder` API**: You can also use the `DeltaTableBuilder` API in <Delta> to create tables. Compared to the DataFrameWriter APIs, this API makes it easier to specify additional information like column comments, table properties, and [generated columns](#deltausegeneratedcolumns).

.. note::
    This feature is new and is in Preview.

  .. code-language-tabs::

    ```python
    # Create table in the metastore
    DeltaTable.createIfNotExists(spark) \
      .tableName("default.people10m") \
      .addColumn("id", "INT") \
      .addColumn("firstName", "STRING") \
      .addColumn("middleName", "STRING") \
      .addColumn("lastName", "STRING", comment = "surname") \
      .addColumn("gender", "STRING") \
      .addColumn("birthDate", "TIMESTAMP") \
      .addColumn("ssn", "STRING") \
      .addColumn("salary", "INT") \
      .execute()

    # Create or replace table with path and add properties
    DeltaTable.createOrReplace(spark) \
      .addColumn("id", "INT") \
      .addColumn("firstName", "STRING") \
      .addColumn("middleName", "STRING") \
      .addColumn("lastName", "STRING", comment = "surname") \
      .addColumn("gender", "STRING") \
      .addColumn("birthDate", "TIMESTAMP") \
      .addColumn("ssn", "STRING") \
      .addColumn("salary", "INT") \
      .property("description", "table with people data") \
      .location("/tmp/delta/people10m") \
      .execute()
    ```

    ```scala
    // Create table in the metastore
    DeltaTable.createOrReplace(spark)
      .tableName("default.people10m")
      .addColumn("id", "INT")
      .addColumn("firstName", "STRING")
      .addColumn("middleName", "STRING")
      .addColumn(
        DeltaTable.columnBuilder("lastName")
          .dataType("STRING")
          .comment("surname")
          .build())
      .addColumn("lastName", "STRING", comment = "surname")
      .addColumn("gender", "STRING")
      .addColumn("birthDate", "TIMESTAMP")
      .addColumn("ssn", "STRING")
      .addColumn("salary", "INT")
      .execute()

    // Create or replace table with path and add properties
    DeltaTable.createOrReplace(spark)
      .addColumn("id", "INT")
      .addColumn("firstName", "STRING")
      .addColumn("middleName", "STRING")
      .addColumn(
        DeltaTable.columnBuilder("lastName")
          .dataType("STRING")
          .comment("surname")
          .build())
      .addColumn("lastName", "STRING", comment = "surname")
      .addColumn("gender", "STRING")
      .addColumn("birthDate", "TIMESTAMP")
      .addColumn("ssn", "STRING")
      .addColumn("salary", "INT")
      .property("description", "table with people data")
      .location("/tmp/delta/people10m")
      .execute()
    ```


See the [API documentation](delta-apidoc.md) for details.

### Partition data

You can partition data to speed up queries or DML that have predicates involving the partition columns.
To partition data when you create a Delta table, specify a partition by columns. The following example partitions by gender.

.. code-language-tabs::

  ```sql
  -- Create table in the metastore
  CREATE TABLE default.people10m (
    id INT,
    firstName STRING,
    middleName STRING,
    lastName STRING,
    gender STRING,
    birthDate TIMESTAMP,
    ssn STRING,
    salary INT
  )
  USING DELTA
  PARTITIONED BY (gender)
  ```

  ```python
  df.write.format("delta").partitionBy("gender").saveAsTable("default.people10m")

  DeltaTable.create(spark) \
    .tableName("default.people10m") \
    .addColumn("id", "INT") \
    .addColumn("firstName", "STRING") \
    .addColumn("middleName", "STRING") \
    .addColumn("lastName", "STRING", comment = "surname") \
    .addColumn("gender", "STRING") \
    .addColumn("birthDate", "TIMESTAMP") \
    .addColumn("ssn", "STRING") \
    .addColumn("salary", "INT") \
    .partitionedBy("gender") \
    .execute()
  ```

  ```scala
  df.write.format("delta").partitionBy("gender").saveAsTable("default.people10m")

  DeltaTable.createOrReplace(spark)
    .tableName("default.people10m")
    .addColumn("id", "INT")
    .addColumn("firstName", "STRING")
    .addColumn("middleName", "STRING")
    .addColumn(
      DeltaTable.columnBuilder("lastName")
        .dataType("STRING")
        .comment("surname")
        .build())
    .addColumn("lastName", "STRING", comment = "surname")
    .addColumn("gender", "STRING")
    .addColumn("birthDate", "TIMESTAMP")
    .addColumn("ssn", "STRING")
    .addColumn("salary", "INT")
    .partitionedBy("gender")
    .execute()
  ```

To determine whether a table contains a specific partition, use the statement `SELECT COUNT(*) > 0 FROM <table-name> WHERE <partition-column> = <value>`. If the partition exists, `true` is returned. For example:

.. code-language-tabs::

  ```sql
  SELECT COUNT(*) > 0 AS `Partition exists` FROM default.people10m WHERE gender = "M"
  ```

  ```python
  display(spark.sql("SELECT COUNT(*) > 0 AS `Partition exists` FROM default.people10m WHERE gender = 'M'"))
  ```

  ```scala
  display(spark.sql("SELECT COUNT(*) > 0 AS `Partition exists` FROM default.people10m WHERE gender = 'M'"))
  ```

### Control data location

For tables defined in the metastore, you can optionally specify the `LOCATION` as a path. Tables created with a specified `LOCATION` are considered unmanaged by the metastore. Unlike a managed table, where no path is specified, an unmanaged table's files are not deleted when you `DROP` the table.

When you run `CREATE TABLE` with a `LOCATION` that _already_ contains data stored using <Delta>, <Delta> does the following:

- If you specify _only the table name and location_, for example:

  ```sql
  CREATE TABLE default.people10m
  USING DELTA
  LOCATION '/tmp/delta/people10m'
  ```

  the table in the metastore automatically inherits the schema, partitioning, and table properties of the existing data. This functionality can be used to "import" data into the metastore.

- If you specify _any configuration_ (schema, partitioning, or table properties), <Delta> verifies that the specification exactly matches the configuration of the existing data.

  .. important::
    If the specified configuration does not _exactly_ match the configuration of the data, <Delta> throws an exception that describes the discrepancy.

.. note::

  The metastore is not the source of truth about the latest information of a Delta table. In fact, the table definition in the metastore may not contain all the metadata like schema and properties. It contains the location of the table, and the table's transaction log at the location is the source of truth. If you query the metastore from a system that is not aware of this Delta-specific customization, you may see incomplete or stale table information.

<a id="deltausegeneratedcolumns"></a>

### Use generated columns

.. note::
    This feature is new and is in Preview.

<Delta> supports generated columns which are a special type of columns whose values are automatically generated based on a user-specified function over other columns in the Delta table. When you write to a table with generated columns and you do not explicitly provide values for them, <Delta> automatically computes the values. For example, you can automatically generate a date column (for partitioning the table by date) from the timestamp column; any writes into the table need only specify the data for the timestamp column. However, if you explicitly provide values for them, the values must satisfy the [constraint](delta-constraints.md) `(<value> <=> <generation expression>) IS TRUE` or the write will fail with an error.

.. important::
   Tables created with generated columns have a higher table writer protocol version than the default. See [_](/versioning.md) to understand table protocol versioning and what it means to have a higher version of a table protocol version.

The following example shows how to create a table with generated columns:

.. code-language-tabs::

  ```python
  DeltaTable.create(spark) \
    .tableName("default.people10m") \
    .addColumn("id", "INT") \
    .addColumn("firstName", "STRING") \
    .addColumn("middleName", "STRING") \
    .addColumn("lastName", "STRING", comment = "surname") \
    .addColumn("gender", "STRING") \
    .addColumn("birthDate", "TIMESTAMP") \
    .addColumn("dateOfBirth", DateType(), generatedAlwaysAs="CAST(birthDate AS DATE)") \
    .addColumn("ssn", "STRING") \
    .addColumn("salary", "INT") \
    .partitionedBy("gender") \
    .execute()
  ```

  ```scala
  DeltaTable.create(spark)
    .tableName("default.people10m")
    .addColumn("id", "INT")
    .addColumn("firstName", "STRING")
    .addColumn("middleName", "STRING")
    .addColumn(
      DeltaTable.columnBuilder("lastName")
        .dataType("STRING")
        .comment("surname")
        .build())
    .addColumn("lastName", "STRING", comment = "surname")
    .addColumn("gender", "STRING")
    .addColumn("birthDate", "TIMESTAMP")
    .addColumn(
      DeltaTable.columnBuilder("dateOfBirth")
       .dataType(DateType)
       .generatedAlwaysAs("CAST(dateOfBirth AS DATE)")
       .build())
    .addColumn("ssn", "STRING")
    .addColumn("salary", "INT")
    .partitionedBy("gender")
    .execute()
  ```

Generated columns are stored as if they were normal columns. That is, they occupy storage.

The following restrictions apply to generated columns:

- A generation expression can use any SQL functions in Spark that always return the same result when given the same argument values, except the following types of functions:
  - User-defined functions.
  - Aggregate functions.
  - Window functions.
  - Functions returning multiple rows.


- For <Delta> 1.1.0 and above, `MERGE` operations support generated columns when you set `spark.databricks.delta.schema.autoMerge.enabled` to true.

<Delta> may be able to generate partition filters for a query whenever a partition column is defined by one of the following expressions:

- `CAST(col AS DATE)` and the type of `col` is `TIMESTAMP`.
- `YEAR(col)` and the type of `col` is `TIMESTAMP`.
- Two partition columns defined by `YEAR(col), MONTH(col)` and the type of `col` is `TIMESTAMP`.
- Three partition columns defined by `YEAR(col), MONTH(col), DAY(col)` and the type of `col` is `TIMESTAMP`.
- Four partition columns defined by `YEAR(col), MONTH(col), DAY(col), HOUR(col)` and the type of `col` is `TIMESTAMP`.
- `SUBSTRING(col, pos, len)` and the type of `col` is `STRING`
- `DATE_FORMAT(col, format)` and the type of `col` is `TIMESTAMP`.
- `DATE_TRUNC(format, col) and the type of the `col` is `TIMESTAMP` or `DATE`.
- `TRUNC(col, format)` and type of the `col` is either `TIMESTAMP` or `DATE`.

If a partition column is defined by one of the preceding expressions, and a query filters data using the underlying base column of a generation expression, <Delta> looks at the relationship between the base column and the generated column, and populates partition filters based on the generated partition column if possible. For example, given the following table:

```python
DeltaTable.create(spark) \
  .tableName("default.events") \
  .addColumn("eventId", "BIGINT") \
  .addColumn("data", "STRING") \
  .addColumn("eventType", "STRING") \
  .addColumn("eventTime", "TIMESTAMP") \
  .addColumn("eventDate", "DATE", generatedAlwaysAs="CAST(eventTime AS DATE)") \
  .partitionedBy("eventType", "eventDate") \
  .execute()
```

If you then run the following query:

```python
spark.sql('SELECT * FROM default.events WHERE eventTime >= "2020-10-01 00:00:00" <= "2020-10-01 12:00:00"')
```

<Delta> automatically generates a partition filter so that the preceding query only reads the data in partition `date=2020-10-01` even if a partition filter is not specified.

As another example, given the following table:

```python
DeltaTable.create(spark) \
  .tableName("default.events") \
  .addColumn("eventId", "BIGINT") \
  .addColumn("data", "STRING") \
  .addColumn("eventType", "STRING") \
  .addColumn("eventTime", "TIMESTAMP") \
  .addColumn("year", "INT", generatedAlwaysAs="YEAR(eventTime)") \
  .addColumn("month", "INT", generatedAlwaysAs="MONTH(eventTime)") \
  .addColumn("day", "INT", generatedAlwaysAs="DAY(eventTime)") \
  .partitionedBy("eventType", "year", "month", "day") \
  .execute()
```

If you then run the following query:

```python
spark.sql('SELECT * FROM default.events WHERE eventTime >= "2020-10-01 00:00:00" <= "2020-10-01 12:00:00"')
```

<Delta> automatically generates a partition filter so that the preceding query only reads the data in partition `year=2020/month=10/day=01` even if a partition filter is not specified.

You can use an [EXPLAIN](https://spark.apache.org/docs/latest/sql-ref-syntax-qry-explain.html) clause and check the provided plan to see whether <Delta> automatically generates any partition filters.

<a id="special-chars-in-col-name"></a>

### Specify default values for columns

Delta enables the specification of [default expressions](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#default-columns) for columns in Delta tables. When users write to these tables without explicitly providing values for certain columns, or when they explicitly use the DEFAULT SQL keyword for a column, Delta automatically generates default values for those columns. For more information, please refer to the [dedicated documentation page](#delta-default-columns).

### Use special characters in column names


By default, special characters such as spaces and any of the characters `,;{}()\n\t=` are not supported in table column names. To include these special characters in a table's column name, enable [column mapping](/versioning.md#column-mapping).

### Default table properties

<Delta> configurations set in the SparkSession override the default [table properties](/delta-batch.md#table-properties) for new <Delta> tables created in the session.
The prefix used in the SparkSession is different from the configurations used in the table properties.

| <Delta> conf | SparkSession conf |
| --- | --- |
| `delta.<conf>` | `spark.databricks.delta.properties.defaults.<conf>` |

For example, to set the `delta.appendOnly = true` property for all new <Delta> tables created in a session, set the following:

```sql
SET spark.databricks.delta.properties.defaults.appendOnly = true
```

To modify table properties of existing tables, use [SET TBLPROPERTIES](/delta-batch.md#table-properties).

<a id="deltadataframereads"></a>

## Read a table

You can load a Delta table as a DataFrame by specifying a table name or a path:

.. code-language-tabs::

  ```sql
  SELECT * FROM default.people10m   -- query table in the metastore

  SELECT * FROM delta.`/tmp/delta/people10m`  -- query table by path
  ```

  ```python
  spark.table("default.people10m")    # query table in the metastore

  spark.read.format("delta").load("/tmp/delta/people10m")  # query table by path
  ```

  ```scala
  spark.table("default.people10m")      // query table in the metastore

  spark.read.format("delta").load("/tmp/delta/people10m")  // create table by path

  import io.delta.implicits._
  spark.read.delta("/tmp/delta/people10m")
  ```

The DataFrame returned automatically reads the most recent snapshot of the table for any query; you never need to run `REFRESH TABLE`. <Delta> automatically uses partitioning and statistics to read the minimum amount of data when there are applicable predicates in the query.

<a id="deltatimetravel"></a>

## Query an older snapshot of a table (time travel)

.. contents:: In this section:
  :local:
  :depth: 1

<Delta> time travel allows you to query an older snapshot of a Delta table. Time travel has many use cases, including:

- Re-creating analyses, reports, or outputs (for example, the output of a machine learning model). This could be useful for debugging or auditing, especially in regulated industries.
- Writing complex temporal queries.
- Fixing mistakes in your data.
- Providing snapshot isolation for a set of queries for fast changing tables.

This section describes the supported methods for querying older versions of tables, data retention concerns, and provides examples.
	
.. note::
  The timestamp of each version N depends on the timestamp of the log file corresponding to the version N in Delta table log. Hence, time travel by timestamp can break if you copy the entire Delta table directory to a new location. Time travel by version will be unaffected.
	
### Syntax

This section shows how to query an older version of a Delta table.

<a id="timestamp-and-version-syntax"></a>

#### SQL `AS OF` syntax

```sql
SELECT * FROM table_name TIMESTAMP AS OF timestamp_expression
SELECT * FROM table_name VERSION AS OF version
```

- `timestamp_expression` can be any one of:
  - `'2018-10-18T22:15:12.013Z'`, that is, a string that can be cast to a timestamp
  - `cast('2018-10-18 13:36:32 CEST' as timestamp)`
  - `'2018-10-18'`, that is, a date string
  - `current_timestamp() - interval 12 hours`
  - `date_sub(current_date(), 1)`
  - Any other expression that is or can be cast to a timestamp
- `version` is a long value that can be obtained from the output of `DESCRIBE HISTORY table_spec`.

Neither `timestamp_expression` nor `version` can be subqueries.

##### Example

```sql
SELECT * FROM default.people10m TIMESTAMP AS OF '2018-10-18T22:15:12.013Z'
SELECT * FROM delta.`/tmp/delta/people10m` VERSION AS OF 123
```

#### DataFrameReader options

DataFrameReader options allow you to create a DataFrame from a Delta table that is fixed to a specific version of the table.

```python
df1 = spark.read.format("delta").option("timestampAsOf", timestamp_string).load("/tmp/delta/people10m")
df2 = spark.read.format("delta").option("versionAsOf", version).load("/tmp/delta/people10m")
```

For `timestamp_string`, only date or timestamp strings are accepted. For example, `"2019-01-01"` and `"2019-01-01T00:00:00.000Z"`.


A common pattern is to use the latest state of the Delta table throughout the execution of a job to update downstream applications.

Because Delta tables auto update, a DataFrame loaded from a Delta table may return different results across invocations if the underlying data is updated. By using time travel, you can fix the data returned by the DataFrame across invocations:


```python
history = spark.sql("DESCRIBE HISTORY delta.`/tmp/delta/people10m`")
latest_version = history.selectExpr("max(version)").collect()
df = spark.read.format("delta").option("versionAsOf", latest_version[0][0]).load("/tmp/delta/people10m")
```

### Examples

- Fix accidental deletes to a table for the user `111`:


```python
yesterday = spark.sql("SELECT CAST(date_sub(current_date(), 1) AS STRING)").collect()[0][0]
df = spark.read.format("delta").option("timestampAsOf", yesterday).load("/tmp/delta/events")
df.where("userId = 111").write.format("delta").mode("append").save("/tmp/delta/events")
```

- Fix accidental incorrect updates to a table:


```python
yesterday = spark.sql("SELECT CAST(date_sub(current_date(), 1) AS STRING)").collect()[0][0]
df = spark.read.format("delta").option("timestampAsOf", yesterday).load("/tmp/delta/events")
df.createOrReplaceTempView("my_table_yesterday")
spark.sql('''
MERGE INTO delta.`/tmp/delta/events` target
  USING my_table_yesterday source
  ON source.userId = target.userId
  WHEN MATCHED THEN UPDATE SET *
''')
```

- Query the number of new customers added over the last week.


```python
last_week = spark.sql("SELECT CAST(date_sub(current_date(), 7) AS STRING)").collect()[0][0]
df = spark.read.format("delta").option("timestampAsOf", last_week).load("/tmp/delta/events")
last_week_count = df.select("userId").distinct().count()
count = spark.read.format("delta").load("/tmp/delta/events").select("userId").distinct().count()
new_customers_count = count - last_week_count
```

### Data retention

To time travel to a previous version, you must retain _both_ the log and the data files for that version.

The data files backing a Delta table are _never_ deleted automatically; data files are deleted only when you run [VACUUM](delta-utility.md#delta-vacuum). `VACUUM` _does not_ delete Delta log files; log files are automatically cleaned up after checkpoints are written.

By default you can time travel to a Delta table up to 30 days old unless you have:

- Run `VACUUM` on your Delta table.
- Changed the data or log file retention periods using the following [table properties](#table-properties):

  - `delta.logRetentionDuration = "interval <interval>"`: controls how long the history for a table is kept. The default is `interval 30 days`.


Each time a checkpoint is written, Delta automatically cleans up log entries older than the retention interval. If you set this config to a large enough value, many log entries are retained. This should not impact performance as operations against the log are constant time. Operations on history are parallel but will become more expensive as the log size increases.

  - `delta.deletedFileRetentionDuration = "interval <interval>"`: controls how long ago a file must have been deleted _before being a candidate for_ `VACUUM`. The default is `interval 7 days`.

    To access 30 days of historical data even if you run `VACUUM` on the Delta table, set `delta.deletedFileRetentionDuration = "interval 30 days"`. This setting may cause your storage costs to go up.

.. note::
    Due to log entry cleanup, instances can arise where you cannot time travel to a version that is less than the retention interval. <Delta> requires all consecutive log entries since the previous checkpoint to time travel to a particular version. For example, with a table initially consisting of log entries for versions [0, 19] and a checkpoint at verison 10, if the log entry for version 0 is cleaned up, then you cannot time travel to versions [1, 9]. Increasing the table property `delta.logRetentionDuration` can help avoid these situations.

<a id="deltadataframewrites"></a>

## Write to a table

<a id="batch-append"></a>

### Append

To atomically add new data to an existing Delta table, use `append` mode:

.. code-language-tabs::

  ```sql
  INSERT INTO default.people10m SELECT * FROM morePeople
  ```

  ```python
  df.write.format("delta").mode("append").save("/tmp/delta/people10m")
  df.write.format("delta").mode("append").saveAsTable("default.people10m")
  ```

  ```scala
  df.write.format("delta").mode("append").save("/tmp/delta/people10m")
  df.write.format("delta").mode("append").saveAsTable("default.people10m")

  import io.delta.implicits._
  df.write.mode("append").delta("/tmp/delta/people10m")
  ```

### Overwrite

To atomically replace all the data in a table, use `overwrite` mode:

.. code-language-tabs::

  ```sql
  INSERT OVERWRITE TABLE default.people10m SELECT * FROM morePeople
  ```

  ```python
  df.write.format("delta").mode("overwrite").save("/tmp/delta/people10m")
  df.write.format("delta").mode("overwrite").saveAsTable("default.people10m")
  ```

  ```scala
  df.write.format("delta").mode("overwrite").save("/tmp/delta/people10m")
  df.write.format("delta").mode("overwrite").saveAsTable("default.people10m")

  import io.delta.implicits._
  df.write.mode("overwrite").delta("/tmp/delta/people10m")
  ```

You can selectively overwrite only the data that matches an arbitrary expression. This feature is available with DataFrames in <Delta> 1.1.0 and above and supported in SQL in <Delta> 2.4.0 and above.

The following command atomically replaces events in January in the target table, which is partitioned by `start_date`,  with the data in `replace_data`:

.. code-language-tabs::

  ```python
  replace_data.write \
    .format("delta") \
    .mode("overwrite") \
    .option("replaceWhere", "start_date >= '2017-01-01' AND end_date <= '2017-01-31'") \
    .save("/tmp/delta/events")
  ```

  ```scala
  replace_data.write
    .format("delta")
    .mode("overwrite")
    .option("replaceWhere", "start_date >= '2017-01-01' AND end_date <= '2017-01-31'")
    .save("/tmp/delta/events")
  ```

  ```sql
  INSERT INTO TABLE events REPLACE WHERE start_data >= '2017-01-01' AND end_date <= '2017-01-31' SELECT * FROM replace_data
  ```

This sample code writes out the data in `replace_data`, validates that it all matches the predicate, and performs an atomic replacement. If you want to write out data that doesn't all match the predicate, to replace the matching rows in the target table, you can disable the constraint check by setting `spark.databricks.delta.replaceWhere.constraintCheck.enabled` to false:

.. code-language-tabs::

  ```python
  spark.conf.set("spark.databricks.delta.replaceWhere.constraintCheck.enabled", False)
  ```

  ```scala
  spark.conf.set("spark.databricks.delta.replaceWhere.constraintCheck.enabled", false)
  ```

  ```sql
  SET spark.databricks.delta.replaceWhere.constraintCheck.enabled=false
  ```

In <Delta> 1.0.0 and below, `replaceWhere` overwrites data matching a predicate over partition columns only. The following command atomically replaces the month in January in the target table, which is partitioned by `date`, with the data in `df`:

.. code-language-tabs::

  ```python
  df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("replaceWhere", "birthDate >= '2017-01-01' AND birthDate <= '2017-01-31'") \
    .save("/tmp/delta/people10m")
  ```

  ```scala
  df.write
    .format("delta")
    .mode("overwrite")
    .option("replaceWhere", "birthDate >= '2017-01-01' AND birthDate <= '2017-01-31'")
    .save("/tmp/delta/people10m")
  ```

In <Delta> 1.1.0 and above, if you want to fall back to the old behavior, you can disable the `spark.databricks.delta.replaceWhere.dataColumns.enabled` flag:

.. code-language-tabs::

  ```python
  spark.conf.set("spark.databricks.delta.replaceWhere.dataColumns.enabled", False)
  ```

  ```scala
  spark.conf.set("spark.databricks.delta.replaceWhere.dataColumns.enabled", false)
  ```

  ```sql
  SET spark.databricks.delta.replaceWhere.dataColumns.enabled=false
  ```


#### Dynamic Partition Overwrites

<Delta> 2.0 and above supports _dynamic_ partition overwrite mode for partitioned tables.

When in dynamic partition overwrite mode, we overwrite all existing data in each logical partition for which the write will commit new data. Any existing logical partitions for which the write does not contain data will remain unchanged. This mode is only applicable when data is being written in overwrite mode: either `INSERT OVERWRITE` in SQL, or a DataFrame write with `df.write.mode("overwrite")`.

Configure dynamic partition overwrite mode by setting the Spark session configuration `spark.sql.sources.partitionOverwriteMode` to `dynamic`. You can also enable this by setting the `DataFrameWriter` option `partitionOverwriteMode` to `dynamic`. If present, the query-specific option overrides the mode defined in the session configuration. The default for `partitionOverwriteMode` is `static`.

.. code-language-tabs::

  ```sql
  SET spark.sql.sources.partitionOverwriteMode=dynamic;
  INSERT OVERWRITE TABLE default.people10m SELECT * FROM morePeople;
  ```

  ```python
  df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("partitionOverwriteMode", "dynamic") \
    .saveAsTable("default.people10m")
  ```

  ```scala
  df.write
    .format("delta")
    .mode("overwrite")
    .option("partitionOverwriteMode", "dynamic")
    .saveAsTable("default.people10m")
  ```
.. note::
  Dynamic partition overwrite conflicts with the option `replaceWhere` for partitioned tables.
  - If dynamic partition overwrite is enabled in the Spark session configuration, and `replaceWhere` is provided as a `DataFrameWriter` option, then <Delta> overwrites the data according to the `replaceWhere` expression (query-specific options override session configurations).
  - You'll receive an error if the `DataFrameWriter` options have both dynamic partition overwrite and `replaceWhere` enabled.

.. important::
  Validate that the data written with dynamic partition overwrite touches only the expected partitions. A single row in the incorrect partition can lead to unintentionally overwriting an entire partition. We recommend using `replaceWhere` to specify which data to overwrite.

  If a partition has been accidentally overwritten, you can use [_](delta-utility.md#restore-a-delta-table-to-an-earlier-state) to undo the change.

For <Delta> support for updating tables, see [_](delta-update.md).

<a id="max-records-per-file"></a>

### Limit rows written in a file

You can use the SQL session configuration `spark.sql.files.maxRecordsPerFile` to specify the maximum number of records to write to a single file for a <Delta> table. Specifying a value of zero or a negative value represents no limit.


You can also use the DataFrameWriter option `maxRecordsPerFile` when using the DataFrame APIs to write to a <Delta> table. When `maxRecordsPerFile` is specified, the value of the SQL session configuration `spark.sql.files.maxRecordsPerFile` is ignored.

.. code-language-tabs::


  ```python
  df.write.format("delta") \
    .mode("append") \
    .option("maxRecordsPerFile", "10000") \
    .save("/tmp/delta/people10m")
  ```

  ```scala
  df.write.format("delta")
    .mode("append")
    .option("maxRecordsPerFile", "10000")
    .save("/tmp/delta/people10m")
  ```

### Idempotent writes

Sometimes a job that writes data to a Delta table is restarted due to various reasons (for example, job encounters a failure). The failed job may or may not have written the data to Delta table before terminating. In the case where the data is written to the Delta table, the restarted job writes the same data to the Delta table which results in duplicate data.

To address this, Delta tables support the following `DataFrameWriter` options to make the writes idempotent:

- `txnAppId`: A unique string that you can pass on each `DataFrame` write. For example, this can be the name of the job.
- `txnVersion`: A monotonically increasing number that acts as transaction version. This number needs to be unique for data that is being written to the Delta table(s). For example, this can be the epoch seconds of the instant when the query is attempted for the first time. Any subsequent restarts of the same job needs to have the same value for `txnVersion`. 

The above combination of options needs to be unique for each new data that is being ingested into the Delta table and the `txnVersion` needs to be higher than the last data that was ingested into the Delta table. For example:
- Last successfully written data contains option values as `dailyETL:23423` (`txnAppId:txnVersion`).
- Next write of data should have `txnAppId = dailyETL` and `txnVersion` as at least `23424` (one more than the last written data `txnVersion`).
- Any attempt to write data with `txnAppId = dailyETL` and `txnVersion` as `23422` or less is ignored because the `txnVersion` is less than the last recorded `txnVersion` in the table.
- Attempt to write data with `txnAppId:txnVersion` as `anotherETL:23424` is successful writing data to the table as it contains a different `txnAppId` compared to the same option value in last ingested data.

You can also configure idempotent writes by setting the Spark session configuration `spark.databricks.delta.write.txnAppId` and `spark.databricks.delta.write.txnVersion`. In addition, you can set `spark.databricks.delta.write.txnVersion.autoReset.enabled` to true to automatically reset `spark.databricks.delta.write.txnVersion` after every write. When both the writer options and session configuration are set, we will use the writer option values.

.. warning::

  This solution assumes that the data being written to Delta table(s) in multiple retries of the job is same. If a write attempt in a Delta table succeeds but due to some downstream failure there is a second write attempt with same txn options but different data, then that second write attempt will be ignored. This can cause unexpected results.

#### Example

.. code-language-tabs::

   ```python
   app_id = ... # A unique string that is used as an application ID.
   version = ... # A monotonically increasing number that acts as transaction version.

   dataFrame.write.format(...).option("txnVersion", version).option("txnAppId", app_id).save(...)
   ```

   ```scala
   val appId = ... // A unique string that is used as an application ID.
   version = ... // A monotonically increasing number that acts as transaction version.

   dataFrame.write.format(...).option("txnVersion", version).option("txnAppId", appId).save(...)
   ```

   ```sql
   SET spark.databricks.delta.write.txnAppId = ...;
   SET spark.databricks.delta.write.txnVersion = ...;
   SET spark.databricks.delta.write.txnVersion.autoReset.enabled = true; -- if set to true, this will reset txnVersion after every write
   ```

<a id="user-metadata"></a>

### Set user-defined commit metadata

You can specify user-defined strings as metadata in commits made by these operations, either using the DataFrameWriter option `userMetadata` or the SparkSession configuration `spark.databricks.delta.commitInfo.userMetadata`. If both of them have been specified, then the option takes preference. This user-defined metadata is readable in the [history](delta-utility.md#delta-history) operation.

.. code-language-tabs::

  ```sql

  SET spark.databricks.delta.commitInfo.userMetadata=overwritten-for-fixing-incorrect-data
  INSERT OVERWRITE default.people10m SELECT * FROM morePeople
  ```

  ```python
  df.write.format("delta") \
    .mode("overwrite") \
    .option("userMetadata", "overwritten-for-fixing-incorrect-data") \
    .save("/tmp/delta/people10m")
  ```

  ```scala
  df.write.format("delta")
    .mode("overwrite")
    .option("userMetadata", "overwritten-for-fixing-incorrect-data")
    .save("/tmp/delta/people10m")
  ```

<a id="schema-validation"></a>

## Schema validation

<Delta> automatically validates that the schema of the DataFrame being written is compatible with the schema of the table. <Delta> uses the following rules to determine whether a write from a DataFrame to a table is compatible:

- All DataFrame columns must exist in the target table. If there are columns in the DataFrame not present in the table,  an exception is raised. Columns present in the table but not in the DataFrame are set to null.
- DataFrame column data types must match the column data types in the target table. If they don't match, an exception is raised.
- DataFrame column names cannot differ only by case. This means that you cannot have columns such as "Foo" and  "foo" defined in the same table. While you can use Spark in case sensitive or insensitive (default) mode, Parquet is case sensitive when storing and returning column information. <Delta> is case-preserving but insensitive when storing the schema and has this restriction to avoid potential mistakes, data corruption, or loss issues.

<Delta> support DDL to add new columns explicitly and the ability to update schema automatically.

If you specify other options, such as `partitionBy`, in combination with append mode, <Delta> validates that they match and throws an error for any mismatch.  When `partitionBy` is not present, appends automatically follow the partitioning of the existing data.

<a id="ddlschema"></a>

## Update table schema

<Delta> lets you update the schema of a table. The following types of changes are supported:


- Adding new columns (at arbitrary positions)
- Reordering existing columns

You can make these changes explicitly using DDL or implicitly using DML.

.. important::
    When you update a Delta table schema, streams that read from that table terminate. If you want the stream to continue you must restart it.

<a id="explicit-schema-update"></a>

### Explicitly update schema

You can use the following DDL to explicitly change the schema of a table.

#### Add columns

```sql
ALTER TABLE table_name ADD COLUMNS (col_name data_type [COMMENT col_comment] [FIRST|AFTER colA_name], ...)
```

By design, nullability is always `true` and cannot be explicitly changed.

To add a column to a nested field, use:

```sql
ALTER TABLE table_name ADD COLUMNS (col_name.nested_col_name data_type [COMMENT col_comment] [FIRST|AFTER colA_name], ...)
```

##### Example

If the schema before running `ALTER TABLE boxes ADD COLUMNS (colB.nested STRING AFTER field1)` is:

```
- root
| - colA
| - colB
| +-field1
| +-field2
```

the schema after is:

```
- root
| - colA
| - colB
| +-field1
| +-nested
| +-field2
```

.. note::
    Adding nested columns is supported only for structs. Arrays and maps are not supported.

#### Change column comment or ordering

```sql
ALTER TABLE table_name ALTER [COLUMN] col_name col_name data_type [COMMENT col_comment] [FIRST|AFTER colA_name]
```

To change a column in a nested field, use:

```sql
ALTER TABLE table_name ALTER [COLUMN] col_name.nested_col_name nested_col_name data_type [COMMENT col_comment] [FIRST|AFTER colA_name]
```

##### Example

If the schema before running `ALTER TABLE boxes CHANGE COLUMN colB.field2 field2 STRING FIRST` is:

```
- root
| - colA
| - colB
| +-field1
| +-field2
```

the schema after is:

```
- root
| - colA
| - colB
| +-field2
| +-field1
```

#### Replace columns

```sql
ALTER TABLE table_name REPLACE COLUMNS (col_name1 col_type1 [COMMENT col_comment1], ...)
```

##### Example

When running the following DDL:

```sql
ALTER TABLE boxes REPLACE COLUMNS (colC STRING, colB STRUCT<field2:STRING, nested:STRING, field1:STRING>, colA STRING)
```

if the schema before is:

```
- root
| - colA
| - colB
| +-field1
| +-field2
```

the schema after is:

```
- root
| - colC
| - colB
| +-field2
| +-nested
| +-field1
| - colA
```


<a id="rename-columns"></a>

#### Rename columns


.. note:: This feature is available in <Delta> 1.2.0 and above. This feature is currently experimental.



To rename columns without rewriting any of the columns' existing data, you must enable column mapping for the table. See [enable column mapping](/versioning.md#column-mapping).


To rename a column:

```sql
ALTER TABLE table_name RENAME COLUMN old_col_name TO new_col_name
```

To rename a nested field:

```sql
ALTER TABLE table_name RENAME COLUMN col_name.old_nested_field TO new_nested_field
```

##### Example

When you run the following command:

```sql
ALTER TABLE boxes RENAME COLUMN colB.field1 TO field001
```

If the schema before is:

```
- root
| - colA
| - colB
| +-field1
| +-field2
```

Then the schema after is:

```
- root
| - colA
| - colB
| +-field001
| +-field2
```


<a id="drop-columns"></a>

#### Drop columns

.. note:: This feature is available in <Delta> 2.0 and above. This feature is currently experimental.

To drop columns as a metadata-only operation without rewriting any data files, you must enable column mapping for the table. See [enable column mapping](/versioning.md#column-mapping).

.. important:: Dropping a column from metadata does not delete the underlying data for the column in files.

To drop a column:

```sql
ALTER TABLE table_name DROP COLUMN col_name
```

To drop multiple columns:

```sql
ALTER TABLE table_name DROP COLUMNS (col_name_1, col_name_2)
```

<a id="change-column-type"></a>

#### Change column type or name

You can change a column's type or name or drop a column by rewriting the table. To do this, use the `overwriteSchema` option:

##### Change a column type

```python
spark.read.table(...) \
  .withColumn("birthDate", col("birthDate").cast("date")) \
  .write \
  .format("delta") \
  .mode("overwrite")
  .option("overwriteSchema", "true") \
  .saveAsTable(...)
```

##### Change a column name

```python
spark.read.table(...) \
  .withColumnRenamed("dateOfBirth", "birthDate") \
  .write \
  .format("delta") \
  .mode("overwrite") \
  .option("overwriteSchema", "true") \
  .saveAsTable(...)
```

<a id="automatic-schema-update"></a>

### Automatic schema update

<Delta> can automatically update the schema of a table as part of a DML transaction (either appending or overwriting), and make the schema compatible with the data being written.

#### Add columns

Columns that are present in the DataFrame but missing from the table are automatically added as part of a write transaction when:

- `write` or `writeStream` have `.option("mergeSchema", "true")`

- `spark.databricks.delta.schema.autoMerge.enabled` is `true`

When both options are specified, the option from the `DataFrameWriter` takes precedence. The added columns are appended to the end of the struct they are present in. Case is preserved when appending a new column.

#### `NullType` columns

Because Parquet doesn't support `NullType`, `NullType` columns are dropped from the DataFrame when writing into Delta tables, but are still stored in the schema. When a different data type is received for that column, <Delta> merges the schema to the new data type. If <Delta> receives a `NullType` for an existing column, the old schema is retained and the new column is dropped during the write.

`NullType` in streaming is not supported. Since you must set schemas when using streaming this should be very rare. `NullType` is also not accepted for complex types such as `ArrayType` and `MapType`.

## Replace table schema

By default, overwriting the data in a table does not overwrite the schema. When overwriting a table using `mode("overwrite")` without `replaceWhere`, you may still want to overwrite the schema of the data being written. You replace the schema and partitioning of the table by setting the `overwriteSchema` option to `true`:

```python
df.write.option("overwriteSchema", "true")
```

## Views on tables

<Delta> supports the creation of views on top of Delta tables just like you might with a data source table.

The core challenge when you operate with views is resolving the schemas. If you alter a Delta table schema, you must recreate derivative views to account for any additions to the schema. For instance, if you add a new column to a Delta table, you must make sure that this column is available in the appropriate views built on top of that base table.

## Table properties

You can store your own metadata as a table property using `TBLPROPERTIES` in `CREATE` and `ALTER`. You can then `SHOW` that metadata. For example:

```sql
ALTER TABLE default.people10m SET TBLPROPERTIES ('department' = 'accounting', 'delta.appendOnly' = 'true');

-- Show the table's properties.
SHOW TBLPROPERTIES default.people10m;

-- Show just the 'department' table property.
SHOW TBLPROPERTIES default.people10m ('department');
```

`TBLPROPERTIES` are stored as part of Delta table metadata. You cannot define new `TBLPROPERTIES` in a `CREATE` statement if a Delta table already exists in a given location.

In addition, to tailor behavior and performance, <Delta> supports certain Delta table properties:

- Block deletes and updates in a Delta table: `delta.appendOnly=true`.
- Configure the [time travel](#deltatimetravel) retention properties: `delta.logRetentionDuration=<interval-string>` and `delta.deletedFileRetentionDuration=<interval-string>`. For details, see [_](#data-retention).


- Configure the number of columns for which statistics are collected: `delta.dataSkippingNumIndexedCols=n`. This property indicates to the writer that statistics are to be collected only for the first `n` columns in the table. Also the data skipping code ignores statistics for any column beyond this column index. This property takes affect only for new data that is written out.

.. note::
  - Modifying a Delta table property is a write operation that will conflict with other [concurrent write operations](concurrency-control.md), causing them to fail. We recommend that you modify a table property only when there are no concurrent write operations on the table.

You can also set `delta.`-prefixed properties during the first commit to a Delta table using Spark configurations. For example, to initialize a Delta table with the property `delta.appendOnly=true`, set the Spark configuration `spark.databricks.delta.properties.defaults.appendOnly` to `true`. For example:

.. code-language-tabs::

  ```sql
  spark.sql("SET spark.databricks.delta.properties.defaults.appendOnly = true")
  ```

  ```python
  spark.conf.set("spark.databricks.delta.properties.defaults.appendOnly", "true")
  ```

  ```scala
  spark.conf.set("spark.databricks.delta.properties.defaults.appendOnly", "true")
  ```

See also the [_](/table-properties.md).

## Syncing table schema and properties to the Hive metastore

You can enable asynchronous syncing of table schema and properties to the metastore by setting `spark.databricks.delta.catalog.update.enabled` to `true`. Whenever the Delta client detects that either of these two were changed due to an update, it will sync the changes to the metastore.

The schema is stored in the table properties in HMS. If the schema is small, it will be stored directly under the key `spark.sql.sources.schema`:

  ```json
  {
    "spark.sql.sources.schema": "{'name':'col1','type':'string','nullable':true, 'metadata':{}},{'name':'col2','type':'string','nullable':true,'metadata':{}}"
  }
  ```

If Schema is large, the schema will be broken down into multiple parts. Appending them together should give the correct schema. E.g.

  ```json
  {
    "spark.sql.sources.schema.numParts": "4",
    "spark.sql.sources.schema.part.1": "{'name':'col1','type':'string','nullable':tr",
    "spark.sql.sources.schema.part.2": "ue, 'metadata':{}},{'name':'co",
    "spark.sql.sources.schema.part.3": "l2','type':'string','nullable':true,'meta",
    "spark.sql.sources.schema.part.4": "data':{}}"
  }
  ```

<a id="table-metadata"></a>

## Table metadata

<Delta> has rich features for exploring table metadata.


It supports `SHOW COLUMNS` and `DESCRIBE TABLE`.

It also provides the following unique commands:

.. contents::
  :local:
  :depth: 1

### `DESCRIBE DETAIL`

Provides information about schema, partitioning, table size, and so on. For details, see [_](delta-utility.md#delta-detail).


### `DESCRIBE HISTORY`

Provides provenance information, including the operation, user, and so on, and operation metrics for each write to a table. Table history is retained for 30 days. For details, see [_](delta-utility.md#delta-history).


<a id="sql-support"></a>

## Configure SparkSession

For many <Delta> operations, you enable integration with Apache Spark DataSourceV2 and Catalog APIs (since 3.0) by setting the following configurations when you create a new `SparkSession`.

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

Alternatively, you can add configurations when submitting your Spark application using `spark-submit` or when starting `spark-shell` or `pyspark` by specifying them as command-line parameters.

```bash
spark-submit --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"  ...
```

```bash
pyspark --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
```

## Configure storage credentials

<Delta> uses Hadoop FileSystem APIs to access the storage systems. The credentails for storage systems usually can be set through Hadoop configurations. <Delta> provides multiple ways to set Hadoop configurations similar to <AS>.

### Spark configurations

When you start a Spark application on a cluster, you can set the Spark configurations in the form of `spark.hadoop.*` to pass your custom Hadoop configurations. For example, Setting a value for `spark.hadoop.a.b.c` will pass the value as a Hadoop configuration `a.b.c`, and <Delta> will use it to access Hadoop FileSystem APIs.

See [Spark doc](http://spark.apache.org/docs/latest/configuration.html#custom-hadoophive-configuration) for more details.

### SQL session configurations

Spark SQL will pass all of the current [SQL session configurations](http://spark.apache.org/docs/latest/configuration.html#runtime-sql-configuration) to <Delta>, and <Delta> will use them to access Hadoop FileSystem APIs. For example, `SET a.b.c=x.y.z` will tell <Delta> to pass the value `x.y.z` as a Hadoop configuration `a.b.c`, and <Delta> will use it to access Hadoop FileSystem APIs.

<a id="hadoop-file-system-options"></a>

### DataFrame options

Besides setting Hadoop file system configurations through the Spark (cluster) configurations or SQL session configurations, Delta supports reading Hadoop file system configurations from `DataFrameReader` and `DataFrameWriter` options (that is, option keys that start with the `fs.` prefix) when the table is read or written, by using `DataFrameReader.load(path)` or `DataFrameWriter.save(path)`.

For example, you can pass your storage credentails through DataFrame options:


.. code-language-tabs::

  ```python
  df1 = spark.read.format("delta") \
    .option("fs.azure.account.key.<storage-account-name>.dfs.core.windows.net", "<storage-account-access-key-1>") \
    .read("...")
  df2 = spark.read.format("delta") \
    .option("fs.azure.account.key.<storage-account-name>.dfs.core.windows.net", "<storage-account-access-key-2>") \
    .read("...")
  df1.union(df2).write.format("delta") \
    .mode("overwrite") \
    .option("fs.azure.account.key.<storage-account-name>.dfs.core.windows.net", "<storage-account-access-key-3>") \
    .save("...")
  ```

  ```scala
  val df1 = spark.read.format("delta")
    .option("fs.azure.account.key.<storage-account-name>.dfs.core.windows.net", "<storage-account-access-key-1>")
    .read("...")
  val df2 = spark.read.format("delta")
    .option("fs.azure.account.key.<storage-account-name>.dfs.core.windows.net", "<storage-account-access-key-2>")
    .read("...")
  df1.union(df2).write.format("delta")
    .mode("overwrite")
    .option("fs.azure.account.key.<storage-account-name>.dfs.core.windows.net", "<storage-account-access-key-3>")
    .save("...")
  ```


You can find the details of the Hadoop file system configurations for your storage in [_](/delta-storage.md).

.. <Delta> replace:: Delta Lake
.. <AS> replace:: Apache Spark