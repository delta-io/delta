---
description: Learn how to get row-level change information from Delta tables using the Delta change data feed
---

# Change data feed

Change Data Feed (CDF) feature allows Delta tables to track row-level changes between versions of a Delta table. When enabled on a Delta table, the runtime records "change events" for all the data written into the table. This includes the row data along with metadata indicating whether the specified row was inserted, deleted, or updated.

You can read the change events in batch queries using DataFrame APIs (that is, `df.read`) and in streaming queries using DataFrame APIs (that is, `df.readStream`).

## Use cases

Change Data Feed is not enabled by default. The following use cases should drive when you enable the change data feed.

- **Silver and Gold tables**: Improve Delta performance by processing only row-level changes following initial `MERGE`, `UPDATE`, or `DELETE` operations to accelerate and simplify ETL and ELT operations.
- **Transmit changes**: Send a change data feed to downstream systems such as Kafka or RDBMS that can use it to incrementally process in later stages of data pipelines.
- **Audit trail table**: Capture the change data feed as a Delta table provides perpetual storage and efficient query capability to see all changes over time, including when deletes occur and what updates were made.

## Enable change data feed

You must explicitly enable the change data feed option using one of the following methods:

- **New table**: Set the table property `delta.enableChangeDataFeed = true` in the `CREATE TABLE` command.

  ```sql
  CREATE TABLE student (id INT, name STRING, age INT) TBLPROPERTIES (delta.enableChangeDataFeed = true)
  ```

- **Existing table**: Set the table property `delta.enableChangeDataFeed = true` in the `ALTER TABLE` command.

  ```sql
  ALTER TABLE myDeltaTable SET TBLPROPERTIES (delta.enableChangeDataFeed = true)
  ```

- **All new tables**:

  ```sql
  set spark.databricks.delta.properties.defaults.enableChangeDataFeed = true;
  ```

.. important::
  Once you enable the change data feed option for a table, you can no longer write to the table using <Delta> 1.2.1 or below. You can always read the table.

  Only changes made _after_ you enable the change data feed are recorded; past changes to a table are not captured.

### Change data storage

<Delta> records change data for `UPDATE`, `DELETE`, and `MERGE` operations in the `_change_data` folder under the Delta table directory.
These records may be skipped when <Delta> detects it can efficiently compute the change data feed directly from the transaction log.
In particular, insert-only operations and full partition deletes will not generate data in the `_change_data` directory.

The files in the `_change_data` folder follow the retention policy of the table. Therefore, if you run the [VACUUM](/delta-utility.md#delta-vacuum) command, change data feed data is also deleted.

## Read changes in batch queries

You can provide either version or timestamp for the start and end. The start and end versions and timestamps are inclusive in the queries.
To read the changes from a particular start version to the _latest_ version of the table, specify only the starting version or timestamp.

You specify a version as an integer and a timestamps as a string in the format `yyyy-MM-dd[ HH:mm:ss[.SSS]]`.

If you provide a version lower or timestamp older than one that has recorded change events, that is, when the change data feed was enabled,
an error is thrown indicating that the change data feed was not enabled.

.. code-language-tabs::
  ```sql
  -- version as ints or longs e.g. changes from version 0 to 10
  SELECT * FROM table_changes('tableName', 0, 10)

  -- timestamp as string formatted timestamps
  SELECT * FROM table_changes('tableName', '2021-04-21 05:45:46', '2021-05-21 12:00:00')

  -- providing only the startingVersion/timestamp
  SELECT * FROM table_changes('tableName', 0)

  -- database/schema names inside the string for table name, with backticks for escaping dots and special characters
  SELECT * FROM table_changes('dbName.`dotted.tableName`', '2021-04-21 06:45:46' , '2021-05-21 12:00:00')

  -- path based tables
  SELECT * FROM table_changes_by_path('\path', '2021-04-21 05:45:46')
  ```

  ```python
  # version as ints or longs
  spark.read.format("delta") \
    .option("readChangeFeed", "true") \
    .option("startingVersion", 0) \
    .option("endingVersion", 10) \
    .table("myDeltaTable")

  # timestamps as formatted timestamp
  spark.read.format("delta") \
    .option("readChangeFeed", "true") \
    .option("startingTimestamp", '2021-04-21 05:45:46') \
    .option("endingTimestamp", '2021-05-21 12:00:00') \
    .table("myDeltaTable")

  # providing only the startingVersion/timestamp
  spark.read.format("delta") \
    .option("readChangeFeed", "true") \
    .option("startingVersion", 0) \
    .table("myDeltaTable")


  # path based tables
  spark.read.format("delta") \
    .option("readChangeFeed", "true") \
    .option("startingTimestamp", '2021-04-21 05:45:46') \
    .load("pathToMyDeltaTable")
  ```

  ```scala
  // version as ints or longs
  spark.read.format("delta")
    .option("readChangeFeed", "true")
    .option("startingVersion", 0)
    .option("endingVersion", 10)
    .table("myDeltaTable")

  // timestamps as formatted timestamp
  spark.read.format("delta")
    .option("readChangeFeed", "true")
    .option("startingTimestamp", "2021-04-21 05:45:46")
    .option("endingTimestamp", "2021-05-21 12:00:00")
    .table("myDeltaTable")

  // providing only the startingVersion/timestamp
  spark.read.format("delta")
    .option("readChangeFeed", "true")
    .option("startingVersion", 0)
    .table("myDeltaTable")

  // path based tables
  spark.read.format("delta")
    .option("readChangeFeed", "true")
    .option("startingTimestamp", "2021-04-21 05:45:46")
    .load("pathToMyDeltaTable")
  ```

## Read changes in streaming queries

.. code-language-tabs::

  ```python
  # providing a starting version
  spark.readStream.format("delta") \
    .option("readChangeFeed", "true") \
    .option("startingVersion", 0) \
    .table("myDeltaTable")

  # providing a starting timestamp
  spark.readStream.format("delta") \
    .option("readChangeFeed", "true") \
    .option("startingTimestamp", "2021-04-21 05:35:43") \
    .load("/pathToMyDeltaTable")

  # not providing a starting version/timestamp will result in the latest snapshot being fetched first
  spark.readStream.format("delta") \
    .option("readChangeFeed", "true") \
    .table("myDeltaTable")

  ```

  ```scala
  // providing a starting version
  spark.readStream.format("delta")
    .option("readChangeFeed", "true")
    .option("startingVersion", 0)
    .table("myDeltaTable")

  // providing a starting timestamp
  spark.readStream.format("delta")
    .option("readChangeFeed", "true")
    .option("startingVersion", "2021-04-21 05:35:43")
    .load("/pathToMyDeltaTable")

  // not providing a starting version/timestamp will result in the latest snapshot being fetched first
  spark.readStream.format("delta")
    .option("readChangeFeed", "true")
    .table("myDeltaTable")
  ```

To get the change data while reading the table, set the option `readChangeFeed` to `true`.
The `startingVersion` or `startingTimestamp` are optional and if not provided the stream returns the latest
snapshot of the table at the time of streaming as an `INSERT` and future changes as change data.
Options like rate limits (`maxFilesPerTrigger`, `maxBytesPerTrigger`) and `excludeRegex` are also supported when reading change data.

.. note::
  Rate limiting can be atomic for versions other than the starting snapshot version. That is, the entire commit version will be rate limited or the entire commit will be returned.

  By default if a user passes in a version or timestamp exceeding the last commit on a table, the error `timestampGreaterThanLatestCommit` will be thrown. CDF can handle the out of range version case, if the user sets the following configuration to `true`.
  
  ```sql
  set spark.databricks.delta.changeDataFeed.timestampOutOfRange.enabled = true;
  ```

  If you provide a start version greater than the last commit on a table or a start timestamp newer than the last commit on a table, then when the preceding configuration is enabled, an empty read result is returned.

  If you provide an end version greater than the last commit on a table or an end timestamp newer than the last commit on a table, then when the preceding configuration is enabled in batch read mode, all changes between the start version and the last commit are be returned.

## What is the schema for the change data feed?

When you read from the change data feed for a table, the schema for the latest table version is used.

.. note:: Most schema change and evolution operations are fully supported. Tables with column mapping enabled do not support all use cases and demonstrate different behavior. See [_](#column-mapping-limitations).

In addition to the data columns from the schema of the Delta table, change data feed contains metadata columns that identify the type of change event:

| Column name                            | Type      | Values                                                               |
|----------------------------------------|-----------|----------------------------------------------------------------------|
| `_change_type`                         | String    | `insert`, `update_preimage` , `update_postimage`, `delete` [(1)](#1) |
| `_commit_version`                      | Long      | The Delta log or table version containing the change.                |
| `_commit_timestamp`                    | Timestamp | The timestamp associated when the commit was created.                |


<a id="1"></a>
**(1)** `preimage` is the value before the update, `postimage` is the value after the update.

<a id="column-mapping-limitations"></a>

## Change data feed limitations for tables with column mapping enabled

With column mapping enabled on a Delta table, you can drop or rename columns in the table without rewriting data files for existing data. With column mapping enabled, change data feed has limitations after performing non-additive schema changes such as renaming or dropping a column, changing data type, or nullability changes.

.. important::

  In <Delta> 2.0 and before, tables with column mapping enabled do not support streaming reads or batch reads on change data feed.

  In <Delta> 2.1, tables with column mapping enabled support batch reads on change data feed as long as there are no non-additive schema changes. Streaming reads of change data feed of tables with column mapping enabled is not supported. 

  In <Delta> 2.2, tables with column mapping enabled support both batch and streaming reads on change data feed as long as there are no non-additive schema changes. 

  In <Delta> 2.3 and above, you can perform batch reads on change data feed for tables with column mapping enabled that have experienced non-additive schema changes. Instead of using the schema of the latest version of the table, read operations use the schema of the end version of the table specified in the query. Queries still fail if the version range specified spans a non-additive schema change.

  In <Delta> 3.0 and above, you can perform streaming read on change data feed for tables with column mapping enabled that have experienced non-additive schema changes by enabling [schema tracking](/delta-streaming.md#schema-tracking).

## Frequently asked questions (FAQ)

### What is the overhead of enabling the change data feed?
There is no significant impact. The change data records are generated in line during the query execution
process, and are generally much smaller than the total size of rewritten files.

### What is the retention policy for change records?
Change records follow the same retention policy as out-of-date table versions, and will be cleaned up through
VACUUM if they are outside the specified retention period.

### When do new records become available in the change data feed?
Change data is committed along with the <Delta> transaction, and will become available at the same time as
the new data is available in the table.

.. <Delta> replace:: Delta Lake
.. <AS> replace:: Apache Spark
