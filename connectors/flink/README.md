# Flink/Delta Connector

[![License](https://img.shields.io/badge/license-Apache%202-brightgreen.svg)](https://github.com/delta-io/delta/blob/master/LICENSE.txt)

Official Delta Lake connector for [Apache Flink](https://flink.apache.org/).

## Table of contents
- [Introduction](#introduction)
  - [APIs](#apis)
  - [Known limitations](#known-limitations)
- [Delta Sink](#delta-sink)
  - [Metrics](#delta-sink-metrics)
  - [Examples](#delta-sink-examples)
- [Delta Source](#delta-source)
  - [Modes](#modes)
    - [Bounded Mode](#bounded-mode)
    - [Continuous Mode](#continuous-mode)
  - [Examples](#delta-source-examples)
- [SQL Support](#sql-support)
- [Usage](#usage)
  - [Maven](#maven)
  - [SBT](#sbt)
- [Building](#building)
- [UML diagrams](#uml-diagrams)
- [FAQ](#frequently-asked-questions-faq)
- [Known Issues](#known-issues)

## Introduction

Flink/Delta Connector is a JVM library to read and write data from Apache Flink applications to Delta tables
utilizing the [Delta Standalone JVM library](https://github.com/delta-io/delta/tree/master/connectors#delta-standalone).
The connector provides exactly-once delivery guarantees.

Flink/Delta Connector includes:
- `DeltaSink` for writing data from Apache Flink to a Delta table.
- `DeltaSource` for reading Delta tables using Apache Flink.

Depending on the version of the connector you can use it with following Apache Flink versions:
  
| Connector's version |    Flink's version    |
|:-------------------:|:---------------------:|
|  0.4.x (Sink Only)  | 1.12.0 <= X <= 1.14.5 |
|        0.5.0        | 1.13.0 <= X <= 1.13.6 |
|        0.6.0        |      X >= 1.15.3      |
|        3.0.0        |      X >= 1.16.1      |

### APIs

See the [Java API docs](https://docs.delta.io/latest/api/java/flink/overview-summary.html) here.

### Known limitations

- For Azure Blob Storage, the current version only supports reading. Writing to Azure Blob Storage is not supported by Flink due to [issue](https://issues.apache.org/jira/browse/FLINK-17444) with class shading.
  However, since Flink 1.17 Azure Data Lake Gen2 is supported – see [FLINK-30128](https://issues.apache.org/jira/browse/FLINK-30128).
- For AWS S3 storage, in order to ensure concurrent transactional writes from different clusters, use [multi-cluster configuration guidelines](https://docs.delta.io/latest/delta-storage.html#multi-cluster-setup). Please see [example](#3-sink-creation-with-multi-cluster-support-for-delta-standalone) for how to use this configuration in Flink Delta Sink. 

## Delta Sink

<div id='delta-sink-metrics'></div>

### Metrics
Delta Sink currently exposes the following Flink metrics:

| metric name |                                        description                                        | update interval |
|:-----------:|:-----------------------------------------------------------------------------------------:|:---------------:|
|    DeltaSinkRecordsOut    |                  Counter for how many records were processed by the sink                  | on every record |
|    DeltaSinkRecordsWritten    |     Counter for how many records were written to the actual files on the file system      |  on checkpoint  |
|    DeltaSinkBytesWritten    | Counter for how many bytes were written to the actual files on the underlying file system |  on checkpoint  |

<div id='delta-sink-examples'></div>

### Examples

#### 1. Sink creation for non-partitioned tables

In this example we show how to create a `DeltaSink` and plug it to an
existing `org.apache.flink.streaming.api.datastream.DataStream`.

```java
import io.delta.flink.sink.DeltaSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.hadoop.conf.Configuration;

public DataStream<RowData> createDeltaSink(
        DataStream<RowData> stream,
        String deltaTablePath,
        RowType rowType) {
    DeltaSink<RowData> deltaSink = DeltaSink
        .forRowData(
            new Path(deltaTablePath),
            new Configuration(),
            rowType)
        .build();
    stream.sinkTo(deltaSink);
    return stream;
}
```

#### 2. Sink creation for partitioned tables

In this example we show how to create a `DeltaSink` for `org.apache.flink.table.data.RowData` to
write data to a partitioned table using one partitioning column `surname`.

```java
import io.delta.flink.sink.DeltaBucketAssigner;
import io.delta.flink.sink.DeltaSinkBuilder;

public DataStream<RowData> createDeltaSink(
        DataStream<RowData> stream,
        String deltaTablePath) {
    String[] partitionCols = { "surname" };
    DeltaSink<RowData> deltaSink = DeltaSink
        .forRowData(
            new Path(deltaTablePath),
            new Configuration(),
            rowType)
        .withPartitionColumns(partitionCols)
        .build();
    stream.sinkTo(deltaSink);
    return stream;
}
```
#### 3. Sink creation with multi cluster support for Delta standalone
In this example we will show how to create `DeltaSink` with [multi-cluster configuration](https://docs.delta.io/latest/delta-storage.html#multi-cluster-setup).

```java
public DataStream<RowData> createDeltaSink(
        DataStream<RowData> stream,
        String deltaTablePath) {
    String[] partitionCols = { "surname" };

    Configuration configuration = new Configuration();
    configuration.set("spark.hadoop.fs.s3a.access.key", "USE_YOUR_S3_ACCESS_KEY_HERE");
    configuration.set("spark.hadoop.fs.s3a.secret.key", "USE_YOUR_S3_SECRET_KEY_HERE");
    configuration.set("spark.delta.logStore.s3a.impl", "io.delta.storage.S3DynamoDBLogStore");
    configuration.set("spark.io.delta.storage.S3DynamoDBLogStore.ddb.region", "eu-central-1");
        
    DeltaSink<RowData> deltaSink = DeltaSink
        .forRowData(
            new Path(deltaTablePath),
            configuration,
            rowType)
        .build();
    stream.sinkTo(deltaSink);
    return stream;
}
```

## Delta Source

### Modes

Delta Source can work in one of two modes, described below.

The `DeltaSource` class provides factory methods to create sources for both modes. Please see [documentation](https://delta-io.github.io/connectors/latest/delta-flink/api/java/index.html) and examples for details.

### Bounded Mode
Suitable for batch jobs, where we want to read content of Delta table for specific table version only. Create a source of this mode using the `DeltaSource.forBoundedRowData` API.

The options relevant to this mode are
- `versionAsOf` - Loads the state of the Delta table at that version.
- `timestampAsOf` - Loads the state of the Delta table at the table version written at or before the given timestamp.
- `columnNames` - Which columns to read. If not provided, the Delta Source source will read all columns.

### Continuous Mode
Suitable for streaming jobs, where we want to continuously check the Delta table for new changes and versions. Create a source of this mode using the `DeltaSource.forContinuousRowData` API.

Note that by default, the Delta Source will load the full state of the latest Delta table, and then start streaming changes. When you use the `startingTimestamp` or `startingVersion` APIs on the `ContinuousDeltaSourceBuilder`, then the Delta Source will process changes only from that corresponding historical version.

The options relevant to this mode are
- `startingVersion` - Starts reading changes from this table version.
- `startingTimestamp` - Starts reading changes from the table version written at or after the given timestamp.
- `updateCheckIntervalMillis` - The interval, in milliseconds, at which we will check the underlying Delta table for any changes.
- `ignoreDeletes` - When set to `true`, the Delta Source will be able to process table versions where data is deleted, and skip those deleted records.
- `ignoreChanges` - When set to `true`, the Delta Source will be able to process table versions where data is changed (i.e. updated), and return those changed records. Note that this can lead to duplicate processing, as some Delta operations, like `UPDATE`, may cause existing rows to be rewritten in new files. Those new files will be treated as new data and be reprocessed. This options subsumes `ignoreDeletes`. Therefore, if you set `ignoreChanges` to `true`, your stream will not be disrupted by either deletions or updates to the source table.
- `columnNames` - Which columns to read. If not provided, the Delta Source source will read all columns.

#### Table schema discovery

Flink Delta source connector will use Delta table log to discover columns and their types.
If user did not specify any columns in source definition, all columns from underlying Delta table will be read.
If user specified a collection of column names, using Delta source builder method, then only those columns will be read from underlying Delta table. 
In both cases, Source connector will discover what are the Delta types for every column and will convert them to corresponding Flink types.

#### Partition column discovery

Flink Delta source connector will use Delta table log to determine which columns are partition columns.
No additional actions are needed from user end.

<div id='delta-source-examples'></div>

### Examples

#### 1. Source creation for Delta table, to read all columns in bounded mode. Suitable for batch jobs. This example loads the latest table version.

```java
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.hadoop.conf.Configuration;

public DataStream<RowData> createBoundedDeltaSourceAllColumns(
        StreamExecutionEnvironment env,
        String deltaTablePath) {

    DeltaSource<RowData> deltaSource = DeltaSource
        .forBoundedRowData(
            new Path(deltaTablePath),
            new Configuration())
        .build();

    return env.fromSource(deltaSource, WatermarkStrategy.noWatermarks(), "delta-source");
}
```

#### 2. Source creation for Delta table, to read all columns in bounded mode. Suitable for batch jobs. This example performs Time Travel and loads a historical version.

```java
public DataStream<RowData> createBoundedDeltaSourceWithTimeTravel(
        StreamExecutionEnvironment env,
        String deltaTablePath) {

    DeltaSource<RowData> deltaSource = DeltaSource
        .forBoundedRowData(
            new Path(deltaTablePath),
            new Configuration())
        // could also use `.versionAsOf(314159)`
        .timestampAsOf("2022-06-28 04:55:00")
        .build();

    return env.fromSource(deltaSource, WatermarkStrategy.noWatermarks(), "delta-source");
}
```

#### 3. Source creation for Delta table, to read only user-defined columns in bounded mode. Suitable for batch jobs. This example loads the latest table version.

```java
public DataStream<RowData> createBoundedDeltaSourceUserColumns(
        StreamExecutionEnvironment env,
        String deltaTablePath,
        String[] columnNames) {

    DeltaSource<RowData> deltaSource = DeltaSource
        .forBoundedRowData(
            new Path(deltaTablePath),
            new Configuration())
        .columnNames(columnNames)
        .build();

    return env.fromSource(deltaSource, WatermarkStrategy.noWatermarks(), "delta-source");
}
```

#### 4. Source creation for Delta table, to read all columns in continuous mode. Suitable for streaming jobs. This example performs Time Travel to get all changes at and after the historical version, and then monitors for changes. It does not load the full table state at that historical version.

```java
public DataStream<RowData> createContinuousDeltaSourceWithTimeTravel(
        StreamExecutionEnvironment env,
        String deltaTablePath) {

    DeltaSource<RowData> deltaSource = DeltaSource
        .forContinuousRowData(
            new Path(deltaTablePath),
            new Configuration())
        // could also use `.startingVersion(314159)`
        .startingTimestamp("2022-06-28 04:55:00")
        .build();

    return env.fromSource(deltaSource, WatermarkStrategy.noWatermarks(), "delta-source");
}
```

#### 5. Source creation for Delta table, to read all columns in continuous mode. Suitable for streaming jobs. This example loads the latest table version and then monitors for changes.

```java
public DataStream<RowData> createContinuousDeltaSourceAllColumns(
        StreamExecutionEnvironment env,
        String deltaTablePath) {

    DeltaSource<RowData> deltaSource = DeltaSource
        .forContinuousRowData(
            new Path(deltaTablePath),
            new Configuration())
        .build();

    return env.fromSource(deltaSource, WatermarkStrategy.noWatermarks(), "delta-source");
}
```

#### 6. Source creation for Delta table, to read only user-defined columns in continuous mode. Suitable for streaming jobs. This example loads the latest table version and then monitors for changes.

```java
public DataStream<RowData> createContinuousDeltaSourceUserColumns(
        StreamExecutionEnvironment env,
        String deltaTablePath,
        String[] columnNames) {

    DeltaSource<RowData> deltaSource = DeltaSource
        .forContinuousRowData(
            new Path(deltaTablePath),
            new Configuration())
        .columnNames(columnNames)
        .build();

    return env.fromSource(deltaSource, WatermarkStrategy.noWatermarks(), "delta-source");
}
```

## SQL Support
Starting from version 3.0.0 the Delta connector can be used for Flink SQL jobs.
Both Delta Source and Delta Sink can be used as Flink Tables for SELECT and INSERT queries.

Flink/Delta SQL connector **must** be used with Delta Catalog. Trying to execute SQL queries on Delta table
using Flink API without Delta Catalog configured will cause SQL job to fail.

_For an example of configuring Flink SQL to write to Delta Lake, see [this blog](https://www.decodable.co/blog/adventures-with-apache-flink-and-delta-lake)._

| Feature support                                | Notes                                                                                   |
|------------------------------------------------|-----------------------------------------------------------------------------------------|
| [CREATE CATALOG](#delta-catalog-configuration) | A Delta Catalog is required for Delta Flink SQL support.                                |
| [CREATE DATABASE](#create-database)            |                                                                                         |
| [CREATE TABLE](#create-table)                  |                                                                                         |
| [CREATE TABLE LIKE](#create-table-like)        |                                                                                         |
| [ALTER TABLE](#alter-table)                    | Support only altering table properties; column and partition changes are not supported. |
| [DROP TABLE](#drop-table)                      | Remove data from metastore leaving Delta table files on filesystem untouched.           |
| [SQL SELECT](#select-query)                    | Supports both batch (default) and streaming modes.                                      |
| [SQL INSERT](#insert-query)                    | Support both streaming and batch mode.                                                  |

### Delta Catalog
The delta log is the source of truth for Delta tables, and the Delta Catalog is the only
Flink catalog implementation that enforces this.
It is required for every interaction with Delta tables via the Flink SQL API. If you attempt to use
any other catalog other than the Delta Catalog, your SQL query will fail.

At the same time, any other Flink connector (Kafka, Filesystem etc.) can be used with Delta Catalog
(so long as it doesn't have any restrictions of its own). This is achieved by Delta Catalog acting
as a proxy for non-Delta tables.

For Delta tables, however, the Delta Catalog ensures that any DDL operation is reflected in the
underlying Delta table. In other words, the Delta Catalog ensures that only valid Delta tables
can be created and used by Flink job.

#### Decorated catalog
Delta Catalog acts as a wrapper around other [Catalog](https://nightlies.apache.org/flink/flink-docs-master/docs/dev/table/catalogs/) implementation.
Currently, we support `in-memory` and `hive` decorated catalogs.
The `in-memory` type is ephemeral and does not persist any data in external metastore. This means that
its bounded only to single session.

The `hive` type is based on Flink's Hive catalog where metadata is persistence external Hive metastore.
In this case, tables defined by user A can be used by user B.

For Delta tables, only minimum information such as database/table name, connector type
and delta table file path will be stored in the metastore.
For Delta tables no information about table properties or schema will be stored in the metastore.
Delta Catalog will store those in `_delta_log`.

#### Delta Catalog Configuration
A catalog is created and named by executing the following query:
```sql
CREATE CATALOG <catalog_name> WITH (
  'type' = 'delta-catalog',
  'catalog-type' = '<decorated-catalog>',
   '<config_key_1>' = '<config_value_1>',
   '<config_key_2>' = '<config_value_2>'
);
USE CATALOG <catalog_name>;
```
Replace `<catalog_name>` with your catalog's name.
Replace `<decorated-catalog>` with the Catalog implementation type that you want to use as the decorated catalog.
Currently, only `in-memory` (default) and `hive` decorated catalogs are supported.

The following properties can be set:
+ `type` - must be `delta-catalog`. This option is required by Flink.
+ `catalog-type` - an optional option that allows to specify type of decorated catalog. Allowed options are:
  + `in-memory`- a default value if no other specified. Will use Flink's In-Memory catalog as decorated catalog.
  + `hive` - Use Flink's Hive catalog as decorated catalog.

Any extra defined property will be passed to the decorated catalog.

#### Hive Metastore
Delta Catalog backed by Hive catalog and use Hive's catalog `hadoop-conf-dir` option call the below query:
```sql
CREATE CATALOG <catalog_name> WITH (
  'type' = 'delta-catalog',
  'catalog-type' = 'hive',
  'hadoop-conf-dir' = '<some-path>'
);
USE CATALOG <catalog_name>;
```

The logic for resolving configuration from `hadoop-conf-dir` depends on [Flink Hive Catalog implementation](https://nightlies.apache.org/flink/flink-docs-release-1.16/docs/connectors/table/hive/hive_catalog/).
It is expected by Flink Hive catalog, that `hadoop-conf-dir` will contain at least one of the files:
- core-site.xml
- hdfs-site.xml
- yarn-site.xml
- mapred-site.xml

The exact list of properties that have to be included in the configuration files depends on your
Hive metastore endpoint/server. The minimum configuration that can be stored in `core-site.xml` file is presented below:
```xml
<configuration>
  <property>
    <name>hive.metastore.uris</name>
    <value>thrift://hive-metastore:9083</value>
    <description>IP address (or fully-qualified domain name) and port of the metastore host</description>
  </property>
</configuration>
```
The `hive-metastore` should be resolved to IP address of hive metastore service.

In order to use Hive Catalog with Flink cluster, an additional [Flink cluster configuration](https://nightlies.apache.org/flink/flink-docs-release-1.16/docs/connectors/table/hive/overview/#dependencies)
is required. in a nutshell, it is required to add `flink-sql-connector-hive-x-x-x.jar` file to Flink's lib folder and
provide Hadoop dependencies, by setting the HADOOP_CLASSPATH environment variable:
`export HADOOP_CLASSPATH='hadoop classpath'`.

Our connector was tested with:
- `flink-sql-connector-hive-2.3.9_2.12-1.16.0.jar`
- Hive 2.3.2 metastore
- Hadoop 3.3.2

It is recommended to use Hadoop version 3.3.2. When using version prior to 3.3.2 we have encountered many issues regarding incompatible
class definitions between Flink fs, Flink Hive connector and Delta Standalone.
For this moment, no tests were conducted for Hadoop version > 3.3.2.

Examples of issues caused by incompatible Hadoop version (`HADOOP_CLASSPATH` env or hadoop dependency in Flink job pom.xml)
while deploying Flink Delta SQL jobs:

```
Caused by: java.lang.IllegalArgumentException:
Cannot invoke org.apache.commons.configuration2.AbstractConfiguration.setListDelimiterHandler on bean class
'class org.apache.commons.configuration2.PropertiesConfiguration' - argument type mismatch -
had objects of type "org.apache.commons.configuration2.convert.DefaultListDelimiterHandler"
but expected signature "org.apache.commons.configuration2.convert.ListDelimiterHandler"
```

```
Caused by: java.lang.LinkageError: loader constraint violation: when resolving method 'void org.apache.hadoop.util.SemaphoredDelegatingExecutor.(com.google.common.util.concurrent.ListeningExecutorService, int, boolean)' the class loader org.apache.flink.util.ChildFirstClassLoader @2486925f of the current class, org/apache/hadoop/fs/s3a/S3AFileSystem, and the class loader 'app' for the method's defining class, org/apache/hadoop/util/SemaphoredDelegatingExecutor, have different Class objects for the type com/google/common/util/concurrent/ListeningExecutorService used in the signature (org.apache.hadoop.fs.s3a.S3AFileSystem is in unnamed module of loader org.apache.flink.util.ChildFirstClassLoader @2486925f, parent loader 'app'; org.apache.hadoop.util.SemaphoredDelegatingExecutor is in unnamed module of loader 'app')
	at org.apache.hadoop.fs.s3a.S3AFileSystem.create(S3AFileSystem.java:769)
	at org.apache.hadoop.fs.FileSystem.create(FileSystem.java:1118)
	at org.apache.hadoop.fs.FileSystem.create(FileSystem.java:1098)
	at org.apache.hadoop.fs.FileSystem.create(FileSystem.java:987)
	at io.delta.storage.S3SingleDriverLogStore.write(S3SingleDriverLogStore.java:299)
```

```
java.lang.NoSuchMethodError: org.apache.hadoop.util.SemaphoredDelegatingExecutor.<init>(Lcom/google/common/util/concurrent/ListeningExecutorService;IZ)V
    at org.apache.hadoop.fs.s3a.S3AFileSystem.create(S3AFileSystem.java:769)
    at org.apache.hadoop.fs.FileSystem.create(FileSystem.java:1195)
    at org.apache.hadoop.fs.FileSystem.create(FileSystem.java:1175)
    at org.apache.hadoop.fs.FileSystem.create(FileSystem.java:1064)
    at io.delta.storage.S3SingleDriverLogStore.write(S3SingleDriverLogStore.java:299)
    at io.delta.standalone.internal.storage.DelegatingLogStore.write(DelegatingLogStore.scala:91)
```

#### Delta Catalog Table Cache
As a performance optimization, the Delta Catalog automatically caches Delta tables,
since these tables can be expensive to recompute.

This cache has default size of 100 (tables) and uses an LRU policy to evict old cached entries.
You can change this value by adding deltaCatalogTableCacheSize to your Flink cluster's
hadoop configuration. Please note that this configuration will have a global effect for every
Delta Catalog instance running on your cluster. See [Hadoop Configuration](#hadoop-configuration) section for details.

### DDL commands
#### CREATE DATABASE
By default, Delta Catalog will use the `default` database.
Use the following example to create a separate database:

```sql
CREATE DATABASE custom_DB;
USE custom_DB;
```

#### CREATE TABLE
To create non-partitioned table use `CREATE TABLE` statement:
```sql
CREATE TABLE testTable (
    id BIGINT,
    data STRING
  ) WITH (
    'connector' = 'delta',
    'table-path' = '<path-to-table>',
    '<arbitrary-user-define-table-property' = '<value>',
    '<delta.*-properties>' = '<value'>
);
```

To create a partitioned table, use `PARTITIONED BY`:
```sql
CREATE TABLE testTable (
    id BIGINT,
    data STRING,
    part_a STRING,
    part_b STRING
  )
  PARTITIONED BY (part_a, part_b);
  WITH (
    'connector' = 'delta',
    'table-path' = '<path-to-table>',
    '<arbitrary-user-define-table-property' = '<value>',
    '<delta.*-properties>' = '<value'>
);
```

Delta connector supports all Flink's table schema [types](https://nightlies.apache.org/flink/flink-docs-master/docs/dev/table/types/#list-of-data-types).

Currently, we do not support computed and metadata columns, primary key and watermark definition in
`CREATE TABLE` statement.

The mandatory DDL options are:
+ `connector` that must be set to 'delta'
+ `table-path` path (filesystem, S3 etc.) of your Delta table.
  If the table doesn't exist on the file system, it will be created for you.

Additionally, to the mandatory options, DDL for Delta table can accept other table properties.
These properties will be persisted into _delta_log for created table. However, they will not be used
by Delta connector during the processing.

Properties NOT allowed as table properties defined in DDL:
+ job-specific-properties like:
  + versionAsOf
  + timestampAsOf
  + startingTimestamp
  + mode
  + ignoreChanges
  + ignoreDeletes
+ Delta Standalone log store configurations such as `delta.logStore.*` properties
+ Parquet Format options such as `parquet.*`

##### Creating the Delta table
When executing `CREATE TABLE` for Delta connector, we can have two situations:
+ Delta table does not exist under `table-path`
+ Delta table already exists under `table-path`

In the first case, Delta Catalog will create Delta table folder and initialize
an empty (zero row) Delta table with schema defined in DDL. Additionally, all table properties defined in DDL
except `connector` and `table-path` will be added to Delta table metadata. On top of that a metastore entry
for new table will be created.

In the second case, where `_delta_log` already exists under specified `tabl-path`, Delta Catalog will throw an exception when:
+ DDL schema does not match `_delta_log` schema.
+ DDL partition definition does not match partition definition from `_delta_log`.
+ Table properties from DDL overrides existing table properties in `_delta_log`

If all above checks were passing, Delta Catalog will add metastore entry for the new table and will
add new table properties to the existing `_delta_log`.

#### CREATE TABLE LIKE
To create a table with the same schema, partitioning, and table properties as another table, use `CREATE TABLE LIKE`.

```sql
CREATE TABLE testTable (
    id BIGINT,
    data STRING
  ) WITH (
    'connector' = 'delta',
    'table-path' = '<path-to-table>'
);

CREATE TABLE likeTestTable
  WITH (
    'connector' = 'delta',
    'table-path' = '%s'
) LIKE testTable;
```

#### ALTER TABLE
Delta connector only supports:
+ altering table name,
+ altering  table property value,
+ adding new table property.

```sql
ALTER TABLE sourceTable SET ('userCustomProp'='myVal1')
ALTER TABLE sourceTable RENAME TO newSourceTable
```

#### DROP TABLE
To delete a table, run:
```sql
DROP TABLE sample;
```

This operation will remove ONLY the metastore entry. No Delta table files will be removed.

### Querying with SQL
#### SELECT query
Delta connector supports both batch (default) and streaming read for Flink jobs.
In order to run `SELECT` query in `batch` mode run:
```sql
SELECT * FROM testTable;
```
Above query will read all records from `testTable` and stop. It is suitable for `BATCH` Flink jobs.

In order to run `SELECT` query in `streaming` mode run:
```sql
SELECT * FROM testTable /*+ OPTIONS('mode' = 'streaming') */;
```
Above query will read all records from `testTable` and will keep monitoring underlying Delta table
for any new data (appends).

Both queries above will read all columns from Delta table. In order to specify subset of columns
that should be read, specify those columns in `SELECT` statement instead using `*` like so:
```sql
SELECT col1, col2, col3 FROM testTable;
```

For more details about Flink `SELECT` statement, please look at [Flink SELECT documentation](https://nightlies.apache.org/flink/flink-docs-master/docs/dev/table/sql/queries/select/).
#### INSERT query
To append new data to the Delta table with a Flink job, use `INSERT INTO` statement.

Inserts three rows into Delta table called `sinkTable` and stops.
```sql
INSERT INTO sinkTable VALUES
            ('a', 'b', 1),
            ('c', 'd', 2),
            ('e', 'f', 3);
```

For examples below it is assumed that `sourceTable` refers to Delta table (Delta connector).
Inserts entire content of table called `sourceTable` into Delta table `sinkTable` and stop. The table schema's must match.
```sql
INSERT INTO sinkTable SELECT * FROM sourceTable;
```

Inserts entire data from `sourceTable` into Delta table `sinkTable` under static partition `region = europe` and stops.
```sql
INSERT INTO sinkTable PARTITION (region='europe') SELECT * FROM sourceTable;
```

Creates a continuous query that will insert entire content of table called `sourceTable` into Delta table `sinkTable` and will continuously monitor `sourceTable` for new data.
```sql
INSERT INTO sinkTable SELECT * FROM sourceTable /*+ OPTIONS('mode' = 'streaming') */;
```

### Hadoop Configuration
Delta Connector will resolve Flink cluster Hadoop configuration in order to use properties such as
Delta log store properties or Delta Catalog cache size.

For SQL jobs, Delta connector will resolve Flink cluster hadoop configuration in specify which takes higher/lower precedence:
+ `HADOOP_HOME` environment  variable,
+ hdfs-default.xml pointed by deprecated flink config option `fs.hdfs.hdfsdefault` (deprecated),
+ `HADOOP_CONF_DIR` environment variable,
+ properties from Flink cluster config prefixed with `flink.hadoop`.

### SQL API Limitations
The Delta connector currently supports only Physical columns. The Metadata and Computed columns
are currently not supported. For details please see [here](https://nightlies.apache.org/flink/flink-docs-master/docs/dev/table/sql/create/#columns).

Other unsupported features:
+ Watermark definition for CREATE TABLE statement.
+ Primary Key definition for CREATE TABLE statement.
+ Schema ALTER queries (create, drop column) including partitions columns.
+ Table and column comments.

## Usage

You can add the Flink/Delta Connector library as a dependency using your favorite build tool. Please note
that it expects the following packages to be provided:

DataStream API Only:
- `delta-standalone`
- `flink-parquet`
- `flink-table-common`
- `hadoop-client`

Additional libraries for Table/SQL API:
- `flink-clients`
- `flink-table-planner_2.12`

Additional libraries for AWS/S3 support
- enabling flink-s3-fs-hadoop plugin on Flink cluster [details here](https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/filesystems/s3/#hadooppresto-s3-file-systems-plugins)
- `hadoop-aws`

### Maven
Please see the following build files for more details.

#### Scala 2.12:

```xml
<project>
    <properties>
        <scala.main.version>2.12</scala.main.version>
        <delta-connectors-version>3.0.0</delta-connectors-version>
        <flink-version>1.16.1</flink-version>
        <hadoop-version>3.1.0</hadoop-version>
    </properties>

    <dependencies>
        <dependency>
            <groupId>io.delta</groupId>
            <artifactId>delta-flink</artifactId>
            <version>${delta-connectors-version}</version>
        </dependency>
        <dependency>
            <groupId>io.delta</groupId>
            <artifactId>delta-standalone_${scala.main.version}</artifactId>
            <version>${delta-connectors-version}</version>
        </dependency>
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-clients_${scala.main.version}</artifactId>
            <version>${flink-version}</version>
        </dependency>
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-parquet_${scala.main.version}</artifactId>
            <version>${flink-version}</version>
        </dependency>
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-table-common</artifactId>
            <version>${flink-version}</version>
            <scope>provided</scope>
        </dependency>

      <!-- Needed for Flink Table/SQL API -->
      <dependency>
        <groupId>org.apache.hadoop</groupId>
        <artifactId>hadoop-client</artifactId>
        <version>${hadoop-version}</version>
      </dependency>
      <dependency>
          <groupId>org.apache.flink</groupId>
          <artifactId>flink-table-runtime_${scala.main.version}</artifactId>
          <version>${flink-version}</version>
          <scope>provided</scope>
      </dependency>
      <!-- End needed for Flink Table/SQL API -->

      <!-- Needed for AWS S3 support -->
      <dependency>
        <groupId>org.apache.hadoop</groupId>
        <artifactId>hadoop-aws</artifactId>
        <version>${hadoop-version}</version>
      </dependency>
      <!-- End needed for AWS S3 support -->
  </dependencies>
</project>
```

#### SBT

Please replace the versions of the dependencies with the ones you are using.

```scala
libraryDependencies ++= Seq(
  "io.delta" %% "delta-flink" % deltaConnectorsVersion,
  "io.delta" %% "delta-standalone" % deltaConnectorsVersion,
  "org.apache.flink" %% "flink-clients" % flinkVersion,
  "org.apache.flink" %% "flink-parquet" % flinkVersion,
  "org.apache.hadoop" % "hadoop-client" % hadoopVersion,
  "org.apache.flink" % "flink-table-common" % flinkVersion % "provided",
  "org.apache.flink" %% "flink-table-runtime" % flinkVersion % "provided")
```

## Building

The project is compiled using [SBT](https://www.scala-sbt.org/1.x/docs/Command-Line-Reference.html).

### Environment requirements

- JDK 8 or above.
- Scala 2.11 or 2.12.

### Build commands

- To compile the project, run `build/sbt flink/compile`
- To test the project, run `build/sbt flink/test`
- To publish the JAR, run `build/sbt flink/publishM2`

## UML diagrams
UML diagrams can be found [here](uml/README.md)

## Frequently asked questions (FAQ)

#### Can I use this connector to append data to a Delta table?

Yes, you can use this connector to append data to either an existing or a new Delta table (if there is no existing
Delta log in a given path then it will be created by the connector).

#### Can I use this connector with other modes (overwrite, upsert etc.) ?

No, currently only append is supported. Other modes may be added in future releases.

#### Do I need to specify the partition columns when creating a Delta table?

If you'd like your data to be partitioned, then you should. If you are using the `DataStream API`, then
you can provide the partition columns using the `RowDataDeltaSinkBuilder.withPartitionColumns(List<String> partitionCols)` API.

#### Why do I need to specify the table schema? Shouldn’t it exist in the underlying Delta table metadata or be extracted from the stream's metadata?

Unfortunately we cannot extract schema information from a generic `DataStream`, and it is also required for interacting
with the Delta log. The sink must be aware of both Delta table's schema and the structure of the events in the stream in
order not to violate the integrity of the table.

#### What if I change the underlying Delta table schema ?

Next commit (after mentioned schema change) performed from the `DeltaSink` to the Delta log will fail unless you call `RowDataDeltaSinkBuilder::withMergeSchema(true)`. In such case Delta Standalone will try to merge both schemas and check for
their compatibility. If this check fails (e.g. the change consisted of removing a column) the commit to the Delta Log will fail, which will cause failure of the Flink job.

## Local Development & Testing

- Before local debugging of `flink` tests in IntelliJ, run all `flink` tests using SBT. It will
  generate `Meta.java` object under your target directory that is providing the connector with correct version of the
  connector.

## Known issues:

- (0.4.x) Due to a dependency conflict with some Apache Flink packages, it may be necessary to shade
  classes from `org.apache.flink.streaming.api.functions.sink.filesystem` package when producing a fat-jar
  with a Flink job that uses this connector before deploying it to a Flink cluster.
  
  If that package is not shaded, you may experience errors like the following:
  
  ```
  Caused by: java.lang.IllegalAccessError: tried to access method org.apache.flink.streaming.api.functions.sink.filesystem.OutputStreamBasedPartFileWriter.<init>(Ljava/lang/Object;Lorg/apache/flink/core/fs/RecoverableFsDataOutputStream;J)V from class org.apache.flink.streaming.api.functions.sink.filesystem.DeltaBulkPartWriter
  ```

  Here is an example configuration for achieving this:

  ```xml
  <plugin>
      <groupId>org.apache.maven.plugins</groupId>
      <artifactId>maven-shade-plugin</artifactId>
      <version>3.3.0</version>
      <executions>
          <execution>
              <phase>package</phase>
              <goals>
                  <goal>shade</goal>
              </goals>
              <configuration>
                  <shadedArtifactAttached>true</shadedArtifactAttached>
                  <relocations>
                      <relocation>
                          <pattern>org.apache.flink.streaming.api.functions.sink.filesystem</pattern>
                          <shadedPattern>shaded.org.apache.flink.streaming.api.functions.sink.filesystem</shadedPattern>
                      </relocation>
                  </relocations>
                  <filters>
                      <filter>
                          <artifact>*:*</artifact>
                          <excludes>
                              <exclude>META-INF/*.SF</exclude>
                              <exclude>META-INF/*.DSA</exclude>
                              <exclude>META-INF/*.RSA</exclude>
                          </excludes>
                      </filter>
                  </filters>
              </configuration>
          </execution>
      </executions>
  </plugin>
  ```
