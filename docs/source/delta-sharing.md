 
---
description: Learn how to perform reads on Delta Sharing tables.
---

# Read Delta Sharing Tables

[Delta Sharing](https://delta.io/sharing/) is an open protocol for secure real-time exchange of large datasets, which enables 
organizations to share data in real time regardless of which computing platforms they use. 
It is a simple REST protocol that securely shares access to part of a cloud dataset and leverages 
modern cloud storage systems, such as S3, ADLS, or GCS, to reliably transfer data.

In Delta Sharing, data provider is the one who owns the original dataset or table, and share it with a broad range of consumers. 
Each table can be configured to share with different options(history, filtering, etc.) We focus on consuming the shared table in this doc.

Delta Sharing data source supports most of the options provided by <AS> DataFrame for performing reads through batch, streaming, or table changes (CDF) APIs on shared tables.
Delta Sharing doesn't support writing to a shared table. Please refer to the [Delta Sharing repo](https://github.com/delta-io/delta-sharing/blob/main/README.md) for more details. 

For Delta Sharing reads on shared tables with advanced <Delta> features such as DeletionVectors and ColumnMapping, 
you enable integration with <AS> DataSourceV2 and Catalog APIs (since 3.1) by setting the same configurations as delta when you create a new `SparkSession`. See [_](delta-batch.md#sql-support).


.. contents:: In this article:
  :local:
  :depth: 2

## Read a snapshot
After you save the [Profile File](#profile-file) and launch Spark with the connector library, you can access shared tables.

.. code-language-tabs::

  ```SQL
  -- A table path is the profile file path following with `#` and the fully qualified name 
  -- of a table (`<share-name>.<schema-name>.<table-name>`).
  CREATE TABLE mytable USING deltaSharing LOCATION '<profile-file-path>#<share-name>.<schema-name>.<table-name>';
  SELECT * FROM mytable;
  ```
  ```python
  # A table path is the profile file path following with `#` and the fully qualified name 
  # of a table (`<share-name>.<schema-name>.<table-name>`).
  table_path = "<profile-file-path>#<share-name>.<schema-name>.<table-name>"
  df = spark.read.format("deltaSharing").load(table_path)
  ```
  ```scala
  // A table path is the profile file path following with `#` and the fully qualified name 
  // of a table (`<share-name>.<schema-name>.<table-name>`).
  val tablePath = "<profile-file-path>#<share-name>.<schema-name>.<table-name>"
  val df = spark.read.format("deltaSharing").load(tablePath)
  ```
  ```java
  // A table path is the profile file path following with `#` and the fully qualified name 
  // of a table (`<share-name>.<schema-name>.<table-name>`).
  String tablePath = "<profile-file-path>#<share-name>.<schema-name>.<table-name>";
  Dataset<Row> df = spark.read.format("deltaSharing").load(tablePath);
  ```

The DataFrame returned automatically reads the most recent snapshot of the table for any query; 

Delta Sharing supports predicates pushdown to allow fetching only needed data from the Delta Sharing server when there are applicable predicates in the query.

## Query an older snapshot of a shared table (time travel)

Once the data provider enables history sharing of the shared table, Delta Sharing time travel allows you to query an older snapshot of a shared table.

.. code-language-tabs::

  ```sql
  SELECT * FROM mytable TIMESTAMP AS OF timestamp_expression
  SELECT * FROM mytable VERSION AS OF version
  ```
  ```python
  spark.read.format("deltaSharing").option("timestampAsOf", timestamp_string).load(tablePath)

  spark.read.format("deltaSharing").option("versionAsOf", version).load(tablePath)
  ```
  ```scala
  spark.read.format("deltaSharing").option("timestampAsOf", timestamp_string).load(tablePath)

  spark.read.format("deltaSharing").option("versionAsOf", version).load(tablePath)
  ```

The `timestamp_expression` and `version` share the same syntax as [<Delta>](delta-batch.md#timestamp-and-version-syntax).

## Read Table Changes (CDF)

Once the data provider turns on CDF on the original <Delta> table and shares it with history through Delta Sharing, the recipient can query CDF of a Delta Sharing table similar to CDF of a <Delta> table.

TODO: check timestamp format.

.. code-language-tabs::

  ```sql
  CREATE TABLE mytable USING deltaSharing LOCATION '<profile-file-path>#<share-name>.<schema-name>.<table-name>';
  
  -- version as ints or longs e.g. changes from version 0 to 10
  SELECT * FROM table_changes('mytable', 0, 10)
  
  -- timestamp as string formatted timestamps
  SELECT * FROM table_changes('mytable', '2021-04-21 05:45:46', '2021-05-21 12:00:00')
  
  -- providing only the startingVersion/timestamp
  SELECT * FROM table_changes('mytable', 0)
  ```
  ```python
  table_path = "<profile-file-path>#<share-name>.<schema-name>.<table-name>"
  
  # version as ints or longs
  spark.read.format("deltaSharing") \
    .option("readChangeFeed", "true") \
    .option("startingVersion", 0) \
    .option("endingVersion", 10) \
    .load(tablePath)
  
  # timestamps as formatted timestamp
  spark.read.format("deltaSharing") \
    .option("readChangeFeed", "true") \
    .option("startingTimestamp", '2021-04-21 05:45:46') \
    .option("endingTimestamp", '2021-05-21 12:00:00') \
    .load(tablePath)
  
  # providing only the startingVersion/timestamp
  spark.read.format("deltaSharing") \
    .option("readChangeFeed", "true") \
    .option("startingVersion", 0) \
    .load(tablePath)
  ```

  ```scala
  val tablePath = "<profile-file-path>#<share-name>.<schema-name>.<table-name>"

  // version as ints or longs
  spark.read.format("deltaSharing")
    .option("readChangeFeed", "true")
    .option("startingVersion", 0)
    .option("endingVersion", 10)
    .load(tablePath)

  // timestamps as formatted timestamp
  spark.read.format("deltaSharing")
    .option("readChangeFeed", "true")
    .option("startingTimestamp", "2024-01-18 05:45:46")
    .option("endingTimestamp", "2024-01-18 12:00:00")
    .load(tablePath)
  
  // providing only the startingVersion/timestamp
  spark.read.format("deltaSharing")
    .option("readChangeFeed", "true")
    .option("startingVersion", 0)
    .load(tablePath)
  ```

## Streaming

Delta Sharing Streaming is deeply integrated with [Spark Structured Streaming](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html) through `readStream`, 
and able to connect with any sink that is able to perform `writeStream`.

Once the data provider shares a table with history, the recipient can perform a streaming query on the table.
When you load a Delta Sharing table as a stream source and use it in a streaming query, the query processes all of the data present in the shared table as well as any new data that arrives after the stream is started.

```scala
val tablePath = "<profile-file-path>#<share-name>.<schema-name>.<table-name>"

spark.readStream.format("deltaSharing").load(tablePath)
```

Delta Sharing Streaming supports the following functionalities in the same way as Delta Streaming: [Limit input rate](delta-streaming.md#limit-input-rate), 
[Ignore updates and deletes](delta-streaming.md#ignore-updates-and-deletes), [Specify initial position](delta-streaming.md#specify-initial-position)

In addition, `maxVersionsPerRpc` is provided to decide how many versions of files are requested from the server in every Delta Sharing rpc. This is to help
reduce the per rpc workload and make the Delta Sharing streaming job more stable. 
Especially when the streaming resumes from a checkpoint and a lots of new versions are accumulated on the shared table on the server. The default is 100.

.. note:: Trigger.AvailableNow is not supported in Delta Sharing Streaming.

## Read Advanced <Delta> Features in Delta Sharing
In order to support advanced <Delta> features in Delta Sharing, "Delta Format Sharing" is introduced since delta-sharing-client 1.0 and delta-sharing-spark 3.1, 
in which the actions of a shared table is returned in <Delta> format, then a <Delta> library is leveraged to read data.

Please do remember to set the spark configurations mentioned in [_](delta-batch.md#sql-support) in order to read shared tables with DeletionVectors and ColumnMapping. 

| Read Table Feature | Available since version |
| - | - |
| Deletion Vectors(#delta-deletion-vectors.md) | 3.1.0 | 
| ColumnMapping(#delta-column-mapping.md) | 3.1.0 |

Batch queries can be performed as is, because it can automatically resolve the responseFormat based on the table features of the shared table.
An additional option `responseFormat=delta` needs to be set for cdf and streaming queries when reading shared tables with DeletionVectors or ColumnMapping enabled.

```scala
import org.apache.spark.sql.SparkSession

val spark = SparkSession
        .builder()
        .appName("...")
        .master("...")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()

val tablePath = "<profile-file-path>#<share-name>.<schema-name>.<table-name>"

// Batch query
spark.read.format("deltaSharing").load(tablePath)

// CDF query
spark.read.format("deltaSharing")
  .option("readChangeFeed", "true")
  .option("responseFormat", "delta")
  .option("startingVersion", 1)
  .load(tablePath)

// Streaming query
spark.readStream.format("deltaSharing").option("responseFormat", "delta").load(tablePath)
```

<a id="profile-file"></a>

## Profile File
A profile file is a JSON file that contains the information for a recipient to access shared data on a Delta Sharing server. There are a few fields in this file as listed below.

| Field Name | Descrption |
| - | - |
| shareCredentialsVersion | The file format version of the profile file. This version will be increased whenever non-forward-compatible changes are made to the profile format. When a client is running an unsupported profile file format version, it should show an error message instructing the user to upgrade to a newer version of their client. |
| endpoint | The url of the sharing server. |
| bearerToken | The [bearer token](https://tools.ietf.org/html/rfc6750) to access the server. |
| expirationTime | The expiration time of the bearer token in [ISO 8601 format](https://www.w3.org/TR/NOTE-datetime). This field is optional and if it is not provided, the bearer token can be seen as never expire. |

Example:

```json
{
  "shareCredentialsVersion": 1,
  "endpoint": "https://sharing.delta.io/delta-sharing/",
  "bearerToken": "<token>",
  "expirationTime": "2024-01-22T00:12:29.0Z"
}
```

.. include:: /shared/replacements.md
