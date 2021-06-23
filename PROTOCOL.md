<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
# Delta Transaction Log Protocol

- [Overview](#overview)
- [Delta Table Specification](#delta-table-specification)
  - [File Types](#file-types)
    - [Data Files](#data-files)
    - [Delta Log Entries](#delta-log-entries)
    - [Checkpoints](#checkpoints)
    - [Last Checkpoint File](#last-checkpoint-file)
  - [Actions](#actions)
    - [Change Metadata](#change-metadata)
      - [Format Specification](#format-specification)
    - [Add File and Remove File](#add-file-and-remove-file)
    - [Transaction Identifiers](#transaction-identifiers)
    - [Protocol Evolution](#protocol-evolution)
    - [Commit Provenance Information](#commit-provenance-information)
- [Action Reconciliation](#action-reconciliation)
- [Requirements for Writers](#requirements-for-writers)
  - [Creation of New Log Entries](#creation-of-new-log-entries)
  - [Consistency Between Table Metadata and Data Files](#consistency-between-table-metadata-and-data-files)
  - [Delta Log Entries](#delta-log-entries-1)
  - [Checkpoints](#checkpoints-1)
    - [Checkpoint Format](#checkpoint-format)
  - [Data Files](#data-files-1)
  - [Append-only Tables](#append-only-tables)
  - [Column Invariants](#column-invariants)
  - [Generated Columns](#generated-columns)
  - [Writer Version Requirements](#writer-version-requirements)
- [Appendix](#appendix)
  - [Per-file Statistics](#per-file-statistics)
  - [Partition Value Serialization](#partition-value-serialization)
  - [Schema Serialization Format](#schema-serialization-format)
    - [Primitive Types](#primitive-types)
    - [Struct Type](#struct-type)
    - [Struct Field](#struct-field)
    - [Array Type](#array-type)
    - [Map Type](#map-type)
    - [Example](#example)
  - [Checkpoint Schema](#checkpoint-schema)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

<font color="red">THIS IS AN IN-PROGRESS DRAFT</font>

# Overview
This document is a specification for the Delta Transaction Protocol, which brings [ACID](https://en.wikipedia.org/wiki/ACID) properties to large collections of data, stored as files, in a distributed file system or object store. The protocol was designed with the following goals in mind:

- **Serializable ACID Writes** - multiple writers can concurrently modify a Delta table while maintaining ACID semantics.
- **Snapshot Isolation for Reads** - readers can read a consistent snapshot of a Delta table, even in the face of concurrent writes.
- **Scalability to billions of partitions or files** - queries against a Delta table can be planned on a single machine or in parallel.
- **Self describing** - all metadata for a Delta table is stored alongside the data. This design eliminates the need to maintain a separate metastore just to read the data and also allows static tables to be copied or moved using standard filesystem tools.
- **Support for incremental processing** - readers can tail the Delta log to determine what data has been added in a given period of time, allowing for efficient streaming.

Delta's transactions are implemented using multi-version concurrency control (MVCC).
As a table changes, Delta's MVCC algorithm keeps multiple copies of the data around rather than immediately replacing files that contain records that are being updated or removed.

Readers of the table ensure that they only see one consistent _snapshot_ of a table at time by using the _transaction log_ to selectively choose which _data files_ to process.

Writers modify the table in two phases:
First, they optimistically write out new data files or updated copies of existing ones.
Then, they _commit_, creating the latest _atomic version_ of the table by adding a new entry to the log.
In this log entry they record which data files to logically add and remove, along with changes to other metadata about the table.

Data files that are no longer present in the latest version of the table can be lazily deleted by the vacuum command after a user-specified retention period (default 7 days).

# Delta Table Specification
A table has a single serial history of atomic versions, which are named using contiguous, monotonically-increasing integers.
The state of a table at a given version is called a _snapshot_ and is defined by the following properties:
 - **Version of the Delta log protocol** that is required to correctly read or write the table
 - **Metadata** of the table (e.g., the schema, a unique identifier, partition columns, and other configuration properties)
 - **Set of files** present in the table, along with metadata about those files
 - **Set of tombstones** for files that were recently deleted
 - **Set of applications-specific transactions** that have been successfully committed to the table

## File Types
A Delta table is stored within a directory and is composed of four different types of files.

Here is an example of a Delta table with three entries in the commit log, stored in the directory `mytable`.
```
/mytable/_delta_log/00000000000000000000.json
/mytable/_delta_log/00000000000000000001.json
/mytable/_delta_log/00000000000000000003.json
/mytable/_delta_log/00000000000000000003.checkpoint.parquet
/mytable/_delta_log/_last_checkpoint
/mytable/part-00000-3935a07c-416b-4344-ad97-2a38342ee2fc.c000.snappy.parquet
```

### Data Files
Data files can be stored in the root directory of the table or in any non-hidden subdirectory (i.e., one whose name does not start with an `_`).
By default, the reference implementation stores data files in directories that are named based on the partition values for data in that file (i.e. `part1=value1/part2=value2/...`).
This directory format is only used to follow existing conventions and is not required by the protocol.
Actual partition values for a file must be read from the transaction log.

### Delta Log Entries
Delta files are stored as JSON in a directory at the root of the table named `_delta_log`, and together make up the log of all changes that have occurred to a table.
Delta files are the unit of atomicity for a table, and are named using the next available version number, zero-padded to 20 digits.

For example:

```
./_delta_log/00000000000000000000.json
```

A delta file, `n.json`, contains an atomic set of [_actions_](#Actions) that should be applied to the previous table state, `n-1.json`, in order to the construct `n`th snapshot of the table.
An action changes one aspect of the table's state, for example, adding or removing a file.

### Checkpoints
Checkpoints are also stored in the `_delta_log` directory, and can be created for any version of the table.

A checkpoint contains the complete replay of all actions up until this version, with invalid actions removed.
Invalid actions are those that have been canceled out by a subsequent ones (for example removing a file that has been added), using the [rules for reconciliation](#Action-Reconciliation)
Checkpoints allow readers to short-cut the cost of reading the log up-to a given point in order to reconstruct a snapshot.

By default, the reference implementation creates a checkpoint every 10 commits.

The checkpoint file name is based on the version of the table that the checkpoint contains.
The format of the checkpoint file name can take one of two forms:

1. A single checkpoint file for version `n` of the table will be named `n.checkpoint.parquet`. For example:

```
00000000000000000010.checkpoint.parquet
```

2. A multi-part checkpoint for version `n` can be fragmented into `p` files. Fragment `o` of `p` is named `n.checkpoint.o.p.parquet`. For example:

```
00000000000000000010.checkpoint.0000000001.0000000003.parquet
00000000000000000010.checkpoint.0000000002.0000000003.parquet
00000000000000000010.checkpoint.0000000003.0000000003.parquet
```

Since it is possible that a writer will fail while writing out one or more parts of a multi-part checkpoint, readers must only use a complete checkpoint, wherein all fragments are present. For performance reasons, readers should search for the most recent earlier checkpoint that is complete.

Checkpoints for a given version must only be created after the associated delta file has been successfully written.

### Last Checkpoint File
The Delta transaction log will often contain many (e.g. 10,000+) files.
Listing such a large directory can be prohibitively expensive.
The last checkpoint file can help reduce the cost of constructing the latest snapshot of the table by providing a pointer to near the end of the log.

Rather than list the entire directory, readers can locate a recent checkpoint by looking at the `_delta_log/_last_checkpoint` file.
Due to the zero-padded encoding of the files in the log, the version id of this recent checkpoint can be used on storage systems that support lexicographically-sorted, paginated directory listing to enumerate any delta files or newer checkpoints that comprise more recent versions of the table.

This last checkpoint file is encoded as JSON and contains the following information:

Field | Description
-|-
version | the version of the table when the last checkpoint was made.
size | The number of actions that are stored in the checkpoint.
parts | The number of fragments if the last checkpoint was written in multiple parts.

## Actions
Actions modify the state of the table and they are stored both in delta files and in checkpoints.
This section lists the space of available actions as well as their schema.

### Change Metadata
The `metaData` action changes the current metadata of the table.
The first version of a table must contain a `metaData` action.
Subsequent` metaData` actions completely overwrite the current metadata of the table.

There can be at most one metadata action in a given version of the table.

The schema of the `metaData` action is as follows:

Field Name | Data Type | Description
-|-|-
id|`GUID`|Unique identifier for this table
name|`String`| User-provided identifier for this table
description|`String`| User-provided description for this table
format|[Format Struct](#Format-Specification)| Specification of the encoding for the files stored in the table
schemaString|[Schema Struct](#Schema-Serialization-Format)| Schema of the table
partitionColumns|`Array[String]`| An array containing the names of columns by which the data should be partitioned
createdTime|`Option[Long]`| The time when this metadata action is created, in milliseconds since the Unix epoch
configuration|`Map[String, String]`| A map containing configuration options for the metadata action

#### Format Specification
Field Name | Data Type | Description
-|-|-
provider|`String`|Name of the encoding for files in this table
options|`Map[String, String]`|A map containing configuration options for the format

In the reference implementation, the provider field is used to instantiate a Spark SQL [`FileFormat`](https://github.com/apache/spark/blob/master/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/FileFormat.scala). As of Spark 2.4.3 there is built-in `FileFormat` support for `parquet`, `csv`, `orc`, `json`, and `text`.

As of Delta Lake 0.3.0, user-facing APIs only allow the creation of tables where `format = 'parquet'` and `options = {}`. Support for reading other formats is present both for legacy reasons and to enable possible support for other formats in the future (See [#87](https://github.com/delta-io/delta/issues/87)).

The following is an example `metaData` action:
```
{
  "metaData":{
    "id":"af23c9d7-fff1-4a5a-a2c8-55c59bd782aa",
    "format":{"provider":"parquet","options":{}},
    "schemaString":"...",
    "partitionColumns":[],
    "configuration":{
      "appendOnly": "true"
    }
  }
}
```

<!-- TODO: forward references configuration options -->

### Add File and Remove File
The `add` and `remove` actions are used to modify the data in a table by adding or removing individual data files respectively.

The path of a file acts as the primary key for the entry in the set of files.
When an `add` action is encountered for a path that is already present in the table, statistics and other information from the latest version should replace that from any previous version.
As such, additional statistics can be added for a path already present in the table by adding it again.

The `remove` action includes a timestamp that indicates when the removal occurred.
Physical deletion of the file can happen lazily after some user-specified expiration time threshold.
This delay allows concurrent readers to continue to execute against a stale snapshot of the data.
A `remove` action should remain in the state of the table as a _tombstone_ until it has expired.
A tombstone expires when the creation timestamp of the delta file exceeds the expiration threshold added to the `remove` action timestamp.

Since actions within a given Delta file are not guaranteed to be applied in order, it is not valid for multiple file operations with the same path to exist in a single version.

The `dataChange` flag on either an `add` or a `remove` can be set to `false` to indicate that an action when combined with other actions in the same atomic version only rearranges existing data or adds new statistics.
For example, streaming queries that are tailing the transaction log can use this flag to skip actions that would not affect the final results.

The schema of the `add` action is as follows:

Field Name | Data Type | Description
-|-|-
path| String | A relative path, from the root of the table, to a file that should be added to the table
partitionValues| Map[String, String] | A map from partition column to value for this file. See also [Partition Value Serialization](#Partition-Value-Serialization)
size| Long | The size of this file in bytes
modificationTime | Long | The time this file was created, as milliseconds since the epoch
dataChange | Boolean | When `false` the file must already be present in the table or the records in the added file must be contained in one or more `remove` actions in the same version
stats | [Statistics Struct](#Per-file-Statistics) | Contains statistics (e.g., count, min/max values for columns) about the data in this file
tags | Map[String, String] | Map containing metadata about this file

The following is an example `add` action:
```
{
  "add": {
    "path":"date=2017-12-10/part-000...c000.gz.parquet",
    "partitionValues":{"date":"2017-12-10"},
    "size":841454,
    "modificationTime":1512909768000,
    "dataChange":true
    "stats":"{\"numRecords\":1,\"minValues\":{\"val..."
  }
}
```

The schema of the `remove` action is as follows:

Field Name | Data Type | Description
-|-|-
path | String | An absolute or relative path to a file that should be removed from the table
deletionTimestamp | Long | The time the deletion occurred, represented as milliseconds since the epoch
dataChange | Boolean | When `false` the records in the removed file must be contained in one or more `add` file actions in the same version
extendedFileMetadata | Boolean | When `true` the fields `partitionValues`, `size`, and `tags` are present
partitionValues| Map[String, String] | A map from partition column to value for this file. See also [Partition Value Serialization](#Partition-Value-Serialization)
size| Long | The size of this file in bytes
tags | Map[String, String] | Map containing metadata about this file

The following is an example `remove` action.
```
{
  "remove":{
    "path":"part-00001-9…..snappy.parquet",
    "deletionTimestamp":1515488792485,
    "dataChange":true
  }
}
```

### Transaction Identifiers
Incremental processing systems (e.g., streaming systems) that track progress using their own application-specific versions need to record what progress has been made, in order to avoid duplicating data in the face of failures and retries during a write.
Transaction identifiers allow this information to be recorded atomically in the transaction log of a delta table along with the other actions that modify the contents of the table.

Transaction identifiers are stored in the form of `appId` `version` pairs, where `appId` is a unique identifier for the process that is modifying the table and `version` is an indication of how much progress has been made by that application.
The atomic recording of this information along with modifications to the table enables these external system can make their writes into a Delta table _idempotent_.

For example, the [Delta Sink for Apache Spark's Structured Streaming](https://github.com/delta-io/delta/blob/master/src/main/scala/org/apache/spark/sql/delta/sources/DeltaSink.scala) ensures exactly-once semantics when writing a stream into a table using the following process:
 1. Record in a write-ahead-log the data that will be written, along with a monotonically increasing identifier for this batch.
 2. Check the current version of the transaction with `appId = streamId` in the target table. If this value is greater than or equal to the batch being written, then this data has already been added to the table and processing can skip to the next batch.
 3. Write the data optimistically into the table.
 4. Attempt to commit the transaction containing both the addition of the data written out and an updated `appId` `version` pair.

The semantics of the application-specific `version` are left up to the external system.
Delta only ensures that the latest `version` for a given `appId` is available in the table snapshot.
The Delta transaction protocol does not, for example, assume monotonicity of the `version` and it would be valid for the `version` to decrease, possibly representing a "rollback" of an earlier transaction.

The schema of the `txn` action is as follows:

Field Name | Data Type | Description
-|-|-
appId | String | A unique identifier for the application performing the transaction
version | Long | An application-specific numeric identifier for this transaction
lastUpdated | Option[Long] | The time when this transaction action is created, in milliseconds since the Unix epoch

The following is an example `txn` action:
```
{
  "txn": {
    "appId":"3ba13872-2d47-4e17-86a0-21afd2a22395",
    "version":364475
  }
}
```

### Protocol Evolution
The `protocol` action is used to increase the version of the Delta protocol that is required to read or write a given table.
Protocol versioning allows a newer client to exclude older readers and/or writers that are missing features required to correctly interpret the transaction log.
The _protocol version_ will be increased whenever non-forward-compatible changes are made to this specification.
In the case where a client is running an invalid protocol version, an error should be thrown instructing the user to upgrade to a newer protocol version of their Delta client library.

Since breaking changes must be accompanied by an increase in the protocol version recorded in a table, clients can assume that unrecognized fields or actions are never required in order to correctly interpret the transaction log.

The schema of the `protocol` action is as follows:

Field Name | Data Type | Description
-|-|-
minReaderVersion | Int | The minimum version of the Delta read protocol that a client must implement in order to correctly *read* this table
minWriterVersion | Int | The minimum version of the Delta write protocol that a client must implement in order to correctly *write* this table

The current version of the Delta protocol is:
```
{
  "protocol":{
    "minReaderVersion":1,
    "minWriterVersion":2
  }
}
```

### Commit Provenance Information
A delta file can optionally contain additional provenance information about what higher-level operation was being performed as well as who executed it.

Implementations are free to store any valid JSON-formatted data via the `commitInfo` action.

An example of storing provenance information related to an `INSERT` operation:
```
{
  "commitInfo":{
    "timestamp":1515491537026,
    "userId":"100121",
    "userName":"michael@databricks.com",
    "operation":"INSERT",
    "operationParameters":{"mode":"Append","partitionBy":"[]"},
    "notebook":{
      "notebookId":"4443029",
      "notebookPath":"Users/michael@databricks.com/actions"},
      "clusterId":"1027-202406-pooh991"
  }  
}
```

# Action Reconciliation
A given snapshot of the table can be computed by replaying the events committed to the table in ascending order by commit version. A given snapshot of a Delta table consists of:
 - A single `protocol` action
 - A single `metaData` action
 - A map from `appId` to transaction `version`
 - A collection of `add` actions with unique `path`s
 - A collection of `remove` actions with unique `path`s. The intersection of the paths in the `add` collection and `remove` collection must be empty. That means a file cannot exist in both the `remove` and `add` collections. The `remove` actions act as _tombstones_. 
 
To achieve the requirements above, related actions from different delta files need to be reconciled with each other:
 - The latest `protocol` action seen wins
 - The latest `metaData` action seen wins
 - For transaction identifiers, the latest `version` seen for a given `appId` wins
 - All `add` actions for different paths need to be accumulated as a list. The latest `add` action (from a more recent delta file) observed for a given path wins.
 - All `remove` actions for different paths need to be accumulated as a list. If a `remove` action is received **later** (from a more recent delta file) for the same path as an `add` operation, the corresponding `add` action should be removed from the `add` collection and the file needs to be tracked as part of the `remove` collection.
 - If an `add` action is received **later** (from a more recent delta file) for the same path as a `remove` operation, the corresponding `remove` action should be removed from the `remove` collection and the file needs to be tracked as part of the `add` collection.
    

# Requirements for Writers
This section documents additional requirements that writers must follow in order to preserve some of the higher level guarantees that Delta provides.

## Creation of New Log Entries
 - Writers MUST never overwrite an existing log entry. When ever possible they should use atomic primitives of the underlying filesystem to ensure concurrent writers do not overwrite each others entries.

## Consistency Between Table Metadata and Data Files
 - Any column that exists in a data file present in the table MUST also be present in the metadata of the table.
 - Values for all partition columns present in the schema MUST be present for all files in the table.
 - Columns present in the schema of the table MAY be missing from data files. Readers SHOULD fill these missing columns in with `null`.

## Delta Log Entries
- A single log entry MUST NOT include more than one action that reconciles with each other.
  - Add / Remove actions with the same `path`
  - More than one Metadata action
  - More than one protocol action
  - More than one SetTransaction with the same `appId`

## Checkpoints
 - A checkpoint MUST only be written after the corresponding log entry has been completely written.
 - When writing multi-part checkpoints, the data must be clustered (either through hash or range partitioning) by the 'path' of an added or removed file, or null otherwise. This ensures deterministic content in each part file in case of multiple attempts to write the files.

### Checkpoint Format

Checkpoint files must be written in [Apache Parquet](https://parquet.apache.org/) format. Each row in the checkpoint corresponds to a single action. The checkpoint **must** contain all information regarding the following actions:
 * The [protocol version](#Protocol-Evolution)
 * The [metadata](#Change-Metadata) of the table
 * Files that have been [added and removed](#Add-File-and-Remove-File)
 * [Transaction identifiers](#Transaction-Identifiers)

Commit provenance information does not need to be included in the checkpoint. All of these actions are stored as their individual columns in parquet as struct fields.

Within the checkpoint, the `add` struct may or may not contain the following columns based on the configuration of the table:
 - partitionValues_parsed: In this struct, the column names correspond to the partition columns and the values are stored in their corresponding data type. This is a required field when the table is partitioned and the table property `delta.checkpoint.writeStatsAsStruct` is set to `true`. If the table is not partitioned, this column can be omitted. For example, for partition columns `year`, `month` and `event` with data types `int`, `int` and `string` respectively, the schema for this field will look like:
 
 ```
|-- add: struct
|    |-- partitionValues_parsed: struct
|    |    |-- year: int
|    |    |-- month: int
|    |    |-- event: string
 ```

 - stats: Column level statistics can be stored as a JSON string in the checkpoint. This field needs to be written when statistics are available and the table property: `delta.checkpoint.writeStatsAsJson` is set to `true` (which is the default). When this property is set to `false`, this field should be omitted from the checkpoint.
 - stats_parsed: The stats can be stored in their [original format](#Per-file Statistics). This field needs to be written when statistics are available and the table property: `delta.checkpoint.writeStatsAsStruct` is set to `true`. When this property is set to `false` (which is the default), this field should be omitted from the checkpoint.

Refer to the [appendix](#checkpoint-schema) for an example on the schema of the checkpoint.

## Data Files
 - Data files MUST be uniquely named and MUST NOT be overwritten. The reference implementation uses a GUID in the name to ensure this property.

## Append-only Tables
When the table property `delta.appendOnly` is set to `true`:
  - New log entries MUST NOT change or remove data from the table.
  - New log entries may rearrange data (i.e. `add` and `remove` actions where `dataChange=false`).

## Column Invariants
 - The `metadata` for a column in the table schema MAY contain the key `delta.invariants`.
 - The value of `delta.invariants` SHOULD be parsed as a boolean SQL expression.
 - Writers MUST abort any transaction that adds a row to the table, where an invariant evaluates to `false` or `null`.

## Generated Columns

 - The `metadata` for a column in the table schema MAY contain the key `delta.generationExpression`.
 - The value of `delta.generationExpression` SHOULD be parsed as a SQL expression.
 - Writers MUST enforce that any data writing to the table satisfy the condition `(<value> <=> <generation expression>) IS TRUE`. `<=>` is the NULL-safe equal operator which performs an equality comparison like the `=` operator but returns `TRUE` rather than NULL if both operands are `NULL`

## Writer Version Requirements

The requirements of the writers according to the protocol versions are summarized in the table below. Each row inherits the requirements from the preceding row.

<br> | Reader Version 1
-|-
Writer Version 2 | - Support [`delta.appendOnly`](#append-only-tables)<br>- Support [Column Invariants](#column-invariants)
Writer Version 3 | Enforce:<br>- `delta.checkpoint.writeStatsAsJson`<br>- `delta.checkpoint.writeStatsAsStruct`<br>- `CHECK` constraints
Writer Version 4 | - Support Change Data Feed<br>- Support [Generated Columns](#generated-columns)

# Appendix

## Per-file Statistics
`add` actions can optionally contain statistics about the data in the file being added to the table.
These statistics can be used for eliminating files based on query predicates or as inputs to query optimization.

Global statistics record information about the entire file.
The following global statistic is currently supported:

Name | Description
-|-
numRecords | The number of records in this file.

Per-column statistics record information for each column in the file and they are encoded, mirroring the schema of the actual data.
For example, given the following data schema:
```
|-- a: struct
|    |-- b: struct
|    |    |-- c: long
```

Statistics could be stored with the following schema:
```
|-- stats: struct
|    |-- numRecords: long
|    |-- minValues: struct
|    |    |-- a: struct
|    |    |    |-- b: struct
|    |    |    |    |-- c: long
|    |-- maxValues: struct
|    |    |-- a: struct
|    |    |    |-- b: struct
|    |    |    |    |-- c: long
```

The following per-column statistics are currently supported:

Name | Description
-|-
nullCount | The number of null values for this column
minValues | A value smaller than all values present in the file for this column
maxValues | A value larger than all values present in the file for this column

## Partition Value Serialization

Partition values are stored as strings, using the following formats. An empty string for any type translates to a `null` partition value.

Type | Serialization Format
-|-
string | No translation required
numeric types | The string representation of the number
date | Encoded as `{year}-{month}-{day}`. For example, `1970-01-01`
timestamp | Encoded as `{year}-{month}-{day} {hour}:{minute}:{second}` For example: `1970-01-01 00:00:00`
boolean | Encoded as the string "true" or "false"
binary | Encoded as a string of escaped binary values. For example, `"\u0001\u0002\u0003"`

## Schema Serialization Format

Delta uses a subset of Spark SQL's JSON Schema representation to record the schema of a table in the transaction log.
A reference implementation can be found in [the catalyst package of the Apache Spark repository](https://github.com/apache/spark/tree/master/sql/catalyst/src/main/scala/org/apache/spark/sql/types).

### Primitive Types

Type Name | Description
-|-
string| UTF-8 encoded string of characters
long| 8-byte signed integer. Range: -9223372036854775808 to 9223372036854775807
integer|4-byte signed integer. Range: -2147483648 to 2147483647
short| 2-byte signed integer numbers. Range: -32768 to 32767
byte| 1-byte signed integer number. Range: -128 to 127
float| 4-byte single-precision floating-point numbers
double| 8-byte double-precision floating-point numbers
boolean| `true` or `false`
binary| A sequence of binary data.
date| A calendar date, represented as a year-month-day triple without a timezone.
timestamp| Microsecond precision timestamp without a timezone.

### Struct Type

A struct is used to represent both the top-level schema of the table as well as struct columns that contain nested columns. A struct is encoded as a JSON object with the following fields:

Field Name | Description
-|-
type | Always the string "struct"
fields | An array of fields

### Struct Field

A struct field represents a top-level or nested column.

Field Name | Description
-|-
name| Name of this (possibly nested) column
type| String containing the name of a primitive type, a struct definition, an array definition or a map definition
nullable| Boolean denoting whether this field can be null
metadata| A JSON map containing information about this column. Keys prefixed with `Delta` are reserved for the implementation. See [TODO](#) for more information on column level metadata that clients must handle when writing to a table.

### Array Type

An array stores a variable length collection of items of some type.

Field Name | Description
-|-
type| Always the string "array"
elementType| The type of element stored in this array represented as a string containing the name of a primitive type, a struct definition, an array definition or a map definition
containsNull| Boolean denoting whether this array can contain one or more null values

### Map Type

A map stores an arbitrary length collection of key-value pairs with a single `keyType` and a single `valueType`.

Field Name | Description
-|-
type| Always the string "map".
keyType| The type of element used for the key of this map, represented as a string containing the name of a primitive type, a struct definition, an array definition or a map definition
valueType| The type of element used for the key of this map, represented as a string containing the name of a primitive type, a struct definition, an array definition or a map definition

### Example

Example Table Schema:
```
|-- a: integer (nullable = false)
|-- b: struct (nullable = true)
|    |-- d: integer (nullable = false)
|-- c: array (nullable = true)
|    |-- element: integer (containsNull = false)
|-- e: array (nullable = true)
|    |-- element: struct (containsNull = true)
|    |    |-- d: integer (nullable = false)
|-- f: map (nullable = true)
|    |-- key: string
|    |-- value: string (valueContainsNull = true)
```

JSON Encoded Table Schema:
```
{
  "type" : "struct",
  "fields" : [ {
    "name" : "a",
    "type" : "integer",
    "nullable" : false,
    "metadata" : { }
  }, {
    "name" : "b",
    "type" : {
      "type" : "struct",
      "fields" : [ {
        "name" : "d",
        "type" : "integer",
        "nullable" : false,
        "metadata" : { }
      } ]
    },
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "c",
    "type" : {
      "type" : "array",
      "elementType" : "integer",
      "containsNull" : false
    },
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "e",
    "type" : {
      "type" : "array",
      "elementType" : {
        "type" : "struct",
        "fields" : [ {
          "name" : "d",
          "type" : "integer",
          "nullable" : false,
          "metadata" : { }
        } ]
      },
      "containsNull" : true
    },
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "f",
    "type" : {
      "type" : "map",
      "keyType" : "string",
      "valueType" : "string",
      "valueContainsNull" : true
    },
    "nullable" : true,
    "metadata" : { }
  } ]
}
```

## Checkpoint Schema
For a table with partition columns: "date", "region" of types date and string respectively and data columns: "asset", "quantity" and "is_available" with data types string, double and boolean, the checkpoint schema will look as follows:

```
|-- metaData: struct
|    |-- id: string
|    |-- name: string
|    |-- description: string
|    |-- format: struct
|    |    |-- provider: string
|    |    |-- options: map<string,string>
|    |-- schemaString: string
|    |-- partitionColumns: array<string>
|    |-- createdTime: long
|    |-- configuration: map<string, string>
|-- protocol: struct
|    |-- minReaderVersion: int
|    |-- minWriterVersion: int
|-- txn: struct
|    |-- appId: string
|    |-- version: long
|-- add: struct
|    |-- path: string
|    |-- partitionValues: map<string,string>
|    |-- size: long
|    |-- modificationTime: long
|    |-- dataChange: boolean
|    |-- stats: string
|    |-- tags: map<string,string>
|    |-- partitionValues_parsed: struct
|    |    |-- date: date
|    |    |-- region: string
|    |-- stats_parsed: struct
|    |    |-- numRecords: long
|    |    |-- minValues: struct
|    |    |    |-- asset: string
|    |    |    |-- quantity: double
|    |    |-- maxValues: struct
|    |    |    |-- asset: string
|    |    |    |-- quantity: double
|    |    |-- nullCounts: struct
|    |    |    |-- asset: long
|    |    |    |-- quantity: long
|-- remove: struct
|    |-- path: string
|    |-- deletionTimestamp: long
|    |-- dataChange: boolean
```
