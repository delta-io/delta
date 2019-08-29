<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
# Delta Transaction Log Protocol

- [Overview](#overview)
- [Delta Table Specification](#delta-table-specification)
  - [File Types](#file-types)
    - [Versions](#versions)
    - [Checkpoints](#checkpoints)
    - [Data Files](#data-files)
    - [Last Checkpoint File](#last-checkpoint-file)
  - [Actions](#actions)
    - [Change Metadata](#change-metadata)
      - [Format Specification](#format-specification)
    - [Add File and Remove File](#add-file-and-remove-file)
    - [Transaction Identifiers](#transaction-identifiers)
    - [Protocol Evolution](#protocol-evolution)
    - [Commit Provenance Information](#commit-provenance-information)
- [Appendix](#appendix)
  - [Per-file Statistics](#per-file-statistics)
  - [Schema Serialization Format](#schema-serialization-format)
    - [Primitive Types](#primitive-types)
    - [Struct Type](#struct-type)
    - [Struct Field](#struct-field)
    - [Array Type](#array-type)
    - [Map Type](#map-type)
    - [Example](#example)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Overview
This document is a specification for the Delta Transaction Protocol, which brings [ACID](https://en.wikipedia.org/wiki/ACID) properties to large collections of data, stored as files, in a distributed file system or object store. The protocol was designed with the following goals in mind:

- **Serializable ACID Writes** - multiple writers can concurrently modify a Delta table while maintaining ACID semantics.
- **Snapshot Isolation for Reads** - readers can read a consistent snapshot of a Delta table, even in the face of concurrent writes.
- **Scalability to billions of partitions or file** - queries against a Delta table can be planned on a single machine or in parallel.
- **Self describing** - all metadata for a Delta table is stored alongside the data, static tables can be copied or moved using standard tools.
- **Support for incremental processing** - readers can tail the Delta log to determine what data has been added in a given period of time, allowing for efficient streaming.

Delta supports the above by implementing MVCC using a set of data files (encoded using Apache Parquet) along with a transaction log that tracks which files are part of the table at any given version.

# Delta Table Specification
A Delta table is comprised of a set of _data files_ along with a _transaction log_ that records the state of the table in a sequence of contiguous, monotonically-increasing _table versions_.

The state of a table at a given version is called a _snapshot_. Snapshots are defined by the following properties:
 - **Version of the Delta log protocol** that is required to correctly read or write the table.
 - **Metadata** of the table (e.g., the schema, a unique identifier, partition columns, and other configuration properties)
 - **Set of files** present in the table, along with metadata about those files.
 - **Set of tombstones** for files that were recently deleted.
 - **Set of applications-specific transactions** that have been successfully committed to the table.

## File Types

### Versions
Table versions are stored as JSON files in a directory at the root of the table named `_delta_log`.
Table version files within this directory are named using the table version number, zero-padded to 20 digits (e.g., `00000000000000000000.json` would contain the first version of the table).

A table version file, `n`, contains an unordered set of [_actions_](#Actions) that should be applied to the previous table state, `n-1`, in order to determine the new state of the table.
An action changes one aspect of the table's state, for example, adding or removing a file.

### Checkpoints
Checkpoints are also stored in the `_delta_log` directory, and can be created for any version of the table.
A checkpoint contains all valid actions of the table at that version, allowing readers to short-cut reading the log up-to that point.

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

It is possible that a writer will fail while writing out one or more parts of a multi-part checkpoint. Readers must only use a complete checkpoint, wherein all fragments are present. For performance reasons, readers should search for the most recent earlier checkpoint that is complete.

Checkpoints for a given version must only be created after the assosciated table version file has been successfully written.

<!-- TODO: requirements on partitioning/ordering in checkpoints -->

### Data Files
Data files can be stored in the root directory of the table or in any non-hidden (i.e., does not start with an `_`) subdirectory.
By default, the reference implementation stores data files in directories that are named based on the partition values for data in that file (i.e. `part1=value1/part2=value2/...`).
This directory format is only used to follow existing conventions and is not required by the protocol.
Actual partition values for a file must be read from the transaction log.

### Last Checkpoint File
The Delta transaction log can contain many table versions.
Listing such a large directory can be prohibitively expensive.
Rather than list the entire directory, readers can locate the most recent checkpoint, and thus a point near the end of the log, by looking at the `_delta_log/_last_checkpoint` file.
This file is encoded as JSON and contains the following information:

Field | Description
-|-
version | the version of the table when the last checkpoint was made.
size | The number of actions that are stored in the checkpoint.
parts | The number of fragments if the last checkpoint was written in multiple parts.

## Actions
Actions modify the state of the table and they are stored both in table version files and in checkpoints.
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
A tombstone expires when the creation timestamp of the table version file exceeds the expiration threshold added to the `remove` action timestamp.

Since actions within a given Delta file are not guaranteed to be applied in order, it is not valid for multiple file operations with the same path to exist in a single version.

The `dataChange` flag on either an `add` or a `remove` can be set to `false` to indicate that an action when combined with other actions in the same table version only rearranges existing data or adds new statistics.
For example, streaming queries that are tailing the transaction log can use this flag to skip actions that would not affect the final results.

The schema of the `add` action is as follows:

Field Name | Data Type | Description
-|-|-
path| String | A relative path, from the root of the table, to a file that should be added to the table
partitionValues| Map[String, String] | A map containing the partition values for this file
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
Transaction identifiers allow external systems that are modifying a Delta table to record application-specific metadata regarding the latest `version` for a given `appId` that has committed successfully.
By recording this information atomically with other actions in the table version file, Delta enables these systems to make their transactions _idempotent_.

For example, the [Delta Sink for Apache Spark's Structured Streaming](https://github.com/delta-io/delta/blob/master/src/main/scala/org/apache/spark/sql/delta/sources/DeltaSink.scala) ensures exactly-once semantics when writing a stream into a table using the following process:
 1. Record in a write-ahead-log the data that will be written, along with a monotonically increasing identifier for this batch.
 2. Check the current version of the transaction with `appId = streamId` in the target table. If this value is greater than or equal to the batch being written, then this data has already been added to the table and processing can skip to the next batch.
 3. Write the data optimistically into the table.
 4. Attempt to commit the transaction containing both the addition of the data written out and an updated appId `version.`

The semantics of the appId version number are left up to the external system, while Delta ensures that the last update to a given `appId` is stored in the table snapshot.
The protocol does not, for example, assume appId version monotonicity and it would be valid for the appId version to decrease, possibly representing a "rollback" of an earlier transaction.

The schema of the `txn` action is as follows:
Field Name | Data Type | Description
-|-|-
appId | String | A unique identifier for the application performing the transaction
version | Long | An application-specific numeric identifier for this transaction

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
The `protocol` action is used to set the version of the Delta protocol that is required to read or write a given table.
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
Table versions can optionally contain additional provenance information about what higher-level operation was being performed as well as who executed it.

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

<!--
# Modifying a Delta Table
A Delta table is modfied using the following
## Creating a New Version
New versions of table must be added to the log _atomically_ and with _mutual-exclusion_, using mechanisms supported by the underlying file system.
For example, on HDFS, a new version can be created by:
 1. Optimistically write any new data files.
 1. Create a temporary file with the desired set of actions.
 1. Renaming the temporary file to the next position in the log, setting the flag to fail if the destination already exists.

- TODO: MVCC
The metadata of the table at any given version must be consistent with the data currently in the table.
For example, it is not valid to change the partitioning of the table without also removing any files present that are not correctly partitioned.
- TODO: Action ordering within a table version. Invalid to include multiple conflicting actions
- TODO: Add example files w/ optional partition directories
- TODO: Add checkpoints
- TODO: Add last checkpoint
- TODO: Parquet file naming. should use guids

## Required Table Options
## Column Invariants
-->

# Appendix

## Per-file Statistics
`add` actions can optionally contain statistics about the data in the file being added to the table.
These statistics can be used for eliminating files based on query predicates or as inputs to query optimization.

Global statistics record information about the entire file.
The following global statistic is currently supported:

Name | Description
-|-
numRecords | The number of records in this file.

Per-column statistics record information for each column in the file and they are encoded mirroring the schema of the actual data.
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
maxValues | A value larger than all values present in the file for this colunn


## Schema Serialization Format

Delta uses a subset of Spark SQL's JSON Schema representation to record the schema of a table in the transaction log.
A reference implementation can be found in [the catalyst package of the Apache Spark repository](https://github.com/apache/spark/tree/master/sql/catalyst/src/main/scala/org/apache/spark/sql/types).

### Primitive Types

Type Name | Description
-|-
string| UTF-8 encoded string of characters
integer|4-byte signed integer. Range: -2147483648 to 2147483647
short| 2-byte signed integer numbers. Range: -32768 to 32767
byte| 1-byte signed integer number. Range: -128 to 127
float| 4-byte single-precision floating point numbers
double| 8-byte double-precision floating point numbers
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

A struct field represents a top level or nested column.

Field Name | Description
-|-
name| Name of this (possibly nested) column
type| String containing the name of a primitive type, a struct definition, an array definition or a map definition
nullable| Boolean denoting whether this field can be null
metadata| A JSON map containing information about this column. Keys prefixed with `Delta` are reserved for the implementation. See [TODO](#) for more information on column level metadata that must clients must handle when writing to a table.

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
