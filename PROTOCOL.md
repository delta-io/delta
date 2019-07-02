* auto-gen TOC:
{:toc}

# Overview
Goals
- Serializable ACID Transactions
- Efficient operation on a variety of storage systems
- Scalability to billions of files / partitions
- Self describing
- Support for incremental processing through streaming
- Evolvable

# Blob Store Requirements
The protocol requires the following from the underlying storage system.
In some cases where the requirements are not met, we can still correctly implement the protocol by running an extra service for that particular cloud.
For example, while S3 does not provide mutual exclusion, we can satisfy this requirement by using a centralized commit service.
 - Atomic File Creation - The tahoe protocol requires that delta files are created atomically in order to support atomicity of higher level operations. When atomic creation is not available this can also be satisfied by atomic rename.
 - Mutual Exclusion - Two clients must not both succeed at creating a particular delta file, as this would violate our requirement of a single linear history.
 - Partial Listing - We may retain an arbitrarily large number of delta log entries both for provenance information and to allow users to read old snapshots of the table. As such, for good performance, we require the blob store to allow us to list files in lexicographic order starting at a given file name.

# Transaction Log Protocol

## File Layout
The log for a delta table is stored in directory on DBFS (e.g., /home/michael/data/_delta_log). This would typically be at the same location as the root of the partitioned data files, though that is not a strict requirement.  In order to modify the state of the table, a writer must append a delta file that contains an unordered list of actions.  Delta files are named using consecutive integers and are zero padded to allow for efficient ordered enumeration.

```
dbfs:/mnt/streaming/tahoe/usage/_delta_log/00000000000000000000.json
dbfs:/mnt/streaming/tahoe/usage/_delta_log/00000000000000000001.json
dbfs:/mnt/streaming/tahoe/usage/_delta_log/00000000000000000002.json
```

Delta files are named using consecutive numbers and the log store ensure that concurrent writers are unable to successfully write the same delta file, giving us an agreed upon linear history for the table.  The state of the table is defined as the result of replaying all of the actions in the delta log in order.

- TODO: MVCC
- TODO: Action ordering within a table version. Invalid to include multiple conflicting actions
- TODO: Add example files w/ optional partition directories
- TODO: Add checkpoints
- TODO: Add last checkpoint
- TODO: Parquet file naming. should use guids

## Table State
At a high level, the current state of the table is defined as follows:
 - The current metadata, including: the schema of the table, a unique identifier, partition columns, and other configuration properties (i.e., if the table is append only)
 - The files present in the table, along with metadata about those files, including: the size of the file, partition values, statistics.
 - Tombstones, which indicate that file was recently deleted and should remain temporarily to allow for reading of stale snapshots.
 - Applications specific transaction versions, which allow applications such as a structured streaming to make changes to the table in an idempotent manner.
 - Protocol Version

The state of the table is changed by appending a new delta file to the log for a given table. Delta files contain actions, such as adding or removing a file. Most importantly, delta files are the unit of atomicity (i.e., each delta file represents a set of actions that will all happen atomically).

## Actions

### Add File

### Remove File

A file operation adds or removes a path from the table. The AddFile operation also includes information about partition values and statistics such as file size that are useful in query planning.

Files are defined to be unique by the path (and thus the same path may not exist in a table twice).  This restriction means we can update a files metadata by adding it again in a subsequent delta file. In this case, when a duplicate path is observed in a later delta, the metadata of the latest action defines the new state of the table.

Relative paths are assumed to be based at the parent of the _delta_log directory.  This allows the log to work with mount points or external file operations (such as cp or mv) that move the entire collection of data.  Absolute paths are also supported, though we need to decide if this mechanism will be exposed to end users.

Removal of a file also includes a timestamp that indicates when the deletion occurred.  Physical deletion of the file can happen lazily after some user specified time threshold.  This delay allows concurrent readers to continue to execute against a stale snapshot of the data.  This tombstone should be maintained in the state of the table until after the threshold has been crossed.

Since actions within a given delta file are not guaranteed to be applied in order, it is not valid for multiple file operations with the same path to exist in a single delta file.

The `dataChange` flag is used to indicate that an operation only rearranges existing data or adds new statistics, and does not result in a net change in the data present in the table.  This flag is useful if we want to use the transaction log as a source for a streaming query.

Stats contains a json encoded set of statistics about the file.  Clients should always assume that statistics can be missing.

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

{
  "remove":{
    "path":"part-00001-9…..snappy.parquet",
    "deletionTimestamp":1515488792485,
    "dataChange":false
  }
}
```

### Transaction Identifiers

The ability to make the application of a delta file idempotent is a useful primitive.  Examples use cases include both streaming appends and directly allowing users to build fault tolerant data pipelines. These use cases are enabled by tracking a version for a user-specified application id.  For any given application id, the table state tracks the most recent version.  Modifications to this version can be atomically committed along with other actions such as appends to make them idempotent.

Note that the semantics of the version number are the responsibility of the higher layer,  and the log replay should not make an assumption other than last update wins and concurrent modifications should fail. For example, the protocol does not assume monotonicity and it would be valid for the version to decrement, "undoing" a given operation.

```
{
  "txn": {
    "appId":"3ba13872-2d47-4e17-86a0-21afd2a22395",
    "version":364475
  }
}
```

### Change Metadata

Field Name | Data Type | Description
-|-|-
id|`GUID`|Unique identifier for this table
name|`String`| User provided identifier for this table
description|`String`| User provided description for this table
format|[Format Struct](#Format-Specification)| Specification of the encoding for the files stored in the table
schemaString|[Schema Struct](#Schema-Specification)| Schema of the table
partitionColumns|`Array[String]`| An array containing the names of columns that the data should be partitioned by.


The metadata of the table contains information that identifies its contents or controls how the data is read (i.e., should we use json or parquet).  The metadata of the table is defined as the metadata action found in the most recent delta that contains one.

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

TODO: Options that are mandatory.

#### Format Specification
Field Name | Data Type | Description
-|-|-
provider|`String`|Name of the encoding for files in this table
options|`Map[String, String]`|A map containing configuration options for the format

In the reference implementation the provider is use to instantiate a Spark SQL [`FileFormat`](https://github.com/apache/spark/blob/master/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/FileFormat.scala). As of Spark 2.4.3 there is built-in `FileFormat` support for `parquet`, `csv`, `orc`, `json`, and `text`.

However, currently, user facing APIs only allow users to create tables where `format = 'parquet'` and `options = {}`. Support for reading other formats remains both for legacy reasons and to enable possible support for other formats in the future.

#### Schema Specification

##### Primitive Types
Type Name | Description
-|-
string| UTF-8 encoded string of characters.
integer|4-byte signed integer. Range: -2147483648 to 2147483647.
short| 2-byte signed integer numbers. Range: -32768 to 32767
byte| 1-byte signed integer number. Range: -128 to 127
float| 4-byte single-precision floating point numbers
double| 8-byte double-precision floating point numbers
boolean| `true` or `false`
binary| A sequence of binary data.
date| A calendar date, represented as a year-month-day triple without a timezone.
timestamp| Microsecond precision timestamp without a timezone.

##### Struct Types

A struct is used to represent both the top level schema of the table as well as columns that are nested within another column of the table. It is encoded as a JSON object with the following fields.

Field Name | Description
-|-
type | Always the string "struct".
fields | An array of fields.

A struct field is encoded as follows:

Field Name | Description
-|-
name| Name of this (possibly nested) column.
type| A string containing the name of a primitive type, a struct definition, an array definition or a map defintion.
nullable| Boolean denoting whether this field can be null.
metadata| A JSON map containing information about this column. Keys prefixed with `delta` are reserved for the implementation. See [TODO](#) for more information on column level metadata that must clients must handle when writing to a table.

##### Array Types

##### Map Types



### Change Protocol Version

Field Name | Data Type | Description
-|-|-
minReaderVersion | Int | The minimum version of the Delta read protocol that a client must implement in order to correctly *read* this table.
minWriterVersion | Int | The minimum version of the Delta write protocol that a client must implement in order to correctly *write* this table.


The protocol versioning action allows for a newer client to exclude older readers and/or writers.  An exclusion should be used whenever non-forward compatible changes are made to the protocol. In the case where a client is running an invalid version, an error should be thrown instructing the user to upgrade to a newer version of DBR.

```
{
  "protocol":{
    "minReaderVersion":1,
    "minWriterVersion":1
  }
}
```

TODO: Discuss ignoring fields

### Commit Provenance Information
Commits can optionally contain additional provenance information about what higher level operation was being performed as well as who executed it.  This information can be used both when looking at the history of the table as well as when throwing a concurrent modification exception.
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

## Checkpoints
As the number of delta files grows, it becomes increasingly expensive to reconstruct the current state of the table from scratch.  In order to mitigate this problem, Delta will regularly produce a checkpoint file, which contains the current state of up until some delta version. Future readers can start with this file and only read deltas that come afterwards.

Note that when producing a checkpoint, it is important to first write out the delta file, ensuring that all writers agree on what the contents of the checkpoint should be.  Ensuring that checkpoints are deterministic means that we can relax the requirements on creation.  In particular, there is no need for mutual exclusion when producing a checkpoint.  If multiple writers try and create the same checkpoint, it does not matter which one wins.

Metadata about the last checkpoint that was written out is stored in a file name _last_checkpoint, and includes its version and its size. This information allows repository to keep a long history of without increasing the cost of loading the current state.  Note that, as this is only an optimization, it does not cause correctness problems if _last_checkpoint is stale due to concurrent writers or eventual consistency.

TODO: Invariants...
