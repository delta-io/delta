<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
# Delta Transaction Log Protocol

- [Overview](#overview)
- [Delta Table Specification](#delta-table-specification)
  - [File Types](#file-types)
    - [Data Files](#data-files)
    - [Deletion Vector Files](#deletion-vector-files)
    - [Change Data Files](#change-data-files)
    - [Delta Log Entries](#delta-log-entries)
    - [Checkpoints](#checkpoints)
    - [Last Checkpoint File](#last-checkpoint-file)
      - [JSON checksum](#json-checksum)
        - [How to URL encode keys and string values](#how-to-url-encode-keys-and-string-values)
  - [Actions](#actions)
    - [Change Metadata](#change-metadata)
      - [Format Specification](#format-specification)
    - [Add File and Remove File](#add-file-and-remove-file)
    - [Add CDC File](#add-cdc-file)
      - [Writer Requirements for AddCDCFile](#writer-requirements-for-addcdcfile)
      - [Reader Requirements for AddCDCFile](#reader-requirements-for-addcdcfile)
    - [Transaction Identifiers](#transaction-identifiers)
    - [Protocol Evolution](#protocol-evolution)
    - [Commit Provenance Information](#commit-provenance-information)
    - [Increase Row ID High-Water Mark](#increase-row-id-high-watermark)
    - [Domain Metadata](#domain-metadata)
      - [Reader Requirements for Domain Metadata](#reader-requirements-for-domain-metadata)
      - [Writer Requirements for Domain Metadata](#writer-requirements-for-domain-metadata)
- [Action Reconciliation](#action-reconciliation)
- [Table Features](#table-features)
  - [Table Features for new and Existing Tables](#table-features-for-new-and-existing-tables)
  - [Enabled Features](#enabled-features)
  - [Disabled Features](#disabled-features)
- [Column Mapping](#column-mapping)
  - [Writer Requirements for Column Mapping](#writer-requirements-for-column-mapping)
  - [Reader Requirements for Column Mapping](#reader-requirements-for-column-mapping)
- [Deletion Vectors](#deletion-vectors)
  - [Deletion Vector Descriptor Schema](#deletion-vector-descriptor-schema)
    - [Derived Fields](#derived-fields)
    - [JSON Example 1 — On Disk with Relative Path (with Random Prefix)](#json-example-1--on-disk-with-relative-path-with-random-prefix)
    - [JSON Example 2 — On Disk with Absolute Path](#json-example-2--on-disk-with-absolute-path)
    - [JSON Example 3 — Inline](#json-example-3--inline)
  - [Reader Requirements for Deletion Vectors](#reader-requirements-for-deletion-vectors)
- [Row IDs](#row-ids)
  - [Reader Requirements for Row IDs](#reader-requirements-for-row-ids)
  - [Writer Requirements for Row IDs](#writer-requirements-for-row-ids)
- [Requirements for Writers](#requirements-for-writers)
  - [Creation of New Log Entries](#creation-of-new-log-entries)
  - [Consistency Between Table Metadata and Data Files](#consistency-between-table-metadata-and-data-files)
  - [Delta Log Entries](#delta-log-entries-1)
  - [Checkpoints](#checkpoints-1)
    - [Checkpoint Format](#checkpoint-format)
  - [Data Files](#data-files-1)
  - [Append-only Tables](#append-only-tables)
  - [Column Invariants](#column-invariants)
  - [CHECK Constraints](#check-constraints)
  - [Generated Columns](#generated-columns)
  - [Identity Columns](#identity-columns)
  - [Writer Version Requirements](#writer-version-requirements)
- [Requirements for Readers](#requirements-for-readers)
  - [Reader Version Requirements](#reader-version-requirements)
- [Appendix](#appendix)
  - [Valid Feature Names in Table Features](#valid-feature-names-in-table-features)
  - [Deletion Vector Format](#deletion-vector-format)
    - [Deletion Vector File Storage Format](#deletion-vector-file-storage-format)
  - [Per-file Statistics](#per-file-statistics)
  - [Partition Value Serialization](#partition-value-serialization)
  - [Schema Serialization Format](#schema-serialization-format)
    - [Primitive Types](#primitive-types)
    - [Struct Type](#struct-type)
    - [Struct Field](#struct-field)
    - [Array Type](#array-type)
    - [Map Type](#map-type)
    - [Column Metadata](#column-metadata)
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
 - **Delta log protocol** consists of two **protocol versions**, and if applicable, corresponding **table features**, that are required to correctly read or write the table
   - **Reader features** only exists when Reader Version is 3
   - **Writer features** only exists when Writer Version is 7
 - **Metadata** of the table (e.g., the schema, a unique identifier, partition columns, and other configuration properties)
 - **Set of files** present in the table, along with metadata about those files
 - **Set of tombstones** for files that were recently deleted
 - **Set of applications-specific transactions** that have been successfully committed to the table

## File Types
A Delta table is stored within a directory and is composed of the following different types of files.

Here is an example of a Delta table with three entries in the commit log, stored in the directory `mytable`.
```
/mytable/_delta_log/00000000000000000000.json
/mytable/_delta_log/00000000000000000001.json
/mytable/_delta_log/00000000000000000003.json
/mytable/_delta_log/00000000000000000003.checkpoint.parquet
/mytable/_delta_log/_last_checkpoint
/mytable/_change_data/cdc-00000-924d9ac7-21a9-4121-b067-a0a6517aa8ed.c000.snappy.parquet
/mytable/part-00000-3935a07c-416b-4344-ad97-2a38342ee2fc.c000.snappy.parquet
/mytable/deletion_vector-0c6cbaaf-5e04-4c9d-8959-1088814f58ef.bin
```

### Data Files
Data files can be stored in the root directory of the table or in any non-hidden subdirectory (i.e., one whose name does not start with an `_`).
By default, the reference implementation stores data files in directories that are named based on the partition values for data in that file (i.e. `part1=value1/part2=value2/...`).
This directory format is only used to follow existing conventions and is not required by the protocol.
Actual partition values for a file must be read from the transaction log.

### Deletion Vector Files
Deletion Vector (DV) files are stored root directory of the table alongside the data files. A DV file contains one or more serialised DV, each describing the set of *invalidated* (or "soft deleted") rows for a particular data file it is associated with.
For data with partition values, DV files are *not* kept in the same directory hierarchy as data files, as each one can contain DVs for files from multiple partitions.
DV files store DVs in a [binary format](#deletion-vector-format).

### Change Data Files
Change data files are stored in a directory at the root of the table named `_change_data`, and represent the changes for the table version they are in. For data with partition values, it is recommended that the change data files are stored within the `_change_data` directory in their respective partitions (i.e. `_change_data/part1=value1/...`). Writers can _optionally_ produce these change data files as a consequence of operations that change underlying data, like `UPDATE`, `DELETE`, and `MERGE` operations to a Delta Lake table. If an operation only adds new data or removes existing data without updating any existing rows, a writer can write only data files and commit them in `add` or `remove` actions without duplicating the data into change data files. When available, change data readers should use the change data files instead of computing changes from the underlying data files.

In addition to the data columns, change data files contain additional columns that identify the type of change event:

Field Name | Data Type | Description
-|-|-
_change_type|`String`| `insert`, `update_preimage` , `update_postimage`, `delete` __(1)__

__(1)__ `preimage` is the value before the update, `postimage` is the value after the update.

### Delta Log Entries
Delta files are stored as JSON in a directory at the root of the table named `_delta_log`, and together with checkpoints make up the log of all changes that have occurred to a table.

Delta files are the unit of atomicity for a table, and are named using the next available version number, zero-padded to 20 digits.

For example:

```
./_delta_log/00000000000000000000.json
```
Delta files use new-line delimited JSON format, where every action is stored as a single line JSON document.
A delta file, `n.json`, contains an atomic set of [_actions_](#Actions) that should be applied to the previous table state, `n-1.json`, in order to the construct `n`th snapshot of the table.
An action changes one aspect of the table's state, for example, adding or removing a file.

### Checkpoints
Checkpoints are also stored in the `_delta_log` directory, and can be created for any version of the table.

A checkpoint contains the complete replay of all actions up until this version, with invalid actions removed.
Invalid actions are those that have been canceled out by a subsequent ones (for example removing a file that has been added), using the [rules for reconciliation](#Action-Reconciliation)
Checkpoints allow readers to short-cut the cost of reading the log up-to a given point in order to reconstruct a snapshot, and allow older JSON Delta log entries to be deleted after a period of time.


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
version | The version of the table when the last checkpoint was made.
size | The number of actions that are stored in the checkpoint.
parts | The number of fragments if the last checkpoint was written in multiple parts. This field is optional.
sizeInBytes | The number of bytes of the checkpoint. This field is optional.
numOfAddFiles | The number of AddFile actions in the checkpoint. This field is optional.
checkpointSchema | The schema of the checkpoint file. This field is optional.
checksum | The checksum of the last checkpoint JSON. This field is optional.

The checksum field is an optional field which contains the MD5 checksum for fields of the last checkpoint json file.
Last checkpoint file readers are encouraged to validate the checksum, if present, and writers are encouraged to write the checksum
while overwriting the file. Refer to [this section](#json-checksum) for rules around calculating the checksum field
for the last checkpoint JSON.

#### JSON checksum
To generate the checksum for the last checkpoint JSON, firstly, the checksum JSON is canonicalized and converted to a string. Then
the 32 character MD5 digest is calculated on the resultant string to get the checksum. Rules for [JSON](https://datatracker.ietf.org/doc/html/rfc8259) canonicalization are:

1. Literal values (`true`, `false`, and `null`) are their own canonical form
2. Numeric values (e.g. `42` or `3.14`) are their own canonical form
3. String values (e.g. `"hello world"`) are canonicalized by preserving the surrounding quotes and [URL-encoding](#how-to-url-encode-keys-and-string-values)
their content, e.g. `"hello%20world"`
4. Object values (e.g. `{"a": 10, "b": {"y": null, "x": "https://delta.io"} }` are canonicalized by:
   * Canonicalize each scalar (leaf) value following the rule for its type (literal, numeric, string)
   * Canonicalize each (string) name along the path to that value
   * Connect path segments by `+`, e.g. `"b"+"y"`
   * Connect path and value pairs by `=`, e.g. `"b"+"y"=null`
   * Sort canonicalized path/value pairs using a byte-order sort on paths. The byte-order sort can be done by converting paths to byte array using UTF-8 charset\
    and then comparing them, e.g. `"a" < "b"+"x" < "b"+"y"`
   * Separate ordered pairs by `,`, e.g. `"a"=10,"b"+"x"="https%3A%2F%2Fdelta.io","b"+"y"=null`

5. Array values (e.g. `[null, "hi ho", 2.71]`) are canonicalized as if they were objects, except the "name" has numeric type instead of string type, and gives the (0-based) 
position of the corresponding array element, e.g. `0=null,1="hi%20ho",2=2.71`

6. Top level `checksum` key is ignored in the canonicalization process. e.g. 
`{"k1": "v1", "checksum": "<anything>", "k3": 23}` is canonicalized to `"k1"="v1","k3"=23`

7. Duplicate keys are not allowed in the last checkpoint JSON and such JSON is considered invalid.

Given the following test sample JSON, a correct implementation of JSON canonicalization should produce the corresponding canonicalized form and checksum value:
e.g.
Json: `{"k0":"'v 0'", "checksum": "adsaskfljadfkjadfkj", "k1":{"k2": 2, "k3": ["v3", [1, 2], {"k4": "v4", "k5": ["v5", "v6", "v7"]}]}}`\
Canonicalized form: `"k0"="%27v%200%27","k1"+"k2"=2,"k1"+"k3"+0="v3","k1"+"k3"+1+0=1,"k1"+"k3"+1+1=2,"k1"+"k3"+2+"k4"="v4","k1"+"k3"+2+"k5"+0="v5","k1"+"k3"+2+"k5"+1="v6","k1"+"k3"+2+"k5"+2="v7"`\
Checksum is `6a92d155a59bf2eecbd4b4ec7fd1f875`

##### How to URL encode keys and string values
The [URL Encoding](https://datatracker.ietf.org/doc/html/rfc3986) spec is a bit flexible to give a reliable encoding. e.g. the spec allows both
uppercase and lowercase as part of percent-encoding. Thus, we require a stricter set of rules for encoding:

1. The string to be encoded must be represented as octets according to the UTF-8 character encoding
2. All octets except a-z / A-Z / 0-9 / "-" / "." / "_" / "~" are reserved
3. Always [percent-encode](https://datatracker.ietf.org/doc/html/rfc3986#section-2) reserved octets
4. Never percent-encode non-reserved octets
5. A percent-encoded octet consists of three characters: `%` followed by its 2-digit hexadecimal value in uppercase letters, e.g. `>` encodes to `%3E`

## Actions
Actions modify the state of the table and they are stored both in delta files and in checkpoints.
This section lists the space of available actions as well as their schema.

### Change Metadata
The `metaData` action changes the current metadata of the table.
The first version of a table must contain a `metaData` action.
Subsequent` metaData` actions completely overwrite the current metadata of the table.

There can be at most one metadata action in a given version of the table.

Every metadata action **must** include required fields at a minimum.

The schema of the `metaData` action is as follows:

Field Name | Data Type | Description | optional/required
-|-|-|-
id|`GUID`|Unique identifier for this table | required
name|`String`| User-provided identifier for this table | optional
description|`String`| User-provided description for this table | optional
format|[Format Struct](#Format-Specification)| Specification of the encoding for the files stored in the table | required
schemaString|[Schema Struct](#Schema-Serialization-Format)| Schema of the table | required
partitionColumns|`Array[String]`| An array containing the names of columns by which the data should be partitioned | required
createdTime|`Option[Long]`| The time when this metadata action is created, in milliseconds since the Unix epoch | optional
configuration|`Map[String, String]`| A map containing configuration options for the metadata action | required

#### Format Specification
Field Name | Data Type | Description
-|-|-
provider|`String`|Name of the encoding for files in this table
options|`Map[String, String]`|A map containing configuration options for the format

In the reference implementation, the provider field is used to instantiate a Spark SQL [`FileFormat`](https://github.com/apache/spark/blob/master/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/FileFormat.scala). As of Spark 2.4.3 there is built-in `FileFormat` support for `parquet`, `csv`, `orc`, `json`, and `text`.

As of Delta Lake 0.3.0, user-facing APIs only allow the creation of tables where `format = 'parquet'` and `options = {}`. Support for reading other formats is present both for legacy reasons and to enable possible support for other formats in the future (See [#87](https://github.com/delta-io/delta/issues/87)).

The following is an example `metaData` action:
```json
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
The `add` and `remove` actions are used to modify the data in a table by adding or removing individual _logical files_ respectively.

Every _logical file_ of the table is represented by a path to a data file, combined with an optional Deletion Vector (DV) that indicates which rows of the data file are no longer in the table. Deletion Vectors are an optional feature, see their [reader requirements](#deletion-vectors) for details.

When an `add` action is encountered for a logical file that is already present in the table, statistics and other information from the latest version should replace that from any previous version.
The primary key for the entry of a logical file in the set of files is a tuple of the data file's `path` and a unique id describing the DV. If no DV is part of this logical file, then its primary key is `(path, NULL)` instead.

The `remove` action includes a timestamp that indicates when the removal occurred.
Physical deletion of physical files can happen lazily after some user-specified expiration time threshold.
This delay allows concurrent readers to continue to execute against a stale snapshot of the data.
A `remove` action should remain in the state of the table as a _tombstone_ until it has expired.
A tombstone expires when *current time* (according to the node performing the cleanup) exceeds the expiration threshold added to the `remove` action timestamp.

In the following statements, `dvId` can refer to either the unique id of a specific Deletion Vector (`deletionVector.uniqueId`) or to `NULL`, indicating that no rows are invalidated. Since actions within a given Delta commit are not guaranteed to be applied in order, a **valid** version is restricted to contain at most one file action *of the same type* (i.e. `add`/`remove`) for any one combination of `path` and `dvId`. Moreover, for simplicity it is required that there is at most one file action of the same type for any `path` (regardless of `dvId`).
That means specifically that for any commit…

 - it is **legal** for the same `path` to occur in an `add` action and a `remove` action, but with two different `dvId`s.
 - it is **legal** for the same `path` to be added and/or removed and also occur in a `cdc` action.
 - it is **illegal** for the same `path` to be occur twice with different `dvId`s within each set of `add` or `remove` actions.

The `dataChange` flag on either an `add` or a `remove` can be set to `false` to indicate that an action when combined with other actions in the same atomic version only rearranges existing data or adds new statistics.
For example, streaming queries that are tailing the transaction log can use this flag to skip actions that would not affect the final results.

The schema of the `add` action is as follows:

Field Name | Data Type | Description | optional/required
-|-|-|-
path| String | A relative path to a data file from the root of the table or an absolute path to a file that should be added to the table. The path is a URI as specified by [RFC 2396 URI Generic Syntax](https://www.ietf.org/rfc/rfc2396.txt), which needs to be decoded to get the data file path. | required
partitionValues| Map[String, String] | A map from partition column to value for this logical file. See also [Partition Value Serialization](#Partition-Value-Serialization) | required
size| Long | The size of this data file in bytes | required
modificationTime | Long | The time this logical file was created, as milliseconds since the epoch | required
dataChange | Boolean | When `false` the logical file must already be present in the table or the records in the added file must be contained in one or more `remove` actions in the same version | required
stats | [Statistics Struct](#Per-file-Statistics) | Contains statistics (e.g., count, min/max values for columns) about the data in this logical file | optional
tags | Map[String, String] | Map containing metadata about this logical file | optional
deletionVector | [DeletionVectorDescriptor Struct](#Deletion-Vectors) | Either null (or absent in JSON) when no DV is associated with this data file, or a struct (described below) that contains necessary information about the DV that is part of this logical file. | optional
baseRowId | Long  | Default generated Row ID of the first row in the file. The default generated Row IDs of the other rows in the file can be reconstructed by adding the physical index of the row within the file to the base Row ID. See also [Row IDs](#row-ids) | optional

The following is an example `add` action:
```json
{
  "add": {
    "path": "date=2017-12-10/part-000...c000.gz.parquet",
    "partitionValues": {"date": "2017-12-10"},
    "size": 841454,
    "modificationTime": 1512909768000,
    "dataChange": true,
    "baseRowId": 4071,
    "stats": "{\"numRecords\":1,\"minValues\":{\"val..."
  }
}
```

The schema of the `remove` action is as follows:

Field Name | Data Type | Description | optional/required
-|-|-|-
path| String | A relative path to a file from the root of the table or an absolute path to a file that should be removed from the table. The path is a URI as specified by [RFC 2396 URI Generic Syntax](https://www.ietf.org/rfc/rfc2396.txt), which needs to be decoded to get the data file path. | required
deletionTimestamp | Option[Long] | The time the deletion occurred, represented as milliseconds since the epoch | optional
dataChange | Boolean | When `false` the records in the removed file must be contained in one or more `add` file actions in the same version | required
extendedFileMetadata | Boolean | When `true` the fields `partitionValues`, `size`, and `tags` are present | optional
partitionValues| Map[String, String] | A map from partition column to value for this file. See also [Partition Value Serialization](#Partition-Value-Serialization) | optional
size| Long | The size of this data file in bytes | optional
tags | Map[String, String] | Map containing metadata about this file | optional
deletionVector | [DeletionVectorDescriptor Struct](#Deletion-Vectors) | Either null (or absent in JSON) when no DV is associated with this data file, or a struct (described below) that contains necessary information about the DV that is part of this logical file. | optional
baseRowId | Long | Default generated Row ID of the first row in the file. The default generated Row IDs of the other rows in the file can be reconstructed by adding the physical index of the row within the file to the base Row ID. See also [Row IDs](#row-ids) | optional

The following is an example `remove` action.
```json
{
  "remove": {
    "path": "part-00001-9…..snappy.parquet",
    "deletionTimestamp": 1515488792485,
    "baseRowId": 4071,
    "dataChange": true
  }
}
```

### Add CDC File
The `cdc` action is used to add a [file](#change-data-files) containing only the data that was changed as part of the transaction. When change data readers encounter a `cdc` action in a particular Delta table version, they must read the changes made in that version exclusively using the `cdc` files. If a version has no `cdc` action, then the data in `add` and `remove` actions are read as inserted and deleted rows, respectively.

The schema of the `cdc` action is as follows:

Field Name | Data Type | Description
-|-|-
path| String | A relative path to a change data file from the root of the table or an absolute path to a change data file that should be added to the table. The path is a URI as specified by [RFC 2396 URI Generic Syntax](https://www.ietf.org/rfc/rfc2396.txt), which needs to be decoded to get the file path.
partitionValues| Map[String, String] | A map from partition column to value for this file. See also [Partition Value Serialization](#Partition-Value-Serialization)
size| Long | The size of this file in bytes
dataChange | Boolean | Should always be set to `false` for `cdc` actions because they _do not_ change the underlying data of the table
tags | Map[String, String] | Map containing metadata about this file

The following is an example of `cdc` action.

```json
{
  "cdc": {
    "path": "_change_data/cdc-00001-c…..snappy.parquet",
    "partitionValues": {},
    "size": 1213,
    "dataChange": false
  }
}
```

#### Writer Requirements for AddCDCFile

For [Writer Versions 4 up to 6](#Writer-Version-Requirements), all writers must respect the `delta.enableChangeDataFeed` configuration flag in the metadata of the table. When `delta.enableChangeDataFeed` is `true`, writers must produce the relevant `AddCDCFile`'s for any operation that changes data, as specified in [Change Data Files](#change-data-files).

For Writer Version 7, all writers must respect the `delta.enableChangeDataFeed` configuration flag in the metadata of the table only if the feature `changeDataFeed` exists in the table `protocol`'s `writerFeatures`.

#### Reader Requirements for AddCDCFile

When available, change data readers should use the `cdc` actions in a given table version instead of computing changes from the underlying data files referenced by the `add` and `remove` actions.
Specifically, to read the row-level changes made in a version, the following strategy should be used:
1. If there are `cdc` actions in this version, then read only those to get the row-level changes, and skip the remaining `add` and `remove` actions in this version.
2. Otherwise, if there are no `cdc` actions in this version, read and treat all the rows in the `add` and `remove` actions as inserted and deleted rows, respectively.
3. The following extra columns should also be generated:

Field Name | Data Type | Description
-|-|-
_commit_version|`Long`| The table version containing the change. This can be got from the name of the Delta log file that contains actions.
_commit_timestamp|`Timestamp`| The timestamp associated when the commit was created. This can be got from the file modification time of the Delta log file that contains actions.

### Transaction Identifiers
Incremental processing systems (e.g., streaming systems) that track progress using their own application-specific versions need to record what progress has been made, in order to avoid duplicating data in the face of failures and retries during a write.
Transaction identifiers allow this information to be recorded atomically in the transaction log of a delta table along with the other actions that modify the contents of the table.

Transaction identifiers are stored in the form of `appId` `version` pairs, where `appId` is a unique identifier for the process that is modifying the table and `version` is an indication of how much progress has been made by that application.
The atomic recording of this information along with modifications to the table enables these external system to make their writes into a Delta table _idempotent_.

For example, the [Delta Sink for Apache Spark's Structured Streaming](https://github.com/delta-io/delta/blob/master/core/src/main/scala/org/apache/spark/sql/delta/sources/DeltaSink.scala) ensures exactly-once semantics when writing a stream into a table using the following process:
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
```json
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

Since breaking changes must be accompanied by an increase in the protocol version recorded in a table or by the addition of a table feature, clients can assume that unrecognized actions, fields, and/or metadata domains are never required in order to correctly interpret the transaction log. Clients must ignore such unrecognized fields, and should not produce an error when reading a table that contains unrecognized fields.

Reader Version 3 and Writer Version 7 add two lists of table features to the protocol action. The capability for readers and writers to operate on such a table is not only dependent on their supported protocol versions, but also on whether they support all features listed in `readerFeatures` and `writerFeatures`. See [Table Features](#table-features) section for more information.

The schema of the `protocol` action is as follows:

Field Name | Data Type | Description
-|-|-
minReaderVersion | Int | The minimum version of the Delta read protocol that a client must implement in order to correctly *read* this table
minWriterVersion | Int | The minimum version of the Delta write protocol that a client must implement in order to correctly *write* this table
readerFeatures | Array[String] | A collection of features that a client must implement in order to correctly read this table (exist only when `minReaderVersion` is set to `3`)
writerFeatures | Array[String] | A collection of features that a client must implement in order to correctly write this table (exist only when `minWriterVersion` is set to `7`)

Some example Delta protocols:
```json
{
  "protocol":{
    "minReaderVersion":1,
    "minWriterVersion":2
  }
}
```

A table that is using table features only for writers:
```json
{
  "protocol":{
    "readerVersion":2,
    "writerVersion":7,
    "writerFeatures":["columnMapping","identityColumns"]
  }
}
```
Reader version 2 in the above example does not support listing reader features but supports Column Mapping. This example is equivalent to the next one, where Column Mapping is represented as a reader table feature.

A table that is using table features for both readers and writers:
```json
{
  "protocol": {
    "readerVersion":3,
    "writerVersion":7,
    "readerFeatures":["columnMapping"],
    "writerFeatures":["columnMapping","identityColumns"]
  }
}
```

### Commit Provenance Information
A delta file can optionally contain additional provenance information about what higher-level operation was being performed as well as who executed it.

Implementations are free to store any valid JSON-formatted data via the `commitInfo` action.

An example of storing provenance information related to an `INSERT` operation:
```json
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

### Increase Row ID High-water Mark
The Row ID high-water mark tracks the largest ID that has been assigned to a row in the table.

The schema of `rowIdHighWaterMark` action is as follows:

Field Name | Data Type | Description | optional/required
-|-|-|-
highWaterMark | Long | Highest Row ID that has been assigned to a row in the table. | required
preservedRowIds | Boolean | When true, the commit that wrote this high-water mark preserved existing Row IDs of rewritten rows. | required

The following is an example `rowIdHighWaterMark` action:
```json
{
  "rowIdHighWaterMark": {
    "highWaterMark": 1432,
    "preservedRowIds": true
  }
}
```

### Domain Metadata
The domain metadata action contains a configuration (string-string map) for a named metadata domain. Two overlapping transactions conflict if they both contain a domain metadata action for the same metadata domain.

There are two types of metadata domains:
1. **User-controlled metadata domains** have names that start with anything other than the `delta.` prefix. Any Delta client implementation or user application can modify these metadata domains, and can allow users to modify them arbitrarily. Delta clients and user applications are encouraged to use a naming convention designed to avoid conflicts with other clients' or users' metadata domains (e.g. `com.databricks.*` or `org.apache.*`).
2. **System-controlled metadata domains** have names that start with the `delta.` prefix. This prefix is reserved for metadata domains defined by the Delta spec, and Delta client implementations must not allow users to modify the metadata for system-controlled domains. A Delta client implementation should only update metadata for system-controlled domains that it knows about and understands. System-controlled metadata domains are used by various table features and each table feature may impose additional semantics on the metadata domains it uses.

The schema of the `domainMetadata` action is as follows:

Field Name | Data Type | Description
-|-|-
domain | String | Identifier for this domain (system- or user-provided)
configuration | Map[String, String] | A map containing configuration for the metadata domain
removed | Boolean | When `true`, the action serves as a tombstone to logically delete a metadata domain. Writers should preserve an accurate pre-image of the configuration.

Enablement:
- The table must be on Writer Version 7.
- A feature name `domainMetadata` must exist in the table's `writerFeatures`.

#### Reader Requirements for Domain Metadata
- Readers must preserve all domains even if they don't understand them, i.e. the snapshot read must include them.
- Any system-controlled domain that requires special attention from a reader is a [breaking change](#protocol-evolution), and must be part of a reader-writer table feature that specifies the desired behavior.

#### Writer Requirements for Domain Metadata
- Writers must not allow users to modify or delete system-controlled domains.
- Writers must only modify or delete system-controlled domains they understand.
- Any system-controlled domain that needs special attention from a writer is a [breaking change](#protocol-evolution), and must be part of a writer table feature that specifies the desired behavior.

The following is an example `domainMetadata` action:
```json
{
  "domainMetadata": {
    "domain": "delta.deltaTableFeatureX",
    "configuration": {"key1": "..."},
    "removed": false
  }
}
```

# Action Reconciliation
A given snapshot of the table can be computed by replaying the events committed to the table in ascending order by commit version. A given snapshot of a Delta table consists of:

 - A single `protocol` action
 - A single `metaData` action
 - At most one `rowIdHighWaterMark` action
 - A collection of `txn` actions with unique `appId`s
 - A collection of `domainMetadata` actions with unique `domain`s.
 - A collection of `add` actions with unique `(path, deletionVector.uniqueId)` keys.
 - A collection of `remove` actions with unique `(path, deletionVector.uniqueId)` keys. The intersection of the primary keys in the `add` collection and `remove` collection must be empty. That means a logical file cannot exist in both the `remove` and `add` collections at the same time; however, the same *data file* can exist with *different* DVs in the `remove` collection, as logically they represent different content. The `remove` actions act as _tombstones_, and only exist for the benefit of the VACUUM command. Snapshot reads only return `add` actions on the read path.
 
To achieve the requirements above, related actions from different delta files need to be reconciled with each other:
 
 - The latest `protocol` action seen wins
 - The latest `metaData` action seen wins
 - The latest `rowIdHighWaterMark` action seen wins
 - For `txn` actions, the latest `version` seen for a given `appId` wins
 - For `domainMetadata`, the latest `domainMetadata` seen for a given `domain` wins. The actions with `removed=true` act as tombstones to suppress earlier versions. Snapshot reads do _not_ return removed `domainMetadata` actions.
 - Logical files in a table are identified by their `(path, deletionVector.uniqueId)` primary key. File actions (`add` or `remove`) reference logical files, and a log can contain any number of references to a single file.
 - To replay the log, scan all file actions and keep only the newest reference for each logical file.
 - `add` actions in the result identify logical files currently present in the table (for queries). `remove` actions in the result identify tombstones of logical files no longer present in the table (for VACUUM).

# Table Features
Table features must only exist on tables that have a supported protocol version. When the table's Reader Version is 3, `readerFeatures` must exist in the `protocol` action, and when the Writer Version is 7, `writerFeatures` must exist in the `protocol` action. `readerFeatures` and `writerFeatures` define the features that readers and writers must implement in order to read and write this table.

Readers and writers must not ignore table features when they are present:
 - to read a table, readers must implement and respect all features listed in `readerFeatures`;
 - to write a table, writers must implement and respect all features listed in `writerFeatures`. Because writers have to read the table (or only the Delta log) before write, they must implement and respect all reader features as well.

## Table Features for New and Existing Tables
It is possible to create a new table or upgrade an existing table to the protocol versions that enables the use of table features. The enablement can be only for readers or both readers and writers.

For new tables, when a new table is created with a Reader Version up to 2 and Writer Version 7, its `protocol` action must only contain `writerFeatures`. When a new table is created with Reader Version 3 and Writer Version 7, its `protocol` action must contain both `readerFeatures` and `writerFeatures`. Creating a table with a Reader Version 3 and Writer Version less than 7 is not allowed.

When upgrading an existing table to Reader Version 3 and/or Writer Version 7, the client should, on a best effort basis, determine which features supported by the original protocol version are used in any historical version of the table, and add only used features to reader and/or writer feature sets. The client must assume a feature has been used, unless it can prove that the feature is *definitely* not used in any historical version of the table that is reachable by time travel. 

For example, given a table on Reader Version 1 and Writer Version 4, along with four versions:
 1. Table property change: set `delta.enableChangeDataFeed` to `true`.
 2. Data change: three rows updated.
 3. Table property change: unset `delta.enableChangeDataFeed`.
 4. Table protocol change: upgrade protocol to Reader Version 3 and Writer Version 7.

To produce Version 4, a writer could look at only Version 3 and discover that Change Data Feed has not been used. But in fact, this feature has been used and the table does contain some Change Data Files for Version 2. This means that, to determine all features that have ever been used by the table, a writer must either scan the whole history (which is very time-consuming) or assume the worst case: all features supported by protocol `(1, 4)` has been used.

## Enabled Features
A feature is enabled when its name is in the `protocol` action’s `readerFeatures` and/or `writerFeatures`. Subsequent read and/or write operations on this table must respect the feature. Clients must not remove the feature from the `protocol` action.

A feature being enabled does not imply that it is active. For example, a table may have the [Append-only Tables](#append-only-tables) feature (feature name `appendOnly`) enabled in `writerFeatures`, but does not satisfy a table property `delta.appendOnly` equals to `true`. In such a case the table is not append-only, and writers are allowed to change, remove, and rearrange data. However, writers must implement the feature to know that the table property `delta.appendOnly` should be checked.

## Disabled Features
A feature is `disabled` if it is in neither `readerFeatures` nor `writerFeatures`. Writers are allowed to `enable` a feature for the table by adding its name to the `readerFeatures` or `writerFeatures`. Reader features should be added to both `readerFeatures` and `writerFeatures` simultaneously, while writer features should be added only to `writerFeatures`. It is not allowed to add features only to `readerFeatures` but not to `writerFeatures`.

# Column Mapping
Delta can use column mapping to avoid any column naming restrictions, and to support the renaming and dropping of columns without having to rewrite all the data. There are two modes of column mapping, by `name` and by `id`. In both modes, every column - nested or leaf - is assigned a unique _physical_ name, and a unique 32-bit integer as an id. The physical name is stored as part of the column metadata with the key `delta.columnMapping.physicalName`. The column id is stored within the metadata with the key `delta.columnMapping.id`.

The column mapping is governed by the table property `delta.columnMapping.mode` being one of `none`, `id`, and `name`. The table property should only be honored if the table's protocol has reader and writer versions and/or table features that support the `columnMapping` table feature. For readers this is Reader Version 2, or Reader Version 3 with the `columnMapping` table feature enabled. For writers this is Writer Version 5 or 6, or Writer Version 7 with  the `columnMapping` table feature enabled.

The following is an example for the column definition of a table that leverages column mapping. See the [appendix](#schema-serialization-format) for a more complete schema definition.
```json
{
    "name" : "e",
    "type" : {
      "type" : "array",
      "elementType" : {
        "type" : "struct",
        "fields" : [ {
          "name" : "d",
          "type" : "integer",
          "nullable" : false,
          "metadata" : { 
            "delta.columnMapping.id": 5,
            "delta.columnMapping.physicalName": "col-a7f4159c-53be-4cb0-b81a-f7e5240cfc49"
          }
        } ]
      },
      "containsNull" : true
    },
    "nullable" : true,
    "metadata" : { 
      "delta.columnMapping.id": 4,
      "delta.columnMapping.physicalName": "col-5f422f40-de70-45b2-88ab-1d5c90e94db1"
    }
  }
```

## Writer Requirements for Column Mapping
In order to support column mapping, writers must:
 - Write `protocol` and `metaData` actions when Column Mapping is turned on for the first time:
   - If the table is on Writer Version 5 or 6: write a `metaData` action to add the `delta.columnMapping.mode` table property;
   - If the table is on Writer Version 7:
     - write a `protocol` action to add the feature `columnMapping` to both `readerFeatures` and `writerFeatures`, and
     - write a `metaData` action to add the `delta.columnMapping.mode` table property.
 - Write data files by using the _physical name_ that is chosen for each column. The physical name of the column is static and can be different than the _display name_ of the column, which is changeable.
 - Write the 32 bit integer column identifier as part of the `field_id` field of the `SchemaElement` struct in the [Parquet Thrift specification](https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift).
 - Track partition values and column level statistics with the physical name of the column in the transaction log.
 - Assign a globally unique identifier as the physical name for each new column that is added to the schema. This is especially important for supporting cheap column deletions in `name` mode. In addition, column identifiers need to be assigned to each column. The maximum id that is assigned to a column is tracked as the table property `delta.columnMapping.maxColumnId`. This is an internal table property that cannot be configured by users. This value must increase monotonically as new columns are introduced and committed to the table alongside the introduction of the new columns to the schema.

## Reader Requirements for Column Mapping
If the table is on Reader Version 2, or if the table is on Reader Version 3 and the feature `columnMapping` is present in `readerFeatures`, readers and writers must read the table property `delta.columnMapping.mode` and do one of the following.

In `none` mode, or if the table property is not present, readers must read the parquet files by using the display names (the `name` field of the column definition) of the columns in the schema.

In `id ` mode, readers must resolve columns by using the `field_id` in the parquet metadata for each file, as given by the column metadata property `delta.columnMapping.id` in the Delta schema. Partition values and column level statistics must be resolved by their *physical names* for each `add` entry in the transaction log. If a data file does not contain field ids, readers must refuse to read that file or return nulls for each column. For ids that cannot be found in a file, readers must return `null` values for those columns.

In `name` mode, readers must resolve columns in the data files by their physical names as given by the column metadata property `delta.columnMapping.physicalName` in the Delta schema. Partition values and column level statistics will also be resolved by their physical names. For columns that are not found in the files, `null`s need to be returned. Column ids are not used in this mode for resolution purposes.

# Deletion Vectors
Enablement:
 - To enable Deletion Vectors on a table, the table must have Reader Version 3 and Writer Version 7. A feature name `deletionVectors` must exist in the table's `readerFeatures` and `writerFeatures`.

When enabled:
 - A table's `add` and `remove` actions can optionally include a Deletion Vector (DV) that provides information about logically deleted rows, that are however still physically present in the underlying data file and must thus be skipped during processing. Readers must read the table considering the existence of DVs.

DVs can be stored and accessed in different ways, indicated by the `storageType` field. The Delta protocol currently supports inline or on-disk storage, where the latter can be accessed either by a relative path derived from a UUID or an absolute path.

# Timestamp without timezone (TimestampNTZ)
This feature introduces a new data type to support timestamps without timezone information. For example: `1970-01-01 00:00:00`, or `1970-01-01 00:00:00.123456`.
The serialization method is described in Sections [Partition Value Serialization](#partition-value-serialization) and [Schema Serialization Format](#schema-serialization-format).

Enablement:
 - To have a column of TimestampNTZ type in a table, the table must have Reader Version 3 and Writer Version 7. A feature name `timestampNTZ` must exist in the table's `readerFeatures` and `writerFeatures`.


## Deletion Vector Descriptor Schema

The schema of the `DeletionVectorDescriptor` struct is as follows:

Field Name | Data Type | Description
-|-|-
storageType | String | A single character to indicate how to access the DV. Legal options are: `['u', 'i', 'p']`.
pathOrInlineDv | String | Three format options are currently proposed:<ul><li>If `storageType = 'u'` then  `<random prefix - optional><base85 encoded uuid>`: The deletion vector is stored in a file with a path relative to the data directory of this Delta table, and the  file name can be reconstructed from the UUID. See Derived Fields for how to reconstruct the file name. The random prefix is recovered as the extra characters before the (20 characters fixed length) uuid.</li><li>If `storageType = 'i'` then `<base85 encoded bytes>`: The deletion vector is stored inline in the log. The format used is the `RoaringBitmapArray` format also used when the DV is stored on disk and described in [Deletion Vector Format](#Deletion-Vector-Format).</li><li>If `storageType = 'p'` then `<absolute path>`: The DV is stored in a file with an absolute path given by this path, which has the same format as the `path` field in the `add`/`remove` actions.</li></ul>
offset | Option[Int] | Start of the data for this DV in number of bytes from the beginning of the file it is stored in. Always `None` (absent in JSON) when `storageType = 'i'`.
sizeInBytes | Int | Size of the serialized DV in bytes (raw data size, i.e. before base85 encoding, if inline).
cardinality | Long | Number of rows the given DV logically removes from the file.

The concrete Base85 variant used is [Z85](https://rfc.zeromq.org/spec/32/), because it is JSON-friendly.

### Derived Fields

Some fields that are necessary to use the DV are not stored explicitly but can be derived in code from the stored fields.

Field Name | Data Type | Description | Computed As
-|-|-|-
uniqueId | String | Uniquely identifies a DV for a given file. This is used for snapshot reconstruction to differentiate the same file with different DVs in successive versions. | If `offset` is `None` then `<storageType><pathOrInlineDv>`. <br> Otherwise `<storageType><pathOrInlineDv>@<offset>`.
absolutePath | String/URI/Path | The absolute path of the DV file. Can be calculated for relative path DVs by providing a parent directory path. | If `storageType='p'`, just use the already absolute path. If `storageType='u'`, the DV is stored at `<parent path>/<random prefix>/deletion_vector_<uuid in canonical textual representation>.bin`. This is not a legal field if `storageType='i'`, as an inline DV has no absolute path.

### JSON Example 1 — On Disk with Relative Path (with Random Prefix)
```json
{
  "storageType" : "u",
  "pathOrInlineDv" : "ab^-aqEH.-t@S}K{vb[*k^",
  "offset" : 4,
  "sizeInBytes" : 40,
  "cardinality" : 6
}
```
Assuming that this DV is stored relative to an `s3://mytable/` directory, the absolute path to be resolved here would be: `s3://mytable/ab/deletion_vector_d2c639aa-8816-431a-aaf6-d3fe2512ff61.bin`.

### JSON Example 2 — On Disk with Absolute Path
```json
{
  "storageType" : "p",
  "pathOrInlineDv" : "s3://mytable/deletion_vector_d2c639aa-8816-431a-aaf6-d3fe2512ff61.bin",
  "offset" : 4,
  "sizeInBytes" : 40,
  "cardinality" : 6
}
```

### JSON Example 3 — Inline
```json
{
  "storageType" : "i",
  "pathOrInlineDv" : "wi5b=000010000siXQKl0rr91000f55c8Xg0@@D72lkbi5=-{L",
  "sizeInBytes" : 40,
  "cardinality" : 6
}
```
The row indexes encoded in this DV are: 3, 4, 7, 11, 18, 29.

## Reader Requirements for Deletion Vectors
If a snapshot contains logical files with records that are invalidated by a DV, then these records *must not* be returned in the output.

## Writer Requirement for Deletion Vectors
When adding a logical file with a deletion vector, then that logical file must have correct `numRecords` information for the data file in the `stats` field.

# Row IDs

Delta provides Row IDs. Row IDs are integers that are used to uniquely identify rows within a table.
Every row has two Row IDs:

- A **fresh** or unstable **Row ID**.
  This ID uniquely identifies the row within one version of the table.
  The fresh ID of a row may change every time the table is updated, even for rows that are not modified. E.g. when a row is copied unchanged during an update operation, it will get a new fresh ID. Fresh IDs can be used to identify rows within one version of the table, e.g. for identifying matching rows in self joins.
- A **stable Row ID**.
  This ID uniquely identifies the row across versions of the table and across updates.
  When a row is updated or copied, the stable Row ID for this row is preserved.

The fresh and stable Row IDs are not required to be equal.

Row IDs are stored in two ways:

- **Default generated Row IDs** use the `baseRowId` field stored in `add` and `remove` actions to generate fresh Row IDs.
  The default generated Row IDs for data files are calculated by adding the `baseRowId` of the file in which a row is contained to the (physical) position (index) of the row within the file.
  Default generated Row IDs require little storage overhead but are reassigned every time a row is updated or moved to a different file (for instance when a row is contained in a file that is compacted by OPTIMIZE).

- **Materialized Row IDs** are stored in a column in the data files.
  This column is hidden from readers and writers, i.e. it is not part of the `schemaString` in the table's `metaData`.
  Instead, the name of this column can be found in the value for the `delta.rowIds.physicalColumnName` key in the `configuration` of the table's `metaData` action.
  This column may contain `null` values meaning that the corresponding row has no materialized Row ID. This column may be omitted if all its values are `null` in the file.
  Materialized Row IDs provide a mechanism for writers to preserve stable Row IDs for rows that are updated or copied.

The fresh Row ID of a row is equal to the default generated Row ID. The stable Row ID of a row is equal to the materialized Row ID of the row when that column is present and the value is not NULL, otherwise it is equal to the default generated Row ID.

Row IDs are defined to be **supported** or **enabled** on a table as follows:
- When the feature `delta.rowIds` exists in the table `protocol`'s `writerFeatures` but the table property `delta.enableRowIds` is not set to true, then we say that row IDs are **supported**.
  In this situation, writers must assign new fresh row IDs, but row IDs cannot yet be relied upon to be present in the table.
- When the feature `delta.rowIds` exists in the table `protocol`'s `writerFeatures` and the table property `delta.enableRowIds` is set to true, then we say that row IDs are **enabled**.
  In this situation, row IDs can be relied upon to be present in the table for all rows.

Enablement:
- The table must be on Writer Version 7.
- The feature `delta.rowIds` must exist in the table `protocol`'s `writerFeatures`.
- The table property `delta.enableRowIds` must be set to `true`.

When enabled:
- Default generated Row IDs must be assigned to all existing rows.
  This means in particular that all files that are part of the table version that sets the table property `delta.enableRowIds` to `true` must have `baseRowId` set.
  A backfill operation may be required to commit `add` and `remove` actions with the `baseRowId` field set for all data files before  the table property `delta.enableRowIds` can be set to `true`.

## Reader Requirements for Row IDs

When Row IDs are enabled (when the table property `delta.enableRowIds` is set to `true`) and Row IDs are to be read, then:
- When requested, readers must reconstruct stable Row IDs as follows:
  1. Readers must use the materialized Row ID if the physical Row ID column determined by `delta.rowIds.physicalColumnName` is present in the data file and the column contains a non `null` value for a row.
  2. Readers must use the default generated Row ID in all other cases.
- Readers cannot read Row IDs while reading change data files from `cdc` actions.

## Writer Requirements for Row IDs

When Row IDs are supported (when the `writerFeatures` field of a table's `protocol` action contains `rowIds`), then:
- Writers must assign unique fresh Row IDs to all rows that they commit, i.e. writers must set the `baseRowId` field in all `add` actions that they commit so that all default generated Row IDs are unique in the table version.
  Writers must never commit duplicate Row IDs in the table in any version.
- Writers must set the `baseRowId` field in `remove` actions to the `baseRowId` value (if present) of the last committed `add` action with the same `path`.
- Writers must include a `rowIdHighWaterMark` action whenever they assign new fresh Row IDs, i.e. whenever they commit an `add` action for a new physical file.
  The `highWaterMark` value of the `rowIdHighWaterMark` action must always be equal to or greater than the highest fresh Row ID committed so far.
  Writers are allowed to reserve Row IDs in a first commit and assign them in following commits. In that case, each commit must include a `rowIdHighWaterMark` action even when the `highWaterMark` value is not changing.

Writers can enable Row IDs by setting `delta.enableRowIds` to `true` in the `configuration` of the table's `metaData`.
This is only allowed if the following requirements are satisfied:
- The feature `rowIds` has been added to the `writerFeatures` field of a table's `protocol` action either in the same version of the table or in an earlier version of the table.
- A physical column name for the materialized Row IDs has been assigned and added to the `configuration` in the table's `metaData` action using the key `delta.rowIds.physicalColumnName`.
  - The assigned column name must be unique. It must not be equal to the name of any other column in the table's schema.
    The assigned column name must remain unique in all future versions of the table.
    If [Column Mapping](#column-mapping) is enabled, then the assigned column name must be distinct from the physical column names of the table.
- The `baseRowId` field is set for all active `add` actions in the version of the table in which `delta.enableRowIds` is set to `true`.
- If the `baseRowId` field is not set in some active `add` action in the table, then writers must first commit new `add` actions that set the `baseRowId` field to replace the `add` actions that do not have the `baseRowId` field set.
  This can be done in the commit that sets `delta.enableRowIds` to `true` or in an earlier commit.
  The assigned `baseRowId` values must satisfy the same requirements as when assigning fresh Row IDs.

When Row IDs are enabled (when the table property `delta.enableRowIds` is set to `true`), then:
- Writers should assign stable Row IDs to all rows.
  - Stable Row IDs must be unique within a version of the table and must not be equal to the fresh Row IDs of other rows in the same version of the table.
  - Writers should preserve the stable Row IDs of rows that are updated or copied using materialized Row IDs.
    - The preserved stable Row ID (i.e. a stable Row ID that is not equal to the fresh Row ID of the same physical row) should be equal to the stable Row ID of the same logical row before it was updated or copied.
    - Materialized Row IDs must be written to the physical column determined by `delta.rowIds.physicalColumnName` in the `configuration` of the table's `metaData` action.
      The value in this physical column must be set to `NULL` for stable Row IDs that are not preserved.
- Writers must set the `preservedRowIds` flag of the `rowIdHighWaterMark` action to `true` whenever all the stable Row IDs of rows that are updated or copied were preserved.
  Writers must set that flag to false otherwise. In particular, writers must set the `preservedRowIds` flag of the `rowIdHighWaterMark` action to `true` if no rows are updated or copied.

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
  - Add / Remove actions with the same `(path, DV)` tuple.
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
 * [Domain Metadata](#Domain-Metadata)

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
 - stats_parsed: The stats can be stored in their [original format](#Per-file-Statistics). This field needs to be written when statistics are available and the table property: `delta.checkpoint.writeStatsAsStruct` is set to `true`. When this property is set to `false` (which is the default), this field should be omitted from the checkpoint.

Refer to the [appendix](#checkpoint-schema) for an example on the schema of the checkpoint.

## Data Files
 - Data files MUST be uniquely named and MUST NOT be overwritten. The reference implementation uses a GUID in the name to ensure this property.

## Append-only Tables
Enablement:
 - The table must be on a Writer Version starting from 2 up to 7.
 - If the table is on Writer Version 7, the feature `appendOnly` must exist in the table `protocol`'s `writerFeatures`.
 - The table property `delta.appendOnly` must be set to `true`.

When enabled:
 - New log entries MUST NOT change or remove data from the table.
 - New log entries may rearrange data (i.e. `add` and `remove` actions where `dataChange=false`).

To remove the append-only restriction, the table property `delta.appendOnly` must be set to `false`, or it must be removed.

## Column Invariants
Enablement:
 - If the table is on a Writer Version starting from 2 up to 6, Column Invariants are always enabled.
 - If the table is on Writer Version 7, the feature `columnInvariants` must exist in the table `protocol`'s `writerFeatures`.

When enabled:
 - The `metadata` for a column in the table schema MAY contain the key `delta.invariants`.
 - The value of `delta.invariants` SHOULD be parsed as a JSON string containing a boolean SQL expression at the key `expression.expression` (that is, `{"expression": {"expression": "<SQL STRING>"}}`).
 - Writers MUST abort any transaction that adds a row to the table, where an invariant evaluates to `false` or `null`.

For example, given the schema string (pretty printed for readability. The entire schema string in the log should be a single JSON line):

```json
{
    "type": "struct",
    "fields": [
        {
            "name": "x",
            "type": "integer",
            "nullable": true,
            "metadata": {
                "delta.invariants": "{\"expression\": { \"expression\": \"x > 3\"} }"
            }
        }
    ]
}
```

Writers should reject any transaction that contains data where the expression `x > 3` returns `false` or `null`.

## CHECK Constraints

Enablement:
- If the table is on a Writer Version starting from 3 up to 6, CHECK Constraints are always enabled.
- If the table is on Writer Version 7, a feature name `checkConstraints` must exist in the table `protocol`'s `writerFeatures`.

CHECK constraints are stored in the map of the `configuration` field in [Metadata](#change-metadata). Each CHECK constraint has a name and is stored as a key value pair. The key format is `delta.constraints.{name}`, and the value is a SQL expression string whose return type must be `Boolean`. Columns referred by the SQL expression must exist in the table schema.

Rows in a table must satisfy CHECK constraints. In other words, evaluating the SQL expressions of CHECK constraints must return `true` for each row in a table.

For example, a key value pair (`delta.constraints.birthDateCheck`, `birthDate > '1900-01-01'`) means there is a CHECK constraint called `birthDateCheck` in the table and the value of the `birthDate` column in each row must be greater than `1900-01-01`.

Hence, a writer must follow the rules below:
- CHECK Constraints may not be added to a table unless the above enablement rules are satisfied. When adding a CHECK Constraint to a table for the first time, writers are allowed to submit a `protocol` change in the same commit to enable the feature in the protocol.
- When adding a CHECK constraint to a table, a writer must validate the existing data in the table and ensure every row satisfies the new CHECK constraint before committing the change. Otherwise, the write must fail and the table must stay unchanged.
- When writing to a table that contains CHECK constraints, every new row being written to the table must satisfy CHECK constraints in the table. Otherwise, the write must fail and the table must stay unchanged.

## Generated Columns

Enablement:
 - If the table is on a Writer Version starting from 4 up to 6, Generated Columns are always enabled.
 - If the table is on Writer Version 7, a feature name `generatedColumns` must exist in the table `protocol`'s `writerFeatures`.

When enabled:
 - The `metadata` for a column in the table schema MAY contain the key `delta.generationExpression`.
 - The value of `delta.generationExpression` SHOULD be parsed as a SQL expression.
 - Writers MUST enforce that any data writing to the table satisfy the condition `(<value> <=> <generation expression>) IS TRUE`. `<=>` is the NULL-safe equal operator which performs an equality comparison like the `=` operator but returns `TRUE` rather than NULL if both operands are `NULL`

## Identity Columns

Delta supports defining Identity columns on Delta tables. Delta will generate unique values for Identity columns when users do not explicitly provide values for them when writing to such tables. To enable Identity Columns:
 - The table must be on Writer Version 6, or
 - The table must be on Writer Version 7, and a feature name `identityColumns` must exist in the table `protocol`'s `writerFeatures`.

When enabled, the `metadata` for a column in the table schema MAY contain the following keys for Identity Column properties:
- `delta.identity.start`: Starting value for the Identity column. This is a long type value. It should not be changed after table creation.
- `delta.identity.step`: Increment to the next Identity value. This is a long type value. It cannot be set to 0. It should not be changed after table creation.
- `delta.identity.highWaterMark`: The highest value generated for the Identity column. This is a long type value. When `delta.identity.step` is positive (negative), this should be the largest (smallest) value in the column.
- `delta.identity.allowExplicitInsert`: True if this column allows explicitly inserted values. This is a boolean type value. It should not be changed after table creation.

When `delta.identity.allowExplicitInsert` is true, writers should meet the following requirements:
- Users should be allowed to provide their own values for Identity columns.

When `delta.identity.allowExplicitInsert` is false, writers should meet the following requirements:
- Users should not be allowed to provide their own values for Identity columns.
- Delta should generate values that satisfy the following requirements
  - The new value does not already exist in the column.
  - The new value should satisfy `value = start + k * step` where k is a non-negative integer.
  - The new value should be higher than `delta.identity.highWaterMark`. When `delta.identity.step` is positive (negative), the new value should be the greater (smaller) than `delta.identity.highWaterMark`.
- Overflow when calculating generated Identity values should be detected and such writes should not be allowed.
- `delta.identity.highWaterMark` should be updated to the new highest value when the write operation commits.

## Writer Version Requirements

The requirements of the writers according to the protocol versions are summarized in the table below. Each row inherits the requirements from the preceding row.

<br> | Requirements
-|-
Writer Version 2 | - Respect [Append-only Tables](#append-only-tables)<br>- Respect [Column Invariants](#column-invariants)
Writer Version 3 | - Enforce `delta.checkpoint.writeStatsAsJson`<br>- Enforce `delta.checkpoint.writeStatsAsStruct`<br>- Respect [`CHECK` constraints](#check-constraints)
Writer Version 4 | - Respect [Change Data Feed](#add-cdc-file)<br>- Respect [Generated Columns](#generated-columns)
Writer Version 5 | Respect [Column Mapping](#column-mapping)
Writer Version 6 | Respect [Identity Columns](#identity-columns)
Writer Version 7 | Respect [Table Features](#table-features) for writers

# Requirements for Readers

This section documents additional requirements that readers must respect in order to produce correct scans of a Delta table.

## Reader Version Requirements

The requirements of the readers according to the protocol versions are summarized in the table below. Each row inherits the requirements from the preceding row.

<br> | Requirements
-|-
Reader Version 2 | Respect [Column Mapping](#column-mapping)
Reader Version 3 | Respect [Table Features](#table-features) for readers<br> - Writer Version must be 7

# Appendix

## Valid Feature Names in Table Features

Feature | Name | Readers or Writers?
-|-|-
[Append-only Tables](#append-only-tables) | `appendOnly` | Writers only
[Column Invariants](#column-invariants) | `invariants` | Writers only
[`CHECK` constraints](#check-constraints) | `checkConstraints` | Writers only
[Generated Columns](#generated-columns) | `generatedColumns` | Writers only
[Change Data Feed](#add-cdc-file) | `changeDataFeed` | Writers only
[Column Mapping](#column-mapping) | `columnMapping` | Readers and writers
[Identity Columns](#identity-columns) | `identityColumns` | Writers only
[Deletion Vectors](#deletion-vectors) | `deletionVectors` | Readers and writers
[Row IDs](#row-ids) | `rowIds` | Writers only
[Timestamp without Timezone](#timestamp-ntz) | `timestampNTZ` | Readers and writers
[Domain Metadata](#domain-metadata) | `domainMetadata` | Writers only

## Deletion Vector Format

Deletion Vectors are basically sets of row indexes, that is 64-bit integers that describe the position (index) of a row in a parquet file starting from zero. We store these sets in a compressed format. The fundamental building block for this is the open source [RoaringBitmap](https://roaringbitmap.org/) library. RoaringBitmap is a flexible format for storing 32-bit integers that automatically switches between three different encodings at the granularity of a 16-bit block (64K values):

- Simple integer array, when the number of values in the block is small.
- Bitmap-compressed, when the number of values in the block is large and scattered.
- Run-length encoded, when the number of values in the block is large, but clustered.

The serialization format is [standardized](https://github.com/RoaringBitmap/RoaringFormatSpec), and both [Java](https://github.com/lemire/RoaringBitmap/) and [C/C++](https://github.com/RoaringBitmap/CRoaring) implementations are available (among others).

The above description only applies to 32-bit bitmaps, but Deletion Vectors use 64-bit integers. In order to extend coverage from 32 to 64 bits, RoaringBitmaps defines a "portable" serialization format in the [RoaringBitmaps Specification](https://github.com/RoaringBitmap/RoaringFormatSpec#extention-for-64-bit-implementations). This format essentially splits the space into an outer part with the most significant 32-bit "keys" indexing the least significant 32-bit RoaringBitmaps in ascending sequence. The spec calls these least signficant 32-bit RoaringBitmaps "buckets".

Bytes | Name | Description
-|-|-
0 – 7 | numBuckets | The number of distinct 32-bit buckets in this bitmap.
`repeat for each bucket b` | | For each bucket in ascending order of keys.
`<start of b>` – `<start of b> + 3` | key | The most significant 32-bit of all the values in this bucket.
`<start of b> + 4` – `<end of b>` | bucketData | A serialized 32-bit RoaringBitmap with all the least signficant 32-bit entries in this bucket.

The 32-bit serialization format then consists of a header that describes all the (least signficant) 16-bit containers, their types (s. above), and their their key (most significant 16-bits).
This is followed by the data for each individual container in a container-specific format.

Reference Implementations of the Roaring format:

- [32-bit Java RoaringBitmap](https://github.com/RoaringBitmap/RoaringBitmap/blob/c7993318d7224cd3cc0244dcc99c8bbc5ddb0c87/RoaringBitmap/src/main/java/org/roaringbitmap/RoaringArray.java#L905-L949)
- [64-bit Java RoaringNavigableBitmap](https://github.com/RoaringBitmap/RoaringBitmap/blob/c7993318d7224cd3cc0244dcc99c8bbc5ddb0c87/RoaringBitmap/src/main/java/org/roaringbitmap/longlong/Roaring64NavigableMap.java#L1253-L1260)

Delta uses the format described above as a black box, but with two additions:

1. We prepend a "magic number", which can be used to make sure we are reading the correct format and also retains the ability to evolve the format in the future.
2. We require that every "key" (s. above) in the bitmap has a 0 as its most significant bit. This ensures that in Java, where values are read signed, we never read negative keys. 

The concrete serialization format is as follows (all numerical values are written in little endian byte order):

Bytes | Name | Description
-|-|-
0 — 3 | magicNumber | 1681511377; Indicates that the following bytes are serialized in this exact format. Future alternative—but related—formats must have a different magic number, for example by incrementing this one.
4 — end | bitmap | A serialized 64-bit bitmap in the portable standard format as defined in the [RoaringBitmaps Specification](https://github.com/RoaringBitmap/RoaringFormatSpec#extention-for-64-bit-implementations). This can be treated as a black box by any Delta implementation that has a native, standard-compliant RoaringBitmap library available to pass these bytes to.

### Deletion Vector File Storage Format

Deletion Vectors can be stored in files in cloud storage or inline in the Delta log.
The format for storing DVs in file storage is one (or more) of DV, using the 64-bit RoaringBitmaps described in the previous section, per file, together with a checksum for each DV:

Bytes | Name | Description
-|-|-
0 — 1 | version | The format version of this file: `1` for the format described here.
`repeat for each DV i` | | For each DV
`<start of i>` — `<start of i> + 3` | dataSize | Size of this DV’s data (without the checksum)
`<start of i> + 4` — `<start of i> + 4 + dataSize - 1` | bitmapData | One 64-bit RoaringBitmap serialised as described above.
`<start of i> + 4 + dataSize` — `<start of i> + 4 + dataSize + 3` | checksum | CRC-32 checksum of `bitmapData`

## Per-file Statistics
`add` actions can optionally contain statistics about the data in the file being added to the table.
These statistics can be used for eliminating files based on query predicates or as inputs to query optimization.

Global statistics record information about the entire file.
The following global statistic is currently supported:

Name | Description
-|-
numRecords | The number of records in this data file.
tightBounds | Whether per-column statistics are currently **tight** or **wide** (see below).

For any logical file where `deletionVector` is not `null`, the `numRecords` statistic *must* be present and accurate. That is, it must equal the number of records in the data file, not the valid records in the logical file.
In the presence of [Deletion Vectors](#Deletion-Vectors) the statistics may be somewhat outdated, i.e. not reflecting deleted rows yet. The flag `stats.tightBounds` indicates whether we have **tight bounds** (i.e. the min/maxValue exists[^1] in the valid state of the file) or **wide bounds** (i.e. the minValue is <= all valid values in the file, and the maxValue >= all valid values in the file). These upper/lower bounds are sufficient information for data skipping.

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
|    |-- tightBounds: boolean
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

Name | Description (`stats.tightBounds=true`) | Description (`stats.tightBounds=false`)
-|-|-
nullCount | The number of `null` values for this column | <p>If the `nullCount` for a column equals the physical number of records (`stats.numRecords`) then **all** valid rows for this column must have `null` values (the reverse is not necessarily true).</p><p>If the `nullCount` for a column equals 0 then **all** valid rows are non-`null` in this column (the reverse is not necessarily true).</p><p>If the `nullCount` for a column is any value other than these two special cases, the value carries no information and should be treated as if absent.</p>
minValues | A value that is equal to the smallest valid value[^1] present in the file for this column. If all valid rows are null, this carries no information. | A value that is less than or equal to all valid values[^1] present in this file for this column. If all valid rows are null, this carries no information.
maxValues | A value that is equal to the largest valid value[^1] present in the file for this column. If all valid rows are null, this carries no information. | A value that is greater than or equal to all valid values[^1] present in this file for this column. If all valid rows are null, this carries no information.

[^1]: String columns are cut off at a fixed prefix length. Timestamp columns are truncated down to milliseconds.

## Partition Value Serialization

Partition values are stored as strings, using the following formats. An empty string for any type translates to a `null` partition value.

Type | Serialization Format
-|-
string | No translation required
numeric types | The string representation of the number
date | Encoded as `{year}-{month}-{day}`. For example, `1970-01-01`
timestamp | Encoded as `{year}-{month}-{day} {hour}:{minute}:{second}` or `{year}-{month}-{day} {hour}:{minute}:{second}.{microsecond}` For example: `1970-01-01 00:00:00`, or `1970-01-01 00:00:00.123456`
timestamp without timezone | Encoded as `{year}-{month}-{day} {hour}:{minute}:{second}` or `{year}-{month}-{day} {hour}:{minute}:{second}.{microsecond}` For example: `1970-01-01 00:00:00`, or `1970-01-01 00:00:00.123456` To use this type, a table must support a feature `timestampNTZ`. See section [Timestamp without timezone (TimestampNTZ)](#timestamp-without-timezone-timestampntz) for more information.
boolean | Encoded as the string "true" or "false"
binary | Encoded as a string of escaped binary values. For example, `"\u0001\u0002\u0003"`

Note: A `timestamp` value in a partition value doesn't store the time zone due to historical reasons.
It means its behavior looks similar to `timestamp without time zone` when it is used in a partition column.

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
decimal| signed decimal number with fixed precision (maximum number of digits) and scale (number of digits on right side of dot). The precision and scale can be up to 38.
boolean| `true` or `false`
binary| A sequence of binary data.
date| A calendar date, represented as a year-month-day triple without a timezone.
timestamp| Microsecond precision timestamp elapsed since the Unix epoch, 1970-01-01 00:00:00 UTC. When this is stored in a parquet file, its `isAdjustedToUTC` must be set to `true`.
timestamp without time zone | Microsecond precision timestamp in a local timezone elapsed since the Unix epoch, 1970-01-01 00:00:00. It doesn't have the timezone information, and a value of this type can map to multiple physical time instants. It should always be displayed in the same way, regardless of the local time zone in effect. When this is stored in a parquet file, its `isAdjustedToUTC` must be set to `false`. To use this type, a table must support a feature `timestampNTZ`. See section [Timestamp without timezone (TimestampNTZ)](#timestamp-without-timezone-timestampntz) for more information.

See Parquet [timestamp type](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#timestamp) for more details about timestamp and `isAdjustedToUTC`.

Note: Existing tables may have `void` data type columns. Behavior is undefined for `void` data type columns but it is recommended to drop any `void` data type columns on reads (as is implemented by the Spark connector).

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
metadata| A JSON map containing information about this column. Keys prefixed with `Delta` are reserved for the implementation. See [Column Metadata](#column-metadata) for more information on column level metadata that clients must handle when writing to a table.

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

### Column Metadata
A column metadata stores various information about the column.
For example, this MAY contain some keys like [`delta.columnMapping`](#column-mapping) or [`delta.generationExpression`](#generated-columns).  
Field Name | Description
-|-
delta.columnMapping.*| These keys are used to store information about the mapping between the logical column name to  the physical name. See [Column Mapping](#column-mapping) for details.
delta.identity.*| These keys are for defining identity columns. See [Identity Columns](#identity-columns) for details.
delta.invariants| JSON string contains SQL expression information. See [Column Invariants](#column-invariants) for details.
delta.generationExpression| SQL expression string. See [Generated Columns](#generated-columns) for details.


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
The following examples uses a table with two partition columns: "date" and "region" of types date and string, respectively, and three data columns: "asset", "quantity", and "is_available" with data types string, double, and boolean. The checkpoint schema will look as follows:

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
|    |-- readerFeatures: array[string]
|    |-- writerFeatures: array[string]
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

Please note, as in the above example, the `readerFeatures` and `writerFeatures` fields do exist in the schema even when the table does not support table features. In such a case values of these two fields are `null`.

For a table that uses column mapping, whether in `id` or `name` mode, the schema of the `add` column will look as follows.

Schema definition:
```
{
  "type" : "struct",
  "fields" : [ {
    "name" : "asset",
    "type" : "string",
    "nullable" : true,
    "metadata" : {
      "delta.columnMapping.id": 1,
      "delta.columnMapping.physicalName": "col-b96921f0-2329-4cb3-8d79-184b2bdab23b"
    }
  }, {
    "name" : "quantity",
    "type" : "double",
    "nullable" : true,
    "metadata" : {
      "delta.columnMapping.id": 2,
      "delta.columnMapping.physicalName": "col-04ee4877-ee53-4cb9-b1fb-1a4eb74b508c"
    }
  }, {
    "name" : "date",
    "type" : "date",
    "nullable" : true,
    "metadata" : {
      "delta.columnMapping.id": 3,
      "delta.columnMapping.physicalName": "col-798f4abc-c63f-444c-9a04-e2cf1ecba115"
    }
  }, {
    "name" : "region",
    "type" : "string",
    "nullable" : true,
    "metadata" : {
      "delta.columnMapping.id": 4,
      "delta.columnMapping.physicalName": "col-19034dc3-8e3d-4156-82fc-8e05533c088e"
    }
  } ]
}
```

Checkpoint schema (just the `add` column):
```
|-- add: struct
|    |-- path: string
|    |-- partitionValues: map<string,string>
|    |-- size: long
|    |-- modificationTime: long
|    |-- dataChange: boolean
|    |-- stats: string
|    |-- tags: map<string,string>
|    |-- partitionValues_parsed: struct
|    |    |-- col-798f4abc-c63f-444c-9a04-e2cf1ecba115: date
|    |    |-- col-19034dc3-8e3d-4156-82fc-8e05533c088e: string
|    |-- stats_parsed: struct
|    |    |-- numRecords: long
|    |    |-- minValues: struct
|    |    |    |-- col-b96921f0-2329-4cb3-8d79-184b2bdab23b: string
|    |    |    |-- col-04ee4877-ee53-4cb9-b1fb-1a4eb74b508c: double
|    |    |-- maxValues: struct
|    |    |    |-- col-b96921f0-2329-4cb3-8d79-184b2bdab23b: string
|    |    |    |-- col-04ee4877-ee53-4cb9-b1fb-1a4eb74b508c: double
|    |    |-- nullCounts: struct
|    |    |    |-- col-b96921f0-2329-4cb3-8d79-184b2bdab23b: long
|    |    |    |-- col-04ee4877-ee53-4cb9-b1fb-1a4eb74b508c: long
```
