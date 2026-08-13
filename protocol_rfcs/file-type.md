# File Data Type
**Associated Github issue for discussions: https://github.com/delta-io/delta/issues/7147**

This protocol change adds support for the `file` data type.
The `file` data type stores a reference to a range of bytes that may be located inline in the value, elsewhere within the same data file, or in an external file.
It is intended for use cases such as file inventories, manifests, and unstructured-data references (for example, images or audio stored in object storage), which are increasingly common with AI/ML workloads.

The `file` data type is the Delta mapping of the Parquet [`FILE` logical type](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#file) (introduced in [apache/parquet-format#585](https://github.com/apache/parquet-format/pull/585)). Delta follows that specification for the physical representation, field set, and byte-resolution rules; this RFC defines how the type is represented in the Delta schema and how it interacts with Delta features.

--------

> ***New Section after the [Clustered Table](#clustered-table) section***

# File Data Type

This feature enables support for the `file` data type, which stores a reference to a range of bytes.
A `file` value resolves to bytes that are located in one of three ways:
- **inline** — the bytes are stored directly in the value,
- **self-reference** — the bytes are stored within the same data file that holds this `file` value, addressed by a byte range with no `uri`, and
- **external** — the bytes are stored in a separate file at a given `uri`.

The schema serialization method is described in [Schema Serialization Format](#schema-serialization-format), and the physical encoding is described in [File data in Parquet](#file-data-in-parquet).

To support this feature:
- The table must be on Reader Version 3 and Writer Version 7.
- The feature `fileType` must exist in the table `protocol`'s `readerFeatures` and `writerFeatures`.

## Example JSON-Encoded Delta Table Schema with File types

```
{
  "type" : "struct",
  "fields" : [ {
    "name" : "profile_image",
    "type" : "file",
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "attachments",
    "type" : {
      "type" : "array",
      "elementType" : {
        "type" : "file"
      },
      "containsNull" : false
    },
    "nullable" : false,
    "metadata" : { }
  } ]
}
```

## File data in Parquet

Delta follows the Parquet `FILE` logical type. A `file` column is stored in Parquet as a group annotated with the `FILE` logical type, and its physical field set, the rules for resolving a value to bytes (inline, self-reference, or external file), the `checksum` encoding, compression, encryption restrictions, and validation are exactly as defined in the [Parquet `FILE` specification](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#file). This RFC does not restate those rules.

For reference, the `FILE` group may contain the following optional fields: `uri`, `offset`, `size`, `content_type`, `checksum`, and `inline`. A value resolves to bytes located inline in the value, within the same data file (a self-reference), or in an external file at a `uri`; `content_type` and `checksum` are metadata describing the resolved bytes. See the Parquet specification for the exact field semantics and resolution rules.

## Writer Requirements for File Data Type

When File type is supported (`writerFeatures` field of a table's `protocol` action contains `fileType`), writers:
- must write a column of type `file` to Parquet as a group annotated with the Parquet `FILE` logical type, conforming to the [Parquet `FILE` specification](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#file) — including its field names and types, byte-resolution rules, `checksum` encoding, and validation. A value that does not resolve to any referent is invalid and must be represented as a column null instead.
- must store additional metadata about a file (for example, a modification timestamp) adjacent to the `file` column, not inside the `FILE`-annotated group.

## Reader Requirements for File Data Type

When File type is supported (`readerFeatures` field of a table's `protocol` action contains `fileType`), readers:
- must recognize and tolerate a `file` data type in a Delta schema.
- must read the `file` column from its Parquet `FILE`-annotated group and resolve each value to bytes per the [Parquet `FILE` specification](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#file), including the self-reference case (locating the bytes within the same data file when `uri` is absent).
- may return a `null` `file` value for a row whose reference is invalid (does not resolve to any referent).
- must make the column available to the engine:
    - [Recommended] Expose and interpret the group as a single `file` value, resolving inline, self-reference, and external bytes on access.
    - [Alternate] Expose the raw physical group (the set of present fields), for example if the engine does not natively support the `file` type.

## Compatibility with other Delta Features

Feature | Support for File Data Type
-|-
Partition Columns | A `file` column cannot be chosen as a partition column (a `file` value is a group and cannot be serialized to a partition-value string), but it can be used as a data column of a partitioned table.
Clustered Tables | A `file` column cannot itself be chosen as a clustering column (a `file` value is a group and is not a comparable data type as a whole), but it can be used as a non-clustering data column of a clustered table. Its comparable leaf fields (for example, `size` or `content_type`) may be used as clustering columns, consistent with [Statistics for File Columns](#statistics-for-file-columns).
Delta Column Statistics | **Supported:** A `file` column supports the `nullCount` statistic, and `minValues` / `maxValues` on its comparable leaf fields. See [Statistics for File Columns](#statistics-for-file-columns). <br/> **Unsupported:** The `file` column as a whole is not a comparable data type, and the `inline` field does not support `minValues` / `maxValues`.
Generated Columns | **Supported:** A `file` column is allowed to be used as a source in a generated column expression. <br/> **Open question:** Whether `file` may be the *result* type of a generated column expression (for example, constructing a `file` reference from other columns) is left open for discussion on the associated issue, and is not specified by this RFC.
Delta CHECK Constraints | **Supported:** A `file` column is allowed to be used for a CHECK constraint expression.
Default Column Values | **Supported:** A `file` column is allowed to have a default column value.
Change Data Feed | **Supported:** A table using the `file` data type is allowed to enable the Delta Change Data Feed. A `file` value is an ordinary column value, so it flows through Change Data Feed and time travel like any other column. See [Time Travel and Change Data Feed](#time-travel-and-change-data-feed) for the distinction between the reference and the referenced bytes.

## Statistics for File Columns

A `file` value is physically a group of leaf fields (see [File data in Parquet](#file-data-in-parquet)), and Delta's [Per-file Statistics](#per-file-statistics) are already encoded mirroring the schema of the data, descending into nested fields. Statistics for a `file` column follow that same per-leaf model, with one exception for the `inline` field:

- The `nullCount` statistic is collected for the `file` column itself (whether the whole `file` value is null), following the standard nested-field statistics encoding.
- `minValues` and `maxValues` are collected per leaf field, for the comparable leaf fields only: `uri` (STRING), `offset` (INT64), `size` (INT64), `content_type` (STRING), and `checksum` (STRING). These follow the standard rules for their respective types (for example, STRING leaves such as `uri` are truncated to a fixed prefix length, as with any string column).
- `minValues` and `maxValues` are **not** collected for the `inline` field, because it is binary content for which min/max provides no data-skipping value and may be large.

Collecting `minValues` / `maxValues` on `uri` in particular enables data skipping on file-inventory and manifest tables that filter by URI (for example, an object-store prefix).

The set of columns for which statistics are collected is otherwise governed by the table's existing statistics configuration (for example, the number of indexed columns).

## Time Travel and Change Data Feed

A `file` value is a reference, and it is stored in the table's data files like any other column value. Delta therefore time-travels and change-data-feeds the **reference**: querying a historical version of the table, or reading `file` columns through the Change Data Feed, returns the reference values exactly as they were written at that version.

For **inline** and **self-reference** values, the referenced bytes are stored within the data file itself, so they are versioned and time-travel with the table like any other column data.

For **external** references (a `uri` is set), Delta makes **no guarantee about the referenced bytes**, because those bytes live outside the Delta transaction log:

- The bytes may be overwritten or deleted independently of the table, so dereferencing a reference read from a historical version (via time travel or Change Data Feed) may fail or may return different bytes than when the reference was written. The `checksum` field, when present, allows a reader to detect that the bytes have changed, but does not allow it to recover the original bytes.
- Availability of the externally-referenced bytes is orthogonal to which table version is queried: time travel of the reference does not imply time travel of the external bytes.

## Non-Goals

The following are out of scope for this RFC:

- **Lifecycle and garbage collection of referenced bytes.** This RFC defines `file` as a reference only; it does not specify how, or whether, the referenced bytes are created, retained, or reclaimed, nor any interaction with `VACUUM`. Referenced bytes are handled out-of-band by the writer or an external system.
- **Access brokering and governance** of the referenced bytes (for example, catalog-vended credentials or signed URLs).

--------

> ***New Sub-Section after the [Variant Type](#variant-type) sub-section within the [Schema Serialization Format](#schema-serialization-format) section***

### File Type

File data uses the Delta type name `file` for Delta schema serialization.

Field Name | Description
-|-
type | Always the string "file"

--------

> ***Update the [Primitive Types](#primitive-types) table in the [Schema Serialization Format](#schema-serialization-format) section***

Add the following row to the Primitive Types table:

> file | A reference to a range of bytes located inline, elsewhere in the same data file, or in an external file. When stored in a Parquet file it is a group annotated with the Parquet `FILE` logical type. To use this type, a table must support the feature `fileType`. See section [File Data Type](#file-data-type).

--------

> ***Add a row to the [Valid Feature Names in Table Features](#valid-feature-names-in-table-features) table***

> [File Data Type](#file-data-type) | `fileType` | Readers and writers
