# File Data Type
**Associated Github issue for discussions: https://github.com/delta-io/delta/issues/7147**

This protocol change adds support for the `file` data type.
The `file` data type stores a reference to a range of bytes that may be located inline in the value, elsewhere within the same data file, or in an external file.
It is intended for use cases such as file inventories, manifests, and unstructured-data references (for example, images or audio stored in object storage), which are increasingly common with AI/ML workloads.

The `file` data type is the Delta mapping of the Parquet `FILE` logical type proposed in [apache/parquet-format#585](https://github.com/apache/parquet-format/pull/585). This RFC is aligned with that proposal: the physical Parquet representation, the field set, and the byte-resolution rules defined here match the Parquet `FILE` type so that a Delta `file` column round-trips through Parquet without loss.

--------

> ***New Section after the [Clustered Table](#clustered-table) section***

# File Data Type

This feature enables support for the `file` data type, which stores a reference to a range of bytes.
A `file` value resolves to bytes that are located in one of three ways:
- **inline** — the bytes are stored directly in the value,
- **self-reference** — the bytes are stored within the same data file that holds this `file` value, addressed by a byte range (`offset` / `size`) with no `path`. The bytes are written between column chunks and are not otherwise referenced by the Parquet footer, so a reader locates them within the current file rather than opening an external one. See [Byte Resolution](#byte-resolution).
- **external** — the bytes are stored in a separate file at a given `path`.

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

The `file` data type is represented in Parquet as a group annotated with the Parquet `FILE` logical type, as specified in [apache/parquet-format#585](https://github.com/apache/parquet-format/pull/585).
The group contains the following fields, identified by name. Field IDs may also be used for projection. All fields are optional:

Struct field name | Parquet primitive type | Description
-|-|-
path | binary (`STRING`) | An opaque location string for an external file (for example, `s3://bucket/file.jpg`). No encoding (such as URI encoding) is applied on top of the user-provided value. If `path` is absent, the value refers to this file (a self-reference).
offset | int64 | The start of the byte range within the referenced data. If absent, readers must treat it as 0.
size | int64 | The byte length of the referenced data. Must be zero or a positive integer if provided; 0 indicates empty referenced data. If absent, the range runs to the end of the referenced data.
content_type | binary (`STRING`) | The media type (MIME type) of the resolved bytes, for example `image/png`.
checksum | binary (`STRING`) | A self-describing integrity token for the resolved bytes, of the form `<algorithm>:base64(<digest bytes>)` (see [Checksum](#checksum)).
inline | binary | The referenced bytes stored inline in the value. If `inline` is set, it supplies the bytes and any locator fields (`path`, `offset`, `size`) that are present are provenance only.

A value resolves to bytes determined by `inline` / `path` / `offset` / `size`; `content_type` and `checksum` are metadata describing whatever is resolved. The resolution rules are given in [Byte Resolution](#byte-resolution).

The annotated Parquet group is, for example:

```
optional group profile_image (FILE) {
  optional binary path (STRING);
  optional int64 offset;
  optional int64 size;
  optional binary content_type (STRING);
  optional binary checksum (STRING);
  optional binary inline;
}
```

### Checksum

The `checksum` field is a self-describing token of the form `<algorithm>:base64(<digest bytes>)`. It generalizes the storage-system eTag. The recognized algorithms are:

Algorithm | Notes
-|-
`ETAG` | The object-store eTag — equality-only, not recomputable.
`MD5` | The usual S3/HTTP eTag and Content-MD5.
`CRC32` | Parquet's page-checksum algorithm (gzip/zlib).
`CRC32C` | Common in object stores, hardware-accelerated.
`SHA-256` | For example, S3 additional checksums.

`checksum` applies to the resolved bytes, except for `ETAG`, which is the object-store eTag for the whole file referenced by `path`.

### Byte Resolution

A value resolves to bytes based on which of `inline`, `path`, `offset`, and `size` are set:

`inline` | `path` | `offset` | `size` | Resolves to
:-:|:-:|:-:|:-:|-
set | – | – | – | The `inline` bytes.
– | set | – | – | The whole external file at `path`.
– | set | set | – | External `path`, `[offset, EOF)`.
– | set | – | set | External `path`, `[0, size)`.
– | set | set | set | External `path`, `[offset, offset + size)`.
– | – | set | – | This file, `[offset, EOF)` (self-reference).
– | – | – | set | This file, `[0, size)` (self-reference).
– | – | set | set | This file, `[offset, offset + size)` (self-reference).
– | – | – | – | Nothing — invalid (use column nullability for a null value).

A self-reference typically points within the same Parquet data file using `offset` and `size`; the bytes are written between column chunks and are not otherwise referenced by the footer. A self-reference is the *absence* of `path`, never an absolute path back to the current file, so a data file containing self-references is renamed or relocated as a single unit.

The referenced bytes (both `inline` and self-reference regions) are compressed with the same `CompressionCodec` as the one configured for the `inline` column; `size` is therefore the length of the compressed region on disk. External referents (`path` set) are opaque to Parquet and stored as-is.

## Writer Requirements for File Data Type

When File type is supported (`writerFeatures` field of a table's `protocol` action contains `fileType`), writers:
- must write a column of type `file` to Parquet as a group annotated with the Parquet `FILE` logical type, containing only fields drawn from the set `path`, `offset`, `size`, `content_type`, `checksum`, and `inline`, with the Parquet primitive types described in [File data in Parquet](#file-data-in-parquet).
- must not rename the fields within a `FILE`-annotated group; the field names above are normative.
- must write every value so that it resolves to a referent according to [Byte Resolution](#byte-resolution); a value with none of `inline`, `path`, `offset`, or `size` set is invalid and must be represented as a column null instead.
- must, when writing a `checksum`, use the `<algorithm>:base64(<digest bytes>)` form with one of the recognized algorithms in [Checksum](#checksum).
- must store additional metadata about a file (for example, a modification timestamp) adjacent to the `file` column, not inside the `FILE`-annotated group.

## Reader Requirements for File Data Type

When File type is supported (`readerFeatures` field of a table's `protocol` action contains `fileType`), readers:
- must recognize and tolerate a `file` data type in a Delta schema.
- must use the correct physical schema (a Parquet `FILE`-annotated group with the optional fields described in [File data in Parquet](#file-data-in-parquet)) when reading a `file` data type from a file.
- must resolve each value to bytes according to [Byte Resolution](#byte-resolution), including the self-reference case (locating the bytes within the same data file when `path` is absent).
- must make the column available to the engine:
    - [Recommended] Expose and interpret the group as a single `file` value, resolving inline, self-reference, and external bytes on access.
    - [Alternate] Expose the raw physical group (the set of present fields), for example if the engine does not natively support the `file` type.

## Compatibility with other Delta Features

Feature | Support for File Data Type
-|-
Partition Columns | **Supported:** A `file` column is allowed to be a non-partitioned column of a partitioned table. <br/> **Unsupported:** A `file` value is a group and cannot be serialized to a partition-value string, so a `file` column cannot be a partition column.
Clustered Tables | **Supported:** A `file` column is allowed to be a non-clustering column of a clustered table. <br/> **Unsupported:** A `file` value is a group and is not a comparable data type as a whole, so a `file` column cannot be a clustering column.
Delta Column Statistics | **Supported:** A `file` column supports the `nullCount` statistic, and `minValues` / `maxValues` on its comparable leaf fields. See [Statistics for File Columns](#statistics-for-file-columns). <br/> **Unsupported:** The `file` column as a whole is not a comparable data type, and the `inline` field does not support `minValues` / `maxValues`.
Generated Columns | **Supported:** A `file` column is allowed to be used as a source in a generated column expression. <br/> **Open question:** Whether `file` may be the *result* type of a generated column expression (for example, constructing a `file` reference from other columns) is left open for discussion on the associated issue, and is not specified by this RFC.
Delta CHECK Constraints | **Supported:** A `file` column is allowed to be used for a CHECK constraint expression.
Default Column Values | **Supported:** A `file` column is allowed to have a default column value.
Change Data Feed | **Supported:** A table using the `file` data type is allowed to enable the Delta Change Data Feed. A `file` value is an ordinary column value, so it flows through Change Data Feed and time travel like any other column. See [Time Travel and Change Data Feed](#time-travel-and-change-data-feed) for the distinction between the reference and the referenced bytes.

## Statistics for File Columns

A `file` value is physically a group of leaf fields (see [File data in Parquet](#file-data-in-parquet)), and Delta's [Per-file Statistics](#per-file-statistics) are already encoded mirroring the schema of the data, descending into nested fields. Statistics for a `file` column follow that same per-leaf model, with one exception for the `inline` field:

- The `nullCount` statistic is collected for the `file` column itself (whether the whole `file` value is null), following the standard nested-field statistics encoding.
- `minValues` and `maxValues` are collected per leaf field, for the comparable leaf fields only: `path` (STRING), `offset` (INT64), `size` (INT64), `content_type` (STRING), and `checksum` (STRING). These follow the standard rules for their respective types (for example, STRING leaves such as `path` are truncated to a fixed prefix length, as with any string column).
- `minValues` and `maxValues` are **not** collected for the `inline` field, because it is binary content for which min/max provides no data-skipping value and may be large.

Collecting `minValues` / `maxValues` on `path` in particular enables data skipping on file-inventory and manifest tables that filter by path (for example, an object-store prefix).

The set of columns for which statistics are collected is otherwise governed by the table's existing statistics configuration (for example, the number of indexed columns).

## Time Travel and Change Data Feed

A `file` value is a reference, and it is stored in the table's data files like any other column value. Delta therefore time-travels and change-data-feeds the **reference**: querying a historical version of the table, or reading `file` columns through the Change Data Feed, returns the reference values exactly as they were written at that version.

Delta makes **no guarantee about the referenced bytes**, because those bytes live outside the Delta transaction log:

- The bytes may be overwritten or deleted independently of the table, so dereferencing a reference read from a historical version (via time travel or Change Data Feed) may fail or may return different bytes than when the reference was written. The `checksum` field, when present, allows a reader to detect that the bytes have changed, but does not allow it to recover the original bytes.
- Availability of the referenced bytes is orthogonal to which table version is queried: time travel of the reference does not imply time travel of the bytes.

## Non-Goals

The following are out of scope for this RFC:

- **Lifecycle and garbage collection of referenced bytes.** This RFC defines `file` as a reference only; it does not specify how, or whether, the referenced bytes are created, retained, or reclaimed. In particular, it does not define a notion of Delta-managed referenced bytes or their interaction with `VACUUM`. Referenced bytes are managed out-of-band by the writer or an external system.
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
