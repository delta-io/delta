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
The group may contain the following fields, identified by name (matched case-sensitively, not by field order). Field IDs, if they exist, may also be used for projection. Every field is optional both in the schema and in the data: a writer may omit any field from the group definition, and any field that is present has repetition type `OPTIONAL`. A group need only define the fields it uses (for example, an inline-only column may define just `inline`, and a whole-file external reference may define just `path`).

A field is *set* when it is present in the group and its value is non-null (and, for string fields, non-empty). A field is *not set* when it is absent from the group, or is present but null or empty. (Implementations are not expected to treat empty strings as null.)

Struct field name | Parquet primitive type | Description
-|-|-
path | binary (`STRING`) | A URI-reference as defined by [RFC 3986](https://datatracker.ietf.org/doc/html/rfc3986), which may be absolute or relative (for example, `s3://bucket/file.jpg`). No additional encoding (such as URI encoding) is applied on top of the user-provided value. If `path` is not set, the value refers to the current file (a self-reference).
offset | int64 | The start of the byte range within the referenced data. Must not be negative. If not set, readers must treat it as 0; if set and non-zero, readers must seek to this offset. `offset` must be set for a self-reference (`path` not set), and is optional for an external reference.
size | int64 | The byte length of the referenced data. Must be zero or a positive integer if set; 0 indicates empty referenced data. `size` must be set whenever `offset` is set. It may be omitted only for a whole-file external reference (`path` set, `offset` not set), in which case the range runs to the end of the referenced file.
content_type | binary (`STRING`) | The media type (MIME type), as defined by [RFC 2046](https://datatracker.ietf.org/doc/html/rfc2046), of the resolved bytes, for example `image/png`. When not set, the type may be assumed to be `application/octet-stream`.
checksum | binary (`STRING`) | A self-describing integrity token for the resolved bytes, of the form `<algorithm>:<digest>` (see [Checksum](#checksum)).
inline | binary | The referenced bytes stored inline in the value. If `inline` is set, it supplies the bytes and any locator fields (`path`, `offset`, `size`) that are set are provenance only.

A value resolves to bytes determined by `inline` / `path` / `offset` / `size`; `content_type` and `checksum` are metadata describing whatever is resolved. The resolution rules are given in [Byte Resolution](#byte-resolution).

Because every field is optional, a group need only define the fields it uses. An example group that defines all fields:

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

A column whose values are always stored inline may define just `inline` (optionally with `content_type`):

```
optional group inline_file (FILE) {
  optional binary inline;
  optional binary content_type (STRING);
}
```

### Checksum

The `checksum` field is a self-describing token of the form `<algorithm>:<digest>`, where `<digest>` is encoded according to the `Encoding` column below. It generalizes the storage-system eTag. Readers should ignore unknown algorithms. The recognized algorithms are:

Algorithm | Encoding | Notes
-|-|-
`ETAG` | opaque | The object-store eTag, not recomputable.
`MD5` | lowercase hex | As defined in [RFC 1321](https://datatracker.ietf.org/doc/html/rfc1321), represented as 32 hex characters.
`CRC32` | lowercase hex | As defined in [RFC 2083](https://datatracker.ietf.org/doc/html/rfc2083), represented as 8 hex characters.
`CRC32C` | lowercase hex | As defined in [RFC 3385](https://datatracker.ietf.org/doc/html/rfc3385), represented as 8 hex characters.
`SHA-256` | lowercase hex | As defined in [RFC 6234](https://datatracker.ietf.org/doc/html/rfc6234), represented as 64 hex characters.

The `<digest>` encodings are:
- **lowercase hex**: the digest bytes rendered as lowercase hexadecimal, two characters per byte and no separators (for example, `MD5:d41d8cd98f00b204e9800998ecf8427e`).
- **opaque**: the token supplied verbatim by the object store, used only for equality comparison and not otherwise interpreted.

`checksum` applies to the resolved bytes, except for `ETAG`, which is the object-store eTag for the whole file referenced by `path`.

### Byte Resolution

A value resolves to bytes based on which of `inline`, `path`, `offset`, and `size` are set. `size` must be set whenever `offset` is set, so every offset-based read carries an explicit `size`; `size` may be omitted only for a whole-file external reference.

`inline` | `path` | `offset` | `size` | Resolves to
:-:|:-:|:-:|:-:|-
set | – | – | – | The `inline` bytes.
– | set | – | – | The whole external file at `path`.
– | set | – | set | External `path`, `[0, size)`.
– | set | set | set | External `path`, `[offset, offset + size)`.
– | – | set | set | This file, `[offset, offset + size)` (self-reference).
– | set | set | – | Invalid (`offset` set without `size`).
– | – | set | – | Invalid (`offset` set without `size`).
– | – | – | set | Invalid (`size` set without `path` or `offset`).
– | – | – | – | Nothing — invalid (use column nullability for a null value).

A self-reference points within the same Parquet data file using `offset` and `size` (both required); the bytes are written between column chunks and are not otherwise referenced by the footer. A self-reference is the *absence* of `path`, never an absolute path back to the current file, so a data file containing self-references is renamed or relocated as a single unit.

The referenced bytes (both `inline` and self-reference regions) are compressed with the same `CompressionCodec` as the one configured for the `inline` column; `size` is therefore the length of the compressed region on disk. External referents (`path` set) are opaque to Parquet and stored as-is.

## Writer Requirements for File Data Type

When File type is supported (`writerFeatures` field of a table's `protocol` action contains `fileType`), writers:
- must write a column of type `file` to Parquet as a group annotated with the Parquet `FILE` logical type, whose fields are drawn from the set `path`, `offset`, `size`, `content_type`, `checksum`, and `inline`, with the Parquet primitive types described in [File data in Parquet](#file-data-in-parquet). The group need only define the fields it uses; any field it does define must be optional.
- must not rename the fields within a `FILE`-annotated group; the field names above are normative.
- must write every value so that it resolves to a referent according to [Byte Resolution](#byte-resolution). In particular, `size` must be set whenever `offset` is set, and a self-reference (`path` not set) must set `offset` (and therefore `size`). A value that does not resolve to any referent is invalid and must be represented as a column null instead.
- must, when writing a `checksum`, use the `<algorithm>:<digest>` form with one of the recognized algorithms and its digest encoding in [Checksum](#checksum).
- must store additional metadata about a file (for example, a modification timestamp) adjacent to the `file` column, not inside the `FILE`-annotated group.

## Reader Requirements for File Data Type

When File type is supported (`readerFeatures` field of a table's `protocol` action contains `fileType`), readers:
- must recognize and tolerate a `file` data type in a Delta schema.
- must use the correct physical schema (a Parquet `FILE`-annotated group with the optional fields described in [File data in Parquet](#file-data-in-parquet)) when reading a `file` data type from a file.
- must resolve each value to bytes according to [Byte Resolution](#byte-resolution), including the self-reference case (locating the bytes within the same data file when `path` is absent).
- may return a `null` `file` value for a row whose reference is invalid (does not resolve to any referent per [Byte Resolution](#byte-resolution)).
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

## Storage Mode

A `file` column optionally carries a **storage mode** that records whether the referenced bytes are `managed` (their lifecycle is intended to be governed by the table/engine) or `external` (owned entirely outside the table), or leaves the column unqualified. The storage mode is a property of the *column*, not of an individual value, and it is stored in the column's [schema metadata](#schema-serialization-format) rather than inside the `file` value. This is intentional: the physical `file` value must round-trip through the Parquet `FILE` logical type (and equivalents such as Iceberg's file reference), whose field set is fixed, so a mode stored inside the value would not survive the round-trip. Keeping the mode in schema metadata also lets it propagate through the Delta transaction log unchanged.

The storage mode is recorded in the `__FILE_TYPE_MODE` key of the metadata of the nearest ancestor [StructField](#struct-field), as a JSON object mapping a field path to one of `MANAGED`, `EXTERNAL`, or `UNKNOWN`. Nested maps and arrays are encoded using the same field-path convention as identifiers in [IcebergCompatV2](#writer-requirements-for-icebergcompatv2) (for example, `arr.element`). This mirrors the representation used for the [Collated String Type](#collated-string-type) (`__COLLATIONS`). A `file` column with no `__FILE_TYPE_MODE` entry is an unqualified `file` reference.

This RFC standardizes only the **representation** of the storage mode, so that a mode set by one engine propagates through the log and is visible to others. It does **not** assign any behavioral difference to `MANAGED` versus `EXTERNAL` in Delta: byte lifecycle, garbage collection, and access brokering are out of scope (see [Non-Goals](#non-goals)). Engines may attach their own semantics to the mode, but must not assume that reading or writing a mode changes how Delta itself manages the referenced bytes.

Example schema for `profile_image FILE MANAGED` (irrelevant fields stripped):

```
{
  "name" : "profile_image",
  "type" : "file",
  "nullable" : true,
  "metadata" : {
    "__FILE_TYPE_MODE" : { "profile_image" : "MANAGED" }
  }
}
```

## Non-Goals

The following are out of scope for this RFC:

- **Lifecycle and garbage collection of referenced bytes.** This RFC defines `file` as a reference only; it does not specify how, or whether, the referenced bytes are created, retained, or reclaimed. In particular, although the [Storage Mode](#storage-mode) may record that a column is `managed`, this RFC does not define any resulting behavior, nor any interaction with `VACUUM`. Referenced bytes are managed out-of-band by the writer or an external system.
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

> ***Add a row to the [Column Metadata](#column-metadata) table***

> \_\_FILE\_TYPE\_MODE | The [storage mode](#storage-mode) (`MANAGED`, `EXTERNAL`, or `UNKNOWN`) of `file`-typed fields, or combinations of maps and arrays, stored in this field. Encoded as a JSON object mapping field path to mode. See [Storage Mode](#storage-mode) for details.

--------

> ***Add a row to the [Valid Feature Names in Table Features](#valid-feature-names-in-table-features) table***

> [File Data Type](#file-data-type) | `fileType` | Readers and writers
