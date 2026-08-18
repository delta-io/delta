# File Data Type
**Associated Github issue for discussions: https://github.com/delta-io/delta/issues/7147**

This protocol change adds support for the `file` data type.
The `file` data type stores a reference to a range of bytes, stored either inline in the value or in an external file.
It is intended for use cases such as file inventories, manifests, and unstructured-data references (for example, images or audio stored in object storage), which are increasingly common with AI/ML workloads.

The `file` data type is the Delta mapping of the Parquet [`FILE` logical type](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#file) (introduced in [apache/parquet-format#585](https://github.com/apache/parquet-format/pull/585)). Delta follows that specification for the physical representation and field set, with two Delta-specific restrictions defined below — self-references are not permitted, and a `uri` must be absolute. This RFC also defines how the type is represented in the Delta schema and how it interacts with Delta features.

--------

> ***New Section after the [Clustered Table](#clustered-table) section***

# File Data Type

This feature enables support for the `file` data type, which stores a reference to a range of bytes.
A `file` value resolves to bytes that are located in one of two ways:
- **inline** — the bytes are stored directly in the value (the `inline` field), or
- **external** — the bytes are stored in a separate file at an absolute `uri` (optionally a byte range within it, via `offset`/`size`).

The Parquet `FILE` type additionally allows a *self-reference* — a byte range within the same data file, addressed by `offset`/`size` with no `uri`. Self-references are **not permitted** in Delta tables (see [Writer Requirements for File Data Type](#writer-requirements-for-file-data-type) for the rationale).

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

Delta follows the Parquet `FILE` logical type. A `file` column is stored in Parquet as a group annotated with the `FILE` logical type; its physical field set, the byte-resolution rules, the `checksum` encoding, compression, and validation are exactly as defined in the [Parquet `FILE` specification](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#file). This RFC does not restate those rules; it adds the two Delta-specific restrictions described below (no self-references, absolute `uri`).

For reference, the `FILE` group may contain the following optional fields: `uri`, `offset`, `size`, `content_type`, `checksum`, and `inline`. In a Delta table a value resolves to bytes either **inline** (the `inline` field) or from an **external** file at an absolute `uri` (optionally a byte range via `offset`/`size`); `content_type` and `checksum` are metadata describing the resolved bytes. See the Parquet specification for the exact field semantics.

## Writer Requirements for File Data Type

When File type is supported (`writerFeatures` field of a table's `protocol` action contains `fileType`), writers:
- must write a column of type `file` to Parquet as a group annotated with the Parquet `FILE` logical type, conforming to the [Parquet `FILE` specification](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#file) (field names and types, `checksum` encoding, and validation), subject to the Delta restrictions below.
- must write an **absolute** `uri` ([RFC 3986](https://datatracker.ietf.org/doc/html/rfc3986)) on every external reference (a value with `inline` not set and `uri` set). Relative URIs are not permitted in Delta tables, because a relative reference has no defined resolution base: `SHALLOW CLONE` leaves data files under the source table's directory, and `OPTIMIZE`/compaction and `DEEP CLONE` move rows into files under a different directory, so a relative `uri` would resolve differently after ordinary operations.
- must **not** write a self-reference — that is, a value in which neither `inline` nor `uri` is set (bytes addressed only by `offset`/`size` within the data file that physically contains the row). A self-reference's `offset` is only meaningful relative to that file, but Delta operations that rewrite data files — `OPTIMIZE`/compaction, `MERGE`/`UPDATE`/`DELETE`, `REORG ... PURGE`, and writing `_change_data` files for Change Data Feed — relocate rows into new files without relocating the referenced byte ranges, which would silently repoint the value at unrelated bytes. Every value must therefore be either **inline** (`inline` set) or **external** (`uri` set).
- may write inline values (`inline` set); doing so is optional. An inline value may additionally carry `uri`/`offset`/`size` locator fields, which per the Parquet specification are *provenance only* — they record where the bytes originally came from and are not used for resolution. Delta does not interpret them, and the absolute-`uri` requirement above does not apply to such a provenance `uri` (it applies only to a `uri` used to resolve an external reference).
- must represent a value that does not resolve to any referent as a column null.
- must store additional metadata about a file (for example, a modification timestamp) adjacent to the `file` column, not inside the `FILE`-annotated group.

## Reader Requirements for File Data Type

When File type is supported (`readerFeatures` field of a table's `protocol` action contains `fileType`), readers:
- must recognize and tolerate a `file` data type in a Delta schema.
- must read the `file` column from its Parquet `FILE`-annotated group and resolve each value to bytes per the [Parquet `FILE` specification](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#file), supporting both **inline** values (the `inline` field) and **external** references (including a byte range when `offset`/`size` are set). Note that although writers are not required to produce inline values, readers must support reading them.
- must, for a row whose reference is invalid (does not resolve to any referent) or is a **self-reference** (`inline` not set and `uri` not set — a form Delta does not permit; see [Writer Requirements for File Data Type](#writer-requirements-for-file-data-type)), either return a `null` `file` value or fail the read. A conforming writer never produces either, so these only arise from a non-conforming writer, and per-file statistics are not expected to account for them.
- must make the column available to the engine:
    - [Recommended] Expose and interpret the group as a single `file` value, resolving inline and external bytes on access.
    - [Alternate] Expose the raw physical group (the set of present fields), for example if the engine does not natively support the `file` type.

## Compatibility with other Delta Features

Feature | Support for File Data Type
-|-
Partition Columns | A `file` column cannot be chosen as a partition column (a `file` value is a group and cannot be serialized to a partition-value string), but it can be used as a data column of a partitioned table.
Clustered Tables | A `file` column cannot itself be chosen as a clustering column (a `file` value is a group and is not a comparable data type as a whole), but it can be used as a non-clustering data column of a clustered table. Its comparable leaf fields (for example, `size` or `content_type`) may be used as clustering columns, addressed by the leaf path defined in [Statistics for File Columns](#statistics-for-file-columns) — encoded in the `clusteringColumns` list as a path-segment array (for example `[["<physical name of the file column>", "size"]]`), the same logical leaf path used for its required per-column statistics.
Delta Column Statistics | **Supported:** `nullCount` on the `file` column's leaf fields, and `minValues` / `maxValues` on its comparable, skipping-useful leaf fields (`uri`, `offset`, `size`, `content_type`). See [Statistics for File Columns](#statistics-for-file-columns). <br/> **Unsupported:** The `file` value as a whole is not a comparable data type; and `minValues` / `maxValues` are not collected for `inline` or `checksum`.
Generated Columns | **Supported:** A `file` column is allowed to be used as a source in a generated column expression, via its leaf fields addressed by logical name (see the leaf-addressing carve-out in [Statistics for File Columns](#statistics-for-file-columns)). <br/> **Open question:** Whether `file` may be the *result* type of a generated column expression (for example, constructing a `file` reference from other columns) is left open for discussion on the associated issue, and is not specified by this RFC.
Delta CHECK Constraints | A `file` column may be used in a CHECK constraint expression through its leaf fields, addressed by logical name (for example, `f.size > 0`). Because a FILE leaf is not a struct field of the Delta schema, this is an explicit carve-out from the usual requirement that referenced columns exist in the schema — see the leaf-addressing rules in [Statistics for File Columns](#statistics-for-file-columns).
Default Column Values | A `file` column must default to `NULL`. There is no Delta-defined way to construct a non-null `file` literal as a default expression, so `NULL` is the only permitted default (as with the Variant type).
Change Data Feed | **Supported:** A table using the `file` data type is allowed to enable the Delta Change Data Feed. A `file` value flows through Change Data Feed and time travel like any other column value. See [Time Travel and Change Data Feed](#time-travel-and-change-data-feed) for the distinction between the reference and the referenced bytes.
Iceberg Compatibility V1 / V2 | **Unsupported:** Under [IcebergCompatV2](#writer-requirements-for-icebergcompatv2) a `file` column is already blocked, because its type allow-list does not include `file`. [IcebergCompatV1](#writer-requirements-for-icebergcompatv1) has no type allow-list (it only blocks `Map`/`Array`/`Void`), so this RFC adds the requirement that a `file` column is not permitted in an IcebergCompatV1 table either. Iceberg has no equivalent type today; interaction with the in-flight IcebergCompatV3 RFC is out of scope for this RFC.
Type Widening | **Unsupported:** No type change to or from `file` is supported.
Map Keys | **Unsupported:** A `file` value is not comparable, so `file` cannot be used as a map key type. `file` is allowed as an array element type and as a map value type (see the schema example above).

## Statistics for File Columns

A `file` value is physically a group with the fixed leaf fields defined by the Parquet `FILE` type (`uri`, `offset`, `size`, `content_type`, `checksum`, `inline`). Although `file` is a single primitive type name in the Delta schema, for [Per-file Statistics](#per-file-statistics) it is treated as that physical group: statistics descend into the FILE leaf fields, exactly as they do for a struct column.

**Leaf addressing.** A FILE leaf is named by extending Delta's [field path](#field-path) formalism — "the ordered sequence of field names along that path" — by one final segment naming the literal `FILE` field (`uri`, `offset`, `size`, `content_type`, `checksum`, or `inline`). The FILE group's inner field names are fixed literals: they are **not** subject to [Column Mapping](#column-mapping) (the Parquet spec requires that they not be renamed) and have no assigned physical name, and they are **not** [struct fields](#struct-field) of the Delta schema. This one logical leaf path is *encoded differently at each site* where a leaf is referenced:

- **Per-file statistics** are nested JSON objects keyed by physical names, so a leaf statistic is keyed by the file column's physical name followed by the literal FILE field name — for example `minValues.<physical name of the file column>.uri` (see the example below).
- **Clustering columns** are stored in the `delta.clustering` domain as a list of path-segment arrays (a `Seq[Seq[String]]`), using physical names when Column Mapping is enabled — so clustering on a FILE leaf is encoded as, for example, `[["<physical name of the file column>", "size"]]`, **not** as a dotted string.
- **CHECK constraints and generated columns** are SQL expression strings over the *logical* schema, so they address a FILE leaf by logical name — for example `f.size`.

Because a FILE leaf is not a struct field of the Delta schema, addressing one in a `CHECK` constraint or generated-column expression is an explicit carve-out from the usual requirement that a referenced column exist in the table schema: `FILE` field names are addressable in SQL expressions (by logical name) despite not being schema fields.

The following statistics are collected per leaf:
- `nullCount` — on each leaf field, counting rows in which that leaf is null. (For example, `nullCount` on `uri` counts rows whose value is stored inline — which have no `uri` — **plus** rows in which the entire `file` value is null; it is real data-skipping information.) Whole-value nullness of the `file` column is **not** separately captured, as is also the case for a struct column: the per-leaf encoding has no group-level `nullCount` slot, so `WHERE <file column> IS NULL` cannot be data-skipped from these statistics. (This differs from the Variant type, which keeps a deliberate column-level scalar `nullCount`.)
- `minValues` / `maxValues` — on the comparable, skipping-useful leaves only: `uri` (STRING), `offset` (INT64), `size` (INT64), and `content_type` (STRING). Standard per-type rules apply (for example, STRING leaves such as `uri` are truncated to a fixed prefix length).
- `minValues` / `maxValues` are **not** collected for `inline` (binary content — no skipping value, and potentially large) or `checksum` (a digest, or an opaque ETAG, is effectively uniformly distributed, so its min/max cannot skip anything).

Collecting `minValues` / `maxValues` on `uri` in particular enables data skipping on file-inventory and manifest tables that filter by URI prefix.

**Indexed-column budget.** Each FILE leaf counts individually toward `delta.dataSkippingNumIndexedCols` (leaf columns are counted structurally, as with any nested leaf, whether or not `minValues`/`maxValues` are collected for them) — so a `file` column contributes **six** indexed leaves: all six carry `nullCount`, and four of them (`uri`, `offset`, `size`, `content_type`) additionally carry `minValues`/`maxValues`. Writers should account for this when a `file` column appears near the front of a wide schema, so it does not silently displace statistics for other columns.

For a `file` column `f`, the per-file statistics therefore take the following shape (leaves without `minValues`/`maxValues` omitted from those structs):

```
|-- stats: struct
|    |-- numRecords: long
|    |-- nullCount: struct
|    |    |-- f: struct
|    |    |    |-- uri: long
|    |    |    |-- offset: long
|    |    |    |-- size: long
|    |    |    |-- content_type: long
|    |    |    |-- checksum: long
|    |    |    |-- inline: long
|    |-- minValues: struct
|    |    |-- f: struct
|    |    |    |-- uri: string
|    |    |    |-- offset: long
|    |    |    |-- size: long
|    |    |    |-- content_type: string
|    |-- maxValues: struct
|    |    |-- f: struct
|    |    |    |-- uri: string
|    |    |    |-- offset: long
|    |    |    |-- size: long
|    |    |    |-- content_type: string
```

## Time Travel and Change Data Feed

A `file` value is a reference, and it is stored in the table's data files like any other column value. Delta therefore time-travels and change-data-feeds the **reference**: querying a historical version of the table, or reading `file` columns through the Change Data Feed, returns the reference values exactly as they were written at that version.

For **inline** values, the bytes are stored within the value itself, so they are versioned and time-travel with the table like any other column data.

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

> file | A reference to a range of bytes located inline in the value or in an external file. When stored in a Parquet file it is a group annotated with the Parquet `FILE` logical type. Self-references are not permitted and a `uri` must be absolute. To use this type, a table must support the feature `fileType`. See section [File Data Type](#file-data-type).

--------

> ***Add a row to the [Valid Feature Names in Table Features](#valid-feature-names-in-table-features) table***

> [File Data Type](#file-data-type) | `fileType` | Readers and writers
