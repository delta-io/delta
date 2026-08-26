# File Data Type
**Associated Github issue for discussions: https://github.com/delta-io/delta/issues/7147**

This protocol change adds support for the `file` data type.
The `file` data type stores a reference to a range of bytes, stored either inline in the value or in an external file.
It is intended for use cases such as file inventories, manifests, and unstructured-data references (for example, images or audio stored in object storage), which are increasingly common with AI/ML workloads.

The `file` data type is the Delta mapping of the Parquet [`FILE` logical type](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#file) (introduced in [apache/parquet-format#585](https://github.com/apache/parquet-format/pull/585)). Delta follows that specification for the physical representation and field set, with one Delta-specific restriction defined below — a `uri` must be absolute. This RFC also defines how the type is represented in the Delta schema and how it interacts with Delta features.

--------

> ***New Section after the [Clustered Table](#clustered-table) section***

# File Data Type

This feature enables support for the `file` data type, which stores a reference to a range of bytes.
A `file` value resolves to bytes that are located in one of two ways:
- **inline** — the bytes are stored directly in the value (the `inline` field), or
- **external** — the bytes are stored in a separate file at an absolute `uri` (optionally a byte range within it, via `offset`/`size`).

These are the only two forms the Parquet `FILE` type provides: `offset`/`size` designate a byte range **within the file referenced by `uri`**, and there is no form that addresses a byte range in the data file that physically contains the value. (An earlier revision of the Parquet type allowed such a *self-reference*; it was removed from the specification in [apache/parquet-format#603](https://github.com/apache/parquet-format/pull/603).)

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

Delta follows the Parquet `FILE` logical type. A `file` column is stored in Parquet as a group annotated with the `FILE` logical type; its physical field set, the byte-resolution rules, the `checksum` encoding, and validation are exactly as defined in the [Parquet `FILE` specification](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#file). This RFC does not restate those rules; it adds the one Delta-specific restriction described below (an absolute `uri`).

For reference, the `FILE` group may contain the following optional fields: `uri`, `offset`, `size`, `content_type`, `checksum`, and `inline`. In a Delta table a value resolves to bytes either **inline** (the `inline` field) or from an **external** file at an absolute `uri` (optionally a byte range via `offset`/`size`); `content_type` and `checksum` are metadata describing the resolved bytes. See the Parquet specification for the exact field semantics.

## Writer Requirements for File Data Type

When File type is supported (`writerFeatures` field of a table's `protocol` action contains `fileType`), writers:
- must write a column of type `file` to Parquet as a group annotated with the Parquet `FILE` logical type, conforming to the [Parquet `FILE` specification](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#file) (field names and types, `checksum` encoding, and validation), subject to the Delta restrictions below.
- must write an **absolute** `uri` ([RFC 3986](https://datatracker.ietf.org/doc/html/rfc3986)) on every external reference (a value with `inline` not set and `uri` set). Relative URIs are not permitted in Delta tables, because a relative reference has no defined resolution base: `SHALLOW CLONE` leaves data files under the source table's directory, and `OPTIMIZE`/compaction and `DEEP CLONE` move rows into files under a different directory, so a relative `uri` would resolve differently after ordinary operations.
- must produce only **inline** (`inline` set) or **external** (`uri` set) values, per the Parquet `FILE` resolution rules. `offset`/`size` are meaningful only together with a `uri` (they designate a byte range within the referenced file); the Parquet type provides no form that addresses a byte range in the containing data file, so no such value can be written.
- may write inline values (`inline` set); doing so is optional. An inline value may additionally carry `uri`/`offset`/`size` locator fields, which per the Parquet specification are *provenance only* — they record where the bytes came from, must denote the same bytes as the inline value, and are not used for resolution. Delta does not interpret them, and the absolute-`uri` requirement above does not apply to such a provenance `uri` (it applies only to a `uri` used to resolve an external reference).
- must represent a value that does not resolve to any referent as a column null.
- must store additional metadata about a file (for example, a modification timestamp) adjacent to the `file` column, not inside the `FILE`-annotated group.

## Reader Requirements for File Data Type

When File type is supported (`readerFeatures` field of a table's `protocol` action contains `fileType`), readers:
- must recognize and tolerate a `file` data type in a Delta schema.
- must read the `file` column from its Parquet `FILE`-annotated group and resolve each value to bytes per the [Parquet `FILE` specification](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#file), supporting both **inline** values (the `inline` field) and **external** references (including a byte range when `offset`/`size` are set). Note that although writers are not required to produce inline values, readers must support reading them.
- must, for a row whose value does not resolve to any referent (an invalid value per the Parquet resolution rules — for example neither `inline` nor `uri` set, or `offset` set without `uri`) or that sets a **relative** `uri` (which Delta does not permit — see [Writer Requirements for File Data Type](#writer-requirements-for-file-data-type)), either return a `null` `file` value or fail the read. A reader must **not** attempt to resolve a relative `uri` by choosing a base (such as the table root or the data file's directory), because Delta defines no such base. A conforming writer never produces either case, so these only arise from a non-conforming writer, and per-file statistics are not expected to account for them.
- must make the column available to the engine:
    - [Recommended] Expose and interpret the group as a single `file` value, resolving inline and external bytes on access.
    - [Alternate] Expose the raw physical group (the set of present fields), for example if the engine does not natively support the `file` type.

## Compatibility with other Delta Features

Feature | Support for File Data Type
-|-
Partition Columns | A `file` column cannot be chosen as a partition column (a `file` value is a group and cannot be serialized to a partition-value string), but it can be used as a data column of a partitioned table.
Clustered Tables | A `file` column cannot itself be chosen as a clustering column (a `file` value is a group and is not a comparable data type as a whole), but it can be used as a non-clustering data column of a clustered table. Its comparable leaf fields (for example, `size` or `content_type`) may be used as clustering columns, addressed by the leaf path defined in [Statistics for File Columns](#statistics-for-file-columns) — encoded in the `clusteringColumns` list as a path-segment array (for example `[["<physical name of the file column>", "size"]]`), the same logical leaf path used for its required per-column statistics.
Delta Column Statistics | **Supported:** `nullCount` on the `file` column's public leaf fields (`uri`, `offset`, `size`, `content_type`, `checksum`), and `minValues` / `maxValues` on its comparable, skipping-useful leaf fields (`uri`, `offset`, `size`, `content_type`). See [Statistics for File Columns](#statistics-for-file-columns). <br/> **Unsupported:** The `file` value as a whole is not a comparable data type; no statistics are collected for the non-public `inline` field; and `minValues` / `maxValues` are not collected for `checksum`.
Generated Columns | **Supported:** A `file` column is allowed to be used as a source in a generated column expression, via its public leaf fields (all except `inline`) addressed by logical name (see the leaf-addressing carve-out in [Statistics for File Columns](#statistics-for-file-columns)). <br/> **Open question:** Whether `file` may be the *result* type of a generated column expression (for example, constructing a `file` reference from other columns) is left open for discussion on the associated issue, and is not specified by this RFC.
Delta CHECK Constraints | A `file` column may be used in a CHECK constraint expression through its **public** leaf fields (`uri`, `offset`, `size`, `content_type`, `checksum`), addressed by logical name (for example, `f.size > 0`). The `inline` field is not a public field and is not referenceable. Because a FILE leaf is not a struct field of the Delta schema, this is an explicit carve-out from the usual requirement that referenced columns exist in the schema — see the leaf-addressing rules in [Statistics for File Columns](#statistics-for-file-columns).
Default Column Values | A `file` column must default to `NULL`. There is no Delta-defined way to construct a non-null `file` literal as a default expression, so `NULL` is the only permitted default (as with the Variant type).
Change Data Feed | **Supported:** A table using the `file` data type is allowed to enable the Delta Change Data Feed. A `file` value flows through Change Data Feed and time travel like any other column value. See [Time Travel and Change Data Feed](#time-travel-and-change-data-feed) for the distinction between the reference and the referenced bytes.
Iceberg Compatibility | **Not supported in currently-released Iceberg versions.** Iceberg has no equivalent type today, so a `file` column cannot be represented in an IcebergCompatV1 or IcebergCompatV2 table: under [IcebergCompatV2](#writer-requirements-for-icebergcompatv2) it is blocked by the type allow-list (which excludes `file`), and [IcebergCompatV1](#writer-requirements-for-icebergcompatv1) has no type allow-list (it only blocks `Map`/`Array`/`Void`), so this RFC adds the requirement that a `file` column is not permitted there either. The same holds for the in-flight IcebergCompatV3 ([#4574](https://github.com/delta-io/delta/issues/4574)). **Support is targeted for Iceberg V4**, via the in-flight IcebergNativeV4 RFC ([#7374](https://github.com/delta-io/delta/pull/7374)), where an equivalent capability is expected. The precise interaction is out of scope for this RFC and will follow that work — including whether the FILE leaf fields carry field IDs (needed to represent them in Iceberg column statistics), which is a followup tied to the Iceberg `FILE` proposal.
Type Widening | **Unsupported:** No type change to or from `file` is supported.
Map Keys | **Unsupported:** A `file` value is not comparable, so `file` cannot be used as a map key type. `file` is allowed as an array element type and as a map value type (see the schema example above).

## Statistics for File Columns

A `file` value is physically a group with the fixed leaf fields defined by the Parquet `FILE` type (`uri`, `offset`, `size`, `content_type`, `checksum`, `inline`). Five of these are **public** fields — `uri`, `offset`, `size`, `content_type`, and `checksum` — exposed for addressing (statistics, clustering columns, CHECK constraints, and generated columns). The `inline` field holds the raw referenced bytes; it is **not** a public field and is neither addressable in expressions nor collected in statistics. Although `file` is serialized with a single type-name string in the Delta schema (like `variant`, and not a [Primitive Type](#primitive-types)), for [Per-file Statistics](#per-file-statistics) it is treated as that physical group: statistics descend into its public FILE leaf fields, exactly as they do for a struct column.

**Leaf addressing.** A FILE leaf is named by extending Delta's [field path](#field-path) formalism — "the ordered sequence of field names along that path" — by one final segment naming the literal public `FILE` field (`uri`, `offset`, `size`, `content_type`, or `checksum`). The FILE group's inner field names are fixed literals: they are **not** subject to [Column Mapping](#column-mapping) (the Parquet spec requires that they not be renamed) and have no assigned physical name, and they are **not** [struct fields](#struct-field) of the Delta schema. This one logical leaf path is *encoded differently at each site* where a leaf is referenced:

- **Per-file statistics** are nested JSON objects keyed by physical names, so a leaf statistic is keyed by the file column's physical name followed by the literal FILE field name — for example `minValues.<physical name of the file column>.uri` (see the example below).
- **Clustering columns** are stored in the `delta.clustering` domain as a list of path-segment arrays (a `Seq[Seq[String]]`), using physical names when Column Mapping is enabled — so clustering on a FILE leaf is encoded as, for example, `[["<physical name of the file column>", "size"]]`, **not** as a dotted string.
- **CHECK constraints and generated columns** are SQL expression strings over the *logical* schema, so they address a FILE leaf by logical name — for example `f.size`.

Because a FILE leaf is not a struct field of the Delta schema, addressing one in a `CHECK` constraint or generated-column expression is an explicit carve-out from the usual requirement that a referenced column exist in the table schema: the public `FILE` field names are addressable in SQL expressions (by logical name) despite not being schema fields.

> Note: this leaf addressing describes the current per-file statistics format — `add.stats`, a JSON object keyed by physical name. Alignment with field-ID-keyed statistics, as proposed in the in-flight [Iceberg V4 Adaptive Metadata Tree RFC](https://github.com/delta-io/delta/issues/6640), is out of scope for this RFC and will follow that specification.

The following statistics are collected per leaf:
- `nullCount` — on each **public** leaf field (`uri`, `offset`, `size`, `content_type`, `checksum`), counting rows in which that leaf is null; it is not collected for the non-public `inline` field. (For example, `nullCount` on `uri` counts rows whose value is stored inline — which have no `uri` — **plus** rows in which the entire `file` value is null; it is real data-skipping information.) Whole-value nullness of the `file` column is **not** separately captured, as is also the case for a struct column: the per-leaf encoding has no group-level `nullCount` slot, so `WHERE <file column> IS NULL` cannot be data-skipped from these statistics. (This differs from the Variant type, which keeps a deliberate column-level scalar `nullCount`.)
- `minValues` / `maxValues` — on the comparable, skipping-useful leaves only: `uri` (STRING), `offset` (INT64), `size` (INT64), and `content_type` (STRING). Standard per-type rules apply (for example, STRING leaves such as `uri` are truncated to a fixed prefix length).
- `minValues` / `maxValues` are **not** collected for `checksum` (a digest, or an opaque ETAG, is effectively uniformly distributed, so its min/max cannot skip anything). The non-public `inline` field has **no** statistics at all — neither `nullCount` nor `minValues`/`maxValues` — as it is binary content with no skipping value and is potentially large.

Collecting `minValues` / `maxValues` on `uri` in particular enables data skipping on file-inventory and manifest tables that filter by URI prefix.

**Indexed-column budget.** Each indexed FILE leaf counts individually toward `delta.dataSkippingNumIndexedCols` (leaf columns are counted structurally, as with any nested leaf, whether or not `minValues`/`maxValues` are collected for them) — so a `file` column contributes **five** indexed leaves (the public fields): all five carry `nullCount`, and four of them (`uri`, `offset`, `size`, `content_type`) additionally carry `minValues`/`maxValues`. The non-public `inline` field is not indexed. Writers should account for this when a `file` column appears near the front of a wide schema, so it does not silently displace statistics for other columns.

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

For **external** references (a `uri` is set), Delta makes **no guarantee about the referenced bytes**, because the referenced files live outside the Delta table (they are not tracked by its transaction log):

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

Like `variant`, `file` is a distinct top-level Delta type — it is **not** one of the [Primitive Types](#primitive-types). It is serialized with a single type-name string (`"type": "file"`) but is physically a group (see [File Data Type](#file-data-type)). To use this type, a table must support the feature `fileType`.

--------

> ***Add a row to the [Valid Feature Names in Table Features](#valid-feature-names-in-table-features) table***

> [File Data Type](#file-data-type) | `fileType` | Readers and writers
