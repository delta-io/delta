# Geospatial Types (geometry and geography)
**Associated Github issue for discussions: https://github.com/delta-io/delta/issues/XXXX**
<!-- TODO before merging this RFC: create the Protocol Change Request issue and replace XXXX here and in protocol_rfcs/README.md. -->

This protocol change documents the `geometry` and `geography` column types and the `geospatial` table feature, which delta-spark and Delta Kernel already implement, and proposes adding them to the Delta protocol so that every connector reads and writes them the same way. Neither the types nor the feature appear in `PROTOCOL.md` today, so a client that follows the spec alone cannot tell what `geometry(OGC:CRS84)` or `geography(OGC:CRS84, spherical)` in a `schemaString` means, or that a table containing them must list a table feature.

Notes for reviewers (not part of the proposed spec text):

- This is a new table feature, so it goes through the RFC process instead of a direct `PROTOCOL.md` edit. It is written after the implementations, like the [User-Defined Types](https://github.com/delta-io/delta/blob/master/protocol_rfcs/user-defined-types.md) RFC.
- The type strings, defaults and feature name in the spec text come from the Kernel `DataTypeJsonSerDe` and `TableFeatures`, and from delta-spark's `GeoSpatialTableFeature`. Nothing here has been checked against delta-rs or Trino.
- The two implementations differ in some details (see "Compatibility" and "Open questions"). Where they differ, the spec text below takes the behavior both can already read, and the difference is listed as an open question rather than silently picked.

## Motivation

Geospatial data is stored in Delta tables today as `geometry` and `geography` columns, but the spec has no entry for these types. The consequences are:

- A reader written from the spec rejects, or mis-reads, a table whose `schemaString` has a `geometry(...)` type string.
- Writers have no spec text for the type string forms (CRS and edge interpolation algorithm), their defaults, or the Parquet representation, so each engine invents its own.
- The `geospatial` table feature, which gates these types, is undefined for clients that don't read the source code of an existing implementation.

Existing features don't cover this. `binary` holds the bytes but loses the CRS and algorithm, and a client has no way to know that the bytes are a geospatial value.

## Feature summary

- Table feature name: `geospatial`. It has no `-dev` suffix because the name is already used by existing implementations. An earlier, experimental name `geospatial-dev` also exists in delta-spark (see "Compatibility" and "Open questions").
- Readers and writers, or writers only: Readers and writers. A reader that ignores the feature can't interpret the `geometry` and `geography` type strings in the schema, and would either fail to parse the schema or return the stored bytes without their CRS and algorithm.
- Minimum protocol versions: reader version 3, writer version 7.
- Required features: None.
- Features it can't be used with: A `geometry` or `geography` column can't be a partition column.
- Supported vs enabled: There is no table property. When `geospatial` is supported, readers and writers MUST handle `geometry` and `geography` columns. A writer MUST add the feature to the protocol in the same commit that first adds a `geometry` or `geography` column to the table schema.
- Table properties: None.

## Compatibility

| Case | Outcome |
|-|-|
| Old reader, table with this feature | Fails cleanly: the protocol has reader version 3 and lists `geospatial` in `readerFeatures`, which a reader that doesn't support the feature MUST reject. |
| Old writer, table with this feature | Fails cleanly, for the same reason with `writerFeatures`. |
| New reader, table without this feature | Works. The schema has no geospatial types, so nothing changes. |
| New writer, table without this feature | Works. The writer only adds the feature when a commit introduces a geospatial column. |
| Existing table that enables the feature (mix of old and new files) | Works. Files written before the column was added don't contain it, and are read as `null` for that column under the existing rule for columns missing from a data file. Old clients are locked out from the commit that adds the feature. |
| Checkpoint writer that doesn't know the feature | Not applicable. The feature adds no action and no checkpoint field; the type only appears inside `metaData.schemaString`. A writer that doesn't support the feature can't write to the table in the first place. |
| Table whose protocol lists `geospatial-dev` but not `geospatial` | Clients that know only `geospatial` reject the table. Clients that know `geospatial-dev` treat it like `geospatial`. See "Open questions". |
| Writers disagreeing on the spelling of an algorithm (`spherical` vs `SPHERICAL`) | Safe if readers compare algorithm names case-insensitively, as required below. |

## Interactions with other features

| Feature | Interaction |
|-|-|
| Column mapping | None, because the type doesn't change how columns are named. A geospatial column uses its physical name in data files like any other column. |
| Deletion vectors | None, because deletion vectors work on row positions and not on column types. |
| Checkpoints (classic, multi-part, V2, sidecars) | None for the checkpoint schema, because no new action or field is added. See "Statistics and data skipping" for `stats` content. |
| Log compaction | None, because no new action is added. |
| Metadata cleanup and VACUUM | None. |
| Version checksum (`.crc`) | None. |
| Action reconciliation | None, because no new action is added. |
| Row tracking | None. |
| Change data feed | None specific to this feature. A change data file stores geospatial columns with the same representation as a data file. |
| Domain metadata | None. |
| In-commit timestamps | None. |
| Type widening | Not applicable: a `geometry` or `geography` column can't be widened. Changing the CRS or algorithm of an existing column is not a widening, and writers MUST NOT do it (see Writer Requirements). |
| Generated columns, CHECK constraints, invariants, default columns, identity columns | Not specified by this RFC. Behavior of these features on geospatial columns is engine-specific today. See "Open questions". |
| Clustering and partitioning | Geospatial columns can't be partition columns. Clustering columns are listed under "Open questions". |
| Statistics and data skipping | `nullCount` works like for any column. `minValues` and `maxValues` for geospatial columns are not specified by this RFC. See "Open questions". |
| IcebergCompatV1/V2/V3, Iceberg writer compat | Geospatial types are not allowed under IcebergCompatV1 and V2. Whether they are allowed under IcebergCompatV3 is already stated by the [IcebergCompatV3 RFC](iceberg-compat-v3.md), which lists `geometry` and `geography` in its type allow-list. This RFC changes neither. |
| Catalog-managed tables | None. |
| Collations, variant, timestamp without timezone, other types | `geometry` and `geography` values have no ordering or comparison defined by this RFC, so they can't be partition columns. |

## Concurrency

None. The feature adds no action and no field. Two concurrent commits that both add a geospatial column to the schema are handled by the existing metadata-change conflict rules.

## Alternatives considered

- **Store the value as `binary` with the CRS in column metadata.** This needs no new type or feature, but old readers silently return bytes without the CRS and algorithm, and column metadata is not part of the type.
- **Make the type string carry only the CRS.** Geography needs an edge interpolation algorithm to be interpreted, so the type string carries both.
- **No table feature (like `void`).** Not possible: unlike `void`, the types introduce a new logical type with a new physical annotation, and an old reader that ignores it returns incomplete values.

## Open questions

1. **Spelling of the algorithm.** delta-spark writes the algorithm in upper case (`geography(OGC:CRS84, SPHERICAL)`), and Kernel writes it in lower case (`geography(OGC:CRS84, spherical)`). Both accept either case when parsing. Kernel keeps the string as written and compares it case-sensitively, so two tables with the same type can compare as different types in Kernel. The spec text makes algorithm names case-insensitive and recommends lower case for writers. Should it fix one spelling?
2. **Supported algorithms and CRS values.** Kernel accepts `spherical`, `vincenty`, `thomas`, `andoyer` and `karney` and any CRS of the form `<authority>:<code>`. delta-spark on Spark 4.1 accepts only `spherical`, and a limited set of CRS values. The spec text lists the five algorithm names, and says a reader MUST NOT interpret values under a CRS or algorithm it doesn't support. Is that the right level, or should the spec limit the algorithms?
3. **Mixed-SRID types.** Spark can represent a geospatial type with no fixed SRID (the type string `geometry(srid:any)`), and delta-spark rejects it in table schemas. The spec text forbids it. Kernel does not appear to reject it on write.
4. **Parquet representation.** The spec text states that a geospatial column is a Parquet `BINARY` column with the Parquet `GEOMETRY` or `GEOGRAPHY` logical type annotation and WKB values, which is what Kernel writes. The physical schema delta-spark writes must be checked against this before acceptance.
5. **Statistics.** Kernel writes `minValues` and `maxValues` for `geometry` columns as the corner points of the bounding box encoded as WKT `POINT` strings, and uses them for data skipping. Whether delta-spark writes statistics for geospatial columns, and whether `geography` columns get statistics, is not verified. The RFC leaves the encoding out until that is settled.
6. **Clustering.** Kernel rejects geospatial columns as clustering columns. Whether delta-spark does the same is not verified, so the spec text does not forbid it.
7. **`geospatial-dev`.** delta-spark still accepts tables whose protocol lists `geospatial-dev`, which Kernel doesn't recognize. Should `PROTOCOL.md` mention the name as a deprecated alias with the same semantics, or leave it as an implementation detail?
8. **Constraints.** Generated columns, CHECK constraints and column defaults on geospatial columns are engine-specific, and delta-spark restricts column defaults and the expressions it allows. Decide whether the protocol says anything.

--------

> ***Add a new "Geospatial Types" section after the [Variant Data Type](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#variant-data-type) and [Variant Shredding](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#variant-shredding) sections.***

# Geospatial Types

This feature enables support for the `geometry` and `geography` data types, which store spatial values together with the coordinate reference system (CRS) they are expressed in. A `geography` also carries the algorithm used to interpolate edges between two points.

To support this feature:
- The table must be on Reader Version 3 and Writer Version 7
- The feature `geospatial` must exist in the table `protocol`'s `readerFeatures` and `writerFeatures`.

## Enablement

The feature has no table property. A table that supports `geospatial` can contain `geometry` and `geography` columns. A writer MUST add `geospatial` to the table `protocol` in the same commit that first introduces a `geometry` or `geography` type anywhere in the table schema, including inside a `struct`, `array` or `map`.

## Schema

`geometry` and `geography` are primitive types. In `metaData.schemaString` a type is a string in one of the following forms. `<crs>` is a CRS identifier of the form `<authority>:<code>`, for example `OGC:CRS84` or `EPSG:3857`, where `<authority>` consists of letters, digits and `_`, and `<code>` is the same optionally preceded by `-`. `<algorithm>` is one of `spherical`, `vincenty`, `thomas`, `andoyer` or `karney`.

Type string | Meaning
-|-
`geometry` | A geometry in the default CRS, `OGC:CRS84`
`geometry(<crs>)` | A geometry in the CRS `<crs>`
`geography` | A geography in the default CRS, `OGC:CRS84`, with the default algorithm, `spherical`
`geography(<crs>)` | A geography in the CRS `<crs>` with the default algorithm
`geography(<algorithm>)` | A geography in the default CRS with the algorithm `<algorithm>`
`geography(<crs>, <algorithm>)` | A geography in the CRS `<crs>` with the algorithm `<algorithm>`

White space around `<crs>` and `<algorithm>` and around the comma is not significant. Algorithm names are case-insensitive. Type names (`geometry`, `geography`) are lower case. Two type strings denote the same type when their CRS values are equal and, for `geography`, their algorithms are equal ignoring case; a type string that omits the CRS or algorithm denotes the same type as one that spells out the default.

Writers SHOULD write the fully spelled-out form, `geometry(<crs>)` and `geography(<crs>, <algorithm>)`, with the algorithm in lower case.

The CRS and algorithm are part of the type. A `geometry` and a `geography`, or two columns that differ in CRS or algorithm, are different types.

Writers MUST NOT use a geospatial type without a fixed CRS (a "mixed-SRID" type) in a table schema.

## Geospatial data in Parquet

A `geometry` or `geography` column is stored in a data file as a Parquet `BINARY` column that has the Parquet [`GEOMETRY` or `GEOGRAPHY` logical type](https://github.com/apache/parquet-format/blob/master/Geospatial.md), respectively, annotated with the CRS and, for `geography`, the edge interpolation algorithm of the column type. Each value is encoded as Well-Known Binary (WKB), as described in the Parquet geospatial specification.

## Reader Requirements for Geospatial Types

When `geospatial` is supported, readers:
- MUST recognize and tolerate all type string forms in the Schema section, in a `schemaString` at any nesting level.
- MUST compare algorithm names case-insensitively.
- MUST NOT interpret the values of a column under a CRS or algorithm other than the one in its type. A reader that doesn't support the CRS or algorithm of a column MUST fail a query that reads that column, or expose the column as raw `binary` values. It MUST NOT silently reinterpret the values.
- MAY expose the column to the engine in any representation that preserves the WKB value, for example as `binary`, if the engine has no geospatial type.

## Writer Requirements for Geospatial Types

When `geospatial` is supported, writers:
- MUST meet all Reader Requirements for Geospatial Types.
- MUST write values as described in "Geospatial data in Parquet", using the CRS and algorithm of the column type.
- MUST NOT use a `geometry` or `geography` column as a partition column.
- MUST NOT change the CRS or algorithm of an existing column, or change a column between `geometry` and `geography`.

## Removing the feature

A writer MUST NOT remove `geospatial` from the protocol while any `geometry` or `geography` type remains in the table schema, including nested types. After the feature is removed, readers can assume that the table schema contains no geospatial type.

## Examples

A table schema with a geometry column, a geography column with the default algorithm, and a geography column nested in a struct:

```json
{
  "type": "struct",
  "fields": [
    { "name": "id", "type": "long", "nullable": false, "metadata": {} },
    { "name": "location", "type": "geometry(OGC:CRS84)", "nullable": true, "metadata": {} },
    { "name": "region", "type": "geography(OGC:CRS84, spherical)", "nullable": true, "metadata": {} },
    {
      "name": "site",
      "type": {
        "type": "struct",
        "fields": [
          { "name": "center", "type": "geography(EPSG:4326, spherical)", "nullable": true, "metadata": {} }
        ]
      },
      "nullable": true,
      "metadata": {}
    }
  ]
}
```

Each of the following pairs denotes the same type:

- `geometry` and `geometry(OGC:CRS84)`
- `geography` and `geography(OGC:CRS84, spherical)`
- `geography(spherical)` and `geography(OGC:CRS84, SPHERICAL)`

The following are different types:

- `geometry(OGC:CRS84)` and `geography(OGC:CRS84)`
- `geometry(OGC:CRS84)` and `geometry(EPSG:3857)`
- `geography(OGC:CRS84, spherical)` and `geography(OGC:CRS84, vincenty)`

A `protocol` action for a table with geospatial columns:

```json
{
  "protocol": {
    "minReaderVersion": 3,
    "minWriterVersion": 7,
    "readerFeatures": ["geospatial"],
    "writerFeatures": ["geospatial"]
  }
}
```

## Changes to existing sections

- In [Primitive Types](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#primitive-types), add two rows after `void`:

  Type Name | Description
  -|-
  geometry | A spatial value in a planar coordinate system, with a fixed CRS. To use this type, a table must support the feature `geospatial`. See section [Geospatial Types](#geospatial-types) for more information.
  geography | A spatial value on a sphere or ellipsoid, with a fixed CRS and an edge interpolation algorithm. To use this type, a table must support the feature `geospatial`. See section [Geospatial Types](#geospatial-types) for more information.

- In the [Partition Value Serialization](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#partition-value-serialization) section, no serialization is defined for `geometry` and `geography`, because they can't be partition columns.

## Appendix changes

- Add `geospatial` to "Valid Feature Names in Table Features", as a reader and writer feature with reader version 3 and writer version 7.
- Add `Geospatial Types` and its subsections to the table of contents.
- No new table property.
