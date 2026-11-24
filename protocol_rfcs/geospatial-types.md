# Geospatial Types (geometry and geography)
**Associated Github issue for discussions: https://github.com/delta-io/delta/issues/XXXX**
<!-- TODO: replace XXXX with the number of the Protocol Change Request issue. The issue has not been created yet. -->

## Motivation

Delta Lake has no way to record that a column holds spatial data. Today a writer that wants to store points, lines or polygons uses a `binary` column of well-known binary (WKB) or a `string` column of well-known text (WKT). The table then carries no coordinate reference system (CRS), no distinction between planar and spherical geometry, and no statistics a reader can use to skip files for a spatial predicate.

Parquet, Apache Iceberg and Apache Spark already have `geometry` and `geography` types. Delta clients already use them under a table feature named `geospatial` (and a preview feature named `geospatial-dev`), but `PROTOCOL.md` does not describe them. This RFC specifies the types, so that independent engines can read and write these tables the same way.

- `geometry` stores planar (Cartesian) geometries in a CRS.
- `geography` stores geometries on an ellipsoid, in a CRS, with an algorithm that defines the edges between two vertices.

## Feature summary

- Table feature name: `geospatial`. This RFC does not use a `-dev` suffix because the feature name is already written into tables: `geospatial` by Delta 4.4 and Delta Kernel, and `geospatial-dev` by the private preview. A renamed feature cannot be un-shipped. Existing tables that support `geospatial-dev` are discussed under Open questions.
- Readers and writers, or writers only: Readers and writers. A reader that ignores the type would read the column as an unknown primitive type and could not interpret the WKB values, and it would not know that file statistics for the column are bounding boxes, not ordered values. Using those statistics for skipping as if they were min/max values returns wrong results.
- Minimum protocol versions: reader version 3, writer version 7.
- Required features: None.
- Features it can't be used with: [Clustered Table](#clustered-table) (a geo column cannot be a clustering column), IcebergCompatV1 and IcebergCompatV2 (see the interaction table).
- Supported vs enabled: There is no table property. When `geospatial` is supported, the table's schema may contain geo types. A writer MUST add the feature in the commit that first adds a geo type to the schema.
- Table properties: None.

## Compatibility

| Case | Outcome |
|-|-|
| Old reader, table with this feature | Fails cleanly. The reader finds an unknown feature in `readerFeatures` and rejects the table. |
| Old writer, table with this feature | Fails cleanly. The writer finds an unknown feature in `writerFeatures` and rejects the table. |
| New reader, table without this feature | Works. The schema has no geo type. |
| New writer, table without this feature | Works. A writer that adds a geo column adds the feature in the same commit. |
| Existing table that enables the feature (mix of old and new files) | Works. Old files have no geo column, and a column missing from a data file is read as `null`. |
| Checkpoint writer that doesn't know the feature | Fails cleanly, because a checkpoint writer is a writer and rejects the table. See Open questions for the checkpoint statistics schema. |

## Interactions with other features

| Feature | Interaction |
|-|-|
| Column mapping | None, because geo types are leaf types. A geo column has a physical name like any other column, and statistics are keyed by physical name. |
| Deletion vectors | None. A deletion vector marks rows of a data file as deleted and does not change the file's statistics, which describe the file before deleted rows are removed, so so the bounding box stays a valid superset of the remaining rows. |
| Checkpoints (classic, multi-part, V2, sidecars) | Statistics are stored as strings (see Statistics). See Open questions for the `stats_parsed` schema. |
| Log compaction | None, because the feature adds no action, so compaction reconciles the same actions as before. |
| Metadata cleanup and VACUUM | None, because the feature adds no file kind and no rule about which files must be kept. |
| Version checksum (`.crc`) | None, because the checksum holds no per-column data, and the feature adds no checksum field. |
| Action reconciliation | None, because no new action is added. |
| Row tracking | None, because row IDs and row commit versions are assigned per row and do not depend on a column's type. |
| Change data feed | None. A change data file stores geo values the same way as a data file. |
| Domain metadata | None, because the feature adds no domain. |
| In-commit timestamps | None, because the feature adds no commit-level information. |
| Type widening | A geo type is neither the source nor the target of a type change. |
| Generated columns, CHECK constraints, invariants, default columns, identity columns | Not affected by this RFC. Expressions over geo columns depend on the engine's functions. |
| Clustering and partitioning | A geo column MUST NOT be a clustering column or a partition column, because a geo value has no total order. |
| Statistics and data skipping | Bounding-box statistics (see Statistics). |
| IcebergCompatV1/V2/V3, Iceberg writer compat | IcebergCompatV3 supports geo types. IcebergCompatV1 and V2 do not. |
| Catalog-managed tables | None, because the feature changes neither how commits are made nor how they are published. |
| Collations, variant, timestamp without timezone, other types | None, because a geo value is a leaf type that is stored and compared on its own, and is not ordered (see Clustering and partitioning). |

## Concurrency

None, because the feature adds no action or field. Two concurrent transactions that both add the feature to the protocol conflict through the existing `protocol` action rules.

## Alternatives considered

- **Store WKB in `binary` columns and the CRS in column metadata.** This needs no new type, but each engine would have to know the metadata convention, and statistics could not be defined for it.
- **A single `geospatial` type with a `kind` property.** Rejected because planar and spherical geometry are different types in Parquet, Iceberg and Spark.

## Open questions

These are for the author and reviewers to decide. This draft proposes an answer where it can, but does not treat them as settled. What existing clients do today is in the PR description, not in this RFC.

1. **Mixed CRS.** Can a column hold values with different CRSs (a "mixed" or "any" CRS)? The grammar below does not define it. Should the protocol define it, or restrict a column to one CRS?
2. **Case of the algorithm name.** This draft makes readers compare the algorithm without regard to case and recommends lower case for writers. Is that acceptable, or should the protocol fix one case?
3. **Physical type in Parquet.** This draft requires WKB but not a Parquet logical type annotation. Should the protocol require the Parquet `GEOMETRY` / `GEOGRAPHY` logical types?
4. **Existing `geospatial-dev` tables.** Should `geospatial-dev` stay a permanent alias that readers and writers treat as `geospatial`?
5. **Checkpoint statistics.** What is the type of a geo column's `minValues` / `maxValues` in the `stats_parsed` checkpoint column? This draft assumes string, the same as in JSON statistics.
6. **Geography bounding boxes across the antimeridian.** The bounding box of a geography value that crosses longitude 180 can have a minimum x greater than its maximum x. This draft does not define that case, and only forbids skipping a file when `xmin > xmax`.
7. **Statistics for geography.** The statistics rules say what a writer MAY write. Should the protocol require bounding boxes for geography columns, or only for geometry?
8. **Partition columns.** This draft forbids geo partition columns. Please confirm that no existing table partitions by a geo column.
9. **Z and M coordinates.** Statistics use only x and y. Should the protocol say so, and how should Z and M be treated?
10. **IcebergCompatV1 and V2.** This draft forbids geo types under them. Please confirm against the Iceberg compatibility rules.

--------

> ***New section after the [Variant Shredding](#variant-shredding) section***

# Geospatial Types

This feature enables the `geometry` and `geography` data types, which store spatial values.

- A `geometry` value is a planar geometry in a coordinate reference system (CRS).
- A `geography` value is a geometry on an ellipsoid, in a CRS, whose edges between two vertices follow an edge interpolation algorithm.

## Enablement

- The table must be on Reader Version 3 and Writer Version 7.
- The feature `geospatial` must exist in the table `protocol`'s `readerFeatures` and `writerFeatures`.

There is no table property. When the feature is supported, the table schema may contain `geometry` and `geography` types.

## Schema

A geo type is written in the [schema](#schema-serialization-format) as a string, at any nesting level, in the same way as a primitive type.

Type string | Meaning
-|-
`geometry` | `geometry` in the default CRS `OGC:CRS84`
`geometry(<crs>)` | `geometry` in the CRS `<crs>`
`geography` | `geography` in the default CRS `OGC:CRS84`, with the default algorithm `spherical`
`geography(<crs>)` | `geography` in the CRS `<crs>`, with the default algorithm `spherical`
`geography(<algorithm>)` | `geography` in the default CRS `OGC:CRS84`, with the algorithm `<algorithm>`
`geography(<crs>, <algorithm>)` | `geography` in the CRS `<crs>`, with the algorithm `<algorithm>`

- `<crs>` is `<authority>:<code>`. `<authority>` is one or more letters, digits or underscores. `<code>` is one or more letters, digits or underscores, and may be preceded by `-`. Examples: `OGC:CRS84`, `EPSG:4326`.
- `<algorithm>` is one of `spherical`, `vincenty`, `thomas`, `andoyer` or `karney`. It is compared without regard to case.
- Spaces are allowed after `(`, before `)`, and around `,`.

A writer SHOULD write `geometry(<crs>)` and `geography(<crs>, <algorithm>)`, with the algorithm in lower case.

## Geo data in data files

A geo value is stored in a data file as a well-known binary (WKB) value, as specified by the [OGC Simple Features specification](https://portal.ogc.org/files/?artifact_id=25355). A null value is stored as a null.

## Statistics for Geospatial Columns

- The `nullCount` stat for a geo column is a LONG, as for any other column.
- The `minValues` stat for a geo column is the lower-left corner of the bounding box of all non-null values in the file, and the `maxValues` stat is the upper-right corner. Each is a string holding a [well-known text](https://portal.ogc.org/files/?artifact_id=25355) `POINT`, for example `POINT(1.0 2.0)`. The coordinates are x (longitude for a geography) and y (latitude for a geography).
- A reader MUST accept `POINT Z`, `POINT M`, `POINT ZM` and the `EMPTY` forms of each, and MUST use only x and y.
- The bounding-box stats are not min and max values in an ordering. A reader MUST NOT apply an ordering comparison to them. A reader MAY skip a file for a spatial predicate only if the predicate cannot be true for any value inside the bounding box.
- A reader MUST NOT skip a file if the x coordinate of its `minValues` is greater than the x coordinate of its `maxValues`.
- A writer MAY omit the stats of a geo column. A reader MUST NOT skip a file for a spatial predicate if the stats are missing.

## Reader Requirements for Geospatial Types

When `geospatial` is supported (`readerFeatures` contains `geospatial`), readers:
- MUST parse the type strings in the Schema section, and MUST fail if a type string does not match one of them, or if an algorithm is not one of the five listed.
- MUST read a geo column from a data file as WKB.
- MUST follow the statistics rules above when using geo statistics for data skipping.

## Writer Requirements for Geospatial Types

When `geospatial` is supported (`writerFeatures` contains `geospatial`), writers:
- MUST meet the reader requirements.
- MUST add `geospatial` to the `protocol` in the commit that first adds a geo type to the table schema.
- MUST write geo values as WKB.
- MUST NOT use a geo column as a clustering column or a partition column.
- MUST write the stats of a geo column, if they write them, as specified above. This applies to every operation that writes a data file or a checkpoint, including `OPTIMIZE`, `CLONE` and `RESTORE`.
- MUST NOT enable IcebergCompatV1 or IcebergCompatV2 on a table whose schema contains a geo type.

## Removing the feature

A writer MAY remove `geospatial` from the `protocol` only if the table schema contains no geo type. After the feature is removed, readers can assume that the table schema has no geo type.

## Examples

A table schema with a geometry column, a geography column and a nested geography column:

```json
{
  "type": "struct",
  "fields": [
    {"name": "location", "type": "geometry(OGC:CRS84)", "nullable": true, "metadata": {}},
    {"name": "region", "type": "geography(OGC:CRS84, spherical)", "nullable": true, "metadata": {}},
    {
      "name": "route",
      "type": {
        "type": "array",
        "elementType": "geography(EPSG:4326, karney)",
        "containsNull": true
      },
      "nullable": true,
      "metadata": {}
    }
  ]
}
```

The matching `protocol` action:

```json
{"protocol": {"minReaderVersion": 3, "minWriterVersion": 7, "readerFeatures": ["geospatial"], "writerFeatures": ["geospatial"]}}
```

The `stats` of a data file with two non-null `location` values, `POINT(1 2)` and `POINT(5 -3)`, and one null value:

```json
{"numRecords": 3, "minValues": {"location": "POINT(1.0 -3.0)"}, "maxValues": {"location": "POINT(5.0 2.0)"}, "nullCount": {"location": 1}}
```

## Changes to existing sections

**[Primitive Types](#primitive-types).** After the last row of the table, which currently reads `void| A column that contains only ...`, add:

Type Name | Description
-|-
geometry | A planar geometry in a CRS. To use this type, a table must support the feature `geospatial`. See section [Geospatial Types](#geospatial-types) for the type string and more information.
geography | A geometry on an ellipsoid in a CRS, with an edge interpolation algorithm. To use this type, a table must support the feature `geospatial`. See section [Geospatial Types](#geospatial-types) for the type string and more information.

**[Clustered Table](#clustered-table).** After "Clustering columns can be specified during the initial creation of a table, ...", add: "A column of type `geometry` or `geography` MUST NOT be a clustering column."

**[Per-file Statistics](#per-file-statistics).** Add a link to the new section "Statistics for Geospatial Columns", next to "Statistics for Variant Columns".

**IcebergCompatV1 and IcebergCompatV2.** Add `geometry` and `geography` to the list of types that a table with the feature MUST NOT contain. IcebergCompatV3 allows them.

## Appendix changes

- "Valid Feature Names in Table Features": add the row

  Feature | Name | Readers or Writers?
  -|-|-
  [Geospatial Types](#geospatial-types) | `geospatial` | Readers and writers

- Table of contents: add "Geospatial Types" with its subsections, after "Variant Shredding".
- "Table Properties": no change.
