# Geospatial types
**Associated Github issue for discussions: https://github.com/delta-io/delta/issues/XXXX**

This protocol change adds support for geospatial types. It consists of two changes to the protocol:
- New reader/writer table feature.
- Two new primitive types (geometry and geography).

--------

> ***Add a new section in front of the [Primitive Types](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#primitive-types) section.***

## Geospatial table feature

This table feature aims to add support for new data types for handling location and shape data, specifically:
1. **GEOMETRY**: For working with shapes on a Cartesian space (a flat surface).
2. **GEOGRAPHY**: For working with shapes on an ellipsoidal surface (like the Earth's surface).

To support this feature:
- The table must be on **Reader version 3** and **Writer version 7**.
- The feature `geospatial` must be listed in the table `protocol`'s `readerFeatures` and `writerFeatures`, either at table creation or added later via protocol upgrade.

When supported:
- **Readers** must use the correct geospatial [type metadata](#type-definitions) when evaluating data skipping predicates based on column statistics.
- **Writers**, if writing statistics, must use the correct [type metadata](#type-definitions) when computing the covering bounding box.
- **Writers** must write data files using the [geospatial Parquet logical types](#geospatial-data-in-parquet).

### Type definitions

In the schema the geospatial types are serialized as:
- `geometry(<crs>)`
- `geography(<crs>, <algorithm>)`

Both types are parameterized by a **Coordinate Reference System (CRS)**. Additionally, the geography type includes an **edge interpolation algorithm**, that accounts for the model of the Earth (spherical, ellipsoidal) that is used to interpolate between vertices of geospatial objects.

#### Coordinate Reference System
Coordinate reference system defines the reference frame for interpreting geographic data. It ensures that coordinates are understood in the correct context.
For example, a CRS for North America may have a different origin (0, 0) than a global CRS, affecting how the coordinates are interpreted.

CRS value can be specified in one of the following formats:
- A standard authority and identifier (`<authority>:<identifier>`), e.g.:
    - `OGC:CRS84`
    - `EPSG:3857`
- A custom definition, which can be provided in one of two ways:
    - Using a Spatial Reference System Identifier (SRID), e.g. `srid:<number>`.
    - Using a projjson reference to a table property where the projjson string is stored, e.g. `projjson:<tableProperty>`.

#### Edge interpolation algorithm

Specifies the algorithm used to interpolate edges between consecutive points. The chosen algorithm determines how intermediate points along an edge are interpreted during spatial operations. It can have one of the following values:

* `spherical`: edges are interpolated as geodesics on a sphere.
* `vincenty`: [https://en.wikipedia.org/wiki/Vincenty%27s_formulae](https://en.wikipedia.org/wiki/Vincenty%27s_formulae)
* `thomas`: Thomas, Paul D. Spheroidal geodesics, reference systems, & local geometry. US Naval Oceanographic Office, 1970.
* `andoyer`: Thomas, Paul D. Mathematical models for navigation systems. US Naval Oceanographic Office, 1965.
* `karney`: [Karney, Charles FF. "Algorithms for geodesics." Journal of Geodesy 87 (2013): 43-55](https://link.springer.com/content/pdf/10.1007/s00190-012-0578-z.pdf), and [GeographicLib](https://geographiclib.sourceforge.io/)

### Per-file statistics

For geospatial types, the most useful statistics for data skipping are **bounding boxes**. A bounding box is a minimal axis-aligned rectangle, parallelepiped, or hyper-parallelepiped, that fully contains all points of all geometries or geographies in a given set. It is typically represented by two points: the **lower-left** and **upper-right** corners. Each point must include **X** and **Y** coordinates, but may also optionally include **Z** (elevation) and **M** (measure) values.

Since a bounding box naturally defines spatial **minimum** and **maximum** values, we can leverage the existing min and max column statistics fields to store the points. The values are encoded in **[Well-Known Text (WKT)](https://libgeos.org/specifications/wkt/)** format. For example:

```json
{
  "numRecords": 23,
  "minValues": {
    "geometryCol": "POINT(-122.419 37.774)"
  },
  "maxValues": {
    "geometryCol": "POINT(-120.503 38.021)"
  },
  "nullCount": {
    "geometryCol": 0
  }
}
```


> ***Add new rows in the [Primitive Types](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#primitive-types) table.***

### Primitive Types
| Type Name | Description                                                                                                                                                              |
| --------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| geometry  | [WKB](https://libgeos.org/specifications/wkb/#standard-wkb) encoded byte array with a given CRS (Coordinate Reference System) value.                                     |
| geography | [WKB](https://libgeos.org/specifications/wkb/#standard-wkb) encoded byte array with a given CRS (Coordinate Reference System) value and an edge interpolation algorithm. |

### Geospatial data in Parquet

The Parquet format now includes [native support for geospatial data](https://github.com/apache/parquet-format/commit/94b9d631aef332c78b8f1482fb032743a9c3c407). This update introduces standardized **geospatial logical types**, which are encoded using the BYTE_ARRAY physical type. In addition, a new `GeospatialStatistics` field has been added to support storing **bounding boxes** in the row group statistics.
