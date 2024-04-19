# Variant Data Type
**Associated Github issue for discussions: https://github.com/delta-io/delta/issues/2864**

This protocol change adds support for the Variant data type.
The Variant data type is beneficial for storing and processing semi-structured data.

--------

> ***New Section after the [Clustered Table](#clustered-table) section***

# Variant Data Type

This feature enables support for the Variant data type, for storing semi-structured data.
The schema serialization method is described in [Schema Serialization Format](#schema-serialization-format).

To support this feature:
- The table must be on Reader Version 3 and Writer Version 7
- The feature `variantType` must exist in the table `protocol`'s `readerFeatures` and `writerFeatures`.

## Example JSON-Encoded Delta Table Schema with Variant types

```
{
  "type" : "struct",
  "fields" : [ {
    "name" : "raw_data",
    "type" : "variant",
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "variant_array",
    "type" : {
      "type" : "array",
      "elementType" : {
        "type" : "variant"
      },
      "containsNull" : false
    },
    "nullable" : false,
    "metadata" : { }
  } ]
}
```

## Variant data in Parquet

The Variant data type is represented as two binary encoded values, according to the [Spark Variant binary encoding specification](https://github.com/apache/spark/blob/master/common/variant/README.md).
The two binary values are named `value` and `metadata`.

When writing Variant data to parquet files, the Variant data is written as a single Parquet struct, with the following fields:

Struct field name | Parquet primitive type | Description
-|-|-
value | binary | The binary-encoded Variant value, as described in [Variant binary encoding](https://github.com/apache/spark/blob/master/common/variant/README.md)
metadata | binary | The binary-encoded Variant metadata, as described in [Variant binary encoding](https://github.com/apache/spark/blob/master/common/variant/README.md)

The parquet struct must include the two struct fields `value` and `metadata`.
Supported writers must write the two binary fields, and supported readers must read the two binary fields.
Struct fields which start with `_` (underscore) can be safely ignored.

## Writer Requirements for Variant Data Type

When Variant type is supported (`writerFeatures` field of a table's `protocol` action contains `variantType`), writers:
- must write a column of type `variant` to parquet as a struct containing the fields `value` and `metadata` and storing values that conform to the [Variant binary encoding specification](https://github.com/apache/spark/blob/master/common/variant/README.md)
- must not write additional, non-ignorable parquet struct fields. Writing additional struct fields with names starting with `_` (underscore) is allowed.

## Reader Requirements for Variant Data Type

When Variant type is supported (`readerFeatures` field of a table's `protocol` action contains `variantType`), readers:
- must recognize and tolerate a `variant` data type in a Delta schema
- must use the correct physical schema (struct-of-binary, with fields `value` and `metadata`) when reading a Variant data type from file
- must make the column available to the engine:
    - [Recommended] Expose and interpret the struct-of-binary as a single Variant field in accordance with the [Spark Variant binary encoding specification](https://github.com/apache/spark/blob/master/common/variant/README.md).
    - [Alternate] Expose the raw physical struct-of-binary, e.g. if the engine does not support Variant.

## Compatibility with other Delta Features

Feature | Support for Variant Data Type
-|-
Partition Columns | **Supported:** A Variant column is allowed to be a non-partitioned column of a partitioned table. <br/> **Unsupported:** Variant is not a comparable data type, so it cannot be included in a partition column.
Clustered Tables | **Supported:** A Variant column is allowed to be a non-clustering column of a clustered table. <br/> **Unsupported:** Variant is not a comparable data type, so it cannot be included in a clustering column.
Delta Column Statistics | **Supported:** A Variant column supports the `nullCount` statistic. <br/> **Unsupported:** Variant is not a comparable data type, so a Variant column does not support the `minValues` and `maxValues` statistics.
Generated Columns | **Supported:** A Variant column is allowed to be used as a source in a generated column expression, as long as the Variant type is not the result type of the generated column expression. <br/> **Unsupported:** The Variant data type is not allowed to be the result type of a generated column expression.
Delta CHECK Constraints | **Supported:** A Variant column is allowed to be used for a CHECK constraint expression.
Default Column Values | **Supported:** A Variant column is allowed to have a default column value.
Change Data Feed | **Supported:** A table using the Variant data type is allowed to enable the Delta Change Data Feed.

--------

> ***New Sub-Section after the [Map Type](#map-type) sub-section within the [Schema Serialization Format](#schema-serialization-format) section***

### Variant Type

Variant data uses the Delta type name `variant` for Delta schema serialization.

Field Name | Description
-|-
type | Always the string "variant"
