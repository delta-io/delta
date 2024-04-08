# Variant Data Type
**Associated Github issue for discussions: https://github.com/delta-io/delta/issues/2864**

This protocol change adds support for the Variant data type.
The Variant data type is beneficial for storing and processing semi-structure data.

--------

> ***New Section after the [Clustered Table](#clustered-table) section***

# Variant Data Type

This feature introduces the Variant data type, for processing with semi-structured data.
The schema serialization method is described in [Schema Serialization Format](#schema-serialization-format).

To support this feature:
- The table must be on Reader Version 3 and Writer Version 7
- The feature `variantType` must exist in the table `protocol`'s `readerFeatures` and `writerFeatures`, either during its creation or at a later stage.

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

The Variant data type is represented as 2 binary encoded values, according to the [Variant binary encoding specification](https://github.com/apache/spark/blob/master/common/variant/README.md).
The 2 binary values are named `value` and `metadata`.

When writing Variant data to parquet files, the Variant data is written as a single Parquet struct, with the following fields:

Struct field name | Parquet primitive type | Description
-|-|-
value | binary | The binary-encoded Variant value, as described in [Variant binary encoding](https://github.com/apache/spark/blob/master/common/variant/README.md)
metadata | binary | The binary-encoded Variant  metadata, as described in [Variant binary encoding](https://github.com/apache/spark/blob/master/common/variant/README.md)

The parquet struct must include the 2 struct fields `value` and `metadata`.
Supported writers must write the 2 binary fields, and supported readers must read the 2 binary fields.
Struct fields which start with `_` (underscore) can be safely ignored.
The only non-ignorable fields must be `value` and `metadata`.

## Writer Requirements for Variant Data Type

When Variant type is supported (`writerFeatures` field of a table's `protocol` action contains `variantType`), writers:
- must write the 2 parquet struct fields, `value` and `metadata`, according to the [Variant binary encoding specification](https://github.com/apache/spark/blob/master/common/variant/README.md)
- must not write additional parquet struct fields

## Reader Requirements for Variant Data Type

When Variant type is supported (`readerFeatures` field of a table's `protocol` action contains `variantType`), readers:
- must be able to read the 2 parquet struct fields, `value` and `metadata`
- can ignore any parquet struct field names starting with `_` (underscore)

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
Convert-to-Delta Command | **Supported:** A Variant Parquet table following the Spark Variant specification, can be converted to Delta with the CONVERT TO DELTA command.
Create Table Clone Command | **Supported:** A Variant table following the Spark Variant specification, can be deep or shallow cloned.

--------

> ***New Sub-Section after the [Map Type](#map-type) sub-section within the [Schema Serialization Format](#schema-serialization-format) section***

### Variant Type

Variant data uses the Delta type name `variant` for Delta schema serialization.

Field Name | Description
-|-
type | Always the string "variant"
