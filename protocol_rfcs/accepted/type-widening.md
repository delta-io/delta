# Type Widening
**Associated Github issue for discussions: https://github.com/delta-io/delta/issues/2623**

This protocol change introduces the Type Widening feature, which enables changing the type of a column or field in an existing Delta table to a wider type.

--------

# Type Widening
> ***New Section after the [Clustered Table](#clustered-table) section***

The Type Widening feature enables changing the type of a column or field in an existing Delta table to a wider type.

The supported type changes are:
- Integer widening:
  - `Byte` -> `Short` -> `Int` -> `Long`
- Floating-point widening:
  - `Float` -> `Double`
  - `Byte`, `Short` or `Int` -> `Double`
- Date widening:
  - `Date` -> `Timestamp without timezone`
- Decimal widening - `p` and `s` denote the decimal precision and scale respectively.
  - `Decimal(p, s)` -> `Decimal(p + k1, s + k2)` where `k1 >= k2 >= 0`.
  - `Byte`, `Short` or `Int` -> `Decimal(10 + k1, k2)` where `k1 >= k2 >= 0`.
  - `Long` -> `Decimal(20 + k1, k2)` where `k1 >= k2 >= 0`.

To support this feature:
- The table must be on Reader version 3 and Writer Version 7.
- The feature `typeWidening` must exist in the table `protocol`'s `readerFeatures` and `writerFeatures`, either during its creation or at a later stage.

When supported:
 - A table may have a metadata property `delta.enableTypeWidening` in the Delta schema set to `true`. Writers must reject widening type changes when this property isn't set to `true`.
 - The `metadata` for a column or field in the table schema may contain the key `delta.typeChanges` storing a history of type changes for that column or field.

### Type Change Metadata

Type changes applied to a table are recorded in the table schema and stored in the `metadata` of their nearest ancestor [StructField](#struct-field) using the key `delta.typeChanges`.
The value for the key `delta.typeChanges` must be a JSON list of objects, where each object contains the following fields:
Field Name | optional/required | Description
-|-|-
`fromType`| required | The type of the column or field before the type change.
`toType`| required | The type of the column or field after the type change.
`fieldPath`| optional | When updating the type of a map key/value or array element only: the path from the struct field holding the metadata to the map key/value or array element that was updated.

The `fieldPath` value is "key", "value" and "element"  when updating resp. the type of a map key, map value and array element.
The `fieldPath` value for nested maps and nested arrays are prefixed by their parents's path, separated by dots.

The following is an example for the definition of a column that went through two type changes:
```json
{
    "name" : "e",
    "type" : "long",
    "nullable" : true,
    "metadata" : { 
      "delta.typeChanges": [
        {
          "fromType": "short",
          "toType": "integer"
        },
        {
          "fromType": "integer",
          "toType": "long"
        }
      ]
    }
  }
```

The following is an example for the definition of a column after changing the type of a map key:
```json
{
    "name" : "e",
    "type" : {
      "type": "map",
      "keyType": "double",
      "valueType": "integer",
      "valueContainsNull": true
    },
    "nullable" : true,
    "metadata" : { 
      "delta.typeChanges": [
        {
          "fromType": "float",
          "toType": "double",
          "fieldPath": "key"
        }
      ]
    }
  }
```

The following is an example for the definition of a column after changing the type of a map value nested in an array:
```json
{
    "name" : "e",
    "type" : {
      "type": "array",
      "elementType": {
        "type": "map",
        "keyType": "string",
        "valueType": "decimal(10, 4)",
        "valueContainsNull": true
      },
      "containsNull": true
    },
    "nullable" : true,
    "metadata" : { 
      "delta.typeChanges": [
        {
          "fromType": "decimal(6, 2)",
          "toType": "decimal(10, 4)",
          "fieldPath": "element.value"
        }
      ]
    }
  }
```

## Writer Requirements for Type Widening

When Type Widening is supported (when the `writerFeatures` field of a table's `protocol` action contains `typeWidening`), then:
- Writers must reject applying any unsupported type change.
- Writers must reject applying type changes not supported by [Iceberg V2](https://iceberg.apache.org/spec/#schema-evolution)
  when either the [Iceberg Compatibility V1](#iceberg-compatibility-v1) or [Iceberg Compatibility V2](#iceberg-compatibility-v2) table feature is supported:
  - `Byte`, `Short` or `Int` -> `Double`
  - `Date`  -> `Timestamp without timezone`
  - Decimal scale increase
  - `Byte`, `Short`, `Int` or `Long` -> `Decimal`
- Writers must record type change information in the `metadata` of the nearest ancestor [StructField](#struct-field). See [Type Change Metadata](#type-change-metadata).
- Writers must preserve the `delta.typeChanges` field in the metadata fields in the schema when the table schema is updated.
- Writers may remove the `delta.typeChanges` metadata in the table schema if all data files use the same field types as the table schema.

When Type Widening is enabled (when the table property `delta.enableTypeWidening` is set to `true`), then:
- Writers should allow updating the table schema to apply a supported type change to a column, struct field, map key/value or array element.

When removing the Type Widening table feature from the table, in the version that removes `typeWidening` from the `writerFeatures` and `readerFeatures` fields of the table's `protocol` action:
- Writers must ensure no `delta.typeChanges` metadata key is present in the table schema. This may require rewriting existing data files to ensure that all data files use the same field types as the table schema in order to fulfill the requirement to remove type widening metadata.
- Writers must ensure that the table property `delta.enableTypeWidening` is not set.

## Reader Requirements for Type Widening
When Type Widening is supported (when the `readerFeatures` field of a table's `protocol` action contains `typeWidening`), then:
- Readers must allow reading data files written before the table underwent any supported type change, and must convert such values to the current, wider type.
- Readers must validate that they support all type changes in the `delta.typeChanges` field in the table schema for the table version they are reading and fail when finding any unsupported type change.

## Writer Requirements for IcebergCompatV1
> ***Change to existing section (underlined)***

When supported and active, writers must:
- Require that Column Mapping be enabled and set to either `name` or `id` mode
- Require that Deletion Vectors are not supported (and, consequently, not active, either). i.e., the `deletionVectors` table feature is not present in the table `protocol`.
- Require that partition column values are materialized into any Parquet data file that is present in the table, placed *after* the data columns in the parquet schema
- Require that all `AddFile`s committed to the table have the `numRecords` statistic populated in their `stats` field
- <ins>When the [Type Widening](#type-widening) table feature is supported, require that no type changes not supported by [Iceberg V2](https://iceberg.apache.org/spec/#schema-evolution) were applied on the table, based on the [Type Change Metadata](#type-change-metadata) recorded in the table schema.<ins>

## Writer Requirements for IcebergCompatV2
> ***Change to existing section (underlined)***

When this feature is supported and enabled, writers must:
- Require that Column Mapping be enabled and set to either `name` or `id` mode
- Require that the nested `element` field of ArrayTypes and the nested `key` and `value` fields of MapTypes be assigned 32 bit integer identifiers. These identifiers must be unique and different from those used in [Column Mapping](#column-mapping), and must be stored in the metadata of their nearest ancestor [StructField](#struct-field) of the Delta table schema. Identifiers belonging to the same `StructField` must be organized as a `Map[String, Long]` and stored in metadata with key `parquet.field.nested.ids`. The keys of the map are "element", "key", or "value", prefixed by the name of the nearest ancestor StructField, separated by dots. The values are the identifiers. The keys for fields in nested arrays or nested maps are prefixed by their parents' key, separated by dots. An [example](#example-of-storing-identifiers-for-nested-fields-in-arraytype-and-maptype) is provided below to demonstrate how the identifiers are stored. These identifiers must be also written to the `field_id` field of the `SchemaElement` struct in the [Parquet Thrift specification](https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift) when writing parquet files.
- Require that IcebergCompatV1 is not active, which means either the `icebergCompatV1` table feature is not present in the table protocol or the table property `delta.enableIcebergCompatV1` is not set to `true`
- Require that Deletion Vectors are not active, which means either the `deletionVectors` table feature is not present in the table protocol or the table property `delta.enableDeletionVectors` is not set to `true`
- Require that partition column values be materialized when writing Parquet data files
- Require that all new `AddFile`s committed to the table have the `numRecords` statistic populated in their `stats` field
- Require writing timestamp columns as int64
- Require that the table schema contains only data types in the following allow-list: [`byte`, `short`, `integer`, `long`, `float`, `double`, `decimal`, `string`, `binary`, `boolean`, `timestamp`, `timestampNTZ`, `date`, `array`, `map`, `struct`].
- <ins>When the [Type Widening](#type-widening) table feature is supported, require that no type changes not supported by [Iceberg V2](https://iceberg.apache.org/spec/#schema-evolution) were applied on the table, based on the [Type Change Metadata](#type-change-metadata) recorded in the table schema.<ins>

### Column Metadata
> ***Change to existing section (underlined)***

A column metadata stores various information about the column.
For example, this MAY contain some keys like [`delta.columnMapping`](#column-mapping) or [`delta.generationExpression`](#generated-columns) or [`CURRENT_DEFAULT`](#default-columns).  
Field Name | Description
-|-
delta.columnMapping.*| These keys are used to store information about the mapping between the logical column name to  the physical name. See [Column Mapping](#column-mapping) for details.
delta.identity.*| These keys are for defining identity columns. See [Identity Columns](#identity-columns) for details.
delta.invariants| JSON string contains SQL expression information. See [Column Invariants](#column-invariants) for details.
delta.generationExpression| SQL expression string. See [Generated Columns](#generated-columns) for details.
<ins>delta.typeChanges</ins>| <ins>JSON string containing information about previous type changes applied to this column. See [Type Change Metadata](#type-change-metadata) for details.</ins>
