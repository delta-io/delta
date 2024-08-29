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
- Date widening:
  - `Date` -> `Timestamp without timezone`
- Decimal precision increase:
  - `Decimal(p1, s)` -> `Decimal(p2, s)` where `p2 >= p1`.

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
          "fieldPath": "element.key"
        }
      ]
    }
  }
```

## Writer Requirements for Type Widening

When Type Widening is supported (when the `writerFeatures` field of a table's `protocol` action contains `enableTypeWidening`), then:
- Writers must reject applying any unsupported type change.
- Writers must record type change information in the `metadata` of the nearest ancestor [StructField](#struct-field). See [Type Change Metadata](#type-change-metadata).
- Writers must preserve the `delta.typeChanges` field in the metadata fields in the schema when the table schema is updated.
- Writers may remove the `delta.typeChanges` metadata in the table schema if all data files use the same column and field types as the table schema. 

When Type Widening is enabled (when the table property `delta.enableTypeWidening` is set to `true`), then:
- Writers should allow updating the table schema to apply a supported type change to a column, struct field, map key/value or array element.

## Reader Requirements for Type Widening
When Type Widening is supported (when the `readerFeatures` field of a table's `protocol` action contains `enableTypeWidening`), then:
- Readers must allow reading data files written before the table underwent any supported type change, and must convert such values to the current, wider type.
- Readers must validate that they support all type changes in the `delta.typeChanges` field in the table schema for the table version they are reading and fail when finding any unsupported type change.

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
