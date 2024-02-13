# Type Widening
**Associated Github issue for discussions: https://github.com/delta-io/delta/issues/2623**

This protocol change introduces the Type Widening feature, which enables changing the type of a column or field in an existing Delta table to a wider type.

--------

# Type Widening
> ***New Section after the [Clustered Table](#clustered-table) section***

The Type Widening feature enables changing the type of a column or field in an existing Delta table
to a wider type.

The **allowed type changes** are:
- Integer widening: `Byte` -> `Short` -> `Int` -> `Long`
- Floating-point widening: `Float` -> `Double`
- Decimal widening: `Decimal(p, s)` -> `Decimal(p + k1, s + k2)` where `k1 >= k2 >= 0`. `p` and `s` denote the decimal precision and scale respectively.
- Date widening: `Date` -> `Timestamp without timezone`

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
`tableVersion`| required | The version of the table when the type change was applied.
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
          "tableVersion": 1,
          "fromType": "short",
          "toType": "int"
        },
        {
          "tableVersion": 5,
          "fromType": "int",
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
      "valueType": "int",
      "valueContainsNull": true
    },
    "nullable" : true,
    "metadata" : { 
      "delta.typeChanges": [
        {
          "tableVersion": 2,
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
          "tableVersion": 2,
          "fromType": "decimal(6, 2)",
          "toType": "decimal(10, 4)",
          "fieldPath": "element.key"
        }
      ]
    }
  }
```

## Writer Requirements for Type Widening

When Type Widening is enabled (when the table property `delta.enableTypeWidening` is set to `true`), then:
- Writers should allow updating the table schema to apply an **allowed type change** to a column, struct field, map key/value or array element.
- Writers must record type change information in the `metadata` of the nearest ancestor [StructField](#struct-field). See [Type Change Metadata](#type-change-metadata).

When Type Widening is supported (when the `writerFeatures` field of a table's `protocol` action contains `enableTypeWidening`), then:
- Writers must preserve the `delta.typeChanges` field in the metadata fields in the schema when a schema is updated.
- Writers can remove an element from a `delta.typeChanges` field in the metadata fields in the schema when all active `add` actions in the latest version of the table have a `defaultRowCommitVersion` value greater or equal to the `tableVersion` value of that `delta.typeChanges` element.
- Writers must set the `defaultRowCommitVersion` field in new `add` actions to the version number of the log enty containing the `add` action.
- Writers must set the `defaultRowCommitVersion` field in recommitted and checkpointed `add` actions and `remove` actions to the `defaultRowCommitVersion` of the last committed `add` action with the same `path`.

The last two requirements related to `defaultRowCommitVersion` are a subset of the requirements from [Writer Requirements for Row Tracking](writer-requirements-for-row-tracking) that may be implemented separately without introducing a dependency on the [Row Tracking](#row-tracking) table feature.

## Reader Requirements for Type Widening
When Type Widening is supported (when the `readerFeatures` field of a table's `protocol` action contains `enableTypeWidening`), then:
- Readers must allow reading data files written before the table underwent any allowed type change and convert the values to the current, wider type.

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
