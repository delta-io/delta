# Collated String Type
**Associated Github issue for discussions: https://github.com/delta-io/delta/issues/2894**

This protocol change adds support for collated strings.

--------

> ***Add a new section in front of the [Primitive Types](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#primitive-types) section.***

### Collations

Collations are a set of rules for how strings are compared. They do not affect how strings are stored. Collations are applied when comparing strings for equality or to determine the sort order of two strings. Case insensitive comparison is one example of a collation where case is ignored when string are compared for equality and the lower cased variant of a string is used to determine its sort order.

Collations can be specified for all string fields in a table schema. It is also possible to store statistics per collation version. This is required because the min and max values of a column can differ based on the used collation or collation version.

By default, all strings are collated using binary collation. That means that strings compare equal if their binary representations are equal. The binary representation is also used to sort them.

#### Collation identifiers

Collations can be referred to using collation identifiers. The Delta format does not specify any collation rules other than binary collation, but supports the concept of collation providers such that engines can use providers like [ICU](https://icu.unicode.org/) and mark statistics accordingly.

A collation identifier consists of 3 parts, which are combined into one identifier using dots as separators. Dots are not allowed to be part of provider and collation names, but can be used in versions.

Part | Description
-|-
Provider | Name of the provider. Must not contain dots
Name | Name of the collation as provided by the provider. Must not contain dots
Version | Version string. Is allowed to contain dots

#### Specifying collations in the table schema

Collations can be specified for all string types in a schema. This includes string fields, but also the key and value type of maps and the element type of arrays. Collations are specified in the metadata of the closest enclosing field.
Collation identifiers are stored in a `collations` object. These object can have 4 keys:

Key | Value | Description
-|-|-
collation | collation identifier | Collation of a string field. Only valid when none of the other keys are present.
elementCollation | `collations object` | Collations of elements in an array. Only valid when none of the other keys are present.
keyCollation | `collations object` | Collations for the key type of a map. Only valid on it's own or with `valueCollation`.
valueCollation | `collations object` | Collations for the value type of a map. Only valid on it's own or with `keyCollation`.

This example provides an overview of how collations are stored. Note that irrelevant fields have been stripped.

```
{
  "type" : "struct",
  "fields" : [ {
    "name" : "col1",
    "type" : "map",
    "keyType": "string"
    "valueType": {
      "type": "array"
      "elementType": {
        "type": "map"
        "keyType: {
          "type": "array"
          "elementType": "string"
        },
        "valueType": {
          "type": "map",
          "keyType": "string",
          "valueType": {
            "type": "struct",
            "fields": [ {
              "name": "f1"
              "type": "string"      
            } ],
          },
          metadata: {
            "collations": { "collation": "ICU.de_DE.73" }
          }
        }
      }
    }
    "metadata": {
      "collations": {
        "keyCollation": { "collation": "ICU.en_US.72" },
        "value": {
          "key": { "element": { "collation": "ICU.en_US.72" } },
          "value": { "key": { "collation": "ICU.en_US.72" } },
        }
      }
    }
  } ]
}
```

> ***Update the string row in the [Primitive Types](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#primitive-types) table.***

### Primitive Types

Type Name | Description
-|-
string| UTF-8 encoded string of characters. A collation can be specified in [Column Metadata](#specifying-collations-in-the-table-schema), otherwise binary collation is used as the default.

> ***Add new rows to the [Column Metadata](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#column-metadata) table.***

Field Name | Description
-|-
collations | Collations for strings stored in the field or combinations of maps and arrays that are stored in this field and do not have nested structs. Refer to [Specifying collations in the table schema](#specifying-collations-in-the-table-schema) for more details.

> ***Edit the [Per-file Statistics](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#per-file-statistics) section and change it from the "Per-column statistics" section onwards.***

Per-column statistics record information for each column in the file and they are encoded, mirroring the schema of the actual data.
For example, given the following data schema:
```
|-- a: struct
|    |-- b: struct
|    |    |-- c: long
|-- d: struct
     |-- e: string collate ICU.en_US.72
```

Statistics could be stored with the following schema:
```
|-- stats: struct
|    |-- numRecords: long
|    |-- tightBounds: boolean
|    |-- minValues: struct
|    |    |-- a: struct
|    |    |    |-- b: struct
|    |    |    |    |-- c: long
|    |-- maxValues: struct
|    |    |-- a: struct
|    |    |    |-- b: struct
|    |    |    |    |-- c: long
|    |-- statsWithCollation: struct
|    |    |-- ICU.en_US.72: struct
|    |    |    |-- minValues: struct
|    |    |    |    |-- d: struct
|    |    |    |    |    | e: string
|    |    |    |-- maxValues: struct
|    |    |    |    |-- d: struct
|    |    |    |    |    | e: string
```

The following per-column statistics are currently supported:

Name | Description (`stats.tightBounds=true`) | Description (`stats.tightBounds=false`)
-|-|-
nullCount | The number of `null` values for this column | <p>If the `nullCount` for a column equals the physical number of records (`stats.numRecords`) then **all** valid rows for this column must have `null` values (the reverse is not necessarily true).</p><p>If the `nullCount` for a column equals 0 then **all** valid rows are non-`null` in this column (the reverse is not necessarily true).</p><p>If the `nullCount` for a column is any value other than these two special cases, the value carries no information and should be treated as if absent.</p>
minValues | A value that is equal to the smallest valid value[^1] present in the file for this column. If all valid rows are null, this carries no information. | A value that is less than or equal to all valid values[^1] present in this file for this column. If all valid rows are null, this carries no information.
maxValues | A value that is equal to the largest valid value[^1] present in the file for this column. If all valid rows are null, this carries no information. | A value that is greater than or equal to all valid values[^1] present in this file for this column. If all valid rows are null, this carries no information.
statsWithCollation | minValues and maxValues for string columns that are not using binary collation. | Has the same semantics as the top level minValues and maxValues, but wraps both minValues and maxValues into an object keyed by the collation used the generate them.

[^1]: String columns are cut off at a fixed prefix length. Timestamp columns are truncated down to milliseconds.
