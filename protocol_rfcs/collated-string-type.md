# Collated String Type
**Associated Github issue for discussions: https://github.com/delta-io/delta/issues/2894**

This protocol change adds support for collated strings. It consists of three changes to the protocol:

* Collations in the table schema
* Per-column statistics are annotated with the collation that was used to collect them
* Domain metadata with active collation version

--------

> ***Add a new section in front of the [Primitive Types](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#primitive-types) section.***

### Collations table feature

Collations are a set of rules for how strings are compared. They are supported by the `collations` table feature. Collations do not affect how strings are stored. Collations are applied when comparing strings for equality or to determine the sort order of two strings. Case insensitive comparison is one example of a collation where case is ignored when string are compared for equality and the lower cased variant of a string is used to determine its sort order.

Each string field can have a collation, which is specified in the table schema. It is also possible to store statistics per collation version. This is required because the min and max values of a column can differ based on the used collation or collation version.

By default, all strings are collated using binary collation. That means that strings compare equal if their binary UTF8 encoded representations are equal. The binary UTF8 encoded representation is also used to sort them. Note that in Delta all strings are encoded in UTF8.

The `collations` table feature is a writer only feature and allows clients that do not support collations to read the table using UTF8 binary collation. To support the table feature clients must preserve collations when they change the schema. Collecting collated statistics is optional and it is valid to store UTF8 binary collated statistics for fields with a collation other than UTF8 binary.

#### Collation identifiers

Collations can be referred to using collation identifiers. The Delta format does not specify any collation rules other than binary collation, but supports the concept of collation providers such that engines can use providers like [ICU](https://icu.unicode.org/) and mark statistics accordingly.

A collation identifier consists of 3 parts, which are combined into one identifier using dots as separators. Dots are not allowed to be part of provider and collation names, but can be used in versions.

Part | Description
-|-
Provider | Name of the provider. Must not contain dots
Name | Name of the collation as provided by the provider. Must not contain dots
Version | Version string. Is allowed to contain dots. This part is optional. Collations without a version are used in the schema because readers are not forced to use a specific version of the collation. Statistics are annotated with versioned collations to guarantee correctness.

#### Specifying collations in the table schema

Collations can be specified for any string type in a schema. This includes string fields, but also the key and value type of maps and the element type of arrays. Collations are stored in the `__COLLATIONS` key of the metadata of the nearest ancestor [StructField](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#struct-field) of the Delta table schema. Nested maps and arrays are encoded the same way as ids in [IcebergCompatV2](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#writer-requirements-for-icebergcompatv2). Collation identifiers are stored without key because the version of a collation is not enforced for reading.

This example provides an overview of how collations are stored in the schema. Note that irrelevant fields have been stripped.

Example schema

```
|-- col2: string
|-- col2: map
|       |-- keyType: string
|       |-- valueType: array
|                    |-- elementType: map
|                                   |-- keyType: string
|                                   |-- valueType: map
|                                                |-- keyType: string
|                                                |-- valueType: struct
|                                                             |-- f1: string
```

Schema with collation information

```
{
  "type" : "struct",
  "fields" : [
    {
      "name" : "col1",
      "type" : "string",
      "metadata": {
        "__COLLATIONS": { "col1": "ICU.de_DE" }
      }
    },
    {
      "name" : "col2",
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
            "metadata": {
              "__COLLATIONS": { "f1": "ICU.de_DE" }
            }
          }
        }
      }
      "metadata": {
        "__COLLATIONS": {
          "col1.key": "ICU.en_US",
          "col1.value.element.key": "ICU.en_US",
          "col1.value.element.value.key": "ICU.en_US"
        }
      }
    }
  ]
}
```

#### Collation versions

The [Domain Metadata](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#domain-metadata) for the `collations` table feature contains hints which version of a collation should be used to read from the table and for which versions of a collations clients should produce statistics when writing to the table. They allow clients to choose a collation version without having to look at the statistics of all AddFiles first. Clients are allowed to ignore the hints.

`collations` [Domain Metadata](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#domain-metadata)

```
{
  "readVersions": {
    "ICU.en_US": "73"
  },
  "writeVersions": {
    "ICU.en_US": ["72", "73"]
  }
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
__COLLATIONS | Collations for strings stored in the field or combinations of maps and arrays that are stored in this field and do not have nested structs. Refer to [Specifying collations in the table schema](#specifying-collations-in-the-table-schema) for more details.

> ***Edit the [Per-file Statistics](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#per-file-statistics) section and change it from the "Per-column statistics" section onwards.***

Per-column statistics record information for each column in the file and they are encoded, mirroring the schema of the actual data. Statistic are optional and it is allowed to provide UTF8 binary statistics for strings when the field has a different collation.
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
