# Collated String Type
**Associated Github issue for discussions: https://github.com/delta-io/delta/issues/2894**

This protocol change adds support for collated strings. It consists of three changes to the protocol:

* Collations in the table schema
* Per-column statistics are annotated with the collation that was used to collect them
* Domain metadata with active collation version

--------
> *** Add New Section after the [Clustered Table](#clustered-table) section***
# Collations Table Feature

To support this feature:
* The table must have Writer Version 7. 
* The feature `collations` must exist in the table's `writerFeatures`.
* The feature `domainMetadata` must exist in the table's `writerFeatures`. 

## Reader Requirements for Collations:

When Collations are supported (when the `writerFeatures` field of a table's protocol action contains `collations`), then:
- Readers could do comparisons and sorting of strings based on the collation specified in the schema. 
- If the collation is not specified for a string type, then the reader must use the default comparison operators for the binary representation of strings under UTF-8 encoding.
- Readers must only do file skipping based on column statistics for a collation if the filter operator used for the data skipping is specified to treat the column as having that same collation. For example, when filtering a string column using the string equality comparison operator that is configured with the collation `ICU.en_US.72`, the reader must not use file skipping statistics from the collation `spark.UTF8_LCASE.75.1`. It should also not use the statistics from `ICU.en_US.69` because the collation version number does not match.

## Writer Requirements for Collations:

When Collations are supported (when the `writerFeatures` field of a table's protocol action contains `collations`), then:
- Writers must write the collation identifier in the schema metadata for a column with non-default collation, i.e., any collation that is not comparing strings using their binary representations under UTF-8 encoding.
- Writers must not write the collation identifier in the schema metadata for a column with default collation (comparisons using binary representation of the strings under UTF-8 encoding).
- Writers could write per-file statistics for string columns with non-default collations in `statsWithCollation`. See [Per-file Statistics](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#per-file-statistics) for more details.
- If a writer adds per-file statistics for a new version of a collation, the writer should also update the `domainMetadata` for the `collations` table feature to include the new collation versions that are used to collect statistics.
- Writers could remove a collation version from the `domainMetadata` for the `collations` table feature if stats collection for the collation version is no longer desired. For example, the engine upgrades their ICU library and now desires a newer version for a collation.

> ***Add a new section in front of the [Primitive Types](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#primitive-types) section.***

### Collations
Collations are a set of rules for how strings are compared. Collations do not affect how strings are stored. Collations are applied when comparing strings for equality or to determine the sort order of two strings. Case insensitive comparison is one example of a collation where case is ignored when string are compared for equality and the lower cased variant of a string is used to determine its sort order.

Each string field can have a collation, which is specified in the table schema. It is also possible to store statistics per collation version. This is required because the min and max values of a column can differ based on the used collation or collation version.

By default, all strings are collated using binary collation. That means that strings compare equal if their binary UTF-8 encoded representations are equal. The binary UTF-8 encoded representation is also used to sort them. Note that in Delta all strings are encoded in UTF-8.

The `collations` table feature is a writer only feature and allows clients that do not support collations to read the table using UTF-8 binary collation. To support the table feature clients must preserve collations when they change the schema. Collecting collated statistics is optional and it is valid to store UTF-8 binary collated statistics for fields with a collation other than UTF-8 binary.

The column level collation indicates the default collation that readers should use to operate on a column. However, readers are responsible for choosing what collation to actually apply on operations. An engine may apply a different collation than the schema collation based on the engine's collation precedence rules. However, an engine must take care to only use column statistics for file skipping from a collation that is identical to the one specified in the filtering operation in all aspects, including the collation version.

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
|-- col1: string
|-- col2: array
|       |-- elementType: map
|                      |-- keyType: string
|                      |-- valueType: struct
|                                   |-- f1: string
```

Schema with collation information

```
{
   "type":"struct",
   "fields":[
      {
         "name":"col1",
         "type":"string",
         "metadata":{
            "__COLLATIONS":{
               "col1":"ICU.de_DE"
            }
         }
      },
      {
         "name":"col2",
         "type":{
            "type":"array",
            "elementType":{
               "type":"map",
               "keyType":"string",
               "valueType":{
                  "type":"struct",
                  "fields":[
                     {
                        "name":"f1",
                        "type":"string",
                        "metadata":{
                           "__COLLATIONS":{
                              "f1":"ICU.de_DE"
                           }
                        }
                     }
                  ]
               }
            }
         },
         "metadata":{
            "__COLLATIONS":{
               "col2.element.key":"ICU.en_US"
            }
         }
      }
   ]
}
```

#### Collation versions

The [Domain Metadata](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#domain-metadata) for the `collations` table feature contains hints for which versions of a collations clients should produce statistics when writing to the table. The hints allow clients to choose a collation version without having to look at the statistics of all AddFiles first. Clients are allowed to ignore the hints.

`collations` [Domain Metadata](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#domain-metadata)

```
{
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

Per-column statistics record information for each column in the file and they are encoded, mirroring the schema of the actual data. Statistic are optional and it is allowed to provide UTF-8 binary statistics for strings when the field has a different collation.
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
