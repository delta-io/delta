# DECFLOAT Data Type
**Associated Github issue for discussions: https://github.com/delta-io/delta/issues/7721**

DECFLOAT is an IEEE 754 decimal floating-point type. This RFC proposes two Delta schema types,
`decfloat(16)` and `decfloat(34)`, and a ReaderWriter table feature named `decFloat`. Experimental
implementations may use `decFloat-preview` until the RFC is accepted.

The Parquet mapping below is the current proposal. An experimental implementation also writes
`com.databricks.spark.decfloat.encoding=bid-le-v1`; whether that marker belongs in the Delta
protocol or remains an implementation detail is open for RFC review.

--------

> ***Add the following DECFLOAT Data Type section to `PROTOCOL.md`.***

# DECFLOAT Data Type

The DECFLOAT type represents an IEEE 754 decimal floating-point value. Delta supports two
precisions:

- `decfloat(16)`: up to 16 significant decimal digits.
- `decfloat(34)`: up to 34 significant decimal digits.

DECFLOAT supports finite values, signed zero, positive and negative infinity, and NaN.

To support DECFLOAT:

- The table must be on Reader Version 3 and Writer Version 7.
- The feature `decFloat` must exist in the table protocol's `readerFeatures` and
  `writerFeatures`.

The feature is required when either DECFLOAT type appears anywhere in the table schema, including
inside a struct, array, or map.

## Schema Serialization

DECFLOAT types are serialized in `metaData.schemaString` using the following case-sensitive type
names:

- `decfloat(16)`
- `decfloat(34)`

For example:

```json
{
  "type": "struct",
  "fields": [
    {
      "name": "d16",
      "type": "decfloat(16)",
      "nullable": true,
      "metadata": {}
    },
    {
      "name": "d34",
      "type": "decfloat(34)",
      "nullable": true,
      "metadata": {}
    }
  ]
}
```

## Parquet Representation

The Parquet mapping is:

| Delta type | Parquet physical type | Parquet logical type | Payload |
| --- | --- | --- | --- |
| `decfloat(16)` | `FIXED_LEN_BYTE_ARRAY(8)` | none | 64-bit BID, little-endian |
| `decfloat(34)` | `FIXED_LEN_BYTE_ARRAY(16)` | none | 128-bit BID, little-endian |

## Reader Requirements

A reader that supports `decFloat` must:

- recognize `decfloat(16)` and `decfloat(34)` in the Delta schema;
- read the corresponding unannotated fixed-length Parquet field;
- require an exact physical width of 8 bytes for `decfloat(16)` and 16 bytes for
  `decfloat(34)`;
- reject an incorrect width or an unexpected Parquet logical type;
- preserve DECFLOAT comparison semantics, including numerically equal values with different BID
  payloads;
- preserve signed zero, infinities, and NaN.

## Writer Requirements

A writer that supports `decFloat` must:

- add the `decFloat` feature when DECFLOAT appears in the table schema;
- write DECFLOAT values using the Parquet representation above;
- preserve DECFLOAT comparison and special-value semantics;
- reject a write if it cannot preserve the DECFLOAT type and representation.

## Statistics and Data Skipping

This proposal defines `nullCount` statistics for DECFLOAT but does not define min/max statistics
or data skipping. Those require a standardized physical representation and comparison contract.

## Compatibility with Other Delta Features

| Feature | Requirement |
| --- | --- |
| Partition columns | A DECFLOAT column must not be used as a partition column. |
| Clustered tables | A DECFLOAT column must not be used as a clustering column. |
| Delta column statistics | `nullCount` is supported. `minValues` and `maxValues` are not supported. |

> ***Add the following rows to the Primitive Types table.***

| Type name | Description |
| --- | --- |
| `decfloat(16)` | IEEE 754 decimal floating-point value with up to 16 significant decimal digits |
| `decfloat(34)` | IEEE 754 decimal floating-point value with up to 34 significant decimal digits |

> ***Add the following rows to the Delta Data Type to Parquet Type Mappings table.***

| Delta type | Parquet physical type | Parquet logical type |
| --- | --- | --- |
| `decfloat(16)` | `FIXED_LEN_BYTE_ARRAY(8)` | |
| `decfloat(34)` | `FIXED_LEN_BYTE_ARRAY(16)` | |

> ***Add `decFloat` to the Valid Feature Names in Table Features as a ReaderWriter feature.***
