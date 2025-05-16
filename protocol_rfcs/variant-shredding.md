# Variant Shredding
**Associated Github issue for discussions: https://github.com/delta-io/delta/issues/4032**

This protocol change adds support for Variant shredding for the Variant data type.
Shredding allows Variant data to be be more efficiently stored and queried.

--------

> ***New Section after the `Variant Data Type` section***

# Variant Shredding

This feature enables support for shredding of the Variant data type, to store and query Variant data more efficiently.
Shredding a Variant value is taking paths from the Variant value, and storing them as a typed column in the file.
The shredding does not duplicate data, so if a value is stored in the typed column, it is removed from the Variant binary.
Storing Variant values as typed columns is faster to access, and enables data skipping with statistics.

The `variantShredding` feature depends on the `variantType` feature.

To support this feature:
- The table must be on Reader Version 3 and Writer Version 7
- The feature `variantType` must exist in the table `protocol`'s `readerFeatures` and `writerFeatures`.
- The feature `variantShredding` must exist in the table `protocol`'s `readerFeatures` and `writerFeatures`.

## Shredded Variant data in Parquet

Shredded Variant data is stored according to the [Parquet Variant Shredding specification](https://github.com/apache/parquet-format/blob/master/VariantShredding.md)
The shredded Variant data written to parquet files is written as a single Parquet struct, with the following fields:

Struct field name | Parquet primitive type | Description
-|-|-
metadata | binary | (required) The binary-encoded Variant metadata, as described in [Parquet Variant binary encoding](https://github.com/apache/parquet-format/blob/master/VariantEncoding.md)
value | binary | (optional) The binary-encoded Variant value, as described in [Parquet Variant binary encoding](https://github.com/apache/parquet-format/blob/master/VariantEncoding.md)
typed_value | * | (optional) This can be any Parquet type, representing the data stored in the Variant. Details of the shredding scheme is found in the [Variant Shredding specification](https://github.com/apache/parquet-format/blob/master/VariantShredding.md)

## Writer Requirements for Variant Shredding

When Variant Shredding is supported (`writerFeatures` field of a table's `protocol` action contains `variantShredding`), writers:
- must respect the `delta.enableVariantShredding` table property configuration. If `delta.enableVariantShredding=false`, a column of type `variant` must not be written as a shredded Variant, but as an unshredded Variant. If `delta.enableVariantShredding=true`, the writer can choose to shred a Variant column according to the [Parquet Variant Shredding specification](https://github.com/apache/parquet-format/blob/master/VariantShredding.md)

## Reader Requirements for Variant Shredding

When Variant type is supported (`readerFeatures` field of a table's `protocol` action contains `variantShredding`), readers:
- must recognize and tolerate a `variant` data type in a Delta schema
- must recognize and correctly process a parquet schema that is either unshredded (only `metadata` and `value` struct fields) or shredded (`metadata`, `value`, and `typed_value` struct fields) when reading a Variant data type from file.

> ***Update the `Per-file Statistics` section***

> After the description and examples starting from: `Per-column statistics record information for each column in the file and they are encoded, mirroring the schema of the actual data. For example, given the following data schema:`

### Statistics for Variant Columns

- The `nullCount` stat for a Variant column is a LONG representing the nullcount for the Variant column itself (nullcount stats are not captured for individual paths within the Variant).
- In JSON, the `minValues` and `maxValues` stats for a Variant column are [binary-encoded](https://github.com/apache/parquet-format/blob/master/VariantEncoding.md) Variant values, with the `metadata` and `value` columns serialized to strings using [z85](https://rfc.zeromq.org/spec/32/) encoding (see example below).
- In Parquet, the `minValues` and `maxValues` stats for a Variant column are Parquet Variant columns, following to the [Parquet Variant specification](https://github.com/apache/parquet-format/blob/master/VariantShredding.md).
- Each path in the Variant `minValues` (`maxValues`) value is the independently computed min (max) stat for the corresponding path in the file's Variant data, so e.g. `minValues.v:a` and `minValues.v:b` could come from different rows in the file.
- Min/max stats may only be written for primitive (leaf) values, packed into a Variant representation.
- Min/max stats may only be written for a path if that path has the same data type in every row of the data file.
- The paths and types inside `minValues` and `maxValues` must be the same within any one file, but can vary from file to file.
- Subject to the above constraints, the writer of a given file determines which Variant leaf paths (if any) to emit statistics for.

For a table with a single Variant column (`varCol: variant`) in its data schema, example statistics in JSON would look like:

```
"stats": {
  "nullCount": {
    "varCol": 2
  }
  "minValues": {
    "varCol": {
      "metadata": "0rSr50S#>uv/"
      "value": "0S&u501fz*ze0(tB98CpzF61K0SSog3i"
    }
  },
  "maxValues": {
    "varCol": {
      "metadata": "0rSr50S#>uv/"
      "value": "0S&u500<bRC42A9vqZe*0rJl65Cb#"
    }
  }
}
```
The corresponding human-readable form is:
```
"stats": {
  "nullCount": {
    "varCol": 2
  }
  "minValues": {
    "varCol": {
      "a": "min-string"
      "b": {
        "c": 10
      }
    }
  },
  "maxValues": {
    "varCol": {
      "a": "variant"
      "b": {
        "c": 500
      }
    }
  }
}
```
