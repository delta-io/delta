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
Storing Variant values as typed columns is faster to access, and enables skipping with statistics.

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
typed_value | * | (optional) This can be any Parquet type, representing the data stored in the Variant. Details of the shredding scheme is found in the [Parquet Variant binary encoding](https://github.com/apache/parquet-format/blob/master/VariantEncoding.md)

## Writer Requirements for Variant Shredding

When Variant Shredding is supported (`writerFeatures` field of a table's `protocol` action contains `variantShredding`), writers:
- must respect the `delta.enableVariantShredding` table property configuration. If `delta.enableVariantShredding=false`, a column of type `variant` must not be written as a shredded Variant, but as an unshredded Variant. If `delta.enableVariantShredding=true`, the writer can choose to shred a Variant column according to the [Parquet Variant Shredding specification](https://github.com/apache/parquet-format/blob/master/VariantShredding.md)

## Reader Requirements for Variant Shredding

When Variant type is supported (`readerFeatures` field of a table's `protocol` action contains `variantShredding`), readers:
- must recognize and tolerate a `variant` data type in a Delta schema
- must tolerate a parquet schema that is either unshredded (only `metadata` and `value` struct fields) or shredded (`metadata`, `value`, and `typed_value` struct fields) when reading a Variant data type from file.
