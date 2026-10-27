# float16 / Float16 primitive types

**Issue for discussion: https://github.com/delta-io/delta/issues/6558**

IEEE 754-2008 16-bit floats, also known as half-precision floats, are useful for machine learning, and are [supported by Parquet](https://parquet.apache.org/docs/file-format/types/logicaltypes/#float16) and [Arrow](https://arrow.apache.org/docs/python/generated/pyarrow.float16.html) (the [formal type spec](https://github.com/apache/arrow/blob/main/format/Schema.fbs) has "half" as one of the precisions for floating point numbers).

--------

## Add a new feature: Float16 primitive type (Float16)

Add a new feature:

> This feature introduces a new data type to the delta protocol, a 16-bit IEEE float, sometimes called "half float", for example in Arrow.
> The serialization method is described in Sections [Partition Value Serialization](#partition-value-serialization) and [Schema Serialization Format](#schema-serialization-format).
>
> To support this feature:
> - To have a column of `Float16` type in a table, the table must have Reader Version 3 and Writer Version 7. A feature name `float16` must exist in the table's `readerFeatures` and `writerFeatures`.

## Add float16 to the Schema Serialization Format

Add an entry for float16 to the Primitive Types table:

> float16 | 2-byte half-precision float-point numbers

## Add float16 to the Partition Value Serialization

Add an entry for float16:

> float16: the string representation of the number

## Add Parquet Type mapping

Add a table entry for float16:

> Physical type: `float16`

## Add entry to the Valid Feature Names in Table Features

In the appendix for Valid Feature Names, add `float16` as a valid feature for readers and writers.
