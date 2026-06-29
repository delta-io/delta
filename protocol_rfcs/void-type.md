# Void Type
**Associated Github issue for discussions: https://github.com/delta-io/delta/issues/7072**

The `materializedVoidType` reader/writer table feature adds support for using the `void` data type (also known as `NullType` in Spark, `UnknownType` in Iceberg, and `UNKNOWN` in Parquet) anywhere in a Delta table schema.

`void` is a data type with a single possible value: `NULL`. A column ends up with this type when the writer has no information about its actual type, typically because every value observed so far has been `NULL` (for example, `CREATE TABLE t AS SELECT NULL AS a`, or schema evolution that adds a column containing only `NULL`s).

Today, `void` columns are represented by omitting them from data files and reconstructing them as all-`NULL` columns on read (the missing columns mechanism). That representation cannot encode four schema shapes - a table whose columns are all `void`, a `struct` whose fields are all `void`, a `void` nested in an `array`, and a `void` nested in a `map` - because in each case omitting the `void` column(s) would leave the enclosing `struct`, `array`, or `map` (or the table itself) with nothing written to a data file, and therefore nowhere to record whether the enclosing value is `NULL`, empty, or how long it is. Writers must reject writing data in those cases.

The `materializedVoidType` table feature lifts these restrictions by storing the `void` columns those shapes require - the **structural** `void` columns - using the Parquet [`UNKNOWN` logical type](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#unknown-always-null): a column whose values are always `NULL` but which is physically present in the data file, so it can carry the information about its enclosing complex value. Older clients cannot read `UNKNOWN` columns, so the representation is gated behind this reader/writer feature.

--------

> ***New section after the [Variant Shredding](/PROTOCOL.md#variant-shredding) section***

# Void Type

`void` is a primitive data type (see [Primitive Types](/PROTOCOL.md#primitive-types)) with a single possible value, `NULL`. A `void` column can be represented in a data file in one of two ways:

- **Omitted** - the column is not written to the data file and is reconstructed on read as an all-`NULL` column, following the [rule](/PROTOCOL.md#consistency-between-table-metadata-and-data-files) that a column present in the table schema but absent from a data file is read as `NULL`.
- **Stored as `UNKNOWN`** - the column is written using the Parquet [`UNKNOWN` logical type](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#unknown-always-null). Its values are always `NULL`, but unlike an omitted column it is physically present in the data file. This representation requires the `materializedVoidType` table feature.

A `void` column may be changed to any other data type through supported schema-evolution operations; this does not require the [Type Widening](/PROTOCOL.md#type-widening) table feature, even when the `void` column is stored as `UNKNOWN`.

## Void columns without the table feature

When the `materializedVoidType` feature is not supported, `void` columns can only be **omitted**. Because a `void` column is never written to a data file, writers must reject **writing data** to a table whose schema contains any of the following shapes, in which omitting the `void` column(s) would leave nowhere to record the nullability or length of an enclosing value:
- a `void` type directly inside an `array` or `map` at any nesting level;
- a `struct` (at any nesting level) whose fields are all `void`; or
- a table whose columns are all `void`.

These restrictions are stated in terms of the **table schema**, not the schema of any individual data file. A table with such a schema can still be created, altered through metadata-only operations, and read. It can be made writable by evolving its schema - for example, by changing a `void` column to another type - or by enabling the `materializedVoidType` feature.

## Void Type table feature

The `materializedVoidType` table feature lifts the restrictions above by allowing those schema shapes: the `void` columns that cannot be omitted - the **structural** `void` columns - are instead stored using the Parquet `UNKNOWN` logical type.

To support this feature:
- The table must be on Reader Version 3 and Writer Version 7.
- The feature `materializedVoidType` must exist in the table `protocol`'s `readerFeatures` and `writerFeatures`.

The feature has a dual purpose:

1. A client that supports `materializedVoidType` is guaranteed to correctly read and write `void` columns that rely on the missing columns mechanism. Enabling the feature for a table that only uses such columns is **optional**; a user may choose to enable it so that only clients capable of handling `void` columns correctly interact with the table.
2. A client that supports `materializedVoidType` is also guaranteed to correctly read and write structural `void` columns (those stored as `UNKNOWN`). Enabling the feature is **required** to write data for a schema that needs the `UNKNOWN` representation, because clients that do not support the feature cannot read `UNKNOWN` columns.

### Structural void columns

A `void` column is **structural** when it cannot be omitted and is therefore stored as `UNKNOWN`, because omitting it would leave an enclosing `struct`, `array`, or `map` (or the table) with nothing written to the data file. This arises only in the schema shapes that the missing columns mechanism cannot represent (the shapes listed in [Void columns without the table feature](#void-columns-without-the-table-feature)):
- a `void` directly inside an `array` or `map` (at any nesting level): the `void` element or value is structural and must be stored as `UNKNOWN`.
- a `struct` whose fields are all `void`, or a table whose columns are all `void`: the writer chooses which `void` column is structural and must store it as `UNKNOWN`.

A `void` column in any other position is never structural: it can be omitted, and does not require the feature. A schema that contains one of the shapes above is said to **require** the `materializedVoidType` feature.

### Writer Requirements for Void Type

When Void Type is supported (when the `writerFeatures` field of a table's `protocol` action contains `materializedVoidType`), writers:
- must write the table's structural `void` columns to data files (see [Structural void columns](#structural-void-columns)).
- should omit any non-structural `void` column from data files.

### Reader Requirements for Void Type

When Void Type is supported (when the `readerFeatures` field of a table's `protocol` action contains `materializedVoidType`), readers:
- must allow a `void` data type anywhere in a Delta table schema.
- must return only `NULL` values for a `void` column regardless of how it is represented.
- must, within a single scan, correctly combine data files that represent the same column differently - omitted, written as an all-`NULL` column, or (after a type change) written with a concrete type - into the requested read schema.

### Removing the Void Type feature

Because clients that do not support the feature cannot read `UNKNOWN` columns, removing `materializedVoidType` requires that the table no longer depend on the `UNKNOWN` representation. In the version that removes `materializedVoidType` from the `writerFeatures` and `readerFeatures` fields of the table's `protocol` action, writers:
- must ensure that the table schema does not require the feature - it must contain none of the shapes in [Structural void columns](#structural-void-columns).
- must ensure that no data file reachable by the table (including via time travel within the retained history) contains a column stored as `UNKNOWN`. This may require rewriting existing data files and/or truncating the table history so that every `void` column is represented by omission.

After the feature is removed, the table reverts to representing `void` columns only by omission, and the shapes that require the feature must again be rejected when writing data.
