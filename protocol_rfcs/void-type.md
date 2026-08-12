# Void Type
**Associated Github issue for discussions: https://github.com/delta-io/delta/issues/7072**

The `materializedVoidType` reader/writer table feature adds support for materializing the `void` data type (also known as `NullType` in Spark, `UnknownType` in Iceberg, and `UNKNOWN` in Parquet) in data files. The feature does not gate the use of `void` in a Delta table schema: clients must continue to support `void` columns represented through the missing columns mechanism even when the feature is not supported.

`void` is a data type with a single possible value: `NULL`. A column ends up with this type when the writer has no information about its actual type, typically because every value observed so far has been `NULL` (for example, `CREATE TABLE t AS SELECT NULL AS a`, or schema evolution that adds a column containing only `NULL`s).

Today, `void` columns are represented by omitting them from data files and reconstructing them as all-`NULL` columns on read (the missing columns mechanism). That representation cannot encode four schema shapes - a table whose non-partition columns are all `void`, a `struct` whose fields are all `void`, a `void` used directly as an `array` element, and a `void` used directly as a `map` key or value - because in each case omitting the `void` column(s) would leave the enclosing `struct`, `array`, or `map` (or the table itself) with nothing written to a data file, and therefore nowhere to record whether the enclosing value is `NULL`, empty, or how long it is. Writers must reject operations that would write new data files in those cases.

The `materializedVoidType` table feature lifts these restrictions by storing enough of the `void` columns in those shapes - the **structural** `void` columns - using the Parquet [`UNKNOWN` logical type](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#unknown-always-null): a column whose values are always `NULL` but which is physically present in the data file, so it can carry information about its enclosing complex value. Only `void` columns in one of the restricted shapes may be selected as structural. `void` partition columns are never structural and must be omitted. Older clients cannot read `UNKNOWN` columns, so the representation is gated behind this reader/writer feature.

--------

# Changes to existing sections

> ***Change the `void` row in the [Primitive Types](/PROTOCOL.md#primitive-types) table to the following:***

Type Name | Description
-|-
void| A column that contains only `null` values. It is omitted from data files unless it is materialized as described in [Void Type](#void-type).

> ***Replace the existing [Void Type](/PROTOCOL.md#void-type) subsection under [Primitive Types](/PROTOCOL.md#primitive-types) with the following:***

#### Void Type

_Note: `void` was never deliberately designed as a Delta feature; the Spark connector has produced such columns for a long time without it being specified here. This section documents that pre-existing behavior post-facto. Because such columns already exist in tables written by earlier clients, the `void` type itself is not gated by any table feature and applies to all tables. Only its materialized representation is gated by the `materializedVoidType` table feature._

`void` is a primitive type with a single possible value, `NULL`, and can appear both as a top-level column and nested inside complex types. A `void` column can be represented in a data file in one of two ways:

- **Omitted** - the column is not written to the data file and readers must reconstruct it as an all-`NULL` column, following the [rule](/PROTOCOL.md#consistency-between-table-metadata-and-data-files) that a column present in the table schema but absent from a data file is read as `NULL`.
- **Stored as `UNKNOWN`** - the column is written using the Parquet [`UNKNOWN` logical type](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#unknown-always-null). Its values are always `NULL`, but unlike an omitted column it is physically present in the data file. This representation requires the `materializedVoidType` table feature and is permitted only for a structural `void` column as defined below.

A `void` column may be changed to any other data type through supported schema-evolution operations; this does not require the [Type Widening](/PROTOCOL.md#type-widening) table feature, even when the `void` column is stored as `UNKNOWN`.

##### Void columns without the table feature

When the `materializedVoidType` feature is not supported, `void` columns can only be omitted. Readers must reconstruct every omitted `void` column as an all-`NULL` column. Writers must reject operations that would write new data files when the table schema contains any of the following shapes, in which omitting the `void` column(s) would leave nowhere to record the nullability or length of an enclosing value:
- a `void` type used directly as an `array` element or `map` key or value, at any nesting level;
- a `struct` (at any nesting level) whose fields are all `void`; or
- a table whose non-partition columns are all `void`.

These restrictions are stated in terms of the **table schema**, not the schema of any individual data file. A table with a restricted schema can still be created, altered through metadata-only operations, and read. It can be made writable by evolving its schema - for example, by changing a `void` column to another type - or by enabling the `materializedVoidType` feature.

##### Void Type table feature

The `materializedVoidType` table feature lifts the restrictions above by allowing the affected schema shapes. Only `void` columns selected to make one of those shapes representable - the **structural** `void` columns - may be materialized. A materialized `void` column must use the Parquet `UNKNOWN` logical type. Every non-structural `void` column, including every `void` partition column, must be omitted.

To support this feature:
- The table must be on Reader Version 3 and Writer Version 7.
- The feature `materializedVoidType` must exist in the table `protocol`'s `readerFeatures` and `writerFeatures`.

The feature has a dual purpose:

1. A client that supports `materializedVoidType` is guaranteed to correctly read and write `void` columns that rely on the missing columns mechanism. Enabling the feature for a table that only uses such columns is **optional**; a user may choose to enable it so that only clients capable of handling `void` columns correctly interact with the table.
2. A client that supports `materializedVoidType` is also guaranteed to correctly read and write structural `void` columns stored as `UNKNOWN`. Enabling the feature is **required** for any operation that would write new data files when the table schema requires the `UNKNOWN` representation. A writer may add the feature in the same commit that first introduces such data files, but must not commit an `UNKNOWN` column unless the feature is supported by the resulting table version.

###### Structural void columns

A `void` column is **structural** when a writer selects it to make an enclosing `struct`, `array`, or `map` (or the table) representable in the data file. Every structural `void` column must be stored as `UNKNOWN`. Writers may select structural `void` columns only from the schema shapes listed in [Void columns without the table feature](#void-columns-without-the-table-feature), and must select enough columns to make each such shape representable:
- a `void` used directly as an `array` element or `map` key or value (at any nesting level) is structural;
- for a `struct` whose fields are all `void`, the writer must select one or more of its `void` fields as structural; and
- for a table whose non-partition columns are all `void`, the writer must select one or more non-partition `void` columns as structural.

A `void` column in any other position is non-structural and must be omitted. In particular, a `void` partition column is never structural. A schema that contains one of the shapes above is said to **require** the `materializedVoidType` feature.

###### Writer Requirements for Void Type

When materialized Void Type is supported (when the `writerFeatures` field of a table's `protocol` action contains `materializedVoidType`), writers:
- must write the table's structural `void` columns to data files (see [Structural void columns](#structural-void-columns)); and
- must omit every non-structural `void` column from data files, regardless of any other supported table feature or table property.

###### Interaction with Materialize Partition Columns

The `materializePartitionColumns` writer feature requires writers to materialize only non-`void` partition columns. Writers must omit `void` partition columns whether or not `materializedVoidType` is also supported. A `void` partition column is not structural, and this exception does not relax the restrictions in [Void columns without the table feature](#void-columns-without-the-table-feature) or the structural-column requirements above.

###### Reader Requirements for Void Type

When Void Type is supported (when the `readerFeatures` field of a table's `protocol` action contains `materializedVoidType`), readers:
- must allow a `void` data type anywhere in a Delta table schema;
- must return only `NULL` values for a column defined as `void` in the table schema, regardless of whether it is omitted or materialized; and
- must, within a single scan, correctly combine data files that represent the same column differently - omitted, written as `UNKNOWN`, or (after a type change) written with a concrete type - into the requested read schema.

###### Removing the Void Type feature

Because clients that do not support the feature are not guaranteed to be able to read `UNKNOWN` columns that correspond to columns in the current table schema, removing `materializedVoidType` requires that the current table version no longer depend on the `UNKNOWN` representation. In the version that removes `materializedVoidType` from the `writerFeatures` and `readerFeatures` fields of the table's `protocol` action, writers:
- must ensure that the table schema does not require the feature - it must contain none of the shapes in [Structural void columns](#structural-void-columns); and
- must ensure that every `UNKNOWN` column in a data file read by the current table version either corresponds to a column that has been dropped from the current table schema or is removed by rewriting the data file. Changing a `void` column to a concrete type does not satisfy this requirement by itself; data files that store that column as `UNKNOWN` must be rewritten.

An `UNKNOWN` column that corresponds to a column dropped from the current table schema may remain in a data file. Readers of the current table version do not request the dropped column, while historical table versions that expose the column continue to advertise `materializedVoidType`. Therefore, the feature may be removed without rewriting data files when every column represented as `UNKNOWN` has been dropped from the current table schema.

After the feature is removed, every `void` column in the current table schema is represented only by omission, and operations that would write new data files must again be rejected when the table schema contains a shape that requires the feature.

> ***Add the following `void` row to the [Delta Data Type to Parquet Type Mappings](/PROTOCOL.md#delta-data-type-to-parquet-type-mappings) table and replace the existing note that `void` columns are not stored in Parquet files with the paragraph below:***

Delta Type Name | Parquet Physical Type | Parquet Logical Type
-|-|-
void (when materialized)| Implementation-defined | `UNKNOWN`

When a structural `void` column is materialized, it must use the `UNKNOWN` logical type and the table must support `materializedVoidType`. Every non-structural `void` column, including every `void` partition column, must be omitted from the Parquet file.
