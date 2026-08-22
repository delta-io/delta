# Materialized Void Type
**Associated Github issue for discussions: https://github.com/delta-io/delta/issues/7072**

The `materializedVoidType` reader/writer table feature adds support for materializing the `void` data type (also known as `NullType` in Spark, `UnknownType` in Iceberg, and `UNKNOWN` in Parquet) in data files. The feature does not gate the use of `void` in a Delta table schema: clients must continue to support `void` columns represented through the missing columns mechanism even when the feature is not supported.

`void` has only one possible value: `NULL`. Writers commonly infer this type when they have no non-`NULL` values from which to infer a concrete type. Examples include `CREATE TABLE t AS SELECT NULL AS a` and schema evolution that adds an all-`NULL` column.

Without this feature, clients omit `void` columns from data files and reconstruct them as all-`NULL` columns when reading. That representation breaks down for four schema shapes:

- a table whose non-partition columns are all `void`;
- a `struct` whose fields are all `void`;
- a `void` used directly as an `array` element; or
- a `void` used directly as a `map` key or value.

Omitting the `void` columns in these cases would leave nothing to write for the table or enclosing `struct`, `array`, or `map`. The file would have nowhere to record whether an enclosing value is `NULL` or empty, or how many elements it contains. Writers must reject operations that would write new data files with one of these shapes.

The `materializedVoidType` feature makes these shapes writable. A writer chooses enough `void` columns to preserve the structure and stores them with Parquet's [`UNKNOWN` logical type](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#unknown-always-null). These are **structural** `void` columns. Only `void` columns in the restricted shapes above may be structural. Because older clients cannot read `UNKNOWN` columns, tables that use this representation must enable the reader/writer feature.

--------

# Changes to existing sections

> ***Change the `void` row in the [Primitive Types](/PROTOCOL.md#primitive-types) table to the following:***

Type Name | Description
-|-
void | A column that contains only `null` values. It is omitted from data files unless it is materialized as described in [Void Type](#void-type).

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

##### Materialized Void Type table feature

The `materializedVoidType` table feature lifts the restrictions above by allowing the affected schema shapes. Only `void` columns selected to make one of those shapes representable - the **structural** `void` columns - may be materialized. A materialized `void` column must use the Parquet `UNKNOWN` logical type. Every non-structural `void` column must be omitted.

To support this feature:

- The table must be on Reader Version 3 and Writer Version 7.
- The feature `materializedVoidType` must exist in the table `protocol`'s `readerFeatures` and `writerFeatures`.

Supporting the feature makes two guarantees:

1. The client can correctly read and write `void` columns that use the missing columns mechanism. If a table uses only these columns, enabling the feature is **optional**. A user may still enable it to prevent clients that mishandle `void` columns from interacting with the table.
2. The client can correctly read and write structural `void` columns stored as `UNKNOWN`. Enabling the feature is **required** for any operation that writes new data files when the table schema needs the `UNKNOWN` representation. A writer may enable the feature in the same commit that first adds such files, but the resulting table version must support the feature.

###### Structural void columns

A `void` column is **structural** when a writer selects it to make an enclosing `struct`, `array`, or `map` (or the table) representable in the data file. Every structural `void` column must be stored as `UNKNOWN`. Writers may select structural `void` columns only from the schema shapes listed in [Void columns without the table feature](#void-columns-without-the-table-feature), and must select enough columns to make each such shape representable:

- a `void` used directly as an `array` element or `map` key or value (at any nesting level) is structural;
- for a `struct` whose fields are all `void`, the writer must select one or more of its `void` fields as structural; and
- for a table whose non-partition columns are all `void`, the writer must select one or more non-partition `void` columns as structural.

A `void` column in any other position is non-structural and must be omitted. In particular, a `void` partition column is never structural. A schema with one of the shapes above **requires** the `materializedVoidType` feature.

###### Writer Requirements for Materialized Void Type

When Materialized Void Type is supported (when the `writerFeatures` field of a table's `protocol` action contains `materializedVoidType`), writers:

- must write the table's structural `void` columns to data files (see [Structural void columns](#structural-void-columns)); and
- must omit every non-structural `void` column from data files, regardless of any other supported table feature or table property.

###### Reader Requirements for Materialized Void Type

When Materialized Void Type is supported (when the `readerFeatures` field of a table's `protocol` action contains `materializedVoidType`), readers:

- must allow the `void` data type anywhere in a Delta table schema;
- must return `NULL` for every value of a column defined as `void` in the table schema; and
- must correctly combine, within one scan, data files that represent the same column in different ways (omitted, written as `UNKNOWN`, or written with a concrete type after a type change) into the requested read schema.

###### Removing the Materialized Void Type feature

Clients that do not support the feature might not be able to read `UNKNOWN` columns that correspond to columns in the current table schema. The feature can therefore be removed only when the current table version no longer depends on the `UNKNOWN` representation.

In the version that removes `materializedVoidType` from `writerFeatures` and `readerFeatures`, writers:

- must ensure that the table schema no longer requires the feature. It must contain none of the shapes listed in [Structural void columns](#structural-void-columns); and
- must ensure that every `UNKNOWN` column in a data file read by the current table version either corresponds to a column dropped from the current schema or is removed by rewriting the data file. Changing a `void` column to a concrete type is not enough; the writer must also rewrite files that store the column as `UNKNOWN`.

After `materializedVoidType` is removed, data files may still contain `UNKNOWN` columns that correspond to columns dropped from the current table schema. Readers should ignore these columns and process only columns referenced by the current table schema. Historical table versions that expose the dropped columns still advertise `materializedVoidType`.

After the feature is removed, every `void` column in the current table schema is represented by omission. Writers must again reject operations that would write new data files when the table schema has a shape that requires the feature.

> ***Add the following `void` row to the [Delta Data Type to Parquet Type Mappings](/PROTOCOL.md#delta-data-type-to-parquet-type-mappings) table and replace the existing note that `void` columns are not stored in Parquet files with the paragraph below:***

Delta Type Name | Parquet Physical Type | Parquet Logical Type
-|-|-
void (when materialized) | Implementation-defined | `UNKNOWN`

When a structural `void` column is materialized, it must use the `UNKNOWN` logical type and the table must support `materializedVoidType`. Every non-structural `void` column must be omitted from the Parquet file.
