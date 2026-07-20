# Materialize Partition Columns

**Associated Github issue for discussions: https://github.com/delta-io/delta/issues/5555**

## Overview

Currently, Delta tables store partition column values primarily in the table metadata (specifically in the `partitionValues` field of `AddFile` actions), and by default these columns are not physically written into the Parquet data files themselves.

This RFC proposes a new writer-only table feature called `materializePartitionColumns`. When supported, this feature requires partition columns to be physically materialized in Parquet data files alongside the data columns, except for `void` partition columns when the table does not support the `materializedVoidType` feature.

This RFC also proposes a new table property, `delta.writePartitionColumnsToParquet`. If the above-mentioned `materializePartitionColumns` feature is not enabled, this property provides a best-effort knob to control materialization of partition columns. In other words, in the absence of the `materializePartitionColumns` feature, partition columns should be materialized if the `delta.writePartitionColumnsToParquet` property is set to true, although it is also correct for writers to ignore this property. The property does not permit materializing a `void` column when `materializedVoidType` is not supported.

## Motivation

This feature provides a mechanism to require partition column materialization at the protocol level, ensuring all writers to the table comply with this requirement during the period when the feature is supported. A `void` partition column is exempt when `materializedVoidType` is not supported because materializing it would introduce a representation that readers are not required to understand. The table property provides a similar enablement knob in a best-effort sense without blocking writers that do not understand the property or table feature.

Materializing partition columns enhances compatibility with Parquet readers that access Parquet files directly and do not interpret Delta’s AddFile metadata, as well as with Iceberg readers, which expect partition columns to be stored within the data files.

Additionally, having partition information embedded in the data files themselves enables more flexible data reorganization strategies. The same parquet files could be linked in future versions of a table that do not have the same (or any) partition columns.

--------

> ***New entry in the Table Properties table***

Property | Description | Details
-|-|-
`delta.writePartitionColumnsToParquet` | Controls whether writers SHOULD write partition columns in newly written data parquet files, in the absence of any writer features that necessitate writing of partition columns (eg. `IcebergCompatV1`). In other words, if no writer feature is enabled that requires materialization of partition columns, writers should read this property to decide whether to materialize partition columns in data parquet files or not. Writer features requirements take precedence over this property's value. A `void` partition column must be omitted when `materializedVoidType` is not supported, regardless of this property's value. Readers should continue to read partition values off of AddFile actions, regardless of the presence of partition values in data files. File-level statistics should not be present for partition columns in partitioned tables in any case. This setting does not apply to writers of files of any other file format. | Boolean field, with valid values `false` and `true`.


> ***New Section after Identity Columns section***
## Materialize Partition Columns

When this feature is supported, partition columns are physically written to Parquet files alongside the data columns, subject to the `void` exception below. To support this feature:
 - The table must be on Writer Version 7, and a feature name `materializePartitionColumns` must exist in the table `protocol`'s `writerFeatures`.

When supported:
 - When the writer feature `materializePartitionColumns` is set in the protocol, writers must materialize partition columns into any newly created data file, except that:
   - when a partition column is `void` and `materializedVoidType` is not supported, writers must omit that partition column; and
   - when a partition column is `void` and `materializedVoidType` is supported, writers must materialize that partition column using the representation defined by the [Void Type](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#void-type) feature.

   This exception does not allow a writer to write data for a schema that independently requires `materializedVoidType`, such as a table whose columns are all `void`. This requirement otherwise mimics the partition column materialization requirement from [IcebergCompatV1](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#iceberg-compatibility-v1)
and
[IcebergCompatV2](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#iceberg-compatibility-v2). As such, the `materializePartitionColumns` feature can be seen as a subset of the requirements imposed by those features, providing the partition column materialization guarantee independently without requiring full
  Iceberg compatibility.
 - When the writer feature `materializePartitionColumns` is not set in the table protocol, writers are not required to write partition columns to data files. Note that other features might still require materialization of partition values, such as [IcebergCompatV1](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#iceberg-compatibility-v1)

This feature does not impose any requirements on readers. All Delta readers must be able to read the table regardless of whether partition columns are materialized in the data files. When both this feature and `materializedVoidType` are supported, the latter feature provides the reader compatibility requirements for materialized `void` partition columns. If partition values are present in both parquet and AddFile metadata, Delta readers should continue to read partition values from AddFile metadata. The `AddFile` partition value for a `void` column is always `null`, whether or not the column is materialized in the data file. Also, [file-level statistics](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#per-file-statistics) should not be written for the partition column as it would repeat information already present in an AddFile's `partitionValues`.

Note that this table feature, as well as [icebergCompatV1](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#iceberg-compatibility-v1) (and related table features that require partition column materialization), if enabled, take priority over the `delta.writePartitionColumnsToParquet` table property. In other words, if a table feature is enabled that requires materialization of partition columns, and table metadata contains a `false` value for `delta.writePartitionColumnsToParquet`, partition columns must be materialized. The `void` exception above also takes priority over this property: without `materializedVoidType`, `void` partition columns must be omitted even when the property is `true`.

| Table feature enablement | Value of `delta.writePartitionColumnsToParquet` table property | Writer requirement |
| ------------------------ | -------------------------------------------------------------- | ------------------ |
| A writer feature requiring partition column materialization (eg. `materializePartitionColumns`) is *enabled* | `false` | Partition columns *must* be materialized in parquet data files |
| A writer feature requiring partition column materialization (eg. `materializePartitionColumns`) is *enabled* | `true` | Partition columns *must* be materialized in parquet data files |
| A writer feature requiring partition column materialization (eg. `materializePartitionColumns`) is *enabled* | unset | Partition columns *must* be materialized in parquet data files |
| No writer feature requiring partition column materialization is enabled | `false` | Partition columns *should not* be materialized in parquet data files |
| No writer feature requiring partition column materialization is enabled | `true` | Partition columns *should* be materialized in parquet data files |
| No writer feature requiring partition column materialization is enabled | unset | No requirement on partition column materialization |

The table above applies directly to non-`void` partition columns. When `materializePartitionColumns` is supported, the following requirements take precedence for a `void` partition column:

| `materializedVoidType` support | Writer requirement for the `void` partition column |
|--------------------------------|----------------------------------------------------|
| Supported                      | The column *must* be materialized                  |
| Not supported                  | The column *must* be omitted                       |

The value of having both the table feature `materializePartitionColumns` and the table property `delta.writePartitionColumnsToParquet` supported is that not every table is going to need the heightened requirement of only allowing writes from writers that understand `materializePartitionColumns`. In other words, `materializePartitionColumns` imposes a writer compatibility edge that `delta.writePartitionColumnsToParquet` does not.
