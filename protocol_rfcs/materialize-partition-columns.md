# Materialize Partition Columns

**Associated Github issue for discussions: https://github.com/delta-io/delta/issues/5555**

## Overview

Currently, Delta tables store partition column values primarily in the table metadata (specifically in the `partitionValues` field of `AddFile` actions), and by default these columns are not physically written into the Parquet data files themselves.

This RFC proposes a new writer-only table feature called `materializePartitionColumns`. When enabled, this feature requires partition columns to be physically materialized in Parquet data files alongside the data columns.

## Motivation

This feature provides a mechanism to require partition column materialization at the protocol level, ensuring all writers to the table comply with this requirement during the period when the feature is enabled.

Materializing partition columns enhances compatibility with Parquet readers that access Parquet files directly and do not interpret Delta’s AddFile metadata, as well as with Iceberg readers, which expect partition columns to be stored within the data files.

Additionally, having partition information embedded in the data files themselves enables more flexible data reorganization strategies. The same parquet files could be linked in future versions of a table that do not have the same (or any) partition columns.

--------


> ***New Section after Identity Columns section***
## Materialize Partition Columns

When this feature is enabled, partition columns are physically written to Parquet files alongside the data columns. To support this feature:
 - The table must be on Writer Version 7, and a feature name `materializePartitionColumns` must exist in the table `protocol`'s `writerFeatures`.

When supported:
 - The table respects metadata property `delta.enableMaterializePartitionColumnsFeature` for enablement of this feature. The writer feature `materializePartitionColumns` is auto-enabled when this property is set to `true`.
 - When the writer feature `materializePartitionColumns` is set in the protocol, writers must materialize partition columns into any newly created data file, placing them after the data columns in the parquet
  schema. This mimics the same partition column materialization requirement from [IcebergCompatV1](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#iceberg-compatibility-v1)
and
[IcebergCompatV2](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#iceberg-compatibility-v2). As such, the `materializePartitionColumns` feature can be seen as a subset of the requirements imposed by those features, providing the partition column materialization guarantee independently without requiring full
  Iceberg compatibility.
 - When the writer feature `materializePartitionColumns` is not set in the table protocol, writers are not required to write partition columns to data files. Note that other features might still require materialization of partition values, such as [IcebergCompatV1](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#iceberg-compatibility-v1)

This feature does not impose any requirements on readers. All Delta readers must be able to read the table regardless of whether partition columns are materialized in the data files. If partition values are present in both parquet and AddFile metadata, Delta readers should continue to read partition values from AddFile metadata.
