# Materialize Partition Columns

## Overview

Currently, Delta tables store partition column values primarily in the table metadata (specifically in the `partitionValues` field of `AddFile` actions), and by default these columns are not physically written into the Parquet data files themselves.

This RFC proposes a new writer-only table feature called `materializePartitionColumns`. When enabled, this feature requires partition columns to be physically materialized in Parquet data files alongside the data columns.

## Motivation

This feature provides a mechanism to require partition column materialization at the protocol level, ensuring all writers to the table comply with this requirement during the period when the feature is enabled.

Materializing partition columns enhances compatibility with Parquet readers that access Parquet files directly and do not interpret Delta’s AddFile metadata, as well as with Iceberg readers, which expect partition columns to be stored within the data files.

Additionally, having partition information embedded in the data files themselves enables more flexible data reorganization strategies, as files can be physically rearranged without strict partition directory constraints while still maintaining partition information.

**For further discussions about this protocol change, please refer to the Github issue - https://github.com/delta-io/delta/issues/5555**

--------


> ***New Section after Identity Columns section***
## Materialize Partition Columns

When this feature is enabled, partition columns are physically written to Parquet files alongside the data columns, which can improve flexibility with respect to data layout changes in the future, and make these data files easier to interpret for readers unfamiliar with partition values. To support this feature:
 - The table must be on Writer Version 7, and a feature name `materializePartitionColumns` must exist in the table `protocol`'s `writerFeatures`.

When supported:
 - The table respects metadata property `delta.enableMaterializePartitionColumnsFeature` for enablement of this feature. The writer feature `materializePartitionColumns` is auto-supported when this property is set to `true`.
 - When the writer feature `materializePartitionColumns` is set in the protocol, writers must require that partition column values are materialized into any newly created data file, placed after the data columns in the parquet schema.
 - When the writer feature `materializePartitionColumns` is not set in the table protocol, writers are not required to write partition columns to data files. Note that other features might still require materialization of partition values, such as [Iceberg Compatibility V1](#iceberg-compatibility-v1)

This feature does not impose any requirements on readers. All Delta readers must be able to read the table regardless of whether partition columns are materialized in the data files. If partition values are present in both parquet and AddFile metadata, readers should continue to read partition values from AddFile metadata.
