# IcebergCompatV3

This protocol change introduces a compatibility flag, which ensures that a delta table can be safely
read and written as an Apache Iceberg™ format table, similar to
[IcebergCompatV1](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#iceberg-compatibility-v1)
and
[IcebergCompatV2](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#iceberg-compatibility-v2).

--------

# IcebergCompatV3
> ***New Section after [Iceberg Compatibility V2](#iceberg-compatibility-v2)***

# Iceberg Compatibility V3

This table feature (`icebergCompatV3`) ensures that Delta tables can be converted to Apache Iceberg™ format, though this table feature does not implement or specify that conversion.

To support this feature:
- Since this table feature depends on Column Mapping, the table must be on Reader Version = 2, or it must be on Reader Version >= 3 and the feature `columnMapping` must exist in the `protocol`'s `readerFeatures`.
- The table must be on Writer Version 7.
- The feature `icebergCompatV3` must exist in the table protocol's `writerFeatures`.

This table feature is enabled when the table property `delta.enableIcebergCompatV3` is set to `true`.

## Writer Requirements for IcebergCompatV3

When this feature is supported and enabled, writers must:
- Require that Column Mapping be enabled and set to either `name` or `id` mode
- Require that Row Tracking to be enabled on the table.
  - Materialized Row ID column must use field ID 2147483540
  - Materialized Row Commit Version column must use field ID 2147483539
- Require that the nested `element` field of ArrayTypes and the nested `key` and `value` fields of MapTypes be assigned 32 bit integer identifiers. The requirement to ID allocation is the same as that in IcebergCompatV2.
- Require that IcebergCompatV1 and IcebergCompatV2 are not active on the table
- Require that partition column values be materialized when writing Parquet data files
- Require that all new `AddFile`s committed to the table have the `numRecords` statistic populated in their `stats` field
- Require writing timestamp columns as int64
- Block replacing partitioned tables with a differently-named partition spec
  - e.g. replacing a table partitioned by `part_a INT` with partition spec `part_b INT` must be blocked
  - e.g. replacing a table partitioned by `part_a INT` with partition spec `part_a LONG` is allowed