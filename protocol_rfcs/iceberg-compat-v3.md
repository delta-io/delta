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

> **NOTE:** Unlike IcebergCompatV1 and IcebergCompatV2, this feature does _NOT_ forbid supporting and enabling Deletion Vectors on the table.

## Writer Requirements for IcebergCompatV3

When this feature is supported and enabled, writers must:
- Require that Column Mapping be enabled and set to either `name` or `id` mode
- Require that Row Tracking to be enabled on the table.
  - Materialized Row ID column must use field ID 2147483540
  - Materialized Row Commit Version column must use field ID 2147483539
- Require that the nested `element` field of ArrayTypes and the nested `key` and `value` fields of MapTypes be assigned 32 bit integer identifiers. The requirement to ID allocation is the same as that in IcebergCompatV2.
- Require that IcebergCompatV1 and IcebergCompatV2 are not active on the table
- Require that partition column values be materialized when writing Parquet data files
- Require that every `AddFile` that backs the Iceberg table has the `numRecords` statistic populated in its `stats` field, including files committed before this feature was enabled
- Require that `timestamp` and `timestampNTZ` columns in data files written while this feature is enabled use the INT64 physical type with microsecond precision. The `timestamp` type must be marked as UTC-adjusted and the `timestampNTZ` type must be marked as non-UTC-adjusted
- Block replacing partitioned tables with a differently-named partition spec
  - e.g. replacing a table partitioned by `part_a INT` with partition spec `part_b INT` must be blocked
  - e.g. replacing a table partitioned by `part_a INT` with partition spec `part_a LONG` is allowed
- Require that the table schema contains only data types in the following allow-list: [`byte`, `short`, `integer`, `long`, `float`, `double`, `decimal`, `string`, `binary`, `boolean`, `timestamp`, `timestampNTZ`, `date`, `array`, `map`, `struct`, `variant`, `geometry`, `geography`].
- Require that every partition column is of one of the following types: [`byte`, `short`, `integer`, `long`, `float`, `double`, `decimal`, `string`, `binary`, `boolean`, `date`, `timestamp`, `timestampNTZ`].
- When the [Type Widening](#type-widening) table feature is supported, require that all type changes applied on the table are in the following allow-list:
  - `byte` -> `short`, `integer`, or `long`
  - `short` -> `integer` or `long`
  - `integer` -> `long`
  - `float` -> `double`
  - `decimal(p, s)` -> `decimal(q, s)` where `q > p`
- Require that any column write default is a literal value. This allows the default to be faithfully exported as an Iceberg schema default value.

## Enablement of IcebergCompatV3

When `icebergCompatV3` is enabled on a table that already contains data, the Writer Requirements above apply to the existing data files as follows:
- `AddFile` metadata requirements, such as the `numRecords` statistic, apply to every `AddFile` in the table. A writer can satisfy them without rewriting the data file by re-committing the `AddFile` for the same data file with the missing metadata populated and `dataChange` set to `false`.
- Physical data file requirements, such as the reserved field IDs on the materialized Row Tracking columns and materialized partition values, apply to every data file in the table. If an existing data file doesn't satisfy them, the writer must either rewrite that file or refuse to enable the feature.
- The INT64 timestamp requirement applies only to data files written while this feature is enabled. Existing data files may keep `timestamp` columns stored as INT96.

> **NOTE:** The Iceberg specification only defines INT64 timestamps in Parquet, so Iceberg readers aren't required to read INT96 timestamps. The Apache Iceberg reference implementation reads them for backward compatibility. Writers that need such files to be readable by every Iceberg reader should rewrite them before or after enabling the feature.


