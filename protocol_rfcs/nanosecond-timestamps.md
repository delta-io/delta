# Table feature name / meaningful name
**Associated Github issue for discussions: https://github.com/delta-io/delta/issues/6081**

Nanosecond resolution timestamps are widely supported in related formats (Arrow, Parquet) and tools (Pandas, Iceberg, etc). However, currently the protocol only supports microsecond resolution timestamps. This proposes adding an additional primitive type for UTC nanosecond timestamps.


--------

## Add a new feature: Nanosecond timestamps (TimestampNanos)

Add a new feature:

> This feature introduces a new data type to support UTC timestamps with nanosecond resolution. For example: `1970-01-01 00:00:00.123456789`.
> The serialization method is described in Sections [Partition Value Serialization](#partition-value-serialization) and [Schema Serialization Format](#schema-serialization-format).
>
> To support this feature:
> - To have a column of TimestampNanos type in a table, the table must have Reader Version 3 and Writer Version 7. A feature name `timestampNanos` must exist in the table's `readerFeatures` and `writerFeatures`.

## Add nanosecond timestamps to the Schema Serialization Format

Add entry for nanosecond timestamps to the Primitive Types table:

> Nanosecond precision timestamp elapsed since the Unix epoch, 1970-01-01 00:00:00 UTC. When this is stored in a parquet file, its `isAdjustedToUTC` must be set to `true`. To use this type, a table must support a feature `timestampNanos`.

## Add nanosecond timestamps to the Partition Value Serialization

Add entry for nanosecond timestamps:

> Encoded as `{year}-{month}-{day} {hour}:{minute}:{second}.{nanosecond}`. For example: `1970-01-01 00:00:00.123456789`. Timestamps may also be encoded as an ISO8601 formatted timestamp adjusted to UTC timestamp such as `1970-01-01T00:00:00.123456789Z`

## Add Parquet Type mapping

Add a table entry for nanosecond timestamps:

> Physical type: `int64`
>
> Logical type: `TIMESTAMP(isAdjustedToUTC = true, units = nanoseconds)`

## Add entry to the Valid Feature Names in Table Features

In the appendix for Valid Feature Names, add `timestampNanos` as a valid feature for readers and writers.
