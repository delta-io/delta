# timestampNanos / Nanosecond timestamp primitive types
**Associated Github issue for discussions: https://github.com/delta-io/delta/issues/6081**

Nanosecond resolution timestamps are widely supported in related formats (Arrow, Parquet) and tools (Pandas, Iceberg, etc). However, currently the protocol only supports microsecond resolution timestamps. This proposes adding two additional primitive type for nanosecond timestamps: UTC, and without timezones, corresponding to the two already existing microsecond timestamp types.


--------

## Add a new feature: Nanosecond timestamps (TimestampNanos and TimestampNanosNtz)

Add a new feature:

> This feature introduces two new data types to support timestamps with nanosecond resolution. One UTC, one without a timezone corresponding to `TimestampNtz`. For example: `1970-01-01 00:00:00.123456789+00:00` and `1970-01-01 00:00:00.123456789`, respectively.
> The serialization method is described in Sections [Partition Value Serialization](#partition-value-serialization) and [Schema Serialization Format](#schema-serialization-format).
>
> To support this feature:
> - To have a column of `TimestampNanos` or `TimestampNanosNtz` type in a table, the table must have Reader Version 3 and Writer Version 7. A feature name `timestampNanos` must exist in the table's `readerFeatures` and `writerFeatures`.
> - The `timestampNtz` feature must also be enabled in `readerFeatures` and `writerFeatures`.

## Add nanosecond timestamps to the Schema Serialization Format

Add entries for nanosecond timestamps to the Primitive Types table:

> nanosecond timestamp: Nanosecond precision timestamp elapsed since the Unix epoch, 1970-01-01 00:00:00 UTC. When this is stored in a parquet file, its `isAdjustedToUTC` must be set to `true`. To use this type, a table must support feature `timestampNanos` and `timestampNtz`.

> nanosecond timestamp without timezone: Nanosecond precision timestamp elapsed since the Unix epoch, 1970-01-01 00:00:00 UTC, in local timezone. It doesn't have the timezone information, and a value of this type can map to multiple physical time instants. It should always be displayed in the same way, regardless of the local time zone in effect. When this is stored in a parquet file, its `isAdjustedToUTC` must be set to `false`. To use this type, a table must support feature `timestampNanos` and `timestampNtz`.

## Add nanosecond timestamps to the Partition Value Serialization

Add entries for nanosecond timestamps:

> nanosecond timestamp: Encoded as `{year}-{month}-{day} {hour}:{minute}:{second}.{nanosecond}`. For example: `1970-01-01 00:00:00.123456789`. Timestamps may also be encoded as an ISO8601 formatted timestamp adjusted to UTC timestamp such as `1970-01-01T00:00:00.123456789Z`

> nanosecond timestamp without timezone: Encoded as `{year}-{month}-{day} {hour}:{minute}:{second}.{nanosecond}`. For example: `1970-01-01 00:00:00.123456789`.

## Add Parquet Type mapping

Add table entries for nanosecond timestamps:

Nanosecond timestamp:

> Physical type: `int64`
>
> Logical type: `TIMESTAMP(isAdjustedToUTC = true, units = nanoseconds)`

Nanosecond timestamp without timezone:

> Physical type: `int64`
>
> Logical type: `TIMESTAMP(isAdjustedToUTC = false, units = nanoseconds)`

## Add entry to the Valid Feature Names in Table Features

In the appendix for Valid Feature Names, add `timestampNanos` as a valid feature for readers and writers.
