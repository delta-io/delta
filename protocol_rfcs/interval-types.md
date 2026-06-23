# Interval Types
**Associated Github issue for discussions: https://github.com/delta-io/delta/issues/7077**

This protocol change adds support for interval types. It consists of two changes to the protocol:

- One new reader/writer table feature
- Two new primitive types (year-month and day-second)

--------

> ***Add a new section in front of the [Primitive Types](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#primitive-types) section.***

# Interval Types Table Feature

This table feature (`intervalTypes`) adds the year-month and day-time interval types from ANSI SQL:

1. **interval year to month**: A signed number of months, e.g. `INTERVAL '1-6' YEAR TO MONTH` represents 18 (1 year + 6 months = 18 months).
2. **interval day to second**: A signed number of microseconds, e.g. `INTERVAL '1 00:00:01.000000' DAY TO SECOND` represents 86,401,000,000 (1 day + 1 second = 86,400,000,000 + 1,000,000 μs).

To support this feature:
- The table must be on **Reader Version 3** and **Writer Version 7**.
- The feature `intervalTypes` must be listed in the table `protocol`'s `readerFeatures` and `writerFeatures`.

## Type Definitions

In the schema, interval types are serialized as

- `interval year to month`
- `interval day to second`

When this table feature is supported:

- Readers must interpret `interval year to month` and `interval day to second` as signed counts of months and microseconds, respectively.
- Writers must serialize an interval field's type in `Metadata.schemaString` as `interval year to month` or `interval day to second`.

## Partition Value Serialization

Intervals can be a partition value, so we define Partition Value Serialization as the ANSI literal form for interval types as defined by the Spark SQL guide [1]. We provide an example below:

```
Interval Year Month: "INTERVAL '1-0' YEAR TO MONTH"
Interval Day Second: "INTERVAL '7 12:34:56.123456' DAY TO SECOND"
```

Where `'1-0'` refers to `years-months` and `'7 12:34:56.123456'` refers to `days hours:minutes:seconds.microseconds`.

## Per-file Statistics

Interval types do not support per-file statistics or data skipping. Writers must not record `minValues` or `maxValues` statistics for interval columns, and readers must not perform data skipping over interval columns. This is consistent with existing tables that contain interval types, which do not support per-file statistics for these columns.

## Parquet Format

We use raw `int32` values to represent year-month intervals and raw `int64` values to represent day-time intervals in Parquet. This allows us to support signed intervals and microsecond precision.

The choice of underlying Parquet representation involves design trade-offs that are still under discussion; these design decisions are tracked in the associated GitHub issue [2].

> ***Add new rows to the [Primitive Types](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#primitive-types) table.***

| Type Name | Description |
| --- | --- |
| interval year to month | Signed duration in the precision of months |
| interval day to second | Signed duration in the precision of microseconds |

# References

[1] https://spark.apache.org/docs/latest/sql-ref-literals.html#interval-literal

[2] https://github.com/delta-io/delta/issues/7077
