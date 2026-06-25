# Interval Types
**Associated Github issue for discussions: https://github.com/delta-io/delta/issues/7077**

This protocol change adds support for interval types. It consists of two changes to the protocol:

- One new reader/writer table feature
- Two new primitive types (year-month and day-second)

--------

> ***Add a new section in front of the [Primitive Types](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#primitive-types) section.***

# Interval Types Table Feature

This table feature (`intervalTypes`) adds the year-month and day-second interval types from ANSI SQL:

1. **interval year to month**: A signed number of months, e.g. `INTERVAL '1-6' YEAR TO MONTH` represents 18 (1 year + 6 months = 18 months).
2. **interval day to second**: A signed number of microseconds, e.g. `INTERVAL '1 00:00:01.000000' DAY TO SECOND` represents 86,401,000,000 (1 day + 1 second = 86,400,000,000 + 1,000,000 μs).

To support this feature:
- The table must be on **Reader Version 3** and **Writer Version 7**.
- The feature `intervalTypes` must be listed in the table `protocol`'s `readerFeatures` and `writerFeatures`.

## Type Definitions

In the schema, interval types are serialized in `Metadata.schemaString` as

- `interval year to month`
- `interval day to second`

These are the canonical type-name strings. ANSI SQL also permits narrowed spellings that denote the same two types: for year-month, `interval year` and `interval month`; for day-second, `interval day`, `interval hour`, `interval minute`, `interval second`, and any `<start> to <end>` range between those fields (e.g. `interval day to minute`, `interval hour to second`). Mixed-family spellings (e.g. `interval month to day`) are not valid. Tables written by existing engines may use these narrowed spellings.

### Reader Requirements

When this table feature is supported, readers must:

- Interpret `interval year to month` as a signed count of months, and `interval day to second` as a signed count of microseconds.
- Accept the narrowed spellings above and normalize each to its family: any year-month spelling is treated as `interval year to month`, and any day-second spelling is treated as `interval day to second`.

### Writer Requirements

When this table feature is supported, writers must:

- Serialize an interval field's type in `Metadata.schemaString` using the canonical `interval year to month` or `interval day to second` form.

## Partition Value Serialization

Intervals can be a partition value, so we define Partition Value Serialization as the ANSI literal form for interval types as defined by the Spark SQL guide [1]. We provide an example below:

```
Interval Year Month: "INTERVAL '1-0' YEAR TO MONTH"
Interval Day Second: "INTERVAL '7 12:34:56.123456' DAY TO SECOND"
```

Where `'1-0'` refers to `years-months` and `'7 12:34:56.123456'` refers to `days hours:minutes:seconds.microseconds`.

## Per-file Statistics

Interval columns do not support `minValues`/`maxValues` statistics or data skipping. Writers must not record `minValues` or `maxValues` for interval columns, and readers must not perform data skipping over interval columns. The per-column `nullCount` and the per-file `numRecords` statistics are unaffected and are still recorded as normal, since they do not require interpreting interval values. This is consistent with existing tables that contain interval types, which do not record `minValues`/`maxValues` for these columns.

## Parquet Format

We use raw `int32` values to represent year-month intervals and raw `int64` values to represent day-second intervals in Parquet. This allows us to support signed intervals & microsecond precision while matching existing interval types.

## Feature Interactions

Beyond the partition-value and statistics behavior described above, interval types have no special interactions with other table features.

> ***Add new rows to the [Primitive Types](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#primitive-types) table.***

| Type Name | Description |
| --- | --- |
| interval year to month | Signed duration in the precision of months |
| interval day to second | Signed duration in the precision of microseconds |

# References

[1] https://spark.apache.org/docs/latest/sql-ref-literals.html#interval-literal

[2] https://github.com/delta-io/delta/issues/7077
