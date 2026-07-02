# Interval Types
**Associated Github issue for discussions: https://github.com/delta-io/delta/issues/7077**

This protocol change adds support for interval types (as defined [here](https://spark.apache.org/docs/latest/sql-ref-datatypes.html)). It consists of two changes to the protocol:

- One new reader/writer table feature
- Two new primitive types (year-month and day-second)

--------

> ***Add a new section in front of the [Primitive Types](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#primitive-types) section.***

# Interval Types Table Feature

This table feature (`intervalTypes`) adds the year-month and day-second [interval types](https://spark.apache.org/docs/latest/sql-ref-datatypes.html) from ANSI SQL:

1. **interval year to month**: A signed number of months, e.g. `INTERVAL '1-6' YEAR TO MONTH` represents 18 (1 year + 6 months = 18 months).
2. **interval day to second**: A signed number of microseconds, e.g. `INTERVAL '1 12:24:36.000022' DAY TO SECOND` represents 131,076,000,022 (1 day + 12 hours + 24 minutes + 36 seconds + 22 microseconds = 86,400,000,000 + 43,200,000,000 + 1,440,000,000 + 36,000,000 + 22 = 131,076,000,022).

To support this feature:
- The table must be on Reader Version 3 and Writer Version 7.
- The feature `intervalTypes` must be listed in the table `protocol`'s `readerFeatures` and `writerFeatures`.

## Type Definitions

In the schema, interval types are serialized in `Metadata.schemaString` (case-sensitive) as

- `interval year to month`
- `interval day to second`

Intervals have two families: year-month (made up of the fields `year` and `month`) and day-second (made up of the fields `day`, `hour`, `minute`, and `second`). `interval year to month` and `interval day to second` are the canonical type-name strings for these two families.

ANSI SQL also permits narrowed spellings that denote the same two families: for year-month, `interval year` and `interval month`; for day-second, `interval day`, `interval hour`, `interval minute`, `interval second`, and any `<start> to <end>` range over the ordered fields `day`, `hour`, `minute`, `second` (e.g. `interval day to minute`, `interval hour to second`). A spelling is not valid if it is multi-family — that is, it combines fields from both families (for example, `interval month to second`). A spelling is also not valid if its fields are in the wrong order, going from a shorter unit to a longer one (for example, `interval second to day` or `interval month to year`).

Regardless of which spelling is used, the stored value is the same: every year-month spelling stores a signed count of months, and every day-second spelling stores a signed count of microseconds. The spelling affects only how a value is displayed, not how it is stored.

Interval types are permitted anywhere a primitive type is permitted: as a top-level column, as a nested struct field, as an array element type, and as a map key or value type. For example:

```
{
  "type": "struct",
  "fields": [
    { "name": "duration_ym", "type": "interval year to month", "nullable": true, "metadata": {} },
    { "name": "duration_dt", "type": "interval day to second", "nullable": false, "metadata": {} },
    {
      "name": "durations",
      "type": { "type": "array", "elementType": "interval day to second", "containsNull": true },
      "nullable": true,
      "metadata": {}
    }
  ]
}
```

### Reader Requirements

When this table feature is supported, readers must:

- Interpret `interval year to month` as a signed count of months, and `interval day to second` as a signed count of microseconds.
- Accept the narrowed spellings above and normalize each to its family: any year-month spelling is treated as `interval year to month`, and any day-second spelling is treated as `interval day to second`.

### Writer Requirements

When this table feature is supported, writers must:

- Serialize an interval field's type in `Metadata.schemaString` using the canonical `interval year to month` or `interval day to second` form.
- Never write an interval type to a table unless the `intervalTypes` feature is present in the table `protocol`'s `readerFeatures` and `writerFeatures`. When a writer introduces an interval type into the schema of a table that does not yet support the feature — whether at table creation or via schema evolution — it must add `intervalTypes` to both lists in the same commit, so that the feature and the interval type are committed together.

## Partition Value Serialization

Intervals can be a partition value, so we define Partition Value Serialization as the ANSI literal form for interval types as defined by the Spark SQL guide [1]. We provide an example below:

```
Interval Year Month: "INTERVAL '1-0' YEAR TO MONTH"
Interval Day Second: "INTERVAL '7 12:34:56.123456' DAY TO SECOND"
```

Where `'1-0'` refers to `years-months` and `'7 12:34:56.123456'` refers to `days hours:minutes:seconds.microseconds`.

Interval partition values must not be used for partition pruning. Consistent with the data-skipping restriction for interval columns (see [Per-file Statistics](#per-file-statistics)), readers must not eliminate files based on interval partition values.

## Per-file Statistics

Interval columns do not support `minValues`/`maxValues` statistics or data skipping. Writers must not record `minValues` or `maxValues` for interval columns, and readers must not perform data skipping over interval columns. The per-column `nullCount` and the per-file `numRecords` statistics are unaffected and are still recorded as normal, since they do not require interpreting interval values.

Since clustered tables require per-column statistics, including `minValues` and `maxValues`, a writer must not use an interval column as a clustering column.

## Parquet Format

Interval values are stored using a raw Parquet physical type with no logical-type annotation:

- `interval year to month` is stored as a Parquet `int32` holding the signed count of months.
- `interval day to second` is stored as a Parquet `int64` holding the signed count of microseconds.

Because no Parquet logical type is written, an interval column is physically indistinguishable from a Parquet `int32`/`int64` (i.e. a Delta `integer`/`long`); the interval semantics are carried solely by the Delta schema in `Metadata.schemaString`. This representation supports signed intervals and microsecond precision. 

## Feature Interactions

Beyond the partition-value and statistics behavior described above, and the restrictions listed in [Error Conditions](#error-conditions), interval types have no special interactions with other table features.

## Error Conditions

- **Unrecognized type-name strings.** Type-name matching is case-sensitive. A reader that encounters an interval type-name string that is not one of the recognized canonical or narrowed spellings, including a multi-family spelling such as `interval month to second`, or a case variant such as `INTERVAL Year To Month`, must reject the schema with an error rather than silently coercing it to a supported type.
- **Feature not present.** A writer must add `intervalTypes` to the table `protocol`'s `readerFeatures` and `writerFeatures` whenever it introduces an interval type into the table's schema — whether at table creation or via schema evolution — if the feature is not already present, committing the feature and the interval type together (see [Writer Requirements](#writer-requirements)). Re-serializing an already-committed schema, such as when writing a checkpoint, does not introduce an interval type and must not add the `intervalTypes` feature.
- **Value overflow on write.** An `interval year to month` value is stored as a signed `int32` count of months, so it must lie in the inclusive range `INTERVAL '-178956970-8' YEAR TO MONTH` to `INTERVAL '178956970-7' YEAR TO MONTH` (roughly ±179 million years). An `interval day to second` value is stored as a signed `int64` count of microseconds, so it must lie in the inclusive range `INTERVAL '-106751991 04:00:54.775808' DAY TO SECOND` to `INTERVAL '106751991 04:00:54.775807' DAY TO SECOND` (roughly ±106,751,991 days, or about 292 thousand years). A writer must reject any value that falls outside these bounds.
- **Malformed or out-of-range partition values.** When reading, a partition value that is not a valid ANSI interval literal, or whose decoded value does not fit the column's underlying `int32`/`int64` range, must be rejected with an error.
- **IcebergCompat incompatibility.** Apache Iceberg has no interval type. When any of the `icebergCompatV1`, `icebergCompatV2`, or `icebergCompatV3` features is enabled, a writer must reject any schema containing an interval type.

> ***Add new rows to the [Primitive Types](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#primitive-types) table.***

| Type Name | Description |
| --- | --- |
| interval year to month | Signed duration in the precision of months |
| interval day to second | Signed duration in the precision of microseconds |

> ***Add new rows to the [Delta Data Type to Parquet Type Mappings](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#delta-data-type-to-parquet-type-mappings) table.***

| Delta Type Name | Parquet Physical Type | Parquet Logical Type |
| --- | --- | --- |
| interval year to month | `int32` | |
| interval day to second | `int64` | |

# References

[1] https://spark.apache.org/docs/latest/sql-ref-literals.html#interval-literal

[2] https://github.com/delta-io/delta/issues/7077
