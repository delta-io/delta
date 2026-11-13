# Parameterized-Precision Timestamps (`timestampPrecision`)
**Associated Github issue for discussions: https://github.com/delta-io/delta/issues/XXXX**

Delta currently supports only **microsecond**-resolution timestamps (`timestamp` and `timestamp without time zone`). Nanosecond-resolution timestamps are widely supported in adjacent formats and tools (Parquet, Arrow, Iceberg, Pandas, and, as of SPARK-56822, Apache Spark itself), and coarser resolutions (second / millisecond) are common in the SQL standard's `TIMESTAMP(p)` syntax. This RFC proposes a **single, parameterized** timestamp type family that carries an explicit fractional-second precision `p ∈ [0, 9]`:

- `timestamp(p)`: adjusted to UTC (`isAdjustedToUTC = true`), the parameterized form of the existing `timestamp`.
- `timestamp without time zone(p)`: zone-less (`isAdjustedToUTC = false`), the parameterized form of the existing `timestamp without time zone`.

`p` is the number of fractional-second digits: `p = 0` is second resolution, `p = 3` millisecond, `p = 6` microsecond, `p = 9` nanosecond. This is the same precision axis used by Spark's `TIMESTAMP(p)` / `TIMESTAMP_NTZ(p)` type-name grammar.

--------

> ***New Section after the [Clustered Table](#clustered-table) section***

# Parameterized-Precision Timestamps

This feature generalizes the microsecond-only `timestamp` and `timestamp without time zone` types into a parameterized family that carries an explicit fractional-second precision `p`, where `p` is an integer in `[0, 9]` (`0` = second, `3` = millisecond, `6` = microsecond, `9` = nanosecond). For example, `1970-01-01T00:00:00.123456789Z` (`timestamp(9)`) and `1970-01-01 00:00:00.123` (`timestamp without time zone(3)`).

The two parameterized types are:
- `timestamp(p)`: adjusted to UTC. When stored in a Parquet file its `isAdjustedToUTC` must be `true`.
- `timestamp without time zone(p)`: zone-less; a value can map to multiple physical instants and must always be displayed the same way regardless of the session time zone. When stored in a Parquet file its `isAdjustedToUTC` must be `false`.

The serialization is described in the sections [Schema Serialization Format](#schema-serialization-format) (type name), [Partition Value Serialization](#partition-value-serialization) (partition values), and the Delta -> Parquet type-mapping table (physical encoding).

To support this feature:
- To have a column of type `timestamp(p)` or `timestamp without time zone(p)` with `p != 6` in a table, the table must have Reader Version 3 and Writer Version 7, and the feature name `timestampPrecision` must exist in the table's `readerFeatures` and `writerFeatures`.
- If a `timestamp without time zone(p)` column is present, the `timestampNtz` feature must also exist in the table's `readerFeatures` and `writerFeatures`.

## Writer Requirements for Parameterized-Precision Timestamps

When Parameterized-Precision Timestamps are supported, writers must:

- Serialize the declared precision `p` as part of the type name in the table schema (see [Schema Serialization Format](#schema-serialization-format)), and serialize `p = 6` columns using the unparameterized `timestamp` / `timestamp without time zone` names.
- Store values in Parquet as `int64` using the **coarsest Parquet `TIMESTAMP` time unit whose resolution is at least `p`**.

  with `isAdjustedToUTC = true` for `timestamp(p)` and `isAdjustedToUTC = false` for `timestamp without time zone(p)`. The Parquet logical type therefore records the physical time unit, not the declared precision `p`; `p` is recovered from the Delta schema.
- Floor each value to the declared precision `p` before writing, so that a value stored in a wider physical unit carries zeros in the positions below `p`.
- Record per-column statistics at the declared precision (see [Per-file Statistics](#per-file-statistics)).
- Serialize partition values at the declared precision (see [Partition Value Serialization](#partition-value-serialization)).
- Reject on overflow: every value must fit inside its Parquet physical time unit representation, and writers must error rather than silently wrap.

| Precision `p` | Physical unit | Representable range (approx.)                          |
  |-|-|--------------------------------------------------------|
| `0` to `3` | `int64` milliseconds | ~292 million years                                     |
| `4` to `6` | `int64` microseconds | ~292,000 years               |
| `7` to `9` | `int64` nanoseconds | ~1677 to 2262 |

> ***Update the [Primitive Types](#primitive-types) table in the [Schema Serialization Format](#schema-serialization-format) section***

Add the following rows to the Primitive Types table:

> `timestamp(p)` | Timestamp with `p` fractional-second digits (`p` an integer in `[0, 9]`) elapsed since the Unix epoch, 1970-01-01 00:00:00 UTC. When stored in a Parquet file, `isAdjustedToUTC` must be `true`. `timestamp(6)` is equivalent to `timestamp` and must be serialized as `timestamp`. To use a precision `p != 6`, a table must support the `timestampPrecision` feature. See section [Parameterized-Precision Timestamps](#parameterized-precision-timestamps).
>
> `timestamp without time zone(p)` | Zone-less timestamp with `p` fractional-second digits (`p` an integer in `[0, 9]`). It has no timezone information and a value can map to multiple physical instants; it must always be displayed the same way regardless of the session time zone. When stored in a Parquet file, `isAdjustedToUTC` must be `false`. `timestamp without time zone(6)` is equivalent to `timestamp without time zone` and must be serialized as such. To use a precision `p != 6`, a table must support the `timestampPrecision` and `timestampNtz` features. See section [Parameterized-Precision Timestamps](#parameterized-precision-timestamps).

> ***Update the footnote `[^1]` under [Per-file Statistics](#per-file-statistics)***

Replace the footnote:

> `[^1]`: String columns are cut off at a fixed prefix length. Timestamp columns (`timestamp`, `timestamp without time zone`, and their parameterized `(p)` forms, for any `p`) are truncated down to milliseconds.

> ***Update the timestamp rows in the [Partition Value Serialization](#partition-value-serialization) table***

Add the following rows:

> `timestamp(p)` | Encoded as `{year}-{month}-{day} {hour}:{minute}:{second}` when `p = 0`, otherwise `{year}-{month}-{day} {hour}:{minute}:{second}.{fraction}` where `{fraction}` has exactly `p` digits (e.g. `1970-01-01 00:00:00.123456789` for `p = 9`). It may also be encoded as an ISO8601 timestamp adjusted to UTC, e.g. `1970-01-01T00:00:00.123456789Z`.
>
> `timestamp without time zone(p)` | Encoded as `{year}-{month}-{day} {hour}:{minute}:{second}` when `p = 0`, otherwise `{year}-{month}-{day} {hour}:{minute}:{second}.{fraction}` where `{fraction}` has exactly `p` digits (e.g. `1970-01-01 00:00:00.123` for `p = 3`).

> ***Update the Delta -> Parquet type-mapping table (the "Delta Type Name | Parquet Physical Type | Parquet Logical Type" table)***

Add the following rows:

```
`timestamp(p)`, `p ∈ {0,1,2,3}` | `int64` | `TIMESTAMP(isAdjustedToUTC = true, units = milliseconds)`
`timestamp(p)`, `p ∈ {4,5}` | `int64` | `TIMESTAMP(isAdjustedToUTC = true, units = microseconds)`
`timestamp(p)`, `p ∈ {7,8,9}` | `int64` | `TIMESTAMP(isAdjustedToUTC = true, units = nanoseconds)`
`timestamp without time zone(p)`, `p ∈ {0,1,2,3}` | `int64` | `TIMESTAMP(isAdjustedToUTC = false, units = milliseconds)`
`timestamp without time zone(p)`, `p ∈ {4,5}` | `int64` | `TIMESTAMP(isAdjustedToUTC = false, units = microseconds)`
`timestamp without time zone(p)`, `p ∈ {7,8,9}` | `int64` | `TIMESTAMP(isAdjustedToUTC = false, units = nanoseconds)`
```

> ***Add a row to the [Valid Feature Names in Table Features](#valid-feature-names-in-table-features) table***

> [Parameterized-Precision Timestamps](#parameterized-precision-timestamps) | `timestampPrecision` | Readers and writers