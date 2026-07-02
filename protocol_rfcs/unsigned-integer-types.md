# Unsigned Integer Types

**Associated Github issue for discussions: https://github.com/delta-io/delta/issues/6176**

This RFC proposes support for **unsigned integer column types** (`uint8`, `uint16`, `uint32`,
`uint64`) in Delta Lake, gated behind a new ReaderWriter table feature `unsignedIntegerTypes`.

Unsigned integers are first-class types in Apache Arrow and Apache Parquet (which defines the
unsigned `INTEGER` / `UINT_8/16/32/64` logical types) and are widely produced by data tooling
(Rust, C/C++, and the Arrow-native Python ecosystem — pandas, Polars, PyArrow). Today, writers
such as `delta-rs` must coerce unsigned columns to signed types, which is lossy for values above
the corresponding signed maximum (e.g. a `uint8` value of `200` cannot be stored as `byte`); see
`delta-io/delta-rs#2175`. This RFC defines a protocol-level representation so unsigned integers
can be stored and read back losslessly and interoperably.

Delta's primitive type system has historically mirrored Spark SQL, which has no unsigned types.
This feature is therefore introduced as an explicit table feature: clients that do not implement
it will refuse to read or write such tables rather than silently misinterpret data.

A reference implementation exists in **`delta-kernel-rs` and `delta-rs`** (Rust + Python),
covering end-to-end write/read roundtrip for all four widths including `uint64` values above
`i64::MAX`, correct unsigned statistics, data-skipping, and unsigned partition columns. The
on-disk Parquet was confirmed portable (read back with correct unsigned values by Arrow C++ and
DuckDB), and a feature-unaware Delta reader was confirmed to reject such tables cleanly. **A Spark
implementation is pending acceptance of this RFC** (see [Engine behavior](#engine-behavior-and-spark-mapping)).

--------

> ***New table feature. Add `unsignedIntegerTypes` to the [Table Features](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#table-features) registry and add the section below.***

# Unsigned Integer Types

Unsigned integer types are supported via the `unsignedIntegerTypes` table feature. This is a
**ReaderWriter feature**: a table containing any column of an unsigned integer type (including
nested within `struct`, `array`, or `map` types) must list `unsignedIntegerTypes` in **both** the
`readerFeatures` and `writerFeatures` of its `protocol` action. Because those fields exist only at
**Reader Version 3** and **Writer Version 7**, such a table must use those protocol versions. This
follows the precedent of the accepted `variantType` and `typeWidening` type features.

> While this RFC is in the *proposed* state (before it is folded into PROTOCOL.md), experimental
> implementations should use the temporary feature name `unsignedIntegerTypes-dev` to signal no
> forward-compatibility guarantee, per the Delta RFC process. The final, stable feature name is
> `unsignedIntegerTypes`; `-dev` tables are not guaranteed to be readable by the stable release and
> the suffix is dropped upon acceptance.

## Reader Requirements for Unsigned Integer Types

When the `unsignedIntegerTypes` feature is supported, readers must:
- Parse the schema primitive type names `uint8`, `uint16`, `uint32`, `uint64`.
- Read the corresponding Parquet columns using **unsigned** interpretation of the physical bits
  (per the [Parquet mapping](#delta-data-type-to-parquet-type-mappings)).
- Interpret per-file statistics and partition values for these columns using **unsigned** ordering
  and parsing.

A reader that does not implement `unsignedIntegerTypes` must not read a table that lists it in
`readerFeatures`.

## Writer Requirements for Unsigned Integer Types

When the feature is supported, writers must:
- Add `unsignedIntegerTypes` to both `readerFeatures` and `writerFeatures` when creating a table
  whose schema contains an unsigned integer column, or when adding/altering such a column. Adding
  an unsigned column to an existing table enables the feature (upgrading the protocol to reader 3 /
  writer 7 as needed); a table without the feature must not contain unsigned columns.
- Serialize types, Parquet data, statistics, and partition values as specified below.

> ***Add to the [Primitive Types](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#primitive-types) list in the Schema Serialization Format section.***

## Primitive Types

Type Name | Description
-|-
uint8 | 1-byte unsigned integer. Range: 0 to 255
uint16 | 2-byte unsigned integer. Range: 0 to 65535
uint32 | 4-byte unsigned integer. Range: 0 to 4294967295
uint64 | 8-byte unsigned integer. Range: 0 to 18446744073709551615

These serialize in a `StructField` like any other primitive type:

```json
{ "name": "id", "type": "uint64", "nullable": false, "metadata": {} }
```

The names `uint8/uint16/uint32/uint64` are adopted for alignment with Apache Arrow and Apache
Parquet logical-type naming (unambiguous about bit width), rather than word-style names such as
`ubyte/ushort/uint/ulong`.

> ***Add to the [Delta Data Type to Parquet Type Mappings](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#delta-data-type-to-parquet-type-mappings) table.***

## Delta Data Type to Parquet Type Mappings

Parquet has no unsigned *physical* type; unsignedness is conveyed by the `INTEGER` *logical* type
with `signed = false` (equivalently the legacy `UINT_8/16/32/64` converted types) on a signed
physical integer. This mirrors how `byte`/`short` are already stored — physical `int32` with
`INT(bitwidth = 8/16, signed = true)`:

Delta type | Parquet physical type | Parquet logical type
-|-|-
uint8 | int32 | `INT(bitwidth = 8, signed = false)`
uint16 | int32 | `INT(bitwidth = 16, signed = false)`
uint32 | int32 | `INT(bitwidth = 32, signed = false)`
uint64 | int64 | `INT(bitwidth = 64, signed = false)`

The value is the physical integer's bit pattern interpreted as unsigned (e.g. a `uint32` of
`4294967295` occupies the `int32` slot as `0xFFFFFFFF`). Parquet column statistics for these
columns are computed using unsigned ordering, as required by the Parquet specification for the
unsigned `INTEGER` logical type.

> ***Update the [Per-file Statistics](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#per-file-statistics) section.***

## Per-file Statistics

`minValues`, `maxValues`, and `nullCount` are produced for unsigned integer columns as for other
numeric types, using **unsigned ordering** for min/max.

`uint8`, `uint16`, and `uint32` min/max values are always `< 2^53` and are encoded as JSON numbers,
like other numeric types.

**`uint64` min/max values are encoded as canonical base-10 decimal strings** — a deliberate
departure from the JSON-number encoding used by other numeric types, because JSON numbers cannot
represent `uint64` values above `2^53 - 1` without precision loss. Readers must parse them as
unsigned 64-bit integers:

```json
{ "minValues": { "id": "0" }, "maxValues": { "id": "18446744073709551615" } }
```

There is precedent for non-numeric statistics encoding in Delta (Variant statistics are stored as
encoded strings). Decimal-string encoding is preferred over the alternative of omitting min/max for
`uint64` values above `2^53 - 1` because it preserves data-skipping for all values; engines that do
not implement the feature never read these statistics (they reject the table), and statistics are
optional, so any reader missing them degrades safely to a full scan. In checkpoints, the
`stats_parsed` (typed) representation stores `uint64` min/max using the native unsigned Parquet type;
the decimal-string encoding applies only to the JSON `stats` string in `add`/`remove`/checkpoint
actions.

> ***Update the [Partition Value Serialization](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#partition-value-serialization) section.***

## Partition Value Serialization

Partition values for unsigned integer columns follow the standard format for numeric types — the
base-10 string representation of the number — which for unsigned values is their canonical unsigned
decimal string (e.g. a `uint64` partition value of `2^64 - 1` is `"18446744073709551615"`). Readers
must parse them as unsigned.

## Compatibility with Other Delta Features

Feature | Behavior with unsigned integer types
-|-
Column Mapping | Supported. The unsigned type is a property of the logical column; the physical Parquet representation (`int32`/`int64` + unsigned logical type) is unaffected by name/id mapping.
Partition Columns | Supported (see [Partition Value Serialization](#partition-value-serialization)).
Clustering / Data Skipping | Supported, using unsigned ordering for min/max (see [Per-file Statistics](#per-file-statistics)).
Change Data Feed | Supported. Unsigned columns appear in CDC data/log with the same semantics as other numeric columns.
Checkpoints (V2 / `stats_parsed`) | Supported. `stats_parsed` uses the native unsigned Parquet type; the JSON `stats` string uses the encoding in [Per-file Statistics](#per-file-statistics).
Default Values | Supported. A default value for an unsigned column must be within the column type's range.
Generated Columns | A generated column may use unsigned columns in its expression; whether an unsigned column may itself be a generated column is left to the implementation and out of scope for this RFC.
Identity Columns | **Not supported.** Identity columns are defined over signed `long`; unsigned identity columns are out of scope.
Type Widening | Out of scope for this RFC. Widening between unsigned widths (`uint8 → uint16 → …`) or unsigned↔signed/decimal is not defined here and would be proposed as a follow-up to the `typeWidening` feature.
Iceberg Compatibility (`icebergCompatV1/V2/V3`) | **Not permitted.** Apache Iceberg has no unsigned integer types, so a writer must reject enabling Iceberg compatibility on a table containing unsigned columns (and reject adding unsigned columns to an Iceberg-compatible table), consistent with how `typeWidening` restricts type changes under Iceberg compatibility.

## Engine behavior and Spark mapping

**Non-implementing engines.** The table-feature gate guarantees safety: an engine that does not
implement `unsignedIntegerTypes` refuses the table (it appears in `readerFeatures`) rather than
misreading it. This was verified against a feature-unaware Delta reader.

**Spark.** Spark SQL has no unsigned integer types. This RFC proposes the following mapping for a
Spark/Delta implementation, chosen so the narrower widths are lossless and `uint64` has a
well-defined, lossless representation:

Delta type | Spark type
-|-
uint8 | ShortType
uint16 | IntegerType
uint32 | LongType
uint64 | DecimalType(20, 0)

`uint8/uint16/uint32` promote to the next wider signed type (lossless, correct ordering).
`uint64` has no wider signed integer type; `Decimal(20, 0)` represents all of `0 … 2^64 - 1`
exactly and is consistent with Spark's existing guidance for values beyond `LongType`. As a
narrower staged first version, an implementation may instead support only `uint8/uint16/uint32`
and reject `uint64`; this RFC recommends the `Decimal(20, 0)` mapping to avoid fragmenting
interoperability.

## Reference implementation

A tested reference implementation exists in `delta-kernel-rs` and `delta-rs` (Rust + Python),
covering the four primitive types and schema (de)serialization, the `unsignedIntegerTypes` table
feature, the Parquet mapping, native unsigned scalars with unsigned ordering, correct unsigned
statistics (including the `uint64` decimal-string encoding), data-skipping/file-pruning, and
unsigned partition columns. End-to-end `write → Parquet → read` roundtrips exactly for all four
widths including `uint64 = 18446744073709551615`. Cross-implementation checks confirmed the Parquet
output is portable (Arrow C++ and DuckDB) and that feature-unaware readers reject cleanly. This
implementation can serve as the basis for the `delta-kernel-rs` / `delta-rs` PRs and
`delta-incubator/dat` acceptance fixtures. **A Spark implementation is not yet done and is pending
agreement on this RFC, in particular the `uint64` mapping above.**
