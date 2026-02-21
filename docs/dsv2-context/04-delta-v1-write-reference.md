# Delta V1 Write Path (Reference)

How the existing Delta Spark connector (DeltaTableV2) performs batch writes today. We do **not** implement the DSv2 write path by copying this exactly; we use it as a reference for semantics (overwrite, truncate, options) and for integration points (e.g. WriteIntoDelta, DeltaLog).

## Entry point: DeltaTableV2 + SupportsWrite

- **Class:** `spark/src/main/scala/org/apache/spark/sql/delta/catalog/DeltaTableV2.scala`
- **Implements:** `Table`, `SupportsRead`, **SupportsWrite**, and others.
- **Method:** `override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder`

## WriteBuilder: WriteIntoDeltaBuilder

- **Class:** `WriteIntoDeltaBuilder` (private class in same file)
- **Extends:** `WriteBuilder` with `SupportsOverwrite`, `SupportsTruncate`, `SupportsDynamicOverwrite`
- **Configuration:**
  - `truncate()` → sets internal “force overwrite” and is used for TRUNCATE TABLE–style overwrite.
  - `overwrite(filters)` → encodes filters as `replaceWhere` in options and sets force overwrite.
  - `overwriteDynamicPartitions()` → sets partition overwrite mode to dynamic in options.
- **Build:** `override def build(): V1Write` (not `buildForBatch()`). So Delta’s default path is **V1**, not native BatchWrite.

## V1Write → InsertableRelation

- `build()` returns a `V1Write` that implements `toInsertableRelation()`.
- That relation’s `insert(data: DataFrame, overwrite: Boolean)`:
  - Merges in a “null as default” option when applicable.
  - Calls **WriteIntoDelta** with:
    - `table.deltaLog`
    - SaveMode (Append vs Overwrite from `forceOverwrite`)
    - `DeltaOptions` from the builder options
    - Partition filters (e.g. from overwrite)
    - The DataFrame to write
    - Catalog table if present
  - Then runs the write via `WriteIntoDelta(...).run(session)`.
  - Re-caches plans that refer to this table.

So the actual write is done by the **TransactionalWrite** / **WriteIntoDelta** path, which uses DeltaLog, OptimisticTransaction, and the classic file listing and commit logic (no Kernel).

## Actual write execution (V1 path)

- **Package:** `org.apache.spark.sql.delta` – `WriteIntoDelta`, `TransactionalWrite`, etc.
- **Key class:** `spark/src/main/scala/org/apache/spark/sql/delta/files/TransactionalWrite.scala` – coordinates job commit protocol, writing files, and committing the Delta log.
- **File writing:** `DeltaFileFormatWriter` – uses Spark’s `FileFormatWriter`-style path, with `WriterCommitMessage` for per-task commit metadata; writes Parquet (or other format) under the table path.

So in the V1 path:

- Driver starts a transaction (OptimisticTransaction) and prepares the write.
- Tasks write files and return commit messages.
- Driver commits the transaction to the Delta log using those messages.

For the **Kernel** path we do the same high-level flow but use Kernel’s `Transaction` and Kernel’s `transformLogicalData` / `getWriteContext` / `generateAppendActions` / `commit` instead of DeltaLog and OptimisticTransaction.

## Options and semantics to mirror (when we add overwrite)

- **replaceWhere** – predicate for conditional overwrite (from `overwrite(filters)`).
- **partitionOverwriteMode** – static vs dynamic (from `overwriteDynamicPartitions()`).
- **Null as default** – `ColumnWithDefaultExprUtils.USE_NULL_AS_DEFAULT_DELTA_OPTION` for schema evolution/defaults.

We don’t need to implement overwrite in the first milestone; this doc is for parity when we do.

## Summary

| Aspect | DeltaTableV2 (current) | Target (SparkTable + Kernel) |
|--------|------------------------|------------------------------|
| Write API | V1Write → InsertableRelation | BatchWrite (native V2) |
| Transaction | OptimisticTransaction + DeltaLog | Kernel Transaction |
| File writing | DeltaFileFormatWriter / FileFormatWriter | Kernel transformLogicalData + ParquetHandler (or Spark Parquet in Kernel format) |
| Commit | Delta log commit on driver | Transaction.commit(engine, dataActions) |
| Overwrite/Truncate | Supported via WriteIntoDeltaBuilder | To be added later via WriteBuilder mixins + Kernel or protocol support |

Use this as the behavioral and options reference when implementing and testing the Kernel-based DSv2 write path.
