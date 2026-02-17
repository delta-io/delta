# SparkTable and Implementation Target

## Where is SparkTable?

The **Kernel-based DSv2 table** is implemented in the **spark/v2** module, not under a folder named `kernel-spark`. (A [PoC](https://github.com/huan233usc/delta/pull/24) uses a `kernel-spark` source tree; we keep implementing in spark/v2 and can name/package classes differently.) The class is:

- **Path:** `spark/v2/src/main/java/io/delta/spark/internal/v2/catalog/SparkTable.java`
- **Package:** `io.delta.spark.internal.v2.catalog`

There is a **kernel-spark** module in the repo (build artifacts under `kernel-spark/target/`); the **source** for the catalog table that uses Delta Kernel is in **spark/v2**. So the implementation target for `SupportsWrite` is:

- **File:** `spark/v2/src/main/java/io/delta/spark/internal/v2/catalog/SparkTable.java`

## Current SparkTable behavior

- **Implements:** `Table`, `SupportsRead` (only).
- **Does not implement:** `SupportsWrite`.
- **Capabilities:** `TableCapability.BATCH_READ`, `TableCapability.MICRO_BATCH_READ`.
- **Read path:** Uses Delta Kernel (Snapshot, Scan, etc.) via `SparkScanBuilder` and micro-batch streaming; see `spark/v2/README.md`.

So today the V2 connector is **read-only**. Writes to the same table identifier go through the legacy Delta source (DeltaTableV2 / V1 write) when the table is resolved as a non–Kernel table, or they fail if only the Kernel table is used.

## Relevant fields and dependencies (existing)

- `identifier`, `tablePath`, `options`, `catalogTable` (optional)
- `snapshotManager`, `initialSnapshot` (Kernel)
- `hadoopConf` (Configuration)
- `schemaProvider` (data schema, partition schema, columns, partition transforms)

All of these will be useful for the write path: we need table path, options, schema, partition columns, and Hadoop/engine config on both driver and executors.

## What we need to add (high level)

1. **Implement `SupportsWrite`** on `SparkTable`.
2. **`newWriteBuilder(LogicalWriteInfo info)`**  
   Return a WriteBuilder that:
   - Takes schema and options from `LogicalWriteInfo`.
   - Can support (later) overwrite/truncate/dynamic overwrite via mixins.
   - `buildForBatch()` returns a **BatchWrite** that uses the Kernel Transaction API.
3. **Advertise `BATCH_WRITE`** in `capabilities()`.
4. **BatchWrite implementation** (likely a new class, e.g. `DeltaKernelBatchWrite`):
   - **Driver:** Create Kernel Table and Transaction; get transaction state; in `createBatchWriterFactory(PhysicalWriteInfo)` return a serializable factory that carries table path, engine config, transaction state, write schema, partition columns, and options.
   - **Executors:** Factory creates DataWriters that convert Spark rows to Kernel batches, call `Transaction.transformLogicalData`, write Parquet (via Kernel engine or Spark’s Parquet), call `Transaction.generateAppendActions`, and return a `WriterCommitMessage` containing the serialized Delta log actions for that task.
   - **Driver:** In `commit(WriterCommitMessage[])`, deserialize all actions and call `Transaction.commit(engine, dataActionsIterable)`; optionally checkpoint.

Overwrite/truncate/dynamic overwrite require extra Kernel or Delta protocol support (e.g. overwrite by filter, or truncate); the first milestone can be **append-only** and still be useful.

## Tests that currently “fail” without write support

In `spark-unified/src/test/scala/org/apache/spark/sql/delta/DataFrameWriterV2WithV2ConnectorSuite.scala`, several tests are marked as expected to fail because they require write operations:

- Append, Overwrite, OverwritePartitions, Replace, CreateOrReplace, and some Create tests.

Once SparkTable implements `SupportsWrite` and a correct BatchWrite, those tests can be re-enabled or adapted (and new tests added for the Kernel write path).

## Summary

| Item | Value |
|------|--------|
| Implementation file | `spark/v2/src/main/java/io/delta/spark/internal/v2/catalog/SparkTable.java` |
| Add interface | `SupportsWrite` |
| Add capability | `TableCapability.BATCH_WRITE` |
| New/related classes | WriteBuilder (e.g. `DeltaKernelWriteBuilder`), BatchWrite (e.g. `DeltaKernelBatchWrite`), DataWriterFactory, DataWriter, WriterCommitMessage type for Delta actions |
| First milestone | Append-only batch write using Kernel Transaction API |
