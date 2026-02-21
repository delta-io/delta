# PoC Reference: huan233usc/delta PR #24

This document summarizes the [PoC PR](https://github.com/huan233usc/delta/pull/24) (fork of delta-io/delta) for learning and comparison. **Do not treat it as the single source of truth**—we can diverge where it makes sense (e.g. module layout, overwrite/row-level scope, API choices).

## What the PoC Adds

### 1. Docs (in that fork)

- **delta-batch-read-flow.md** – Batch read flow description
- **delta-batch-write-flow.md** – Batch write flow description  
- **delta-extension-rules-explained.md** – Extension rules

(Exact content isn’t available here; the PR shows 730 / 697 / 595 line additions. Use for inspiration only.)

### 2. kernel-spark module (different from our spark/v2)

The fork introduces a **kernel-spark** source tree (our repo has spark/v2, not kernel-spark source):

| Area | Classes |
|------|--------|
| **catalog** | `SparkTable.java` – Table + SupportsRead + **SupportsWrite** + **SupportsRowLevelOperations** + SupportsMetadataColumns |
| **read** | SparkScanBuilder, SparkScan, SparkBatch, DeltaParquetFileFormatV2, ProtocolAndMetadataAdapterV2 |
| **write** | SparkWriteBuilder, **SparkBatchWrite**, SparkDataWriter, SparkDataWriterFactory, **SparkWriterCommitMessage**, SparkDVDataWriter, SparkDVDataWriterFactory, SparkDVCommitMessage, SparkRowLevelOperation, SparkRowLevelOperationBuilder |

### 3. SparkTable write path (from the diff)

- **Capabilities:** Adds `TableCapability.BATCH_WRITE`.
- **newWriteBuilder(LogicalWriteInfo):**
  - Uses **Snapshot.buildUpdateTableTransaction** (not `Table.createTransactionBuilder`):
    - `UpdateTableTransactionBuilder txnBuilder = initialSnapshot.buildUpdateTableTransaction("kernel-spark", Operation.WRITE);`
  - Returns `SparkWriteBuilder(txnBuilder, hadoopConf, info.schema(), info.queryId())`.
- **Schema / partitioning:** SparkTable holds `schema`, `dataSchema`, `partitionSchema`, `partColNames` as fields (precomputed from snapshot).
- **Engine:** Holds an `Engine` instance (and hadoopConf); uses `PathBasedSnapshotManager` and `DefaultEngine.create(hadoopConf)`.

So for **existing table** appends, the PoC correctly uses **Snapshot.buildUpdateTableTransaction** → UpdateTableTransactionBuilder → Transaction. Our context doc 02 emphasized Table.createTransactionBuilder; for appends to an existing table, **Snapshot.buildUpdateTableTransaction** is the right Kernel API in this repo (see `kernel/kernel-api/.../Snapshot.java`, `UpdateTableTransactionBuilder`, and `WriteRunner.java`).

### 4. Row-level operations (DELETE/UPDATE/MERGE)

- **SupportsRowLevelOperations:** `newRowLevelOperationBuilder(RowLevelOperationInfo)` returns `SparkRowLevelOperationBuilder`.
- **SupportsMetadataColumns:** Exposes `_file` and `_pos` for DV-based deletes/updates.
- **Deletion vectors:** SparkDVDataWriter, SparkDVDataWriterFactory, SparkDVCommitMessage for DV-based writes.

We can implement **batch write first** (append-only) and add row-level ops later; the PoC shows one possible structure.

### 5. Time travel in scan

- **newScanBuilder** uses a `resolveSnapshot(options)` helper: supports `versionAsOf` and `timestampAsOf`, returns the appropriate snapshot and recomputes data/partition schema for that snapshot. Our spark/v2 may do this differently; worth aligning behavior.

### 6. name()

- PoC: `name()` returns `identifier.name()` (simple name). Our spark/v2 SparkTable returns full catalog path or `delta.\`path\``. Tests (e.g. DataFrameWriterV2WithV2ConnectorSuite) may expect a specific format; we can choose what’s right for our codebase.

### 7. Kernel API usage

- **Write transaction:** Snapshot.buildUpdateTableTransaction(engineInfo, Operation.WRITE) → UpdateTableTransactionBuilder.build(engine) → Transaction. Same getTransactionState / getWriteContext / transformLogicalData / generateAppendActions / commit flow as in our Kernel docs and WriteRunner.
- **Create table:** Not shown in the SparkTable diff; create-table likely still uses Table.createTransactionBuilder(engine, engineInfo, Operation.CREATE_TABLE) elsewhere or in a different code path.

## Takeaways for our implementation

1. **Existing-table appends:** Use **Snapshot.buildUpdateTableTransaction("...", Operation.WRITE)** then **.build(engine)** to get the Transaction. We already have this API in `kernel/kernel-api` and use it in WriteRunner and tests.
2. **WriteBuilder payload:** Passing `txnBuilder` (or built Transaction + state), `hadoopConf`, write schema, and query id to the WriteBuilder is a reasonable split; we can do the same or pass a minimal context and build the transaction inside the BatchWrite.
3. **Commit message:** A dedicated type (e.g. SparkWriterCommitMessage) holding serialized Delta log actions per task is the right idea; we can define our own format.
4. **Module layout:** We implement in **spark/v2** (no kernel-spark source in our repo). Class names and packages can differ (e.g. DeltaKernelBatchWrite vs SparkBatchWrite).
5. **Scope:** Starting with **append-only batch write** is fine; row-level ops and overwrite/truncate can follow. The PoC’s broader scope (row-level, metadata columns, DV) is useful as a reference when we add those features.

## Link

- **PR:** https://github.com/huan233usc/delta/pull/24/changes
