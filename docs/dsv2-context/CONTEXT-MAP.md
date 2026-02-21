# Context Map: DSv2 Write Path in SparkTable

This document ties together all context needed to implement **SupportsWrite** (and the native BatchWrite path) for the Delta Spark V2 connector in `spark/v2/.../SparkTable.java`.

---

## 1. Goal

- **Add batch write support** to the Kernel-based Delta table so that `df.writeTo("...").append(df)` (and eventually overwrite/truncate) goes through the **DSv2 write path** and the **Delta Kernel Transaction API**, instead of failing or falling back to the legacy Delta V1 path.
- **Implementation target:** `spark/v2/src/main/java/io/delta/spark/internal/v2/catalog/SparkTable.java` (and new classes for WriteBuilder, BatchWrite, DataWriterFactory, DataWriter, WriterCommitMessage).

---

## 2. Doc Map

| Need | Doc |
|------|-----|
| Spark’s write API (SupportsWrite, WriteBuilder, BatchWrite, LogicalWriteInfo, PhysicalWriteInfo, DataWriter, WriterCommitMessage) | [01-dsv2-write-api.md](01-dsv2-write-api.md) |
| Kernel write flow (Snapshot.buildUpdateTableTransaction for existing table, Table.createTransactionBuilder for new table, Transaction, transformLogicalData, getWriteContext, generateAppendActions, commit) | [02-kernel-write-api.md](02-kernel-write-api.md) |
| Where SparkTable lives, current capabilities, what to add | [03-spark-table-and-target.md](03-spark-table-and-target.md) |
| How DeltaTableV2 does writes today (V1 path, options, overwrite) | [04-delta-v1-write-reference.md](04-delta-v1-write-reference.md) |
| PoC reference (huan233usc/delta PR #24) – learn from it; not golden truth | [05-poc-reference-huan233usc-pr24.md](05-poc-reference-huan233usc-pr24.md) |
| External design docs (Google/Atlassian) | [00-README.md](00-README.md) (links only; require sign-in) |

---

## 3. End-to-end flow (target design)

- **Driver – analysis/planning**
  - User runs a write (e.g. `df.writeTo("catalog.schema.t").append(df)`).
  - Spark resolves the table to `SparkTable`; calls `newWriteBuilder(LogicalWriteInfo)`.
  - We return a **WriteBuilder** that uses `LogicalWriteInfo.schema()` and `LogicalWriteInfo.options()`.

- **Driver – execution start**
  - Spark calls `WriteBuilder.buildForBatch()` → we create and return a **BatchWrite**.
  - Inside BatchWrite we:
    - **Existing table:** Use the table’s **Snapshot** and call `snapshot.buildUpdateTableTransaction(engineInfo, Operation.WRITE)` → `UpdateTableTransactionBuilder.build(engine)` to get the **Transaction** (see [05-poc-reference-huan233usc-pr24.md](05-poc-reference-huan233usc-pr24.md) and Kernel’s WriteRunner).
    - **New table (CTAS):** Use `Table.forPath(engine, tablePath).createTransactionBuilder(engine, engineInfo, Operation.CREATE_TABLE)` with `withSchema` / `withPartitionColumns`, then `build(engine)`.
    - Get **transaction state** `Row` and (if needed) serialize it for executors.

- **Driver – create writer factory**
  - Spark calls `BatchWrite.createBatchWriterFactory(PhysicalWriteInfo)`.
  - We return a **DataWriterFactory** that is serializable and carries:
    - Table path, Hadoop/engine config (or a serializable representation).
    - Serialized **transaction state** (and any other needed metadata: schema, partition columns, options).
  - Spark serializes this factory and sends it to executors.

- **Executors – per-task write**
  - Each task gets a **DataWriter** from the factory.
  - DataWriter receives Spark rows (e.g. `InternalRow`). For each (possibly grouped by Delta partition):
    - Convert to Kernel **FilteredColumnarBatch** (using Kernel types or an adapter).
    - Call **Transaction.transformLogicalData(engine, txnState, dataIter, partitionValues)**.
    - Call **Transaction.getWriteContext(engine, txnState, partitionValues)**.
    - Write Parquet files (via **engine.getParquetHandler().writeParquetFiles(...)** or a Spark-side writer that produces Kernel **DataFileStatus**).
    - Call **Transaction.generateAppendActions(engine, txnState, dataFiles, writeContext)** to get Delta log action Rows.
  - **DataWriter.commit()** serializes these action Rows (for this task) into a **WriterCommitMessage** and returns it.
  - On failure, **DataWriter.abort()**; no Kernel commit.

- **Driver – finalize**
  - If all tasks committed: **BatchWrite.commit(WriterCommitMessage[])**.
    - Deserialize all messages into one **Iterable&lt;Row&gt;** of Delta log actions.
    - Call **Transaction.commit(engine, dataActionsIterable)**.
    - Optionally **Table.checkpoint(...)** if `TransactionCommitResult.isReadyForCheckpoint()`.
  - If any task failed: **BatchWrite.abort(WriterCommitMessage[])**; no Kernel commit; optionally clean up.

---

## 4. Key design choices to settle

1. **New table (CTAS) vs existing table**  
   - **Existing table:** Use `Snapshot.buildUpdateTableTransaction(engineInfo, Operation.WRITE)` (no schema/partition set; they come from the snapshot).  
   - **New table:** Use `Table.createTransactionBuilder(engine, engineInfo, Operation.CREATE_TABLE)` with `withSchema`/`withPartitionColumns`.

2. **Engine on executors**  
   - Use the same Engine (e.g. DefaultEngine with same Hadoop conf) on executors. The factory must carry a serializable config (e.g. Hadoop Configuration); each task builds an Engine from it.

3. **Partitioning**  
   - Spark may repartition by partition columns before write; then each task’s data belongs to one or more Delta partitions. Either:
   - One DataWriter per Spark partition, and inside it loop over Delta partitions and call transformLogicalData/getWriteContext/generateAppendActions per partition, or
   - Let Spark partition the RDD by Delta partition columns so each task is single-partition (simpler Kernel usage).

4. **Row/batch conversion**  
   - Spark supplies InternalRow (or similar); Kernel expects FilteredColumnarBatch (Kernel schema). We need a conversion layer (Spark → Kernel types) and must handle partition column presence/absence per Kernel’s expectations.

5. **WriterCommitMessage format**  
   - Encapsulate the list of Delta log action Rows (or their serialized form) for that task. Driver deserializes and merges into one iterable for `txn.commit`.

6. **Overwrite / truncate**  
   - Out of scope for first milestone. Later: implement SupportsOverwrite, SupportsTruncate, SupportsDynamicOverwrite on the WriteBuilder and map to Kernel or protocol support (if available) or fail with clear message.

---

## 5. Capabilities and interfaces

- **SparkTable:** Add `SupportsWrite`; in `capabilities()` add `TableCapability.BATCH_WRITE`.
- **WriteBuilder:** New class (e.g. `DeltaKernelWriteBuilder`) implementing `WriteBuilder`, taking LogicalWriteInfo and SparkTable (or minimal context: path, options, catalog table). First version: only `buildForBatch()`; no overwrite/truncate.
- **BatchWrite:** New class (e.g. `DeltaKernelBatchWrite`) implementing `BatchWrite`: createBatchWriterFactory, commit, abort.
- **DataWriterFactory / DataWriter:** New classes; factory serializable and carrying table path, serialized txn state, schema, partition columns, engine config. DataWriter does the per-task Kernel calls and returns a WriterCommitMessage (serialized actions).

---

## 6. Files to add or touch

| Area | File(s) |
|------|--------|
| Table + SupportsWrite | `spark/v2/.../SparkTable.java` – implement SupportsWrite, newWriteBuilder, add BATCH_WRITE |
| Write builder | New: e.g. `spark/v2/.../write/DeltaKernelWriteBuilder.java` |
| Batch write | New: e.g. `spark/v2/.../write/DeltaKernelBatchWrite.java` |
| Writer factory / writer | New: e.g. `DeltaKernelDataWriterFactory.java`, `DeltaKernelDataWriter.java` |
| Commit message | New or reuse: a type that holds serialized Delta log actions (e.g. byte[] or list of Row bytes) |
| Tests | Un-skip or add tests in `DataFrameWriterV2WithV2ConnectorSuite`; add unit/integration tests for append |

---

## 7. External docs (manual read)

See [00-README.md](00-README.md) for links to:

- DSv2/MCP Google Docs
- Delta as DSv2 in DBR (Atlassian)
- Spark DSv2 Delta Lake connector using Delta Kernel (Atlassian)

Use them for product/design alignment; the local docs above are enough to implement the first version of the DSv2 write path in SparkTable.
