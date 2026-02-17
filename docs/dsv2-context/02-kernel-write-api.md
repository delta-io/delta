# Delta Kernel Write API

Summary of the Delta Kernel APIs used to append data to a Delta table. The DSv2 write path in SparkTable must map Spark’s BatchWrite flow onto this Kernel flow.

## Overview

Kernel models a write as a **transaction**: the driver creates a transaction, tasks write data files and produce **Delta log actions** (e.g. AddFile), and the driver **commits** the transaction with those actions. Kernel handles protocol, schema, partitioning, and conflict resolution.

## Main types

- **Table** – `io.delta.kernel.Table` (e.g. `Table.forPath(engine, tablePath)`); used for new-table transactions.
- **Snapshot** – `io.delta.kernel.Snapshot`; used for **existing-table** write transactions via `buildUpdateTableTransaction(engineInfo, Operation.WRITE)`.
- **UpdateTableTransactionBuilder** – `io.delta.kernel.transaction.UpdateTableTransactionBuilder` (from `Snapshot.buildUpdateTableTransaction(...)`); for appends/updates to an existing table.
- **TransactionBuilder** – `io.delta.kernel.TransactionBuilder` (from `Table.createTransactionBuilder(...)`); for creating a new table.
- **Transaction** – `io.delta.kernel.Transaction` (from either builder’s `.build(engine)`).
- **Engine** – `io.delta.kernel.engine.Engine` (Hadoop conf, Parquet handler, etc.); in Spark we use `DefaultEngine` or a session-aware engine.
- **DataWriteContext** – `io.delta.kernel.DataWriteContext` (target directory and stats columns for one partition).
- **TransactionCommitResult** – result of `Transaction.commit(...)` (e.g. new version, checkpoint hint).

## Step-by-step flow (from Kernel USER_GUIDE and examples)

### 1. Resolve schema and table existence

- **Existing table:** Load snapshot, get schema and partition columns; validate that the write schema matches.
- **New table (e.g. CTAS):** Schema comes from the query; partition columns from the plan (e.g. `PARTITION BY`).

### 2. Create transaction (driver)

**Existing table (append):** Use the snapshot already loaded for the table and build an *update* transaction (Kernel 3.4+):

```java
// initialSnapshot was loaded when the table was resolved
UpdateTableTransactionBuilder txnBuilder =
    initialSnapshot.buildUpdateTableTransaction("Spark-Delta-Kernel", Operation.WRITE);
Transaction txn = txnBuilder.build(engine);
```

- No `withSchema` / `withPartitionColumns` for plain appends; schema and partition columns come from the snapshot.
- Optional: `withTransactionId(applicationId, version)`, `withMaxRetries`, `withTablePropertiesAdded`, etc. on the builder.

**New table (e.g. CTAS):** Use the table and create-transaction API:

```java
Table table = Table.forPath(engine, tablePath);
TransactionBuilder txnBuilder = table.createTransactionBuilder(
    engine,
    "Spark-Delta-Kernel",
    Operation.CREATE_TABLE);
txnBuilder = txnBuilder.withSchema(engine, dataSchema);
txnBuilder = txnBuilder.withPartitionColumns(engine, partitionColumns);
Transaction txn = txnBuilder.build(engine);
```

- Optional: `withTransactionId(engine, applicationId, version)` for idempotent writes; `withTableProperties`, `withMaxRetries`, etc.

### 3. Get transaction state (driver → workers)

- `Row txnState = txn.getTransactionState(engine);`
- This state must be **serialized and sent to executors** (e.g. as JSON or bytes). Each task will deserialize it and use it for transform + write context + generateAppendActions.

### 4. Per-task work (executor)

For each Spark partition (or each logical partition of data):

1. **Partition values**  
   Build `Map<String, Literal>` for this partition (empty for unpartitioned table).

2. **Transform logical → physical**  
   Kernel may drop partition columns from data, reorder for Iceberg compat, etc.

   ```java
   CloseableIterator<FilteredColumnarBatch> physicalData = Transaction.transformLogicalData(
       engine, txnState, dataIterator, partitionValues);
   ```

3. **Write context**  
   Tells the connector where to write and which stats to collect.

   ```java
   DataWriteContext writeContext = Transaction.getWriteContext(
       engine, txnState, partitionValues);
   ```

4. **Write Parquet files**  
   Using the engine’s Parquet handler (or custom writer that produces Kernel’s `DataFileStatus`).

   ```java
   CloseableIterator<DataFileStatus> dataFiles = engine.getParquetHandler()
       .writeParquetFiles(
           writeContext.getTargetDirectory(),
           physicalData,
           writeContext.getStatisticsColumns());
   ```

5. **Generate Delta log actions**  
   Converts file statuses into AddFile (and similar) actions as `Row`s.

   ```java
   CloseableIterator<Row> partitionDataActions = Transaction.generateAppendActions(
       engine, txnState, dataFiles, writeContext);
   ```

6. **Send actions to driver**  
   The task serializes these `Row`s (or a compact representation) and returns them as the **WriterCommitMessage** for that task. The driver will collect all such messages and pass them to `BatchWrite.commit(messages)`.

### 5. Commit (driver)

- Deserialize all task results into a single `Iterable<Row>` of Delta log actions.
- Commit the transaction:

  ```java
  CloseableIterable<Row> dataActionsIterable = ...; // from all tasks
  TransactionCommitResult result = txn.commit(engine, dataActionsIterable);
  ```

- Optional: if `result.isReadyForCheckpoint()`, call `Table.forPath(engine, tablePath).checkpoint(engine, result.getVersion())`.

## Constraints and notes

- **Partitioning:** Connector must partition data by table partition columns before calling `transformLogicalData` and `getWriteContext`; one call per partition (or one for unpartitioned).
- **Schema:** Logical data must match table schema (or new table schema). Kernel validates in `transformLogicalData`.
- **Concurrent writes:** Kernel can retry on conflict; after too many retries it throws. Spark does not retry the whole job; we may need to surface this to the user or document retry behavior.
- **Column mapping / defaults / variant:** Kernel may block or limit some writes (see Kernel API and USER_GUIDE). Our connector should align with Kernel’s current support matrix.
- **Engine:** Must use the same Engine (or equivalent config) on driver and executors (e.g. Hadoop conf, table path resolution). SparkTable already has `hadoopConf` and uses `DefaultEngine`; the write path must pass a serializable description of the engine/config to tasks.

## Where this lives in the repo

- **API:** `kernel/kernel-api/` – `Table`, `Transaction`, `TransactionBuilder`, `DataWriteContext`, `TransactionCommitResult`, `Operation`.
- **Examples:** `kernel/examples/kernel-examples/` – e.g. `CreateTableAndInsertData.java`, `BaseTableWriter.java`.
- **USER_GUIDE:** `kernel/USER_GUIDE.md` – “Step 4: Build append support in your connector”.

## Mapping to Spark DSv2

| Spark DSv2 | Kernel |
|------------|--------|
| `SupportsWrite.newWriteBuilder(LogicalWriteInfo)` | Create Table, validate schema, later build Transaction (e.g. in BatchWrite). |
| `WriteBuilder.buildForBatch()` → `BatchWrite` | Transaction created when starting the batch write (driver). |
| `BatchWrite.createBatchWriterFactory(PhysicalWriteInfo)` | Serialize table path, engine config, **transaction state** (and schema/partition columns) into the factory. |
| `DataWriterFactory.createWriter(partitionId, taskId)` | Allocate per-partition state; no Kernel call yet. |
| `DataWriter.write(InternalRow)` / batch | Convert Spark rows to Kernel `FilteredColumnarBatch`; buffer by partition; when a partition is “full”, run transformLogicalData → writeParquetFiles → generateAppendActions; accumulate actions. |
| `DataWriter.commit()` → `WriterCommitMessage` | Serialize the list of Delta log action Rows (for this task) into the commit message. |
| `BatchWrite.commit(WriterCommitMessage[])` | Deserialize all messages into one `Iterable<Row>`, then `txn.commit(engine, dataActionsIterable)`. |
| `BatchWrite.abort(...)` | No Kernel commit; optionally clean up staged files if any. |

The exact split (e.g. one DataWriter per Spark partition vs per Delta partition) and how we buffer and call Kernel’s Parquet handler from Spark tasks is an implementation detail to design in the CONTEXT-MAP and the actual code.
