# Spark DataSource V2 Write API

Summary of the Spark DSv2 write path used to implement native batch writes (e.g. for Delta Kernel-based tables).

## Table contract: SupportsWrite

- **Interface:** `org.apache.spark.sql.connector.catalog.SupportsWrite`
- **Meaning:** The `Table` supports writes. Spark will use the table as a write target when the user runs `DataFrameWriter` V2 APIs (e.g. `df.writeTo("catalog.schema.table").append(...)`).
- **Method:** `WriteBuilder newWriteBuilder(LogicalWriteInfo info)`
  - `LogicalWriteInfo`: schema from the query, options, and query id.
  - Returns a `WriteBuilder` that can be configured (overwrite, truncate, etc.) and then used to build either a **V1Write** (legacy) or a **BatchWrite** (native V2).

## WriteBuilder

- **Package:** `org.apache.spark.sql.connector.write.WriteBuilder`
- **Role:** Configure write mode and then build the actual write implementation.
- **Methods:**
  - `BatchWrite buildForBatch()` – returns a `BatchWrite` for native V2 batch writes (what we want for Kernel).
  - `StreamingWrite buildForStreaming()` – for streaming; out of scope for initial write support.
- **Optional mixins** (same package):
  - `SupportsOverwrite` – `overwrite(filters)` for conditional overwrite.
  - `SupportsTruncate` – `truncate()` for full table overwrite.
  - `SupportsDynamicOverwrite` – `overwriteDynamicPartitions()` for dynamic partition overwrite.

If the table does **not** implement these mixins, Spark only allows append. For parity with DeltaTableV2 we will eventually want SupportsOverwrite, SupportsTruncate, and SupportsDynamicOverwrite on the builder.

## LogicalWriteInfo

- **Package:** `org.apache.spark.sql.connector.write.LogicalWriteInfo`
- Provides:
  - **Query id** – job/query identifier.
  - **Schema** – schema of the data to be written (from the plan).
  - **Options** – write options (case-insensitive map).

Used when constructing the `WriteBuilder` and when building `BatchWrite` / writer factories.

## BatchWrite (native V2 batch path)

- **Interface:** `org.apache.spark.sql.connector.write.BatchWrite`
- **Role:** Drives the whole batch write on the Spark side.

### Flow

1. **Driver**
   - Spark calls `createBatchWriterFactory(PhysicalWriteInfo physicalInfo)`.
   - `PhysicalWriteInfo` contains the **path** Spark chose for the write (e.g. a staging path) and **custom metrics** (optional). For Delta we may use or ignore the path depending on whether we write directly to the table path or via Kernel.
   - The returned `DataWriterFactory` must be **serializable** (it is sent to executors).

2. **Executors**
   - For each partition/task, Spark uses the factory to create a `DataWriter`.
   - Spark feeds rows to `DataWriter.write(InternalRow)` (or the appropriate method).
   - On success: `DataWriter.commit()` → returns a `WriterCommitMessage` (connector-defined).
   - On failure: `DataWriter.abort()`.

3. **Driver**
   - After all tasks finish:
     - If all committed: `BatchWrite.commit(WriterCommitMessage[] messages)`.
     - If any failed: `BatchWrite.abort(WriterCommitMessage[] messages)`.

### Key types

- **DataWriterFactory** – creates `DataWriter` instances; must be serializable.
- **DataWriter** – per-partition writer; `write(record)`, `commit()`, `abort()`.
- **WriterCommitMessage** – opaque message from each successful task; passed to `commit(messages)` or `abort(messages)`.
- **PhysicalWriteInfo** – path and metrics for the write job.

### Important details

- Spark may **retry failed tasks** (new `DataWriter`), but does **not** retry the whole job. So commit logic must be idempotent or handle partial commits (e.g. Kernel’s transaction and conflict handling).
- The **schema** for the write is the one from `LogicalWriteInfo` (and the plan). It must match what the table expects (or we reject / adapt as per Kernel and Delta semantics).

## V1Write (legacy path)

- **Interface:** `org.apache.spark.sql.connector.write.V1Write`
- **Method:** `InsertableRelation toInsertableRelation()`
- **Role:** The table says “use the old InsertableRelation path.” Spark then calls `InsertableRelation.insert(data, overwrite)` with a DataFrame.
- **Current Delta behavior:** DeltaTableV2’s `WriteIntoDeltaBuilder.build()` returns a `V1Write` that wraps `WriteIntoDelta` (see [04-delta-v1-write-reference.md](04-delta-v1-write-reference.md)). So today Delta uses the V1 write path, not `BatchWrite`.

For the Kernel-based SparkTable we want a **native V2 path**: implement `SupportsWrite`, return a `WriteBuilder` that builds a `BatchWrite` (not `V1Write`), and inside that use the Delta Kernel Transaction API to create the transaction, write files via executors, and commit on the driver.

## Capabilities

- A table that supports batch write should advertise `TableCapability.BATCH_WRITE` in `Table.capabilities()`.
- Current SparkTable only has `BATCH_READ` and `MICRO_BATCH_READ`; adding write support means adding `BATCH_WRITE`.

## References in this repo

- `org.apache.spark.sql.connector.catalog.SupportsWrite`
- `org.apache.spark.sql.connector.write.{WriteBuilder, BatchWrite, LogicalWriteInfo, PhysicalWriteInfo, DataWriterFactory, DataWriter, WriterCommitMessage}`
- Delta’s use of `WriterCommitMessage`: `spark/src/main/scala/org/apache/spark/sql/delta/files/DeltaFileFormatWriter.scala` (used from the V1/FileFormat path, but shows the pattern).
