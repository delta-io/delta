# POC implementation decisions and shortcuts

This doc records **decisions** (with alternatives and rationale) and **POC shortcuts** (explicit cut corners) for the Kernel write path e2e. Code comments reference this doc where relevant.

**Context:** The only **design** decision we documented so far is in [06-write-path-design-kernel-vs-spark-parquet.md](06-write-path-design-kernel-vs-spark-parquet.md): **Option A** (Kernel Parquet path) vs **Option B** (Spark/Delta Parquet, same as read/V1). The two implementation issues below (§1 and §2) are **consequences of choosing Option A**. Option B would avoid or simplify both; see doc 06 §7.

---

## 1. Hadoop Configuration serialization (driver → executor)

**Problem:** `DataWriterFactory` is serialized and sent to executors. It previously held `org.apache.hadoop.conf.Configuration`, which is **not** `Serializable`, causing `NotSerializableException` when Spark ran the write job.

**Options considered:**

| Option | Description | Pros | Cons |
|--------|-------------|------|------|
| **A. Map&lt;String,String&gt;** | Copy all config entries into a `Map`; reconstruct `Configuration` on the executor from the map. | Simple; no new dependencies; works with Java serialization. | Copies every config entry; any programmatic config set after copy is not visible on executor. |
| **B. Spark task context** | Do not store config in the factory; obtain Hadoop config on the executor from Spark’s task/catalog (e.g. `SparkSession.active().sessionState.newHadoopConf()` or similar). | No serialization of config. | Depends on Spark internals; factory/createWriter may not have access to SparkSession; not all clusters expose config the same way. |
| **C. Kryo or custom serialization** | Register a custom serializer for `Configuration`. | Could preserve Configuration object as-is. | Extra dependency; non-standard; other serializable fields in the factory must play well with it. |

**Choice:** **A.** Implemented in `HadoopConfSerialization` (toMap/fromMap). Documented in class Javadoc as a POC choice; sufficient for e2e and local/standalone tests. A production path could revisit B if Spark provides a stable way to obtain Hadoop config on executors.

**Code:** `spark/v2/.../write/HadoopConfSerialization.java`, `DeltaKernelDataWriterFactory` (holds `hadoopConfMap`, reconstructs in `createWriter`).

---

## 2. Schema for Kernel: table schema vs. write schema

**Problem:** Kernel’s `Transaction.transformLogicalData` requires the **data schema** to match the **table schema** exactly, including **nullability**. Spark’s `LogicalWriteInfo.schema()` comes from the query and can differ (e.g. `id` non-null in the query vs. nullable in the table). We hit `KernelException: The schema of the data to be written to the table doesn't match the table schema` (table: `id` nullable=true; data: `id` nullable=false).

**Options considered:**

| Option | Description | Pros | Cons |
|--------|-------------|------|------|
| **A. Use table schema for Kernel** | Pass the schema from the table’s snapshot (via `initialSnapshot.getSchema()`) through the factory to the writer. Use it for Kernel (convert to Kernel schema, build batch, adapter). | Correct: Kernel sees data schema matching table; no nullability mismatch. | Assumes column **order** of InternalRow matches table schema; we do not reorder or validate. |
| **B. Relax write schema nullability** | Before sending to Kernel, convert write schema to “all columns nullable” (or merge with table nullability). | Single schema (write) with tweaked nullability. | Still need table schema to know nullability; more logic; same outcome as A for the common case. |
| **C. Change Kernel** | Allow Kernel to accept data that is “stricter” (e.g. non-null) when table allows null. | No connector change. | Kernel API change; out of scope for this POC. |

**Choice:** **A.** We pass **table schema** (from snapshot, converted to Spark `StructType`) from `DeltaKernelBatchWrite` into `DeltaKernelDataWriterFactory` and `DeltaKernelDataWriter`. The writer uses it for `SchemaUtils.convertSparkSchemaToKernelSchema` and for `InternalRowToKernelRowAdapter`. Column order is assumed to match between InternalRow (query output) and table; for “Append: basic append” they do. Documented in writer/factory Javadocs.

**Code:** `DeltaKernelBatchWrite.createBatchWriterFactory` (computes `tableSchemaSpark` from `initialSnapshot.getSchema()`), `DeltaKernelDataWriterFactory` (field `tableSchema`), `DeltaKernelDataWriter` (uses `tableSchema` for kernel schema and adapter).

---

## 3. POC shortcuts (called out in code and here)

- **Hadoop config:** Stored and sent as a flat Map; no attempt to use Spark’s own config propagation to executors. See §1.
- **Schema:** We use table schema for Kernel and assume InternalRow column order matches table schema. We do **not** validate or reorder columns; schema evolution / column reorder would require more logic.
- **Partitioning:** Append path is **unpartitioned only** (empty `partitionValues`). Partitioned tables are not supported in this POC.
- **Overwrite / truncate / dynamic overwrite:** Not implemented; only append is supported.
- **Empty batches:** If a task has no rows, we return a commit message with an empty list of actions; driver still commits. Kernel accepts empty action sets for a task.

---

## 4. Files added or changed for e2e

| File | Purpose |
|------|--------|
| `HadoopConfSerialization.java` | POC: serialize Configuration as Map for executor. |
| `DeltaKernelDataWriterFactory.java` | Holds `hadoopConfMap` and `tableSchema`; creates Configuration and DataWriter on executor. |
| `DeltaKernelBatchWrite.java` | Builds table schema from snapshot; passes it and hadoopConfMap to factory. |
| `DeltaKernelDataWriter.java` | Uses table schema for Kernel and adapter. |
| `DataFrameWriterV2WithV2ConnectorSuite.scala` | "Append: basic append" removed from `shouldFail` so the test runs. |

Test run (reference):  
` sbt "spark/testOnly org.apache.spark.sql.delta.DataFrameWriterV2WithV2ConnectorSuite -- -z \"Append: basic append\"" `
