# DSv2 Write Path: Kernel Parquet vs. Spark/Delta Parquet

## 1. Introduction

The Delta Spark V2 connector needs a **batch write path** so `df.writeTo(...).append(df)` uses DSv2 and commits via the Delta Kernel Transaction API. The core design choice is **how each write task produces Parquet files**: use the **Kernel Parquet stack** (transform + write) or **reuse the Spark/Delta Parquet path** (DeltaParquetFileFormat) and use Kernel only for commit. This doc outlines both options, tradeoffs, and a comparison grid.

## 2. Question / Situation / Problem

**Goal:** Implement `SupportsWrite` so append goes through DSv2 and Kernel’s `Transaction.commit`, producing correct Parquet data files.

**Tension:**

- **Kernel’s path** (`transformLogicalData` → `getParquetHandler().writeParquetFiles` → `generateAppendActions`) does not yet support all use cases (column mapping, variant, defaults are blocked/TODO) and requires feeding **Kernel types** (e.g. `FilteredColumnarBatch`), i.e. converting Spark **InternalRow** → Kernel Row/ColumnarBatch. On **read**, the connector avoided row conversion by using **DeltaParquetFileFormatV2** to produce InternalRow directly; conversion was observed to be slow.
- **Spark/Delta’s path** (e.g. `DeltaFileFormatWriter` + `DeltaParquetFileFormat`) already handles InternalRow, column mapping, Iceberg compat, and DBR optimizations. Parquet format is the same for V1/V2. We could use it to write files and use Kernel only to **commit** (map file list + metadata → Delta log actions).

**Problem:** Choose how the write task produces Parquet—Kernel end-to-end vs. Spark/Delta Parquet with Kernel only for commit—and document tradeoffs.

## 3. Potential Solutions

**Option A — Kernel Parquet path (current impl):** Per task: buffer InternalRow → convert to Kernel Row → ColumnarBatch/FilteredColumnarBatch → `transformLogicalData` → `writeParquetFiles` (Kernel) → `generateAppendActions` → WriterCommitMessage. Driver calls `Transaction.commit`. Kernel owns transform, write, and action generation; connector supplies data in Kernel format.

**Option B — Spark/Delta Parquet path:** Per task: use Spark write machinery with **DeltaParquetFileFormatV2** (or refactored shared abstraction for read+write). Data stays **InternalRow**; no conversion. Write yields file list + metadata. **Bridge:** map that to Kernel’s action row format (e.g. AddFile). Driver still calls `Transaction.commit`. Spark/Delta owns Parquet I/O; Kernel owns transaction and commit; connector implements the bridge.

## 4. Pros and Cons

| | **Option A: Kernel Parquet** | **Option B: Spark/Delta Parquet** |
|--|------------------------------|-----------------------------------|
| **Pros** | Single stack (Kernel only); no V1 write dependency; aligns with Kernel engine contract; one path for any Kernel engine. | No row conversion; reuses read-path pattern (DeltaParquetFileFormatV2); full Delta feature parity (column mapping, Iceberg, etc.); DBR optimizations in same class; same Parquet format as V1. |
| **Cons** | InternalRow→Kernel conversion cost (read path showed conversion is slow); Kernel blocks column mapping, variant, defaults; two Parquet implementations (Kernel vs. Spark) risk divergence. | Must implement and maintain bridge (file list → Kernel action rows); two stacks (Spark I/O, Kernel commit); refactor may be needed to share format for read+write. |

## 5. Comparison Grid

Three dimensions; each cell gives a one-line **why** for that option on that criterion.

| | **Engineering difficulty** | **Latency / performance** | **Future-proof** |
|--|---------------------------|----------------------------|-------------------|
| **Option A: Kernel Parquet** | Harder: must implement InternalRow→Kernel conversion and all missing Kernel features (column mapping, variant, etc.). | Worse: per-row conversion overhead; Kernel Parquet writer may not match Spark’s. | Better: single Kernel contract; Kernel feature additions automatically benefit this path. |
| **Option B: Spark/Delta Parquet** | Easier: same Parquet format as V1; reuse DeltaParquetFileFormat path; main work is bridge (file list → actions). | Better: no row conversion; Spark’s Parquet writer; same as V1/DBR. | Worse: bridge must stay in sync with Kernel action format; Kernel evolution may require bridge updates. |

## 6. Recommendations

- Prototype Option B’s bridge to validate feasibility.
- Benchmark both options (narrow/wide, partitioned).
- Check Kernel roadmap: if Kernel will support column mapping/variant/defaults soon, A’s future-proof edge grows; else B’s feature parity wins near term.

**Summary:** Option A is fully in-Kernel but has conversion cost and feature gaps. Option B reuses the read-path idea (Spark Parquet, InternalRow), wins on difficulty and latency; A wins on future-proofing.

**POC status:** The current implementation uses **Option A** (Kernel Parquet path with InternalRow→Kernel conversion). The e2e test "Append: basic append" passes. POC shortcuts and decisions (e.g. config serialization, schema nullability) are documented in [08-poc-decisions-and-shortcuts.md](08-poc-decisions-and-shortcuts.md).

## 7. Are the POC issues because of Option A? Would Option B be simpler?

**Yes.** The two issues we hit in the POC are **direct consequences of choosing the Kernel Parquet path (Option A)**:

| Issue | Why it’s tied to Option A | With Option B (Spark/Delta Parquet, same as read/V1) |
|-------|----------------------------|------------------------------------------------------|
| **Configuration serialization** | We run Kernel’s engine (DefaultEngine + ParquetHandler) on the executor, so we must pass Hadoop config in our DataWriterFactory. Configuration isn’t Serializable → we had to add Map-based serialization. | The executor would use **Spark’s** write path (e.g. DeltaParquetFileFormat). Spark already has Hadoop config on the executor (task/catalog). We wouldn’t need to serialize and ship Configuration ourselves for Parquet writing. |
| **Schema nullability mismatch** | Kernel’s `Transaction.transformLogicalData` does a **strict** schema check (data schema must match table schema, including nullability). Query schema (LogicalWriteInfo) often differs (e.g. non-null `id`). We had to pass table schema through and use it for Kernel. | We wouldn’t call `transformLogicalData` at all. We’d write Parquet with Spark from InternalRow; Spark’s writer doesn’t require exact nullability match with the table. We’d only use Kernel to **commit** (file list → actions → `Transaction.commit`). No data schema vs table schema check in our path. |

**Conclusion:** Using the **same Parquet write path as read/V1** (Option B) would avoid both problems: no need to ship Hadoop config for the Parquet writer, and no Kernel schema/nullability check on our data. The main new work for Option B would be the **bridge** (file list + metadata → Kernel action rows) and wiring Spark’s FileFormatWriter into the DSv2 write task. So yes—**it would be simpler** in terms of the issues we just fixed; the tradeoff is implementing and maintaining that bridge and staying in sync with Kernel’s action format (as in §4–5).
