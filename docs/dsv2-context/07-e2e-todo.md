# E2E todo: simple append test (Kernel write path POC)

Goal: run a single end-to-end test that creates a Delta table, appends rows via the Kernel-based write path (SparkTable → DeltaKernelBatchWrite → DeltaKernelDataWriter), and reads them back.

## Test used

- **Suite:** `DataFrameWriterV2WithV2ConnectorSuite` (extends `OpenSourceDataFrameWriterV2Tests` with `V2ForceTest`).
- **Test:** `Append: basic append`.
- **Flow:** `CREATE TABLE table_name (id bigint, data string) USING delta` → `spark.table("source").writeTo("table_name").append()` → `checkAnswer(spark.table("table_name"), ...)`.
- **V2 routing:** `V2ForceTest` sets `V2_ENABLE_MODE=STRICT`. Unified build uses `DeltaCatalog` (Java), which returns `SparkTable` from `loadCatalogTable` in STRICT mode, so `writeTo("table_name")` uses our `DeltaKernelWriteBuilder` / `DeltaKernelBatchWrite` / `DeltaKernelDataWriter` path.

## Status: e2e passing

- [x] Un-skip **"Append: basic append"** in `DataFrameWriterV2WithV2ConnectorSuite.shouldFail`.
- [x] **Run the test** and fix runtime failures:
  - **Serialization:** Resolved by serializing Hadoop config as `Map<String,String>` (see [08-poc-decisions-and-shortcuts.md](08-poc-decisions-and-shortcuts.md) §1 and `HadoopConfSerialization.java`).
  - **Schema nullability:** Resolved by using **table schema** (from snapshot) for the Kernel path instead of `LogicalWriteInfo.schema()`, so nullability matches (see doc 08 §2 and `DeltaKernelDataWriter` / factory).
- [x] Test passes: `Tests: succeeded 1, failed 0`.

## Optional (not done in POC)

- If any future test fails on table name format: fix `SparkTable.name()` to return `catalog.schema.table` (e.g. `default.table_name`) where required. The "Create" tests remain skipped for this reason.

## How to run

```bash
# From repo root (project name is "spark", not "sparkUnified")
sbt "spark/testOnly org.apache.spark.sql.delta.DataFrameWriterV2WithV2ConnectorSuite -- -z \"Append: basic append\""
```

## Decisions and shortcuts

Implementation choices (Hadoop config serialization, table schema for Kernel) and POC shortcuts are documented in **[08-poc-decisions-and-shortcuts.md](08-poc-decisions-and-shortcuts.md)**. Code comments in the write path reference that doc where relevant.
