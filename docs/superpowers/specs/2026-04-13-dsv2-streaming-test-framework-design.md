# DSv2 Streaming Test Framework for Catalog-Managed Delta Tables

## Goal

A combinatorial Java test framework in the sparkuctest module that verifies DSv2 streaming reads produce correct results across different table creation and mutation strategies. For each combination, streaming results are compared against equivalent batch reads.

This PR delivers the framework harness only. Concrete test cases are added in stacked PRs.

## Context

The existing sparkuctest module (`spark/unitycatalog/src/test/java/io/sparkuctest/`) provides integration tests for Delta + Unity Catalog. It uses JUnit 5 `@TestFactory` for dynamic test generation, `UCDeltaTableIntegrationBaseTest` for Spark/UC lifecycle, and `withNewTable()` for table lifecycle management.

Streaming tests already exist in `UCDeltaStreamingTest` and `UCDeltaTableDataFrameStreamingTest`, but they are hand-written individual tests. This framework enables systematic combinatorial coverage driven by a test matrix (see "Delta Table Combination for Streaming" doc).

## Core Abstractions

### `TableSetup` (interface)

Defines how to create and populate a table before streaming reads.

```java
public interface TableSetup {
    /** Human-readable name for test output (e.g., "SimpleCreateTable"). */
    String name();

    /** Table schema DDL (e.g., "id INT, value STRING"). */
    String schema();

    /**
     * Create and populate the table. Called once before streaming.
     * The table already exists (created by the framework via withNewTable).
     * This method inserts initial data and/or performs mutations.
     */
    void setUp(SparkSession spark, String tableName);

    /**
     * Add more data mid-stream for incremental mode.
     * Called between processAllAvailable() calls.
     */
    default void addIncrementalData(SparkSession spark, String tableName, int round) {}

    /** How many incremental rounds to run (0 = snapshot only). */
    default int incrementalRounds() { return 0; }
}
```

Each row in the "Delta Table Combination for Streaming" doc maps to one `TableSetup` implementation, added in later stacked PRs.

### `AssertionMode` (enum)

Two verification modes:

- **`SNAPSHOT`** — single `Trigger.AvailableNow()` streaming read, compare final result to batch `SELECT *`.
- **`INCREMENTAL`** — continuous streaming query with `processAllAvailable()` between `addIncrementalData()` calls, verify at each step that accumulated streaming output matches batch.

### `DSv2StreamingTestBase` (abstract class)

Extends `UCDeltaTableIntegrationBaseTest`. Subclasses declare which `TableSetup` implementations to run.

```java
public abstract class DSv2StreamingTestBase extends UCDeltaTableIntegrationBaseTest {
    protected abstract List<TableSetup> tableSetups();

    @TestFactory
    Stream<DynamicNode> streamingTests() { ... }
}
```

## Test Execution Flow

### Snapshot Mode

1. `withNewTable(name, setup.schema(), TableType.MANAGED, ...)` creates the table.
2. `setup.setUp(spark, tableName)` populates it.
3. Streaming: `spark.readStream().format("delta").table(tableName)` into a memory sink with `Trigger.AvailableNow()`, `awaitTermination()`.
4. Batch: `spark.read().table(tableName)`.
5. Assert streaming result equals batch result (order-independent, sorted by all columns).

### Incremental Mode

1. Same table creation + initial `setUp()`.
2. Start a continuous streaming query into a memory sink.
3. `processAllAvailable()` for initial data, assert equals batch.
4. For each round `1..incrementalRounds()`:
   - `setup.addIncrementalData(spark, tableName, round)`
   - `processAllAvailable()`
   - Assert accumulated streaming output matches batch `SELECT *`
5. Stop query, final batch comparison.

### Result Comparison

`assertStreamingEqualsBatch(queryName, tableName)` compares:
- `SELECT * FROM <memory_sink> ORDER BY 1` (streaming accumulated output)
- `SELECT * FROM <tableName> ORDER BY 1` (batch read)

Both converted to `List<List<String>>` via existing `SparkSQLExecutor` pattern.

### Dynamic Test Generation

`@TestFactory` produces: `tableSetups()` x `AssertionMode` (filtered — `INCREMENTAL` skipped when `incrementalRounds() == 0`).

Test names: `"SimpleCreateTable / SNAPSHOT"`, `"SimpleCreateTable / INCREMENTAL"`, etc.

## File Layout

All files under `spark/unitycatalog/src/test/java/io/sparkuctest/`:

```
TableSetup.java                    # Interface
AssertionMode.java                 # Enum (SNAPSHOT, INCREMENTAL)
DSv2StreamingTestBase.java         # Framework base class with @TestFactory
SimpleCreateTable.java             # Minimal sample implementation for validation
```

## Design Decisions

**Table type: MANAGED only.** The focus is catalog-managed delta tables. Hardcoded in the base class. EXTERNAL coverage can be added later as an override if needed.

**Java, not Scala.** Consistent with sparkuctest. The plain Spark streaming API (`readStream`, `writeStream`, `Trigger.AvailableNow()`, `processAllAvailable()`) is fully callable from Java. No dependency on Scala-only `StreamTest` DSL.

**Memory sink for streaming output.** Uses `format("memory").queryName(name)` for snapshot mode. For incremental mode, also uses memory sink — accumulated results queryable via `spark.sql("SELECT * FROM <queryName>")`.

**Checkpoint management.** JUnit `@TempDir` per test class, counter-based unique checkpoint paths per query (same pattern as existing `UCDeltaTableDataFrameStreamingTest`).

**One sample `TableSetup` ships with the framework.** `SimpleCreateTable` does `INSERT INTO ... VALUES (1), (2), (3)` with one incremental round adding `(4), (5)`. Proves the harness works end-to-end without being a real test case.

## Stacking Plan

- **PR 1 (this):** Framework harness — `TableSetup`, `AssertionMode`, `DSv2StreamingTestBase`, `SimpleCreateTable`
- **PR 2+:** Concrete `TableSetup` implementations covering the test matrix rows (CreateWithPartitions, TableAfterMerge, TableAfterDelete, etc.)
