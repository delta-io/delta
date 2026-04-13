# DSv2 Streaming Test Framework Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a combinatorial Java test framework that verifies DSv2 streaming reads match batch reads across different table setups, for catalog-managed Delta tables.

**Architecture:** Four new Java files in the sparkuctest package. `TableSetup` interface defines table creation/population strategies. `DSv2StreamingTestBase` extends `UCDeltaTableIntegrationBaseTest` and uses `@TestFactory` to generate cartesian product of `TableSetup` x `AssertionMode`. A sample `SimpleCreateTable` validates the harness end-to-end.

**Tech Stack:** Java 17, JUnit 5 (`@TestFactory`, `DynamicTest`), Spark Structured Streaming API, AssertJ, Delta Lake, Unity Catalog.

**Spec:** `docs/superpowers/specs/2026-04-13-dsv2-streaming-test-framework-design.md`

---

## File Map

All files created under `spark/unitycatalog/src/test/java/io/sparkuctest/`:

| File | Responsibility |
|------|---------------|
| `TableSetup.java` | Interface defining table creation/population strategy |
| `AssertionMode.java` | Enum: SNAPSHOT vs INCREMENTAL verification |
| `DSv2StreamingTestBase.java` | Framework base class — `@TestFactory`, streaming execution, batch comparison |
| `SimpleCreateTable.java` | Minimal `TableSetup` implementation for harness validation |

---

### Task 1: Create `TableSetup` interface

**Files:**
- Create: `spark/unitycatalog/src/test/java/io/sparkuctest/TableSetup.java`

- [ ] **Step 1: Create `TableSetup.java`**

```java
/*
 * Copyright (2026) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.sparkuctest;

import org.apache.spark.sql.SparkSession;

/**
 * Defines how to create and populate a Delta table for streaming test verification.
 *
 * <p>Each implementation represents one row in the "Delta Table Combination for Streaming" test
 * matrix. The framework creates the table via {@code withNewTable()} using {@link #schema()}, then
 * calls {@link #setUp} to populate it. For incremental mode, {@link #addIncrementalData} is called
 * between streaming micro-batch checkpoints.
 */
public interface TableSetup {

  /** Human-readable name for test output (e.g., "SimpleCreateTable"). */
  String name();

  /** Table schema DDL (e.g., "id INT, value STRING"). */
  String schema();

  /**
   * Populate the table with initial data and/or perform mutations. The table already exists
   * (created by the framework via {@code withNewTable}).
   */
  void setUp(SparkSession spark, String tableName);

  /**
   * Add more data mid-stream for incremental mode. Called between {@code processAllAvailable()}
   * calls. Only invoked when {@link #incrementalRounds()} > 0.
   *
   * @param round 1-based round number
   */
  default void addIncrementalData(SparkSession spark, String tableName, int round) {}

  /** Number of incremental data rounds. Return 0 for snapshot-only setups. */
  default int incrementalRounds() {
    return 0;
  }
}
```

- [ ] **Step 2: Verify it compiles**

Run: `build/sbt sparkUnityCatalog/test:compile 2>&1 | tail -5`
Expected: `[success]`

- [ ] **Step 3: Commit**

```bash
git add spark/unitycatalog/src/test/java/io/sparkuctest/TableSetup.java
git commit -m "Add TableSetup interface for DSv2 streaming test framework"
```

---

### Task 2: Create `AssertionMode` enum

**Files:**
- Create: `spark/unitycatalog/src/test/java/io/sparkuctest/AssertionMode.java`

- [ ] **Step 1: Create `AssertionMode.java`**

```java
/*
 * Copyright (2026) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.sparkuctest;

/**
 * Verification modes for DSv2 streaming tests.
 *
 * <p>Controls how streaming results are checked against batch reads.
 */
public enum AssertionMode {
  /**
   * Single {@code Trigger.AvailableNow()} streaming read. Compares the final accumulated streaming
   * output against a batch {@code SELECT *} from the same table.
   */
  SNAPSHOT,

  /**
   * Continuous streaming query with {@code processAllAvailable()} between {@link
   * TableSetup#addIncrementalData} calls. Verifies at each step that accumulated streaming output
   * matches the current batch {@code SELECT *}. Only runs when {@link
   * TableSetup#incrementalRounds()} > 0.
   */
  INCREMENTAL
}
```

- [ ] **Step 2: Verify it compiles**

Run: `build/sbt sparkUnityCatalog/test:compile 2>&1 | tail -5`
Expected: `[success]`

- [ ] **Step 3: Commit**

```bash
git add spark/unitycatalog/src/test/java/io/sparkuctest/AssertionMode.java
git commit -m "Add AssertionMode enum for DSv2 streaming test framework"
```

---

### Task 3: Create `DSv2StreamingTestBase` framework base class

**Files:**
- Create: `spark/unitycatalog/src/test/java/io/sparkuctest/DSv2StreamingTestBase.java`

- [ ] **Step 1: Create `DSv2StreamingTestBase.java`**

```java
/*
 * Copyright (2026) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.sparkuctest;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.junit.jupiter.api.DynamicNode;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.api.io.TempDir;

/**
 * Combinatorial test framework for verifying DSv2 streaming reads against batch reads.
 *
 * <p>Subclasses declare which {@link TableSetup} implementations to run via {@link #tableSetups()}.
 * The framework generates a test for each {@code TableSetup x AssertionMode} combination using
 * JUnit 5 {@code @TestFactory}. INCREMENTAL mode is skipped for setups with {@code
 * incrementalRounds() == 0}.
 *
 * <p>All tables are created as MANAGED (catalog-managed Delta tables).
 */
public abstract class DSv2StreamingTestBase extends UCDeltaTableIntegrationBaseTest {

  private static final long STREAMING_TIMEOUT_MS = 60_000L;

  @TempDir private Path tempDir;

  private int checkpointCount;

  /** Subclasses return the list of table setups to test. */
  protected abstract List<TableSetup> tableSetups();

  /**
   * Generates dynamic tests: one per {@code TableSetup x AssertionMode} combination. INCREMENTAL
   * mode is skipped for setups where {@code incrementalRounds() == 0}.
   */
  @TestFactory
  Stream<DynamicNode> streamingTests() {
    return tableSetups().stream()
        .flatMap(
            setup -> {
              Stream.Builder<DynamicNode> tests = Stream.builder();
              // SNAPSHOT always runs
              tests.add(
                  DynamicTest.dynamicTest(
                      setup.name() + " / SNAPSHOT",
                      () -> runStreamingTest(setup, AssertionMode.SNAPSHOT)));
              // INCREMENTAL only when the setup provides incremental data
              if (setup.incrementalRounds() > 0) {
                tests.add(
                    DynamicTest.dynamicTest(
                        setup.name() + " / INCREMENTAL",
                        () -> runStreamingTest(setup, AssertionMode.INCREMENTAL)));
              }
              return tests.build();
            });
  }

  private void runStreamingTest(TableSetup setup, AssertionMode mode) throws Exception {
    String tableName = "dsv2_stream_" + UUID.randomUUID().toString().replace("-", "");
    withNewTable(
        tableName,
        setup.schema(),
        TableType.MANAGED,
        fullTableName -> {
          setup.setUp(spark(), fullTableName);
          if (mode == AssertionMode.SNAPSHOT) {
            runSnapshotTest(setup, fullTableName);
          } else {
            runIncrementalTest(setup, fullTableName);
          }
        });
  }

  /**
   * Snapshot mode: stream all existing data with AvailableNow, then compare to batch read.
   */
  private void runSnapshotTest(TableSetup setup, String tableName) throws Exception {
    String queryName = "snapshot_" + UUID.randomUUID().toString().replace("-", "");
    spark()
        .readStream()
        .format("delta")
        .table(tableName)
        .writeStream()
        .format("memory")
        .queryName(queryName)
        .outputMode("append")
        .trigger(Trigger.AvailableNow())
        .option("checkpointLocation", checkpoint())
        .start()
        .awaitTermination(STREAMING_TIMEOUT_MS);

    assertStreamingEqualsBatch(queryName, tableName);
  }

  /**
   * Incremental mode: start continuous stream, verify after initial data, then add data in rounds
   * and verify at each step.
   */
  private void runIncrementalTest(TableSetup setup, String tableName) throws Exception {
    String queryName = "incremental_" + UUID.randomUUID().toString().replace("-", "");
    StreamingQuery query =
        spark()
            .readStream()
            .format("delta")
            .table(tableName)
            .writeStream()
            .format("memory")
            .queryName(queryName)
            .outputMode("append")
            .option("checkpointLocation", checkpoint())
            .start();
    try {
      // Verify initial data
      query.processAllAvailable();
      assertStreamingEqualsBatch(queryName, tableName);

      // Incremental rounds
      for (int round = 1; round <= setup.incrementalRounds(); round++) {
        setup.addIncrementalData(spark(), tableName, round);
        query.processAllAvailable();
        assertStreamingEqualsBatch(queryName, tableName);
      }
    } finally {
      query.stop();
    }
  }

  /**
   * Asserts that the streaming memory sink contains the same rows as a batch read of the table.
   * Comparison is order-independent (both sides sorted by first column).
   */
  private void assertStreamingEqualsBatch(String queryName, String tableName) {
    List<List<String>> streamingRows = toSortedStringRows(
        spark().sql("SELECT * FROM " + queryName));
    List<List<String>> batchRows = toSortedStringRows(
        spark().read().table(tableName));
    assertThat(streamingRows)
        .as("Streaming output for %s should match batch read of %s", queryName, tableName)
        .isEqualTo(batchRows);
  }

  /**
   * Converts a DataFrame to a sorted list of string rows for deterministic comparison. Rows are
   * sorted lexicographically by all columns concatenated.
   */
  private static List<List<String>> toSortedStringRows(Dataset<Row> df) {
    Row[] rows = (Row[]) df.collect();
    return java.util.Arrays.stream(rows)
        .map(
            row -> {
              java.util.List<String> cells = new java.util.ArrayList<>();
              for (int i = 0; i < row.length(); i++) {
                cells.add(row.isNullAt(i) ? "null" : row.get(i).toString());
              }
              return cells;
            })
        .sorted(
            (a, b) -> {
              for (int i = 0; i < Math.min(a.size(), b.size()); i++) {
                int cmp = a.get(i).compareTo(b.get(i));
                if (cmp != 0) return cmp;
              }
              return Integer.compare(a.size(), b.size());
            })
        .collect(Collectors.toList());
  }

  private String checkpoint() throws IOException {
    Path ckDir = tempDir.resolve("ck-" + checkpointCount++);
    Files.createDirectory(ckDir);
    return ckDir.toString();
  }
}
```

- [ ] **Step 2: Verify it compiles**

Run: `build/sbt sparkUnityCatalog/test:compile 2>&1 | tail -5`
Expected: `[success]`

- [ ] **Step 3: Commit**

```bash
git add spark/unitycatalog/src/test/java/io/sparkuctest/DSv2StreamingTestBase.java
git commit -m "Add DSv2StreamingTestBase framework for combinatorial streaming tests"
```

---

### Task 4: Create `SimpleCreateTable` sample implementation and verify end-to-end

**Files:**
- Create: `spark/unitycatalog/src/test/java/io/sparkuctest/SimpleCreateTable.java`

- [ ] **Step 1: Create `SimpleCreateTable.java`**

```java
/*
 * Copyright (2026) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.sparkuctest;

import org.apache.spark.sql.SparkSession;

/**
 * Minimal {@link TableSetup} for framework validation. Inserts three rows, then adds two more in
 * one incremental round.
 */
public class SimpleCreateTable implements TableSetup {

  @Override
  public String name() {
    return "SimpleCreateTable";
  }

  @Override
  public String schema() {
    return "id INT, value STRING";
  }

  @Override
  public void setUp(SparkSession spark, String tableName) {
    spark.sql(String.format("INSERT INTO %s VALUES (1, 'a'), (2, 'b'), (3, 'c')", tableName));
  }

  @Override
  public void addIncrementalData(SparkSession spark, String tableName, int round) {
    spark.sql(String.format("INSERT INTO %s VALUES (4, 'd'), (5, 'e')", tableName));
  }

  @Override
  public int incrementalRounds() {
    return 1;
  }
}
```

- [ ] **Step 2: Create a concrete test class that uses the framework**

Create `spark/unitycatalog/src/test/java/io/sparkuctest/DSv2StreamingSampleTest.java`:

```java
/*
 * Copyright (2026) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.sparkuctest;

import java.util.List;

/**
 * Sample test class that exercises the DSv2 streaming test framework with a minimal {@link
 * SimpleCreateTable} setup. Validates the framework harness works end-to-end.
 */
public class DSv2StreamingSampleTest extends DSv2StreamingTestBase {

  @Override
  protected List<TableSetup> tableSetups() {
    return List.of(new SimpleCreateTable());
  }
}
```

- [ ] **Step 3: Verify it compiles**

Run: `build/sbt sparkUnityCatalog/test:compile 2>&1 | tail -5`
Expected: `[success]`

- [ ] **Step 4: Run the sample test**

Run: `build/sbt 'sparkUnityCatalog/testOnly io.sparkuctest.DSv2StreamingSampleTest' 2>&1 | tail -20`
Expected: Tests pass — should see `SimpleCreateTable / SNAPSHOT` and `SimpleCreateTable / INCREMENTAL` both passing.

- [ ] **Step 5: Commit**

```bash
git add spark/unitycatalog/src/test/java/io/sparkuctest/SimpleCreateTable.java \
        spark/unitycatalog/src/test/java/io/sparkuctest/DSv2StreamingSampleTest.java
git commit -m "Add SimpleCreateTable sample and DSv2StreamingSampleTest for harness validation"
```

---

### Task 5: Format and create stacked branch

- [ ] **Step 1: Run google-java-format**

Run: `build/sbt sparkUnityCatalog/javafmt`
Expected: `[success]`

- [ ] **Step 2: Commit formatting changes (if any)**

Run: `git -C /home/timothy.wang/delta diff --stat`

If there are changes:
```bash
git add spark/unitycatalog/src/test/java/io/sparkuctest/TableSetup.java \
        spark/unitycatalog/src/test/java/io/sparkuctest/AssertionMode.java \
        spark/unitycatalog/src/test/java/io/sparkuctest/DSv2StreamingTestBase.java \
        spark/unitycatalog/src/test/java/io/sparkuctest/SimpleCreateTable.java \
        spark/unitycatalog/src/test/java/io/sparkuctest/DSv2StreamingSampleTest.java
git commit -m "Format DSv2 streaming test framework files"
```

- [ ] **Step 3: Create a stacked branch and push**

```bash
git stack create dsv2-streaming-test-framework
git stack push origin --publish
```

- [ ] **Step 4: Verify tests pass on the branch**

Run: `build/sbt 'sparkUnityCatalog/testOnly io.sparkuctest.DSv2StreamingSampleTest' 2>&1 | tail -20`
Expected: Both `SimpleCreateTable / SNAPSHOT` and `SimpleCreateTable / INCREMENTAL` pass.
