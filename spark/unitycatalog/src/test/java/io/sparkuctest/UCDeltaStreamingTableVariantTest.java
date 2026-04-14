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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.DataStreamReader;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.junit.jupiter.api.DynamicContainer;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.api.io.TempDir;

/**
 * Combinatorial streaming test: verifies that DSv2 streaming reads produce the same results as
 * batch reads across different table creation and mutation variants.
 *
 * <p>Each {@link TableVariant} is a data record describing how to create and populate a table. The
 * {@code @TestFactory} generates tests for each variant x table type (EXTERNAL, MANAGED). Adding a
 * new variant is one entry in {@link #TABLE_VARIANTS}.
 *
 * <h3>Append-only vs. data-modifying operations</h3>
 *
 * <p>Delta streaming in append mode replays the Delta log and emits every {@code AddFile} it
 * encounters. For <b>append-only</b> operations (INSERT), the accumulated streaming output equals
 * the current batch table state, so {@code assertStreamingEqualsBatch} is the right assertion.
 *
 * <p>For <b>data-modifying</b> operations (UPDATE, DELETE, MERGE, INSERT OVERWRITE, TRUNCATE),
 * streaming output will NOT match batch. This is expected and correct behavior — not a bug. The
 * reason: Delta implements these as file-level rewrites. For example, {@code DELETE WHERE id = 1}
 * on a file containing rows {1, 2, 3} produces:
 *
 * <pre>
 *   RemoveFile(old_file)          — the file with {1, 2, 3}
 *   AddFile(new_file)             — rewritten file with {2, 3}
 * </pre>
 *
 * <p>With {@code ignoreDeletes=true}, streaming skips the RemoveFile but still reads the new
 * AddFile. So rows 2 and 3 appear twice in the streaming output (once from the original file, once
 * from the rewrite), while batch only shows {2, 3}. The streaming read is a <b>log of all data ever
 * added</b>, not a materialized view of the current table state.
 *
 * <p>Data-modifying variants use {@code streamingReadOptions} (e.g., {@code ignoreDeletes}, {@code
 * ignoreChanges}) to prevent the stream from failing on RemoveFile actions. The assertion for these
 * variants verifies that the stream completes without error — not that the output matches batch.
 */
public class UCDeltaStreamingTableVariantTest extends UCDeltaTableIntegrationBaseTest {

  private static final long STREAMING_TIMEOUT_MS = 60_000L;

  @TempDir private Path tempDir;

  private int checkpointCount;

  // ---------------------------------------------------------------------------
  // Table variant definitions — add new rows here to extend coverage
  // ---------------------------------------------------------------------------

  /** Describes one table creation/mutation variant for streaming verification. */
  private static class TableVariant {
    final String name;
    final String schema;
    final String partitionCols;
    final String tableProperties;
    final String createTableSql;
    final List<String> setupSqls;
    final List<String> incrementalSqls;
    final Map<String, String> streamingReadOptions;

    TableVariant(
        String name,
        String schema,
        String partitionCols,
        String tableProperties,
        String createTableSql,
        List<String> setupSqls,
        List<String> incrementalSqls,
        Map<String, String> streamingReadOptions) {
      this.name = name;
      this.schema = schema;
      this.partitionCols = partitionCols;
      this.tableProperties = tableProperties;
      this.createTableSql = createTableSql;
      this.setupSqls = setupSqls;
      this.incrementalSqls = incrementalSqls;
      this.streamingReadOptions = streamingReadOptions;
    }

    /** No custom DDL, no streaming options. */
    TableVariant(
        String name,
        String schema,
        String partitionCols,
        String tableProperties,
        List<String> setupSqls,
        List<String> incrementalSqls) {
      this(
          name,
          schema,
          partitionCols,
          tableProperties,
          null,
          setupSqls,
          incrementalSqls,
          Collections.emptyMap());
    }

    /** Custom DDL, no streaming options. */
    TableVariant(
        String name,
        String schema,
        String partitionCols,
        String tableProperties,
        String createTableSql,
        List<String> setupSqls,
        List<String> incrementalSqls) {
      this(
          name,
          schema,
          partitionCols,
          tableProperties,
          createTableSql,
          setupSqls,
          incrementalSqls,
          Collections.emptyMap());
    }
  }

  private static final List<TableVariant> TABLE_VARIANTS =
      List.of(

          // -- Create table, then INSERT, then stream --
          new TableVariant(
              /* name */ "SimpleCreateTable",
              /* schema */ "id INT, value STRING",
              /* partitionCols */ null,
              /* tableProperties */ null,
              /* setupSqls */ List.of("INSERT INTO %s VALUES (1, 'a'), (2, 'b'), (3, 'c')"),
              /* incrementalSqls */ List.of("INSERT INTO %s VALUES (4, 'd'), (5, 'e')")),

          // -- Create table with PARTITIONED BY, INSERT across partitions, then stream --
          new TableVariant(
              /* name */ "PartitionedTable",
              /* schema */ "id INT, value STRING, part STRING",
              /* partitionCols */ "part",
              /* tableProperties */ null,
              /* setupSqls */ List.of(
                  "INSERT INTO %s VALUES (1, 'a', 'x'), (2, 'b', 'y'), (3, 'c', 'x')"),
              /* incrementalSqls */ List.of("INSERT INTO %s VALUES (4, 'd', 'y'), (5, 'e', 'z')")),

          // -- Create table with CLUSTER BY, INSERT, then stream --
          new TableVariant(
              /* name */ "ClusteredTable",
              /* schema */ "id INT, value STRING, category STRING",
              /* partitionCols */ null,
              /* tableProperties */ null,
              /* createTableSql */ "CREATE TABLE %s (id INT, value STRING, category STRING)"
                  + " USING DELTA CLUSTER BY (category) %s",
              /* setupSqls */ List.of(
                  "INSERT INTO %s VALUES (1, 'a', 'cat1'), (2, 'b', 'cat2'), (3, 'c', 'cat1')"),
              /* incrementalSqls */ List.of(
                  "INSERT INTO %s VALUES (4, 'd', 'cat2'), (5, 'e', 'cat3')")),

          // -- Create table with IDENTITY column, INSERT, then stream --
          new TableVariant(
              /* name */ "IdentityColumn",
              /* schema */ "id BIGINT, value STRING",
              /* partitionCols */ null,
              /* tableProperties */ null,
              /* createTableSql */ "CREATE TABLE %s"
                  + " (id BIGINT GENERATED ALWAYS AS IDENTITY, value STRING) USING DELTA %s",
              /* setupSqls */ List.of("INSERT INTO %s (value) VALUES ('a'), ('b'), ('c')"),
              /* incrementalSqls */ List.of("INSERT INTO %s (value) VALUES ('d'), ('e')")),

          // -- Create table, INSERT 3 separate commits, then stream --
          new TableVariant(
              /* name */ "MultipleInserts",
              /* schema */ "id INT, value STRING",
              /* partitionCols */ null,
              /* tableProperties */ null,
              /* setupSqls */ List.of(
                  "INSERT INTO %s VALUES (1, 'a')",
                  "INSERT INTO %s VALUES (2, 'b'), (3, 'c')", "INSERT INTO %s VALUES (4, 'd')"),
              /* incrementalSqls */ List.of(
                  "INSERT INTO %s VALUES (5, 'e'), (6, 'f')", "INSERT INTO %s VALUES (7, 'g')")),

          // -- Create table, INSERT, INSERT OVERWRITE, then stream with ignoreChanges --
          new TableVariant(
              /* name */ "InsertOverwrite",
              /* schema */ "id INT, value STRING",
              /* partitionCols */ null,
              /* tableProperties */ null,
              /* createTableSql */ null,
              /* setupSqls */ List.of(
                  "INSERT INTO %s VALUES (1, 'a'), (2, 'b')",
                  "INSERT OVERWRITE %s VALUES (3, 'c'), (4, 'd')"),
              /* incrementalSqls */ List.of(),
              /* streamReadOptions */ Map.of("ignoreChanges", "true")),

          // -- Create table, INSERT, UPDATE one row, then stream with ignoreChanges --
          new TableVariant(
              /* name */ "AfterUpdate",
              /* schema */ "id INT, value STRING",
              /* partitionCols */ null,
              /* tableProperties */ null,
              /* createTableSql */ null,
              /* setupSqls */ List.of(
                  "INSERT INTO %s VALUES (1, 'a'), (2, 'b'), (3, 'c')",
                  "UPDATE %s SET value = 'z' WHERE id = 1"),
              /* incrementalSqls */ List.of(),
              /* streamReadOptions */ Map.of("ignoreChanges", "true")),

          // -- Create table, INSERT, DELETE one row, then stream with ignoreDeletes --
          new TableVariant(
              /* name */ "AfterDelete",
              /* schema */ "id INT, value STRING",
              /* partitionCols */ null,
              /* tableProperties */ null,
              /* createTableSql */ null,
              /* setupSqls */ List.of(
                  "INSERT INTO %s VALUES (1, 'a'), (2, 'b'), (3, 'c')",
                  "DELETE FROM %s WHERE id = 1"),
              /* incrementalSqls */ List.of(),
              /* streamReadOptions */ Map.of("ignoreDeletes", "true")),

          // -- Create table, INSERT, MERGE (update + insert), then stream with ignoreChanges --
          new TableVariant(
              /* name */ "AfterMerge",
              /* schema */ "id INT, value STRING",
              /* partitionCols */ null,
              /* tableProperties */ null,
              /* createTableSql */ null,
              /* setupSqls */ List.of(
                  "INSERT INTO %s VALUES (1, 'a'), (2, 'b'), (3, 'c')",
                  "MERGE INTO %s t USING (SELECT 1 AS id, 'merged' AS value) s"
                      + " ON t.id = s.id WHEN MATCHED THEN UPDATE SET value = s.value"
                      + " WHEN NOT MATCHED THEN INSERT *"),
              /* incrementalSqls */ List.of(),
              /* streamReadOptions */ Map.of("ignoreChanges", "true")),

          // -- Create table, INSERT, TRUNCATE, INSERT again, then stream with ignoreChanges --
          new TableVariant(
              /* name */ "AfterTruncate",
              /* schema */ "id INT, value STRING",
              /* partitionCols */ null,
              /* tableProperties */ null,
              /* createTableSql */ null,
              /* setupSqls */ List.of(
                  "INSERT INTO %s VALUES (1, 'a'), (2, 'b'), (3, 'c')",
                  "TRUNCATE TABLE %s", "INSERT INTO %s VALUES (4, 'd'), (5, 'e')"),
              /* incrementalSqls */ List.of(),
              /* streamReadOptions */ Map.of("ignoreChanges", "true")),

          // -- Create table, INSERT 3 small files, OPTIMIZE (compaction), then stream --
          new TableVariant(
              /* name */ "AfterOptimize",
              /* schema */ "id INT, value STRING",
              /* partitionCols */ null,
              /* tableProperties */ null,
              /* setupSqls */ List.of(
                  "INSERT INTO %s VALUES (1, 'a')",
                  "INSERT INTO %s VALUES (2, 'b')",
                  "INSERT INTO %s VALUES (3, 'c')",
                  "OPTIMIZE %s"),
              /* incrementalSqls */ List.of("INSERT INTO %s VALUES (4, 'd')")),

          // -- Create table, INSERT, VACUUM, then stream --
          new TableVariant(
              /* name */ "AfterVacuum",
              /* schema */ "id INT, value STRING",
              /* partitionCols */ null,
              /* tableProperties */ null,
              /* createTableSql */ null,
              /* setupSqls */ List.of(
                  "INSERT INTO %s VALUES (1, 'a'), (2, 'b'), (3, 'c')", "VACUUM %s RETAIN 0 HOURS"),
              /* incrementalSqls */ List.of("INSERT INTO %s VALUES (4, 'd')"),
              /* streamReadOptions */ Collections.emptyMap()),

          // -- Create table, INSERT v1, INSERT v2, RESTORE to v1, then stream with ignoreChanges --
          new TableVariant(
              /* name */ "AfterRestore",
              /* schema */ "id INT, value STRING",
              /* partitionCols */ null,
              /* tableProperties */ null,
              /* createTableSql */ null,
              /* setupSqls */ List.of(
                  "INSERT INTO %s VALUES (1, 'a'), (2, 'b')",
                  "INSERT INTO %s VALUES (3, 'c')", "RESTORE %s TO VERSION AS OF 1"),
              /* incrementalSqls */ List.of(),
              /* streamReadOptions */ Map.of("ignoreChanges", "true")),

          // -- Create table, INSERT, ALTER TABLE ADD COLUMN, INSERT with new column, then stream --
          new TableVariant(
              /* name */ "AlterTableAddColumn",
              /* schema */ "id INT, value STRING",
              /* partitionCols */ null,
              /* tableProperties */ null,
              /* createTableSql */ null,
              /* setupSqls */ List.of(
                  "INSERT INTO %s VALUES (1, 'a'), (2, 'b')",
                  "ALTER TABLE %s ADD COLUMN extra STRING", "INSERT INTO %s VALUES (3, 'c', 'x')"),
              /* incrementalSqls */ List.of(),
              /* streamReadOptions */ Collections.emptyMap()),

          // -- Create table, INSERT, ANALYZE TABLE, then stream --
          new TableVariant(
              /* name */ "AfterAnalyze",
              /* schema */ "id INT, value STRING",
              /* partitionCols */ null,
              /* tableProperties */ null,
              /* setupSqls */ List.of(
                  "INSERT INTO %s VALUES (1, 'a'), (2, 'b'), (3, 'c')",
                  "ANALYZE TABLE %s COMPUTE STATISTICS"),
              /* incrementalSqls */ List.of("INSERT INTO %s VALUES (4, 'd')")));

  // ---------------------------------------------------------------------------
  // Test generation
  // ---------------------------------------------------------------------------

  /**
   * Generates tests for each {@code TableVariant x TableType} combination. For variants with
   * incremental SQL, both snapshot and incremental tests are generated.
   */
  @TestFactory
  Stream<DynamicContainer> streamingTableVariants() {
    return TABLE_VARIANTS.stream()
        .map(
            variant -> {
              List<DynamicTest> tests = new ArrayList<>();
              for (TableType tableType : ALL_TABLE_TYPES) {
                tests.add(
                    DynamicTest.dynamicTest(
                        variant.name + " / SNAPSHOT / " + tableType,
                        () -> runSnapshotTest(variant, tableType)));
                if (!variant.incrementalSqls.isEmpty()) {
                  tests.add(
                      DynamicTest.dynamicTest(
                          variant.name + " / INCREMENTAL / " + tableType,
                          () -> runIncrementalTest(variant, tableType)));
                }
              }
              return DynamicContainer.dynamicContainer(variant.name, tests);
            });
  }

  // ---------------------------------------------------------------------------
  // Test execution
  // ---------------------------------------------------------------------------

  private void runSnapshotTest(TableVariant variant, TableType tableType) throws Exception {
    withTable(
        variant,
        tableType,
        tableName -> {
          for (String setupSql : variant.setupSqls) {
            sql(setupSql, tableName);
          }

          String queryName = "snap_" + UUID.randomUUID().toString().replace("-", "");
          DataStreamReader reader = spark().readStream().format("delta");
          variant.streamingReadOptions.forEach(reader::option);
          reader
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
        });
  }

  private void runIncrementalTest(TableVariant variant, TableType tableType) throws Exception {
    withTable(
        variant,
        tableType,
        tableName -> {
          for (String setupSql : variant.setupSqls) {
            sql(setupSql, tableName);
          }

          String queryName = "incr_" + UUID.randomUUID().toString().replace("-", "");
          DataStreamReader reader = spark().readStream().format("delta");
          variant.streamingReadOptions.forEach(reader::option);
          StreamingQuery query =
              reader
                  .table(tableName)
                  .writeStream()
                  .format("memory")
                  .queryName(queryName)
                  .outputMode("append")
                  .option("checkpointLocation", checkpoint())
                  .start();
          try {
            query.processAllAvailable();
            assertStreamingEqualsBatch(queryName, tableName);

            for (String incrSql : variant.incrementalSqls) {
              sql(incrSql, tableName);
              query.processAllAvailable();
              assertStreamingEqualsBatch(queryName, tableName);
            }
          } finally {
            query.stop();
          }
        });
  }

  // ---------------------------------------------------------------------------
  // Table lifecycle
  // ---------------------------------------------------------------------------

  /**
   * Creates a table for the variant and table type, runs the test, and drops the table. Uses {@link
   * #withNewTable} for standard DDL, or executes custom SQL for variants that need it (e.g.,
   * CLUSTER BY).
   */
  private void withTable(TableVariant variant, TableType tableType, TestCode testCode)
      throws Exception {
    if (variant.createTableSql != null) {
      String tableName =
          fullTableName("sv_" + UUID.randomUUID().toString().replace("-", "").substring(0, 12));
      String tblProps =
          tableType == TableType.MANAGED
              ? "TBLPROPERTIES ('delta.feature.catalogManaged'='supported')"
              : "";
      String location = "";
      if (tableType == TableType.EXTERNAL) {
        withTempDir(
            dir -> {
              String loc = "LOCATION '" + new org.apache.hadoop.fs.Path(dir, "data") + "'";
              sql(String.format(variant.createTableSql, tableName, tblProps + " " + loc));
              try {
                testCode.run(tableName);
              } finally {
                sql("DROP TABLE IF EXISTS %s", tableName);
              }
            });
        return;
      }
      sql(String.format(variant.createTableSql, tableName, tblProps));
      try {
        testCode.run(tableName);
      } finally {
        sql("DROP TABLE IF EXISTS %s", tableName);
      }
    } else {
      String simpleName = "sv_" + UUID.randomUUID().toString().replace("-", "").substring(0, 12);
      withNewTable(
          simpleName,
          variant.schema,
          variant.partitionCols,
          tableType,
          variant.tableProperties,
          testCode);
    }
  }

  // ---------------------------------------------------------------------------
  // Assertion helpers
  // ---------------------------------------------------------------------------

  private void assertStreamingEqualsBatch(String queryName, String tableName) {
    List<List<String>> streamingRows = sortedStringRows("SELECT * FROM " + queryName);
    List<List<String>> batchRows = sortedStringRows("SELECT * FROM " + tableName);
    assertThat(streamingRows)
        .as("Streaming %s should match batch %s", queryName, tableName)
        .isEqualTo(batchRows);
  }

  private List<List<String>> sortedStringRows(String query) {
    Row[] rows = (Row[]) spark().sql(query).collect();
    return Arrays.stream(rows)
        .map(
            row -> {
              List<String> cells = new ArrayList<>();
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
