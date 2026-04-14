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
          // ===== Table creation variants =====
          new TableVariant(
              "SimpleCreateTable",
              "id INT, value STRING",
              null,
              null,
              List.of("INSERT INTO %s VALUES (1, 'a'), (2, 'b'), (3, 'c')"),
              List.of("INSERT INTO %s VALUES (4, 'd'), (5, 'e')")),
          new TableVariant(
              "PartitionedTable",
              "id INT, value STRING, part STRING",
              "part",
              null,
              List.of("INSERT INTO %s VALUES (1, 'a', 'x'), (2, 'b', 'y'), (3, 'c', 'x')"),
              List.of("INSERT INTO %s VALUES (4, 'd', 'y'), (5, 'e', 'z')")),
          new TableVariant(
              "ClusteredTable",
              "id INT, value STRING, category STRING",
              null,
              null,
              "CREATE TABLE %s (id INT, value STRING, category STRING)"
                  + " USING DELTA CLUSTER BY (category) %s",
              List.of("INSERT INTO %s VALUES (1, 'a', 'cat1'), (2, 'b', 'cat2'), (3, 'c', 'cat1')"),
              List.of("INSERT INTO %s VALUES (4, 'd', 'cat2'), (5, 'e', 'cat3')")),
          new TableVariant(
              "IdentityColumn",
              "id BIGINT, value STRING",
              null,
              null,
              "CREATE TABLE %s (id BIGINT GENERATED ALWAYS AS IDENTITY, value STRING)"
                  + " USING DELTA %s",
              List.of("INSERT INTO %s (value) VALUES ('a'), ('b'), ('c')"),
              List.of("INSERT INTO %s (value) VALUES ('d'), ('e')")),

          // ===== Table states after INSERT variants =====
          new TableVariant(
              "MultipleInserts",
              "id INT, value STRING",
              null,
              null,
              List.of(
                  "INSERT INTO %s VALUES (1, 'a')",
                  "INSERT INTO %s VALUES (2, 'b'), (3, 'c')", "INSERT INTO %s VALUES (4, 'd')"),
              List.of(
                  "INSERT INTO %s VALUES (5, 'e'), (6, 'f')", "INSERT INTO %s VALUES (7, 'g')")),
          new TableVariant(
              "InsertOverwrite",
              "id INT, value STRING",
              null,
              null,
              null,
              List.of(
                  "INSERT INTO %s VALUES (1, 'a'), (2, 'b')",
                  "INSERT OVERWRITE %s VALUES (3, 'c'), (4, 'd')"),
              List.of(),
              Map.of("ignoreChanges", "true")),

          // ===== Table states after DML =====
          new TableVariant(
              "AfterUpdate",
              "id INT, value STRING",
              null,
              null,
              null,
              List.of(
                  "INSERT INTO %s VALUES (1, 'a'), (2, 'b'), (3, 'c')",
                  "UPDATE %s SET value = 'z' WHERE id = 1"),
              List.of(),
              Map.of("ignoreChanges", "true")),
          new TableVariant(
              "AfterDelete",
              "id INT, value STRING",
              null,
              null,
              null,
              List.of(
                  "INSERT INTO %s VALUES (1, 'a'), (2, 'b'), (3, 'c')",
                  "DELETE FROM %s WHERE id = 1"),
              List.of(),
              Map.of("ignoreDeletes", "true")),
          new TableVariant(
              "AfterMerge",
              "id INT, value STRING",
              null,
              null,
              null,
              List.of(
                  "INSERT INTO %s VALUES (1, 'a'), (2, 'b'), (3, 'c')",
                  "MERGE INTO %s t USING (SELECT 1 AS id, 'merged' AS value) s"
                      + " ON t.id = s.id WHEN MATCHED THEN UPDATE SET value = s.value"
                      + " WHEN NOT MATCHED THEN INSERT *"),
              List.of(),
              Map.of("ignoreChanges", "true")),
          new TableVariant(
              "AfterTruncate",
              "id INT, value STRING",
              null,
              null,
              null,
              List.of(
                  "INSERT INTO %s VALUES (1, 'a'), (2, 'b'), (3, 'c')",
                  "TRUNCATE TABLE %s", "INSERT INTO %s VALUES (4, 'd'), (5, 'e')"),
              List.of(),
              Map.of("ignoreChanges", "true")),

          // ===== Table states after utility operations =====
          new TableVariant(
              "AfterOptimize",
              "id INT, value STRING",
              null,
              null,
              List.of(
                  "INSERT INTO %s VALUES (1, 'a')",
                  "INSERT INTO %s VALUES (2, 'b')",
                  "INSERT INTO %s VALUES (3, 'c')",
                  "OPTIMIZE %s"),
              List.of("INSERT INTO %s VALUES (4, 'd')")),
          new TableVariant(
              "AfterVacuum",
              "id INT, value STRING",
              null,
              null,
              null,
              List.of(
                  "INSERT INTO %s VALUES (1, 'a'), (2, 'b'), (3, 'c')", "VACUUM %s RETAIN 0 HOURS"),
              List.of("INSERT INTO %s VALUES (4, 'd')"),
              Collections.emptyMap()),
          new TableVariant(
              "AfterRestore",
              "id INT, value STRING",
              null,
              null,
              null,
              List.of(
                  "INSERT INTO %s VALUES (1, 'a'), (2, 'b')",
                  "INSERT INTO %s VALUES (3, 'c')", "RESTORE %s TO VERSION AS OF 1"),
              List.of(),
              Map.of("ignoreChanges", "true")),

          // ===== ALTER TABLE variants =====
          new TableVariant(
              "AlterTableAddColumn",
              "id INT, value STRING",
              null,
              null,
              null,
              List.of(
                  "INSERT INTO %s VALUES (1, 'a'), (2, 'b')",
                  "ALTER TABLE %s ADD COLUMN extra STRING", "INSERT INTO %s VALUES (3, 'c', 'x')"),
              List.of(),
              Collections.emptyMap()),

          // ===== ANALYZE =====
          new TableVariant(
              "AfterAnalyze",
              "id INT, value STRING",
              null,
              null,
              List.of(
                  "INSERT INTO %s VALUES (1, 'a'), (2, 'b'), (3, 'c')",
                  "ANALYZE TABLE %s COMPUTE STATISTICS"),
              List.of("INSERT INTO %s VALUES (4, 'd')")));

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
