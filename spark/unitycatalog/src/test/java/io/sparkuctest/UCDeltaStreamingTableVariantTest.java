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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.spark.sql.streaming.DataStreamReader;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.junit.jupiter.api.DynamicContainer;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.api.io.TempDir;

/**
 * Verifies DSv2 streaming reads match batch reads across table creation/mutation variants.
 *
 * <p>For <b>append-only</b> variants, streaming output equals the batch table state. For
 * <b>data-modifying</b> variants (UPDATE/DELETE/MERGE/OVERWRITE), streaming output differs because
 * Delta rewrites files: e.g., DELETE removes the old file and creates a new one without the deleted
 * row, so streaming sees rows from both files. These variants use {@code streamingReadOptions}
 * (ignoreDeletes/ignoreChanges) and only assert the stream completes without error.
 */
public class UCDeltaStreamingTableVariantTest extends UCDeltaTableIntegrationBaseTest {

  private static final long STREAMING_TIMEOUT_MS = 60_000L;
  @TempDir private Path tempDir;
  private int checkpointCount;

  private static class TableVariant {
    final String name;
    final String schema;
    final String partitionCols;
    final String tableProperties;
    final String createTableSql;
    final List<String> setupSqls;
    final List<String> incrementalSqls;
    final Map<String, String> streamingReadOptions;

    /** Append-only variant using default DDL. */
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

    /** Full constructor for custom DDL and/or streaming read options. */
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

    boolean isAppendOnly() {
      return streamingReadOptions.isEmpty();
    }
  }

  // Add new variants here. Each is tested with SNAPSHOT + INCREMENTAL x EXTERNAL + MANAGED.
  private static final List<TableVariant> TABLE_VARIANTS =
      List.of(

          // -- Create table, INSERT, then stream --
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

          // -- Regression: NULL values in columns trigger NPE on MANAGED tables --
          // Before fix: ColumnVectorWithFilter.closeIfFreeable() inherited the default
          // ColumnVector implementation which calls close() -> delegate.close() ->
          // releaseMemory() -> sets nulls=null. When the Parquet reader reuses the vector
          // for the next batch, putNotNulls() dereferences the null array -> NPE.
          // Only MANAGED tables are affected because they enable deletion vectors by default,
          // which is the only code path wrapping columns in ColumnVectorWithFilter.
          new TableVariant(
              /* name */ "NullsInColumns",
              /* schema */ "id INT, value STRING, opt_int INT",
              /* partitionCols */ null,
              /* tableProperties */ null,
              /* setupSqls */ List.of(
                  "INSERT INTO %s VALUES (1, null, null), (2, 'b', null), (null, null, null)"),
              /* incrementalSqls */ List.of("INSERT INTO %s VALUES (3, 'c', 3), (null, 'x', 1)")),

          // -- Regression: BOOLEAN columns with NULLs trigger NPE through bit-packing path --
          // Same root cause as NullsInColumns but exercises the BOOLEAN-specific code path
          // in OnHeapColumnVector where null flags interact with bit-packing.
          new TableVariant(
              /* name */ "BooleanNulls",
              /* schema */ "id INT, flag BOOLEAN, val INT",
              /* partitionCols */ null,
              /* tableProperties */ null,
              /* setupSqls */ List.of(
                  "INSERT INTO %s VALUES"
                      + " (1, true, 10), (2, null, 20), (3, false, null), (4, null, null)"),
              /* incrementalSqls */ List.of(
                  "INSERT INTO %s VALUES (5, true, null), (6, null, 60)")),

          // -- Regression: VARIANT columns trigger ClassCastException on MANAGED tables --
          // Before fix: ColumnVectorWithFilter.getChild() unconditionally casts dataType()
          // to StructType to compute the number of children. For non-struct types like
          // VARIANT, ARRAY, and MAP, this cast fails with ClassCastException. The fix
          // guards with an instanceof check and passes through to the delegate directly.
          new TableVariant(
              /* name */ "VariantType",
              /* schema */ "id INT, data VARIANT",
              /* partitionCols */ null,
              /* tableProperties */ null,
              /* createTableSql */ null,
              /* setupSqls */ List.of(
                  "INSERT INTO %s SELECT 1, parse_json('{\"key\": \"value\"}')"
                      + " UNION ALL SELECT 2, parse_json('[1,2,3]')"),
              /* incrementalSqls */ List.of(
                  "INSERT INTO %s SELECT 3, parse_json('{\"nested\": {\"a\": 1}}')"),
              /* streamReadOptions */ Collections.emptyMap()));

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

  /**
   * Snapshot test: runs all setupSqls to populate the table, then starts a single streaming read
   * with {@link Trigger#AvailableNow()} (processes all existing data and stops). For append-only
   * variants, asserts streaming output matches batch. For data-modifying variants, asserts the
   * stream completes without error and produces rows (streaming != batch due to file rewrites — see
   * class javadoc).
   */
  private void runSnapshotTest(TableVariant v, TableType tableType) throws Exception {
    withTable(
        v,
        tableType,
        fullName -> {
          // 1. Run all setup SQL to bring the table to the desired state.
          for (String s : v.setupSqls) sql(s, fullName);

          // 2. Stream with AvailableNow: processes all Delta log entries, then stops.
          String qn = "snap_" + UUID.randomUUID().toString().replace("-", "");
          DataStreamReader reader = spark().readStream().format("delta");
          v.streamingReadOptions.forEach(reader::option);
          reader
              .table(fullName)
              .writeStream()
              .format("memory")
              .queryName(qn)
              .outputMode("append")
              .trigger(Trigger.AvailableNow())
              .option("checkpointLocation", checkpoint())
              .start()
              .awaitTermination(STREAMING_TIMEOUT_MS);

          // 3. Verify output.
          if (v.isAppendOnly()) {
            assertStreamingEqualsBatch(qn, fullName);
          } else {
            // Data-modifying: streaming output has duplicates from file rewrites (see class
            // javadoc). Just verify the stream produced rows without error.
            assertThat(sql("SELECT * FROM %s", qn)).as("Stream should produce rows").isNotEmpty();
          }
        });
  }

  /**
   * Incremental test: runs setupSqls, starts a continuous streaming query, then feeds additional
   * data round-by-round via incrementalSqls. After each round, calls {@link
   * StreamingQuery#processAllAvailable()} and verifies the accumulated streaming output matches the
   * current batch table state. Tests that streaming correctly picks up new commits as they arrive.
   */
  private void runIncrementalTest(TableVariant v, TableType tableType) throws Exception {
    withTable(
        v,
        tableType,
        fullName -> {
          // 1. Run all setup SQL to bring the table to the initial state.
          for (String s : v.setupSqls) sql(s, fullName);

          // 2. Start a continuous streaming query (no trigger = runs until stopped).
          String qn = "incr_" + UUID.randomUUID().toString().replace("-", "");
          DataStreamReader reader = spark().readStream().format("delta");
          v.streamingReadOptions.forEach(reader::option);
          StreamingQuery query =
              reader
                  .table(fullName)
                  .writeStream()
                  .format("memory")
                  .queryName(qn)
                  .outputMode("append")
                  .option("checkpointLocation", checkpoint())
                  .start();
          try {
            // 3. Process initial data and verify.
            query.processAllAvailable();
            assertStreamingEqualsBatch(qn, fullName);

            // 4. Feed incremental data round-by-round, verifying after each.
            for (String incrSql : v.incrementalSqls) {
              sql(incrSql, fullName);
              query.processAllAvailable();
              assertStreamingEqualsBatch(qn, fullName);
            }
          } finally {
            query.stop();
          }
        });
  }

  /**
   * Creates a table for the given variant and table type, runs the test, then drops the table. Uses
   * {@link #withNewTable} for standard DDL, or executes custom SQL for variants that need DDL not
   * expressible through withNewTable (e.g., CLUSTER BY). The custom SQL format string has two %s
   * placeholders: table name and a suffix for TBLPROPERTIES + LOCATION.
   */
  private void withTable(TableVariant v, TableType tableType, TestCode testCode) throws Exception {
    if (v.createTableSql != null) {
      String fullName =
          fullTableName("sv_" + UUID.randomUUID().toString().replace("-", "").substring(0, 12));
      String tblProps =
          tableType == TableType.MANAGED
              ? "TBLPROPERTIES ('delta.feature.catalogManaged'='supported')"
              : "";
      if (tableType == TableType.EXTERNAL) {
        withTempDir(
            dir -> {
              String loc = "LOCATION '" + new org.apache.hadoop.fs.Path(dir, "data") + "'";
              sql(String.format(v.createTableSql, fullName, tblProps + " " + loc));
              try {
                testCode.run(fullName);
              } finally {
                sql("DROP TABLE IF EXISTS %s", fullName);
              }
            });
        return;
      }
      sql(String.format(v.createTableSql, fullName, tblProps));
      try {
        testCode.run(fullName);
      } finally {
        sql("DROP TABLE IF EXISTS %s", fullName);
      }
    } else {
      String name = "sv_" + UUID.randomUUID().toString().replace("-", "").substring(0, 12);
      withNewTable(name, v.schema, v.partitionCols, tableType, v.tableProperties, testCode);
    }
  }

  /** Asserts streaming memory sink has same rows as batch SELECT * (both sorted by first col). */
  private void assertStreamingEqualsBatch(String queryName, String tableName) {
    List<List<String>> streaming = sql("SELECT * FROM %s ORDER BY 1", queryName);
    List<List<String>> batch = sql("SELECT * FROM %s ORDER BY 1", tableName);
    assertThat(streaming).as("Streaming should match batch for %s", tableName).isEqualTo(batch);
  }

  /**
   * Returns a fresh checkpoint directory. Each call creates a unique subdirectory under @TempDir.
   */
  private String checkpoint() throws IOException {
    Path ckDir = tempDir.resolve("ck-" + checkpointCount++);
    Files.createDirectory(ckDir);
    return ckDir.toString();
  }
}
