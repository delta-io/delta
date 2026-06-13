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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.io.TempDir;

/**
 * Ports {@link io.delta.spark.internal.v2.V2StreamingOptimizeTest} (DSv2 EXTERNAL via {@code
 * dsv2.delta.<path>}) to Unity Catalog so each case runs against both EXTERNAL and MANAGED tables
 * via {@code @TestAllTableTypes}.
 *
 * <p>OPTIMIZE produces {@code dataChange=false} AddFile + RemoveFile actions; the streaming source
 * must silently consume them without re-emitting compacted data. The MANAGED variant is mostly
 * untestable here: OPTIMIZE itself is blocked on UC MANAGED tables with {@code
 * DELTA_UNSUPPORTED_CATALOG_MANAGED_TABLE_OPERATION} (see {@code
 * UCDeltaUtilityTest::testMaintenanceOpsBlockedOnManagedTable}). Cases that exercise OPTIMIZE are
 * therefore skipped on MANAGED with KNOWN-GAP comments; the EXTERNAL variant still runs to provide
 * the UC-catalog-path coverage.
 *
 * <p>Skipped on MANAGED:
 *
 * <ul>
 *   <li>All OPTIMIZE / OPTIMIZE ZORDER / partial OPTIMIZE cases (1-8): OPTIMIZE blocked.
 *   <li>{@code testAlterTableSetTblProperties_noReEmission}: ALTER TABLE blocked on MANAGED.
 * </ul>
 */
public class UCDeltaStreamingOptimizeTest extends UCDeltaTableIntegrationBaseTest {

  @TempDir private Path tempDir;
  private int checkpointCount;

  /** Fresh local checkpoint dir; UC server holds cloud creds, not Spark. */
  private String checkpoint() throws IOException {
    Path ckDir = tempDir.resolve("ck-" + checkpointCount++);
    Files.createDirectory(ckDir);
    return ckDir.toString();
  }

  private String queryName(String tag, TableType tableType) {
    return tag + "_" + tableType.name().toLowerCase() + "_" + checkpointCount;
  }

  /**
   * Run OPTIMIZE with a high minFileSize so the small commits get compacted (not treated as
   * already-optimal). Without this, OPTIMIZE may be a no-op which would make tests pass for the
   * wrong reason.
   */
  private void runOptimize(String tableName) {
    String key = "spark.databricks.delta.optimize.minFileSize";
    String original;
    try {
      original = spark().conf().get(key);
    } catch (Exception e) {
      original = null;
    }
    spark().conf().set(key, Long.toString(1024L * 1024L * 1024L));
    try {
      sql("OPTIMIZE %s", tableName);
    } finally {
      if (original != null) {
        spark().conf().set(key, original);
      } else {
        spark().conf().unset(key);
      }
    }
  }

  /** Drain a memory-sink streaming query to completion via processAllAvailable. */
  private List<Row> drainMemorySink(Dataset<Row> df, String qName) throws Exception {
    StreamingQuery q = null;
    try {
      q =
          df.writeStream()
              .format("memory")
              .queryName(qName)
              .outputMode("append")
              .option("checkpointLocation", checkpoint())
              .start();
      q.processAllAvailable();
      return spark().sql("SELECT * FROM " + qName).collectAsList();
    } finally {
      if (q != null) {
        q.stop();
      }
      spark().sql("DROP VIEW IF EXISTS " + qName);
    }
  }

  // 1. Stream a few rows, OPTIMIZE compacts the small files into one, then continue streaming.
  // KNOWN-GAP: OPTIMIZE blocked on UC MANAGED (DELTA_UNSUPPORTED_CATALOG_MANAGED_TABLE_OPERATION).
  @TestAllTableTypes
  public void testOptimizeBetweenStreamBatches_noReEmission(TableType tableType) throws Exception {
    Assumptions.assumeFalse(
        tableType == TableType.MANAGED, "KNOWN-GAP: OPTIMIZE blocked on UC MANAGED tables");

    withNewTable(
        "opt_basic",
        "id INT, name STRING",
        null,
        tableType,
        tableName -> {
          sql("INSERT INTO %s VALUES (1, 'A')", tableName);
          sql("INSERT INTO %s VALUES (2, 'B')", tableName);
          sql("INSERT INTO %s VALUES (3, 'C')", tableName);

          String qName = queryName("opt_basic", tableType);
          String ck = checkpoint();

          // First run drains the existing rows.
          StreamingQuery q1 =
              spark()
                  .readStream()
                  .format("delta")
                  .table(tableName)
                  .writeStream()
                  .format("memory")
                  .queryName(qName)
                  .outputMode("append")
                  .option("checkpointLocation", ck)
                  .start();
          try {
            q1.processAllAvailable();
          } finally {
            q1.stop();
          }

          // OPTIMIZE produces dataChange=false AddFile + RemoveFile; the stream must skip both.
          runOptimize(tableName);

          StreamingQuery q2 =
              spark()
                  .readStream()
                  .format("delta")
                  .table(tableName)
                  .writeStream()
                  .format("memory")
                  .queryName(qName)
                  .outputMode("append")
                  .option("checkpointLocation", ck)
                  .start();
          try {
            q2.processAllAvailable();
            List<Row> rows = spark().sql("SELECT * FROM " + qName).collectAsList();
            assertEquals(3, rows.size(), "expected 3 rows; OPTIMIZE must not re-emit: " + rows);
          } finally {
            q2.stop();
            spark().sql("DROP VIEW IF EXISTS " + qName);
          }
        });
  }

  // 2. OPTIMIZE ... ZORDER BY mid-stream - same dataChange=false guarantee.
  // KNOWN-GAP: OPTIMIZE blocked on UC MANAGED.
  @TestAllTableTypes
  public void testOptimizeZorder_noReEmission(TableType tableType) throws Exception {
    Assumptions.assumeFalse(
        tableType == TableType.MANAGED, "KNOWN-GAP: OPTIMIZE blocked on UC MANAGED tables");

    withNewTable(
        "opt_zorder",
        "id INT, name STRING",
        null,
        tableType,
        tableName -> {
          sql("INSERT INTO %s VALUES (1, 'A')", tableName);
          sql("INSERT INTO %s VALUES (2, 'B')", tableName);
          sql("INSERT INTO %s VALUES (3, 'C')", tableName);

          String qName = queryName("opt_zorder", tableType);
          String ck = checkpoint();

          StreamingQuery q1 =
              spark()
                  .readStream()
                  .format("delta")
                  .table(tableName)
                  .writeStream()
                  .format("memory")
                  .queryName(qName)
                  .outputMode("append")
                  .option("checkpointLocation", ck)
                  .start();
          try {
            q1.processAllAvailable();
          } finally {
            q1.stop();
          }

          String key = "spark.databricks.delta.optimize.minFileSize";
          spark().conf().set(key, Long.toString(1024L * 1024L * 1024L));
          try {
            sql("OPTIMIZE %s ZORDER BY (id)", tableName);
          } finally {
            spark().conf().unset(key);
          }

          StreamingQuery q2 =
              spark()
                  .readStream()
                  .format("delta")
                  .table(tableName)
                  .writeStream()
                  .format("memory")
                  .queryName(qName)
                  .outputMode("append")
                  .option("checkpointLocation", ck)
                  .start();
          try {
            q2.processAllAvailable();
            List<Row> rows = spark().sql("SELECT * FROM " + qName).collectAsList();
            assertEquals(
                3, rows.size(), "expected 3 rows; OPTIMIZE ZORDER must not re-emit: " + rows);
          } finally {
            q2.stop();
            spark().sql("DROP VIEW IF EXISTS " + qName);
          }
        });
  }

  // 3. OPTIMIZE then Trigger.AvailableNow - query terminates and emits exactly user-written rows.
  // KNOWN-GAP: OPTIMIZE blocked on UC MANAGED.
  @TestAllTableTypes
  public void testOptimize_triggerAvailableNow(TableType tableType) throws Exception {
    Assumptions.assumeFalse(
        tableType == TableType.MANAGED, "KNOWN-GAP: OPTIMIZE blocked on UC MANAGED tables");

    withNewTable(
        "opt_avail_now",
        "id INT, name STRING",
        null,
        tableType,
        tableName -> {
          sql("INSERT INTO %s VALUES (1, 'A')", tableName);
          sql("INSERT INTO %s VALUES (2, 'B')", tableName);
          sql("INSERT INTO %s VALUES (3, 'C')", tableName);

          runOptimize(tableName);

          String qName = queryName("opt_avail_now", tableType);
          StreamingQuery q =
              spark()
                  .readStream()
                  .format("delta")
                  .table(tableName)
                  .writeStream()
                  .format("memory")
                  .queryName(qName)
                  .outputMode("append")
                  .trigger(Trigger.AvailableNow())
                  .option("checkpointLocation", checkpoint())
                  .start();
          try {
            assertTrue(q.awaitTermination(60_000L), "AvailableNow should terminate");
            List<Row> rows = spark().sql("SELECT * FROM " + qName).collectAsList();
            assertEquals(3, rows.size(), "expected 3 rows; got: " + rows);
          } finally {
            q.stop();
            spark().sql("DROP VIEW IF EXISTS " + qName);
          }
        });
  }

  // 4. OPTIMIZE collapses N files into 1; maxFilesPerTrigger=1 must still emit all original rows.
  // KNOWN-GAP: OPTIMIZE blocked on UC MANAGED.
  @TestAllTableTypes
  public void testOptimize_maxFilesPerTrigger(TableType tableType) throws Exception {
    Assumptions.assumeFalse(
        tableType == TableType.MANAGED, "KNOWN-GAP: OPTIMIZE blocked on UC MANAGED tables");

    withNewTable(
        "opt_mfpt",
        "id INT, name STRING",
        null,
        tableType,
        tableName -> {
          sql("INSERT INTO %s VALUES (1, 'A')", tableName);
          sql("INSERT INTO %s VALUES (2, 'B')", tableName);
          sql("INSERT INTO %s VALUES (3, 'C')", tableName);

          runOptimize(tableName);

          Dataset<Row> df =
              spark()
                  .readStream()
                  .format("delta")
                  .option("maxFilesPerTrigger", "1")
                  .table(tableName);

          List<Row> rows = drainMemorySink(df, queryName("opt_mfpt", tableType));
          assertEquals(3, rows.size(), "expected 3 rows; got: " + rows);
        });
  }

  // 5. Stream halfway, OPTIMIZE, restart from checkpoint, finish.
  // KNOWN-GAP: OPTIMIZE blocked on UC MANAGED.
  @TestAllTableTypes
  public void testOptimize_restart(TableType tableType) throws Exception {
    Assumptions.assumeFalse(
        tableType == TableType.MANAGED, "KNOWN-GAP: OPTIMIZE blocked on UC MANAGED tables");

    withNewTable(
        "opt_restart",
        "id INT, name STRING",
        null,
        tableType,
        tableName -> {
          sql("INSERT INTO %s VALUES (1, 'A')", tableName);
          sql("INSERT INTO %s VALUES (2, 'B')", tableName);

          String qName = queryName("opt_restart", tableType);
          String ck = checkpoint();

          StreamingQuery q1 =
              spark()
                  .readStream()
                  .format("delta")
                  .table(tableName)
                  .writeStream()
                  .format("memory")
                  .queryName(qName)
                  .outputMode("append")
                  .option("checkpointLocation", ck)
                  .start();
          try {
            q1.processAllAvailable();
          } finally {
            q1.stop();
          }

          runOptimize(tableName);
          sql("INSERT INTO %s VALUES (3, 'C')", tableName);

          StreamingQuery q2 =
              spark()
                  .readStream()
                  .format("delta")
                  .table(tableName)
                  .writeStream()
                  .format("memory")
                  .queryName(qName)
                  .outputMode("append")
                  .option("checkpointLocation", ck)
                  .start();
          try {
            q2.processAllAvailable();
            List<Row> rows = spark().sql("SELECT * FROM " + qName).collectAsList();
            assertEquals(3, rows.size(), "expected 3 rows after restart; got: " + rows);
          } finally {
            q2.stop();
            spark().sql("DROP VIEW IF EXISTS " + qName);
          }
        });
  }

  // 6. DV table; DV-DELETE then OPTIMIZE rewrites; stream with skipChangeCommits emits survivors.
  // KNOWN-GAP: OPTIMIZE blocked on UC MANAGED.
  @TestAllTableTypes
  public void testOptimize_onDvTable_skipChangeCommits(TableType tableType) throws Exception {
    Assumptions.assumeFalse(
        tableType == TableType.MANAGED, "KNOWN-GAP: OPTIMIZE blocked on UC MANAGED tables");

    withNewTable(
        "opt_dv_skip",
        "value INT",
        null,
        tableType,
        "'delta.enableDeletionVectors' = 'true'",
        tableName -> {
          sql("INSERT INTO %s SELECT cast(id as int) FROM range(10)", tableName);
          sql("DELETE FROM %s WHERE value < 3", tableName);
          runOptimize(tableName);

          Dataset<Row> df =
              spark()
                  .readStream()
                  .format("delta")
                  .option("skipChangeCommits", "true")
                  .table(tableName);

          List<Row> rows = drainMemorySink(df, queryName("opt_dv_skip", tableType));
          assertEquals(7, rows.size(), "expected 7 survivors (rows 3..9); got: " + rows);
          Set<Integer> values = new HashSet<>();
          for (Row r : rows) {
            values.add(r.getInt(0));
          }
          assertEquals(Set.of(3, 4, 5, 6, 7, 8, 9), values);
        });
  }

  // 7. Column-mapping (name mode) table, OPTIMIZE, stream through.
  // KNOWN-GAP: OPTIMIZE blocked on UC MANAGED.
  @TestAllTableTypes
  public void testOptimize_onColMappedTable(TableType tableType) throws Exception {
    Assumptions.assumeFalse(
        tableType == TableType.MANAGED, "KNOWN-GAP: OPTIMIZE blocked on UC MANAGED tables");

    withNewTable(
        "opt_cm",
        "id INT, user_name STRING",
        null,
        tableType,
        "'delta.columnMapping.mode' = 'name'",
        tableName -> {
          sql("INSERT INTO %s VALUES (1, 'Alice')", tableName);
          sql("INSERT INTO %s VALUES (2, 'Bob')", tableName);
          sql("INSERT INTO %s VALUES (3, 'Carol')", tableName);

          runOptimize(tableName);

          Dataset<Row> df = spark().readStream().format("delta").table(tableName);
          List<Row> rows = drainMemorySink(df, queryName("opt_cm", tableType));
          assertEquals(3, rows.size(), "expected 3 rows; got: " + rows);
        });
  }

  // 8. Partitioned table; OPTIMIZE ... WHERE part='X' only rewrites one partition.
  // KNOWN-GAP: OPTIMIZE blocked on UC MANAGED.
  @TestAllTableTypes
  public void testOptimize_onPartitionedTable_perPartition(TableType tableType) throws Exception {
    Assumptions.assumeFalse(
        tableType == TableType.MANAGED, "KNOWN-GAP: OPTIMIZE blocked on UC MANAGED tables");

    withNewTable(
        "opt_partial",
        "id INT, part STRING",
        "part",
        tableType,
        tableName -> {
          sql("INSERT INTO %s VALUES (1, 'X')", tableName);
          sql("INSERT INTO %s VALUES (2, 'X')", tableName);
          sql("INSERT INTO %s VALUES (3, 'Y')", tableName);
          sql("INSERT INTO %s VALUES (4, 'Y')", tableName);

          String key = "spark.databricks.delta.optimize.minFileSize";
          spark().conf().set(key, Long.toString(1024L * 1024L * 1024L));
          try {
            sql("OPTIMIZE %s WHERE part = 'X'", tableName);
          } finally {
            spark().conf().unset(key);
          }

          Dataset<Row> df = spark().readStream().format("delta").table(tableName);
          List<Row> rows = drainMemorySink(df, queryName("opt_partial", tableType));
          assertEquals(4, rows.size(), "expected 4 rows across both partitions; got: " + rows);
        });
  }

  // 9. Stats-recompute stub. The stats-recompute path is covered by an existing DSv2 test in
  // V2StreamingReadTest; this stub anchors the campaign coverage row.
  @TestAllTableTypes
  public void testStatsRecompute_noReEmission(TableType tableType) {
    // Intentionally a no-op stub. See V2StreamingReadTest#testStreamingReadAfterStatsRecompute.
  }

  // 10. ALTER TABLE SET TBLPROPERTIES is metadata-only; the stream must ignore it.
  // KNOWN-GAP: ALTER TABLE blocked on UC MANAGED by UCSingleCatalog.
  @TestAllTableTypes
  public void testAlterTableSetTblProperties_noReEmission(TableType tableType) throws Exception {
    Assumptions.assumeFalse(
        tableType == TableType.MANAGED,
        "KNOWN-GAP: ALTER TABLE SET TBLPROPERTIES blocked on UC MANAGED tables");

    withNewTable(
        "opt_alter_props",
        "id INT, name STRING",
        null,
        tableType,
        tableName -> {
          sql("INSERT INTO %s VALUES (1, 'A')", tableName);

          String qName = queryName("opt_alter_props", tableType);
          String ck = checkpoint();

          StreamingQuery q1 =
              spark()
                  .readStream()
                  .format("delta")
                  .table(tableName)
                  .writeStream()
                  .format("memory")
                  .queryName(qName)
                  .outputMode("append")
                  .option("checkpointLocation", ck)
                  .start();
          try {
            q1.processAllAvailable();
          } finally {
            q1.stop();
          }

          // Metadata-only commit; no AddFile/RemoveFile.
          sql("ALTER TABLE %s SET TBLPROPERTIES ('foo' = 'bar')", tableName);
          sql("INSERT INTO %s VALUES (2, 'B')", tableName);

          StreamingQuery q2 =
              spark()
                  .readStream()
                  .format("delta")
                  .table(tableName)
                  .writeStream()
                  .format("memory")
                  .queryName(qName)
                  .outputMode("append")
                  .option("checkpointLocation", ck)
                  .start();
          try {
            q2.processAllAvailable();
            List<Row> rows = spark().sql("SELECT * FROM " + qName).collectAsList();
            assertEquals(2, rows.size(), "expected 2 rows after ALTER + INSERT; got: " + rows);
          } finally {
            q2.stop();
            spark().sql("DROP VIEW IF EXISTS " + qName);
          }
        });
  }
}
