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
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.junit.jupiter.api.io.TempDir;

/**
 * Streaming reads on Liquid-clustered Delta tables (CLUSTER BY) via Unity Catalog.
 *
 * <p>Ports {@link io.delta.spark.internal.v2.V2StreamingClusteredTest} (file-path EXTERNAL via
 * dsv2.delta.&lt;path&gt;) to the UC catalog access path with {@code @TestAllTableTypes}. The
 * MANAGED variant is the bug-surfacing target; EXTERNAL via UC is a useful control.
 *
 * <p>Skipped cases (cannot translate to UC managed SQL):
 *
 * <ul>
 *   <li>ALTER CLUSTER BY mid-stream (case 7): UC blocks ALTER TABLE on MANAGED tables.
 *   <li>ALTER CLUSTER BY NONE (case 10): same UC ALTER block.
 * </ul>
 */
public class UCDeltaStreamingClusteredTest extends UCDeltaTableIntegrationBaseTest {

  @TempDir private Path tempDir;
  private int checkpointCount;

  /** Fresh local checkpoint dir; UC server holds cloud creds, not Spark. */
  private String checkpoint() throws IOException {
    Path ckDir = tempDir.resolve("ck-" + checkpointCount++);
    Files.createDirectory(ckDir);
    return ckDir.toString();
  }

  /**
   * Creates a clustered table directly (not via withNewTable) because the standard helper does not
   * support CLUSTER BY. Mirrors the pattern in UCDeltaTableBlockMetadataUpdateTest.
   */
  private void withNewClusteredTable(
      String simpleName,
      String schemaSql,
      String clusterCols,
      TableType tableType,
      TestCode testCode)
      throws Exception {
    if (tableType == TableType.EXTERNAL) {
      // External clustered tables need a LOCATION; use the existing temp-dir helper.
      withTempDir(
          dir -> {
            org.apache.hadoop.fs.Path tablePath = new org.apache.hadoop.fs.Path(dir, simpleName);
            String name = fullTableName(simpleName);
            sql(
                "CREATE TABLE %s (%s) USING DELTA CLUSTER BY (%s) LOCATION '%s'",
                name, schemaSql, clusterCols, tablePath.toString());
            try {
              testCode.run(name);
            } finally {
              sql("DROP TABLE IF EXISTS %s", name);
            }
          });
    } else {
      String name = fullTableName(simpleName);
      sql(
          "CREATE TABLE %s (%s) USING DELTA CLUSTER BY (%s)"
              + " TBLPROPERTIES ('delta.feature.catalogManaged'='supported')",
          name, schemaSql, clusterCols);
      try {
        testCode.run(name);
      } finally {
        sql("DROP TABLE IF EXISTS %s", name);
      }
    }
  }

  /** 1. Stream from clustered table, basic. */
  @TestAllTableTypes
  public void testStreamingReadClusteredBasic(TableType tableType) throws Exception {
    withNewClusteredTable(
        "uc_clustered_basic",
        "id INT, name STRING",
        "id",
        tableType,
        tableName -> {
          sql("INSERT INTO %s VALUES (1, 'Alice'), (2, 'Bob')", tableName);
          String queryName =
              "uc_clustered_basic_" + tableType.name().toLowerCase() + "_" + checkpointCount;
          Dataset<Row> input = spark().readStream().table(tableName);
          assertTrue(input.isStreaming());
          input
              .writeStream()
              .format("memory")
              .queryName(queryName)
              .outputMode("append")
              .trigger(Trigger.AvailableNow())
              .option("checkpointLocation", checkpoint())
              .start()
              .awaitTermination();

          List<Row> rows =
              spark().sql("SELECT * FROM " + queryName + " ORDER BY id").collectAsList();
          assertEquals(2, rows.size(), "expected 2 rows; got: " + rows);
          assertEquals(1, rows.get(0).getInt(0));
          assertEquals("Alice", rows.get(0).getString(1));
          assertEquals(2, rows.get(1).getInt(0));
          assertEquals("Bob", rows.get(1).getString(1));
          spark().sql("DROP VIEW IF EXISTS " + queryName);
        });
  }

  /** 2. Clustered + DV (DELETE then stream). */
  @TestAllTableTypes
  public void testStreamingReadClusteredWithDV(TableType tableType) throws Exception {
    withNewClusteredTable(
        "uc_clustered_dv",
        "value INT",
        "value",
        tableType,
        tableName -> {
          // Seed 10 rows then DELETE first 3 to materialize a deletion vector.
          sql("INSERT INTO %s VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)", tableName);
          sql("DELETE FROM %s WHERE value < 3", tableName);

          String queryName =
              "uc_clustered_dv_" + tableType.name().toLowerCase() + "_" + checkpointCount;
          spark()
              .readStream()
              .table(tableName)
              .writeStream()
              .format("memory")
              .queryName(queryName)
              .outputMode("append")
              .trigger(Trigger.AvailableNow())
              .option("checkpointLocation", checkpoint())
              .start()
              .awaitTermination();

          List<Row> rows =
              spark().sql("SELECT * FROM " + queryName + " ORDER BY value").collectAsList();
          assertEquals(7, rows.size(), "expected 7 rows after DV; got: " + rows);
          for (int i = 0; i < 7; i++) {
            assertEquals(i + 3, rows.get(i).getInt(0));
          }
          spark().sql("DROP VIEW IF EXISTS " + queryName);
        });
  }

  /** 3. Clustered + column mapping name mode. */
  @TestAllTableTypes
  public void testStreamingReadClusteredWithColumnMapping(TableType tableType) throws Exception {
    // Column mapping requires extra TBLPROPERTIES; build the CREATE inline.
    String simpleName = "uc_clustered_cm";
    if (tableType == TableType.EXTERNAL) {
      withTempDir(
          dir -> {
            org.apache.hadoop.fs.Path tablePath = new org.apache.hadoop.fs.Path(dir, simpleName);
            String name = fullTableName(simpleName);
            sql(
                "CREATE TABLE %s (id INT, user_name STRING) USING DELTA CLUSTER BY (id)"
                    + " LOCATION '%s'"
                    + " TBLPROPERTIES ('delta.columnMapping.mode' = 'name')",
                name, tablePath.toString());
            try {
              runColumnMappingBody(name, tableType);
            } finally {
              sql("DROP TABLE IF EXISTS %s", name);
            }
          });
    } else {
      String name = fullTableName(simpleName);
      sql(
          "CREATE TABLE %s (id INT, user_name STRING) USING DELTA CLUSTER BY (id)"
              + " TBLPROPERTIES ('delta.feature.catalogManaged'='supported',"
              + " 'delta.columnMapping.mode'='name')",
          name);
      try {
        runColumnMappingBody(name, tableType);
      } finally {
        sql("DROP TABLE IF EXISTS %s", name);
      }
    }
  }

  private void runColumnMappingBody(String tableName, TableType tableType) throws Exception {
    sql("INSERT INTO %s VALUES (1, 'Alice'), (2, 'Bob')", tableName);
    String queryName = "uc_clustered_cm_" + tableType.name().toLowerCase() + "_" + checkpointCount;
    spark()
        .readStream()
        .table(tableName)
        .writeStream()
        .format("memory")
        .queryName(queryName)
        .outputMode("append")
        .trigger(Trigger.AvailableNow())
        .option("checkpointLocation", checkpoint())
        .start()
        .awaitTermination();

    List<Row> rows = spark().sql("SELECT * FROM " + queryName + " ORDER BY id").collectAsList();
    assertEquals(2, rows.size(), "expected 2 rows; got: " + rows);
    spark().sql("DROP VIEW IF EXISTS " + queryName);
  }

  /** 4. Clustered + restart picks up new commits. */
  @TestAllTableTypes
  public void testStreamingReadClusteredRestart(TableType tableType) throws Exception {
    withNewClusteredTable(
        "uc_clustered_restart",
        "id INT, name STRING",
        "id",
        tableType,
        tableName -> {
          sql("INSERT INTO %s VALUES (1, 'Alice')", tableName);

          String queryName =
              "uc_clustered_restart_" + tableType.name().toLowerCase() + "_" + checkpointCount;
          String checkpointDir = checkpoint();

          // First run.
          StreamingQuery q1 =
              spark()
                  .readStream()
                  .table(tableName)
                  .writeStream()
                  .format("memory")
                  .queryName(queryName)
                  .outputMode("append")
                  .option("checkpointLocation", checkpointDir)
                  .start();
          try {
            q1.processAllAvailable();
          } finally {
            q1.stop();
          }

          sql("INSERT INTO %s VALUES (2, 'Bob')", tableName);

          StreamingQuery q2 =
              spark()
                  .readStream()
                  .table(tableName)
                  .writeStream()
                  .format("memory")
                  .queryName(queryName)
                  .outputMode("append")
                  .option("checkpointLocation", checkpointDir)
                  .start();
          try {
            q2.processAllAvailable();
            List<Row> rows =
                spark().sql("SELECT * FROM " + queryName + " ORDER BY id").collectAsList();
            assertEquals(2, rows.size(), "expected 2 rows after restart; got: " + rows);
          } finally {
            q2.stop();
            spark().sql("DROP VIEW IF EXISTS " + queryName);
          }
        });
  }

  /** 5. Clustered + Trigger.AvailableNow. */
  @TestAllTableTypes
  public void testStreamingReadClusteredTriggerAvailableNow(TableType tableType) throws Exception {
    withNewClusteredTable(
        "uc_clustered_avail_now",
        "id INT, name STRING",
        "id",
        tableType,
        tableName -> {
          sql("INSERT INTO %s VALUES (1, 'A'), (2, 'B'), (3, 'C')", tableName);

          String queryName =
              "uc_clustered_avail_now_" + tableType.name().toLowerCase() + "_" + checkpointCount;
          spark()
              .readStream()
              .table(tableName)
              .writeStream()
              .format("memory")
              .queryName(queryName)
              .outputMode("append")
              .trigger(Trigger.AvailableNow())
              .option("checkpointLocation", checkpoint())
              .start()
              .awaitTermination();

          List<Row> rows =
              spark().sql("SELECT * FROM " + queryName + " ORDER BY id").collectAsList();
          assertEquals(3, rows.size(), "expected 3 rows; got: " + rows);
          spark().sql("DROP VIEW IF EXISTS " + queryName);
        });
  }

  /** 6. Clustered + maxFilesPerTrigger=1: 3 single-row commits split into 3 batches. */
  @TestAllTableTypes
  public void testStreamingReadClusteredMaxFilesPerTrigger(TableType tableType) throws Exception {
    withNewClusteredTable(
        "uc_clustered_mfpt",
        "id INT, name STRING",
        "id",
        tableType,
        tableName -> {
          sql("INSERT INTO %s VALUES (1, 'A')", tableName);
          sql("INSERT INTO %s VALUES (2, 'B')", tableName);
          sql("INSERT INTO %s VALUES (3, 'C')", tableName);

          String queryName =
              "uc_clustered_mfpt_" + tableType.name().toLowerCase() + "_" + checkpointCount;
          spark()
              .readStream()
              .option("maxFilesPerTrigger", "1")
              .table(tableName)
              .writeStream()
              .format("memory")
              .queryName(queryName)
              .outputMode("append")
              .trigger(Trigger.AvailableNow())
              .option("checkpointLocation", checkpoint())
              .start()
              .awaitTermination();

          List<Row> rows =
              spark().sql("SELECT * FROM " + queryName + " ORDER BY id").collectAsList();
          assertEquals(3, rows.size(), "expected 3 rows; got: " + rows);
          spark().sql("DROP VIEW IF EXISTS " + queryName);
        });
  }

  // Cases 7 and 10 (ALTER CLUSTER BY mid-stream / DROP CLUSTERING) cannot translate to UC
  // MANAGED: ALTER TABLE on managed tables is blocked by UCSingleCatalog. See
  // UCDeltaTableBlockMetadataUpdateTest::testAlterTableOperationsAreBlocked.

  /** 8. Stream + OPTIMIZE produces dataChange=false; should not re-emit rows. */
  @TestAllTableTypes
  public void testStreamingReadClusteredWithOptimize(TableType tableType) throws Exception {
    withNewClusteredTable(
        "uc_clustered_opt",
        "id INT, name STRING",
        "id",
        tableType,
        tableName -> {
          sql("INSERT INTO %s VALUES (1, 'A')", tableName);
          sql("INSERT INTO %s VALUES (2, 'B')", tableName);
          sql("INSERT INTO %s VALUES (3, 'C')", tableName);

          String queryName =
              "uc_clustered_opt_" + tableType.name().toLowerCase() + "_" + checkpointCount;
          String checkpointDir = checkpoint();

          // First run drains the existing 3 rows.
          StreamingQuery q1 =
              spark()
                  .readStream()
                  .table(tableName)
                  .writeStream()
                  .format("memory")
                  .queryName(queryName)
                  .outputMode("append")
                  .option("checkpointLocation", checkpointDir)
                  .start();
          try {
            q1.processAllAvailable();
          } finally {
            q1.stop();
          }

          // OPTIMIZE produces a dataChange=false rewrite. Streaming source must skip it.
          sql("OPTIMIZE %s", tableName);

          StreamingQuery q2 =
              spark()
                  .readStream()
                  .table(tableName)
                  .writeStream()
                  .format("memory")
                  .queryName(queryName)
                  .outputMode("append")
                  .option("checkpointLocation", checkpointDir)
                  .start();
          try {
            q2.processAllAvailable();
            List<Row> rows =
                spark().sql("SELECT * FROM " + queryName + " ORDER BY id").collectAsList();
            // No re-emission expected; output is original 3 rows only.
            assertEquals(3, rows.size(), "expected 3 rows after OPTIMIZE; got: " + rows);
          } finally {
            q2.stop();
            spark().sql("DROP VIEW IF EXISTS " + queryName);
          }
        });
  }

  /** 9. Stream + multi-column CLUSTER BY. */
  @TestAllTableTypes
  public void testStreamingReadClusteredMultiColumn(TableType tableType) throws Exception {
    withNewClusteredTable(
        "uc_clustered_multi",
        "event_date DATE, region STRING, value INT",
        "event_date, region",
        tableType,
        tableName -> {
          sql(
              "INSERT INTO %s VALUES "
                  + "(DATE'2024-01-01', 'us', 1),"
                  + "(DATE'2024-01-02', 'eu', 2)",
              tableName);

          String queryName =
              "uc_clustered_multi_" + tableType.name().toLowerCase() + "_" + checkpointCount;
          spark()
              .readStream()
              .table(tableName)
              .writeStream()
              .format("memory")
              .queryName(queryName)
              .outputMode("append")
              .trigger(Trigger.AvailableNow())
              .option("checkpointLocation", checkpoint())
              .start()
              .awaitTermination();

          List<Row> rows =
              spark().sql("SELECT * FROM " + queryName + " ORDER BY value").collectAsList();
          assertEquals(2, rows.size(), "expected 2 rows; got: " + rows);
          spark().sql("DROP VIEW IF EXISTS " + queryName);
        });
  }
}
