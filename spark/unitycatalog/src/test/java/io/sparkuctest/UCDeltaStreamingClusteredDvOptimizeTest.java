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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.io.TempDir;

/**
 * UC port of {@link io.delta.spark.internal.v2.V2StreamingClusteredDvOptimizeTest} - the Liquid-
 * clustered x deletion-vectors x OPTIMIZE triple-feature composition.
 *
 * <p>All cases here run OPTIMIZE, which is blocked on UC MANAGED tables via {@code
 * DELTA_UNSUPPORTED_CATALOG_MANAGED_TABLE_OPERATION} (see {@code
 * UCDeltaUtilityTest#testMaintenanceOpsBlockedOnManagedTable}). Each case therefore aborts on
 * MANAGED via {@link Assumptions#assumeTrue}; EXTERNAL runs the full body.
 *
 * <p>Cases that additionally need ALTER CLUSTER BY ({@code testAlterClusterBy_*}, {@code
 * testDropClustering_*}) work fine on EXTERNAL but cannot translate to UC MANAGED at all - {@code
 * UCSingleCatalog.alterTable()} throws on ALTER. They remain in the same aborted-on- MANAGED
 * bucket.
 */
public class UCDeltaStreamingClusteredDvOptimizeTest extends UCDeltaTableIntegrationBaseTest {

  /** 1 GiB - guarantees small per-commit files are below the OPTIMIZE compaction threshold. */
  private static final long ONE_GIB = 1024L * 1024L * 1024L;

  @TempDir private Path tempDir;
  private int checkpointCount;

  private String checkpoint() throws IOException {
    Path ckDir = tempDir.resolve("ck-" + checkpointCount++);
    Files.createDirectory(ckDir);
    return ckDir.toString();
  }

  /**
   * Creates a clustered DV-enabled table directly (the standard {@code withNewTable} helper does
   * not support CLUSTER BY). Mirrors the pattern in {@link UCDeltaStreamingClusteredTest}.
   */
  private void withNewClusteredDvTable(
      String simpleName,
      String schemaSql,
      String clusterCols,
      TableType tableType,
      TestCode testCode)
      throws Exception {
    if (tableType == TableType.EXTERNAL) {
      withTempDir(
          dir -> {
            org.apache.hadoop.fs.Path tablePath = new org.apache.hadoop.fs.Path(dir, simpleName);
            String name = fullTableName(simpleName);
            sql(
                "CREATE TABLE %s (%s) USING DELTA CLUSTER BY (%s) LOCATION '%s' "
                    + "TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')",
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
          "CREATE TABLE %s (%s) USING DELTA CLUSTER BY (%s) "
              + "TBLPROPERTIES ('delta.feature.catalogManaged'='supported', "
              + "'delta.enableDeletionVectors' = 'true')",
          name, schemaSql, clusterCols);
      try {
        testCode.run(name);
      } finally {
        sql("DROP TABLE IF EXISTS %s", name);
      }
    }
  }

  /** Force OPTIMIZE to compact even tiny files by raising minFileSize past their actual size. */
  private void runOptimize(String tableName) {
    String prev = null;
    try {
      prev = spark().conf().get("spark.databricks.delta.optimize.minFileSize");
    } catch (java.util.NoSuchElementException ignored) {
      // not previously set
    }
    spark().conf().set("spark.databricks.delta.optimize.minFileSize", Long.toString(ONE_GIB));
    try {
      sql("OPTIMIZE %s", tableName);
    } finally {
      if (prev != null) {
        spark().conf().set("spark.databricks.delta.optimize.minFileSize", prev);
      } else {
        spark().conf().unset("spark.databricks.delta.optimize.minFileSize");
      }
    }
  }

  private static Set<Integer> asIntSet(List<Row> rows) {
    Set<Integer> ids = new HashSet<>();
    for (Row r : rows) {
      ids.add(r.getInt(0));
    }
    return ids;
  }

  /**
   * Mirror of {@code V2StreamingClusteredDvOptimizeTest#testBasic_clustered_dv_optimize_-
   * noReEmission}. Aborted on MANAGED because OPTIMIZE is blocked.
   */
  @TestAllTableTypes
  public void testBasic_clustered_dv_optimize_noReEmission(TableType tableType) throws Exception {
    Assumptions.assumeTrue(
        tableType == TableType.EXTERNAL,
        "UC-INFRA-BLOCKED: OPTIMIZE throws DELTA_UNSUPPORTED_CATALOG_MANAGED_TABLE_OPERATION on "
            + "MANAGED");
    withNewClusteredDvTable(
        "cdvopt_basic",
        "value INT",
        "value",
        tableType,
        tableName -> {
          spark()
              .range(10)
              .selectExpr("cast(id as int) as value")
              .coalesce(1)
              .write()
              .format("delta")
              .mode("append")
              .saveAsTable(tableName);
          sql("DELETE FROM %s WHERE value < 3", tableName);

          String queryName =
              "cdvopt_basic_" + tableType.name().toLowerCase() + "_" + checkpointCount;
          String ck = checkpoint();

          // First drain: consume the initial 7 surviving rows.
          Dataset<Row> df1 = spark().readStream().format("delta").table(tableName);
          StreamingQuery q1 =
              df1.writeStream()
                  .format("memory")
                  .queryName(queryName)
                  .outputMode("append")
                  .option("checkpointLocation", ck)
                  .start();
          q1.processAllAvailable();
          q1.stop();

          runOptimize(tableName);

          // Restart: OPTIMIZE is dataChange=false, should not re-emit.
          Dataset<Row> df2 = spark().readStream().format("delta").table(tableName);
          StreamingQuery q2 =
              df2.writeStream()
                  .format("memory")
                  .queryName(queryName)
                  .outputMode("append")
                  .option("checkpointLocation", ck)
                  .start();
          q2.processAllAvailable();
          q2.stop();

          List<Row> rows = spark().sql("SELECT * FROM " + queryName).collectAsList();
          assertEquals(7, rows.size(), () -> "Expected 7 rows after OPTIMIZE no-reemit: " + rows);
          Set<Integer> expected = new HashSet<>();
          for (int i = 3; i < 10; i++) {
            expected.add(i);
          }
          assertEquals(expected, asIntSet(rows));
          spark().sql("DROP VIEW IF EXISTS " + queryName);
        });
  }

  /**
   * Mirror of {@code V2StreamingClusteredDvOptimizeTest#testStreamRowCount_acrossOptimize_-
   * clustered_dv}.
   */
  @TestAllTableTypes
  public void testStreamRowCount_acrossOptimize_clustered_dv(TableType tableType) throws Exception {
    Assumptions.assumeTrue(
        tableType == TableType.EXTERNAL,
        "UC-INFRA-BLOCKED: OPTIMIZE throws DELTA_UNSUPPORTED_CATALOG_MANAGED_TABLE_OPERATION on "
            + "MANAGED");
    withNewClusteredDvTable(
        "cdvopt_rowcount",
        "value INT",
        "value",
        tableType,
        tableName -> {
          int numCommits = 6;
          for (int i = 0; i < numCommits; i++) {
            sql("INSERT INTO %s VALUES (%d)", tableName, i);
          }
          sql("DELETE FROM %s WHERE value IN (1, 4)", tableName);
          runOptimize(tableName);

          String queryName =
              "cdvopt_rowcount_" + tableType.name().toLowerCase() + "_" + checkpointCount;
          StreamingQuery q =
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
                  .start();
          q.awaitTermination();

          List<Row> rows = spark().sql("SELECT * FROM " + queryName).collectAsList();
          assertEquals(numCommits - 2, rows.size());
          Set<Integer> expected = new HashSet<>();
          for (int i = 0; i < numCommits; i++) {
            if (i != 1 && i != 4) expected.add(i);
          }
          assertEquals(expected, asIntSet(rows));
          spark().sql("DROP VIEW IF EXISTS " + queryName);
        });
  }

  /**
   * Mirror of {@code V2StreamingClusteredDvOptimizeTest#testRestart_clustered_dv_optimizeBetween-
   * Runs}.
   */
  @TestAllTableTypes
  public void testRestart_clustered_dv_optimizeBetweenRuns(TableType tableType) throws Exception {
    Assumptions.assumeTrue(
        tableType == TableType.EXTERNAL,
        "UC-INFRA-BLOCKED: OPTIMIZE throws DELTA_UNSUPPORTED_CATALOG_MANAGED_TABLE_OPERATION on "
            + "MANAGED");
    withNewClusteredDvTable(
        "cdvopt_restart",
        "value INT",
        "value",
        tableType,
        tableName -> {
          spark()
              .range(8)
              .selectExpr("cast(id as int) as value")
              .coalesce(1)
              .write()
              .format("delta")
              .mode("append")
              .saveAsTable(tableName);

          String queryName =
              "cdvopt_restart_" + tableType.name().toLowerCase() + "_" + checkpointCount;
          String ck = checkpoint();

          Dataset<Row> df1 =
              spark()
                  .readStream()
                  .format("delta")
                  .option("skipChangeCommits", "true")
                  .table(tableName);
          StreamingQuery q1 =
              df1.writeStream()
                  .format("memory")
                  .queryName(queryName)
                  .outputMode("append")
                  .option("checkpointLocation", ck)
                  .start();
          q1.processAllAvailable();
          q1.stop();
          long firstDrain =
              spark().sql("SELECT COUNT(*) FROM " + queryName).collectAsList().get(0).getLong(0);
          assertEquals(8L, firstDrain);

          // DV-delete (skipped by skipChangeCommits) + OPTIMIZE.
          sql("DELETE FROM %s WHERE value IN (2, 5)", tableName);
          runOptimize(tableName);

          // Add a real data commit after OPTIMIZE.
          sql("INSERT INTO %s VALUES (100), (101)", tableName);

          Dataset<Row> df2 =
              spark()
                  .readStream()
                  .format("delta")
                  .option("skipChangeCommits", "true")
                  .table(tableName);
          StreamingQuery q2 =
              df2.writeStream()
                  .format("memory")
                  .queryName(queryName)
                  .outputMode("append")
                  .option("checkpointLocation", ck)
                  .start();
          q2.processAllAvailable();
          q2.stop();

          // The restart should yield the original-8 rows from the first drain PLUS the (100,101)
          // commit emitted on resume. The DV-delete and OPTIMIZE contribute nothing.
          long total =
              spark().sql("SELECT COUNT(*) FROM " + queryName).collectAsList().get(0).getLong(0);
          assertEquals(firstDrain + 2L, total);
          spark().sql("DROP VIEW IF EXISTS " + queryName);
        });
  }

  /**
   * Mirror of {@code V2StreamingClusteredDvOptimizeTest#testAvailableNow_clustered_dv_optimize}.
   */
  @TestAllTableTypes
  public void testAvailableNow_clustered_dv_optimize(TableType tableType) throws Exception {
    Assumptions.assumeTrue(
        tableType == TableType.EXTERNAL,
        "UC-INFRA-BLOCKED: OPTIMIZE throws DELTA_UNSUPPORTED_CATALOG_MANAGED_TABLE_OPERATION on "
            + "MANAGED");
    withNewClusteredDvTable(
        "cdvopt_avail_now",
        "value INT",
        "value",
        tableType,
        tableName -> {
          for (int i = 0; i < 5; i++) {
            sql("INSERT INTO %s VALUES (%d)", tableName, i);
          }
          sql("DELETE FROM %s WHERE value = 2", tableName);
          runOptimize(tableName);

          String queryName =
              "cdvopt_avail_now_" + tableType.name().toLowerCase() + "_" + checkpointCount;
          StreamingQuery q =
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
                  .start();
          assertTrue(q.awaitTermination(60_000L), "AvailableNow should terminate");

          List<Row> rows = spark().sql("SELECT * FROM " + queryName).collectAsList();
          assertEquals(4, rows.size(), () -> "Expected 4 rows post-OPTIMIZE: " + rows);
          assertEquals(Set.of(0, 1, 3, 4), asIntSet(rows));
          spark().sql("DROP VIEW IF EXISTS " + queryName);
        });
  }

  /**
   * Mirror of {@code V2StreamingClusteredDvOptimizeTest#testMaxFilesPerTrigger_clustered_dv_-
   * optimize}.
   */
  @TestAllTableTypes
  public void testMaxFilesPerTrigger_clustered_dv_optimize(TableType tableType) throws Exception {
    Assumptions.assumeTrue(
        tableType == TableType.EXTERNAL,
        "UC-INFRA-BLOCKED: OPTIMIZE throws DELTA_UNSUPPORTED_CATALOG_MANAGED_TABLE_OPERATION on "
            + "MANAGED");
    withNewClusteredDvTable(
        "cdvopt_mfpt",
        "value INT",
        "value",
        tableType,
        tableName -> {
          for (int i = 0; i < 5; i++) {
            sql("INSERT INTO %s VALUES (%d)", tableName, i);
          }
          sql("DELETE FROM %s WHERE value = 3", tableName);
          runOptimize(tableName);

          String queryName =
              "cdvopt_mfpt_" + tableType.name().toLowerCase() + "_" + checkpointCount;
          StreamingQuery q =
              spark()
                  .readStream()
                  .format("delta")
                  .option("maxFilesPerTrigger", "1")
                  .table(tableName)
                  .writeStream()
                  .format("memory")
                  .queryName(queryName)
                  .outputMode("append")
                  .trigger(Trigger.AvailableNow())
                  .option("checkpointLocation", checkpoint())
                  .start();
          q.awaitTermination();

          List<Row> rows = spark().sql("SELECT * FROM " + queryName).collectAsList();
          assertEquals(Set.of(0, 1, 2, 4), asIntSet(rows));
          spark().sql("DROP VIEW IF EXISTS " + queryName);
        });
  }

  /**
   * Mirror of {@code V2StreamingClusteredDvOptimizeTest#testAlterClusterBy_then_dvDelete_then_-
   * optimize}. Aborted on MANAGED because both ALTER CLUSTER BY and OPTIMIZE are blocked.
   */
  @TestAllTableTypes
  public void testAlterClusterBy_then_dvDelete_then_optimize(TableType tableType) throws Exception {
    Assumptions.assumeTrue(
        tableType == TableType.EXTERNAL,
        "UC-INFRA-BLOCKED: ALTER TABLE and OPTIMIZE are both blocked on MANAGED");
    withNewClusteredDvTable(
        "cdvopt_alter_cb",
        "col1 INT, col2 INT",
        "col1",
        tableType,
        tableName -> {
          sql("INSERT INTO %s VALUES (1, 10), (2, 20), (3, 30), (4, 40)", tableName);
          sql("ALTER TABLE %s CLUSTER BY (col2)", tableName);
          sql("DELETE FROM %s WHERE col1 = 2", tableName);
          runOptimize(tableName);

          String queryName =
              "cdvopt_alter_cb_" + tableType.name().toLowerCase() + "_" + checkpointCount;
          StreamingQuery q =
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
                  .start();
          q.awaitTermination();

          List<Row> rows =
              spark().sql("SELECT * FROM " + queryName + " ORDER BY col1").collectAsList();
          assertEquals(3, rows.size(), () -> "Expected 3 surviving rows: " + rows);
          assertEquals(1, rows.get(0).getInt(0));
          assertEquals(3, rows.get(1).getInt(0));
          assertEquals(4, rows.get(2).getInt(0));
          spark().sql("DROP VIEW IF EXISTS " + queryName);
        });
  }

  /** Mirror of {@code V2StreamingClusteredDvOptimizeTest#testDropClustering_with_dv}. */
  @TestAllTableTypes
  public void testDropClustering_with_dv(TableType tableType) throws Exception {
    Assumptions.assumeTrue(
        tableType == TableType.EXTERNAL,
        "UC-INFRA-BLOCKED: ALTER TABLE and OPTIMIZE are both blocked on MANAGED");
    withNewClusteredDvTable(
        "cdvopt_drop_cluster",
        "value INT",
        "value",
        tableType,
        tableName -> {
          sql("INSERT INTO %s VALUES (1), (2), (3), (4)", tableName);
          sql("ALTER TABLE %s CLUSTER BY NONE", tableName);
          sql("DELETE FROM %s WHERE value = 2", tableName);
          runOptimize(tableName);

          String queryName =
              "cdvopt_drop_cluster_" + tableType.name().toLowerCase() + "_" + checkpointCount;
          StreamingQuery q =
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
                  .start();
          q.awaitTermination();

          List<Row> rows = spark().sql("SELECT * FROM " + queryName).collectAsList();
          assertEquals(3, rows.size(), () -> "Expected 3 surviving rows: " + rows);
          assertEquals(Set.of(1, 3, 4), asIntSet(rows));
          spark().sql("DROP VIEW IF EXISTS " + queryName);
        });
  }

  /** Mirror of {@code V2StreamingClusteredDvOptimizeTest#testMultiColumnClusterBy_dv_optimize}. */
  @TestAllTableTypes
  public void testMultiColumnClusterBy_dv_optimize(TableType tableType) throws Exception {
    Assumptions.assumeTrue(
        tableType == TableType.EXTERNAL,
        "UC-INFRA-BLOCKED: OPTIMIZE throws DELTA_UNSUPPORTED_CATALOG_MANAGED_TABLE_OPERATION on "
            + "MANAGED");
    withNewClusteredDvTable(
        "cdvopt_multi_cb",
        "col1 INT, col2 STRING",
        "col1, col2",
        tableType,
        tableName -> {
          sql("INSERT INTO %s VALUES (1, 'A'), (2, 'B'), (3, 'C'), (4, 'D')", tableName);
          sql("DELETE FROM %s WHERE col1 = 2", tableName);
          runOptimize(tableName);

          String queryName =
              "cdvopt_multi_cb_" + tableType.name().toLowerCase() + "_" + checkpointCount;
          StreamingQuery q =
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
                  .start();
          q.awaitTermination();

          List<Row> rows =
              spark().sql("SELECT * FROM " + queryName + " ORDER BY col1").collectAsList();
          assertEquals(3, rows.size(), () -> "Expected 3 surviving rows: " + rows);
          assertEquals(1, rows.get(0).getInt(0));
          assertEquals("A", rows.get(0).getString(1));
          assertEquals(3, rows.get(1).getInt(0));
          assertEquals("C", rows.get(1).getString(1));
          assertEquals(4, rows.get(2).getInt(0));
          assertEquals("D", rows.get(2).getString(1));
          spark().sql("DROP VIEW IF EXISTS " + queryName);
        });
  }

  /**
   * Mirror of {@code V2StreamingClusteredDvOptimizeTest#testRowCount_optimize_zorder_on_clustered_-
   * dv}. Aborted on MANAGED: the MANAGED OPTIMIZE rejection ({@code DELTA_UNSUPPORTED_CATALOG_-
   * MANAGED_TABLE_OPERATION}) preempts the V2 test's expected ZORDER-on-clustered error, so the V2
   * contract (Delta rejects ZORDER on clustered) cannot be observed here.
   */
  @TestAllTableTypes
  public void testRowCount_optimize_zorder_on_clustered_dv(TableType tableType) throws Exception {
    Assumptions.assumeTrue(
        tableType == TableType.EXTERNAL,
        "UC-INFRA-BLOCKED: OPTIMIZE on MANAGED throws a different error than ZORDER-on-clustered");
    withNewClusteredDvTable(
        "cdvopt_zorder",
        "value INT",
        "value",
        tableType,
        tableName -> {
          sql("INSERT INTO %s VALUES (1), (2), (3), (4)", tableName);
          sql("DELETE FROM %s WHERE value = 2", tableName);

          AnalysisException thrown =
              assertThrows(
                  AnalysisException.class,
                  () -> {
                    String prev = null;
                    try {
                      prev = spark().conf().get("spark.databricks.delta.optimize.minFileSize");
                    } catch (java.util.NoSuchElementException ignored) {
                      // not previously set
                    }
                    spark()
                        .conf()
                        .set("spark.databricks.delta.optimize.minFileSize", Long.toString(ONE_GIB));
                    try {
                      sql("OPTIMIZE %s ZORDER BY (value)", tableName);
                    } finally {
                      if (prev != null) {
                        spark().conf().set("spark.databricks.delta.optimize.minFileSize", prev);
                      } else {
                        spark().conf().unset("spark.databricks.delta.optimize.minFileSize");
                      }
                    }
                  });
          assertTrue(
              thrown.getMessage() != null
                  && (thrown.getMessage().contains("DELTA_CLUSTERING_WITH_ZORDER_BY")
                      || thrown.getMessage().toLowerCase().contains("zorder")),
              () -> "Unexpected error message: " + thrown.getMessage());
        });
  }
}
