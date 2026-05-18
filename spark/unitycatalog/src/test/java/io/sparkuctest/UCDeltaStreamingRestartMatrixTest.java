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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * UC port of {@link io.delta.spark.internal.v2.V2StreamingRestartMatrixTest}.
 *
 * <p>Restart-from-checkpoint matrix on UC EXTERNAL + MANAGED. Each test writes initial commits,
 * drains a stream to a parquet sink + checkpoint, then writes more commits and restarts the stream
 * from the same checkpoint. The total parquet sink contents at the end pins the row delta across
 * runs.
 *
 * <p>Cases requiring ALTER TABLE (ADD COLUMN, RENAME COLUMN, CLUSTER BY), OPTIMIZE, or direct
 * {@code _delta_log} mutation are skipped on UC MANAGED via {@link Assumptions#assumeFalse} (UC
 * blocks ALTER on MANAGED via the catalogManaged kill switch; OPTIMIZE is blocked on catalogManaged
 * tables; direct {@code _delta_log} access is hidden behind UC).
 */
public class UCDeltaStreamingRestartMatrixTest extends UCDeltaTableIntegrationBaseTest {

  @TempDir private Path tempDir;
  private int counter;

  /** Fresh local sink + checkpoint dirs. */
  private Path freshSinkDir() throws IOException {
    Path dir = tempDir.resolve("out-" + counter++);
    Files.createDirectory(dir);
    return dir;
  }

  private Path freshCheckpointDir() throws IOException {
    Path dir = tempDir.resolve("ck-" + counter++);
    Files.createDirectory(dir);
    return dir;
  }

  /**
   * Drains {@code streamingDF} into a parquet sink at {@code outputDir} with checkpoint at {@code
   * checkpointDir}. Returns the parquet sink contents.
   */
  private List<Row> runWithParquetSink(
      Dataset<Row> streamingDF, Path outputDir, Path checkpointDir, Trigger trigger)
      throws Exception {
    StreamingQuery query = null;
    try {
      DataStreamWriter<Row> writer =
          streamingDF
              .writeStream()
              .format("parquet")
              .outputMode("append")
              .option("path", outputDir.toString())
              .option("checkpointLocation", checkpointDir.toString());
      if (trigger != null) {
        writer = writer.trigger(trigger);
      }
      query = writer.start();
      query.processAllAvailable();
      if (trigger != null) {
        query.awaitTermination(60_000L);
      }
    } finally {
      if (query != null) {
        query.stop();
      }
    }
    return spark().read().parquet(outputDir.toString()).collectAsList();
  }

  /** Format epoch millis as the session-timezone "yyyy-MM-dd HH:mm:ss.SSS" string Spark parses. */
  private String formatTs(long millis) {
    String tz = spark().sessionState().conf().sessionLocalTimeZone();
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    sdf.setTimeZone(TimeZone.getTimeZone(tz));
    return sdf.format(new Date(millis));
  }

  /** Skip a test on UC MANAGED with a KNOWN-GAP comment. */
  private void skipManaged(TableType tableType, String reason) {
    Assumptions.assumeFalse(tableType == TableType.MANAGED, "KNOWN-GAP: " + reason);
  }

  /** Creates a clustered table directly because the standard helper does not support CLUSTER BY. */
  private void withNewClusteredTable(
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

  /** 1. DV table; DV-only DELETE between runs; restart with skipChangeCommits. */
  @TestAllTableTypes
  public void testRestart_onDvTable_withDvDeleteBetweenRuns(TableType tableType) throws Exception {
    withNewTable(
        "uc_rs_dv",
        "id INT",
        null,
        tableType,
        "'delta.enableDeletionVectors' = 'true'",
        tableName -> {
          spark()
              .range(0, 10)
              .selectExpr("cast(id as int) as id")
              .coalesce(1)
              .write()
              .format("delta")
              .mode("append")
              .saveAsTable(tableName);

          Path checkpointDir = freshCheckpointDir();
          Path outputDir = freshSinkDir();

          Dataset<Row> df1 = spark().readStream().table(tableName);
          runWithParquetSink(df1, outputDir, checkpointDir, /* trigger= */ null);

          // DV-only DELETE: rewrites a single file with a deletion vector attached.
          sql("DELETE FROM %s WHERE id %% 2 = 0", tableName);

          Dataset<Row> df2 =
              spark().readStream().option("skipChangeCommits", "true").table(tableName);
          List<Row> rows = runWithParquetSink(df2, outputDir, checkpointDir, /* trigger= */ null);
          assertEquals(
              10,
              rows.size(),
              () -> "Run 1 emitted 10 rows; restart drops DV DELETE. Got: " + rows);
        });
  }

  /** 2. CM-name; ADD COLUMN (additive) between runs. */
  @TestAllTableTypes
  public void testRestart_onColMappedNameTable_addColumnBetweenRuns(TableType tableType)
      throws Exception {
    skipManaged(tableType, "ALTER TABLE ADD COLUMN blocked on UC MANAGED.");
    withNewTable(
        "uc_rs_cm_name_addcol",
        "id INT, name STRING",
        null,
        tableType,
        "'delta.columnMapping.mode' = 'name'",
        tableName -> {
          sql("INSERT INTO %s VALUES (1, 'Alice'), (2, 'Bob')", tableName);

          Path checkpointDir = freshCheckpointDir();
          Path outputDir = freshSinkDir();

          Dataset<Row> df1 = spark().readStream().table(tableName);
          runWithParquetSink(df1, outputDir, checkpointDir, /* trigger= */ null);

          sql("ALTER TABLE %s ADD COLUMN extra STRING", tableName);
          sql("INSERT INTO %s VALUES (3, 'Carol', 'x')", tableName);

          Dataset<Row> df2 = spark().readStream().table(tableName);
          List<Row> rows = runWithParquetSink(df2, outputDir, checkpointDir, /* trigger= */ null);
          assertEquals(3, rows.size(), () -> "Expected 3 rows across runs; got: " + rows);
        });
  }

  /** 3. CM-id; ADD COLUMN (additive) between runs. */
  @TestAllTableTypes
  public void testRestart_onColMappedIdTable_addColumnBetweenRuns(TableType tableType)
      throws Exception {
    skipManaged(tableType, "ALTER TABLE ADD COLUMN blocked on UC MANAGED.");
    withNewTable(
        "uc_rs_cm_id_addcol",
        "id INT, name STRING",
        null,
        tableType,
        "'delta.columnMapping.mode' = 'id', "
            + "'delta.minReaderVersion' = '2', "
            + "'delta.minWriterVersion' = '5'",
        tableName -> {
          sql("INSERT INTO %s VALUES (1, 'Alice'), (2, 'Bob')", tableName);

          Path checkpointDir = freshCheckpointDir();
          Path outputDir = freshSinkDir();

          Dataset<Row> df1 = spark().readStream().table(tableName);
          runWithParquetSink(df1, outputDir, checkpointDir, /* trigger= */ null);

          sql("ALTER TABLE %s ADD COLUMN extra STRING", tableName);
          sql("INSERT INTO %s VALUES (3, 'Carol', 'x')", tableName);

          Dataset<Row> df2 = spark().readStream().table(tableName);
          List<Row> rows = runWithParquetSink(df2, outputDir, checkpointDir, /* trigger= */ null);
          assertEquals(3, rows.size(), () -> "Expected 3 rows across runs; got: " + rows);
        });
  }

  /**
   * 4. CM-name; RENAME COLUMN (non-additive) between runs.
   *
   * <p>Skipped unconditionally: UC EXTERNAL routes to DSv1 (no {@code schemaTrackingLocation}
   * support through that path); UC MANAGED blocks {@code ALTER TABLE} via {@code
   * UCSingleCatalog.alterTable()}. The non-UC path is covered by {@code
   * V2StreamingRestartMatrixTest#testRestart_onColMappedNameTable_renameColumnBetweenRuns}.
   */
  @TestAllTableTypes
  public void testRestart_onColMappedNameTable_renameColumnBetweenRuns(TableType tableType) {
    Assumptions.assumeTrue(
        false,
        "KNOWN-GAP: UC EXTERNAL → DSv1 has no schemaTrackingLocation; UC MANAGED blocks "
            + "ALTER TABLE via UCSingleCatalog. Covered by the non-UC V2 restart matrix test.");
  }

  /** 5. Row tracking; project _metadata.row_id across restart. */
  @TestAllTableTypes
  public void testRestart_onRowTrackedTable_projectRowId(TableType tableType) throws Exception {
    withNewTable(
        "uc_rs_rt",
        "id LONG, name STRING",
        null,
        tableType,
        "'delta.enableRowTracking' = 'true'",
        tableName -> {
          sql("INSERT INTO %s VALUES (1, 'A'), (2, 'B')", tableName);

          Path checkpointDir = freshCheckpointDir();
          Path outputDir = freshSinkDir();

          Dataset<Row> df1 =
              spark().readStream().table(tableName).selectExpr("id", "_metadata.row_id AS row_id");
          runWithParquetSink(df1, outputDir, checkpointDir, /* trigger= */ null);

          sql("INSERT INTO %s VALUES (3, 'C')", tableName);

          Dataset<Row> df2 =
              spark().readStream().table(tableName).selectExpr("id", "_metadata.row_id AS row_id");
          List<Row> rows = runWithParquetSink(df2, outputDir, checkpointDir, /* trigger= */ null);
          assertEquals(3, rows.size(), () -> "Expected 3 rows across restart; got: " + rows);
        });
  }

  /** 6. ICT table with startingTimestamp on a fresh stream (no checkpoint yet). */
  @TestAllTableTypes
  public void testRestart_onIctTable_withStartingTimestamp(TableType tableType) throws Exception {
    withNewTable(
        "uc_rs_ict",
        "id INT",
        null,
        tableType,
        "'delta.enableInCommitTimestamps' = 'true'",
        tableName -> {
          sql("INSERT INTO %s VALUES (1), (2)", tableName);

          Path checkpointDir = freshCheckpointDir();
          Path outputDir = freshSinkDir();

          String farPast = formatTs(0L);
          Dataset<Row> df1 =
              spark().readStream().option("startingTimestamp", farPast).table(tableName);
          runWithParquetSink(df1, outputDir, checkpointDir, /* trigger= */ null);

          sql("INSERT INTO %s VALUES (3)", tableName);

          // Restart re-applies startingTimestamp; the checkpoint should ignore it.
          Dataset<Row> df2 =
              spark().readStream().option("startingTimestamp", farPast).table(tableName);
          List<Row> rows = runWithParquetSink(df2, outputDir, checkpointDir, /* trigger= */ null);
          assertEquals(3, rows.size(), () -> "Expected 3 rows total; got: " + rows);
        });
  }

  /** 7. v2-checkpoint table; force a v2 checkpoint between runs. */
  @TestAllTableTypes
  public void testRestart_onV2CheckpointTable_forceCheckpointBetweenRuns(TableType tableType)
      throws Exception {
    skipManaged(
        tableType,
        "Forcing a checkpoint between runs uses DeltaLog.forTable(path).checkpoint(); UC MANAGED"
            + " hides the path.");
    withNewTable(
        "uc_rs_v2cp",
        "id BIGINT",
        null,
        tableType,
        "'delta.checkpointPolicy' = 'v2'",
        tableName -> {
          spark().range(0, 5).write().format("delta").mode("append").saveAsTable(tableName);

          Path checkpointDir = freshCheckpointDir();
          Path outputDir = freshSinkDir();

          Dataset<Row> df1 = spark().readStream().table(tableName);
          runWithParquetSink(df1, outputDir, checkpointDir, /* trigger= */ null);

          spark().range(5, 10).write().format("delta").mode("append").saveAsTable(tableName);
          // Force a v2 checkpoint between runs.
          String location = describeLocation(tableName);
          org.apache.spark.sql.delta.DeltaLog.forTable(spark(), location).checkpoint();
          spark().range(10, 15).write().format("delta").mode("append").saveAsTable(tableName);

          Dataset<Row> df2 = spark().readStream().table(tableName);
          List<Row> rows = runWithParquetSink(df2, outputDir, checkpointDir, /* trigger= */ null);
          assertEquals(15, rows.size(), () -> "Expected 15 rows after restart; got: " + rows);
        });
  }

  /** 8. Clustered table; ALTER CLUSTER BY between runs. */
  @TestAllTableTypes
  public void testRestart_onClusteredTable_alterClusterBetweenRuns(TableType tableType)
      throws Exception {
    skipManaged(tableType, "ALTER TABLE CLUSTER BY blocked on UC MANAGED.");
    withNewClusteredTable(
        "uc_rs_clustered_alter",
        "id INT, name STRING",
        "id",
        tableType,
        tableName -> {
          sql("INSERT INTO %s VALUES (1, 'A'), (2, 'B')", tableName);

          Path checkpointDir = freshCheckpointDir();
          Path outputDir = freshSinkDir();

          Dataset<Row> df1 = spark().readStream().table(tableName);
          runWithParquetSink(df1, outputDir, checkpointDir, /* trigger= */ null);

          sql("ALTER TABLE %s CLUSTER BY (name)", tableName);
          sql("INSERT INTO %s VALUES (3, 'C')", tableName);

          Dataset<Row> df2 = spark().readStream().table(tableName);
          List<Row> rows = runWithParquetSink(df2, outputDir, checkpointDir, /* trigger= */ null);
          assertEquals(
              3, rows.size(), () -> "Expected 3 rows after ALTER CLUSTER BY; got: " + rows);
        });
  }

  /** 9. TimestampNtz partition value crossing a DST boundary. */
  @TestAllTableTypes
  public void testRestart_onTimestampNtzTable_withDstBoundary(TableType tableType)
      throws Exception {
    withNewTable(
        "uc_rs_tsntz_dst",
        "id INT, ts TIMESTAMP_NTZ",
        "ts",
        tableType,
        null,
        tableName -> {
          sql("INSERT INTO %s VALUES (1, TIMESTAMP_NTZ'2024-03-10 01:30:00')", tableName);

          Path checkpointDir = freshCheckpointDir();
          Path outputDir = freshSinkDir();

          Dataset<Row> df1 = spark().readStream().table(tableName);
          runWithParquetSink(df1, outputDir, checkpointDir, /* trigger= */ null);

          sql("INSERT INTO %s VALUES (2, TIMESTAMP_NTZ'2024-03-10 03:30:00')", tableName);

          Dataset<Row> df2 = spark().readStream().table(tableName);
          List<Row> rows = runWithParquetSink(df2, outputDir, checkpointDir, /* trigger= */ null);
          assertEquals(2, rows.size(), () -> "Expected 2 rows across DST boundary; got: " + rows);
        });
  }

  /** 10. appendOnly + restart. */
  @TestAllTableTypes
  public void testRestart_onAppendOnlyTable_basic(TableType tableType) throws Exception {
    withNewTable(
        "uc_rs_appendonly",
        "id INT, name STRING",
        null,
        tableType,
        "'delta.appendOnly' = 'true'",
        tableName -> {
          sql("INSERT INTO %s VALUES (1, 'A')", tableName);

          Path checkpointDir = freshCheckpointDir();
          Path outputDir = freshSinkDir();

          Dataset<Row> df1 = spark().readStream().table(tableName);
          runWithParquetSink(df1, outputDir, checkpointDir, /* trigger= */ null);

          sql("INSERT INTO %s VALUES (2, 'B'), (3, 'C')", tableName);

          Dataset<Row> df2 = spark().readStream().table(tableName);
          List<Row> rows = runWithParquetSink(df2, outputDir, checkpointDir, /* trigger= */ null);
          assertEquals(3, rows.size(), () -> "Expected 3 rows across restart; got: " + rows);
        });
  }

  /** 11. Partitioned table; partition column listed last in the schema. */
  @TestAllTableTypes
  public void testRestart_onPartitionedTable_partLast(TableType tableType) throws Exception {
    withNewTable(
        "uc_rs_part_last",
        "id INT, part INT",
        "part",
        tableType,
        null,
        tableName -> {
          sql("INSERT INTO %s VALUES (1, 0), (2, 0), (3, 1)", tableName);

          Path checkpointDir = freshCheckpointDir();
          Path outputDir = freshSinkDir();

          Dataset<Row> df1 = spark().readStream().table(tableName);
          runWithParquetSink(df1, outputDir, checkpointDir, /* trigger= */ null);

          sql("INSERT INTO %s VALUES (4, 1), (5, 2)", tableName);

          Dataset<Row> df2 = spark().readStream().table(tableName);
          List<Row> rows = runWithParquetSink(df2, outputDir, checkpointDir, /* trigger= */ null);
          assertEquals(5, rows.size(), () -> "Expected 5 rows across restart; got: " + rows);
        });
  }

  /** 12. Variant; smoke restart. */
  @TestAllTableTypes
  public void testRestart_onVariantTable_basic(TableType tableType) throws Exception {
    withNewTable(
        "uc_rs_variant",
        "id INT, v VARIANT",
        null,
        tableType,
        "'delta.feature.variantType' = 'supported'",
        tableName -> {
          sql("INSERT INTO %s SELECT 1, parse_json('{\"a\": 1}')", tableName);

          Path checkpointDir = freshCheckpointDir();
          Path outputDir = freshSinkDir();

          Dataset<Row> df1 =
              spark().readStream().table(tableName).selectExpr("id", "to_json(v) AS v_json");
          runWithParquetSink(df1, outputDir, checkpointDir, /* trigger= */ null);

          sql("INSERT INTO %s SELECT 2, parse_json('{\"b\": 2}')", tableName);

          Dataset<Row> df2 =
              spark().readStream().table(tableName).selectExpr("id", "to_json(v) AS v_json");
          List<Row> rows = runWithParquetSink(df2, outputDir, checkpointDir, /* trigger= */ null);
          assertEquals(
              2, rows.size(), () -> "Expected 2 variant rows across restart; got: " + rows);
        });
  }

  /** 13. OPTIMIZE between runs. */
  @TestAllTableTypes
  public void testRestart_acrossOptimize(TableType tableType) throws Exception {
    skipManaged(tableType, "OPTIMIZE blocked on UC catalog-managed tables.");
    withNewTable(
        "uc_rs_optimize",
        "id INT",
        tableType,
        tableName -> {
          sql("INSERT INTO %s VALUES (1)", tableName);
          sql("INSERT INTO %s VALUES (2)", tableName);

          Path checkpointDir = freshCheckpointDir();
          Path outputDir = freshSinkDir();

          Dataset<Row> df1 = spark().readStream().table(tableName);
          runWithParquetSink(df1, outputDir, checkpointDir, /* trigger= */ null);

          sql("OPTIMIZE %s", tableName);
          sql("INSERT INTO %s VALUES (3)", tableName);

          Dataset<Row> df2 = spark().readStream().table(tableName);
          List<Row> rows = runWithParquetSink(df2, outputDir, checkpointDir, /* trigger= */ null);
          assertEquals(
              3, rows.size(), () -> "Expected 3 rows across OPTIMIZE + restart; got: " + rows);
        });
  }

  /** 14. RESTORE between runs, skipChangeCommits=true on restart. */
  @TestAllTableTypes
  public void testRestart_acrossRestore_skipChangeCommits(TableType tableType) throws Exception {
    withNewTable(
        "uc_rs_restore",
        "id INT",
        tableType,
        tableName -> {
          spark()
              .range(0, 5)
              .selectExpr("cast(id as int) as id")
              .write()
              .format("delta")
              .mode("append")
              .saveAsTable(tableName); // v1
          spark()
              .range(5, 10)
              .selectExpr("cast(id as int) as id")
              .write()
              .format("delta")
              .mode("append")
              .saveAsTable(tableName); // v2

          Path checkpointDir = freshCheckpointDir();
          Path outputDir = freshSinkDir();

          Dataset<Row> df1 = spark().readStream().table(tableName);
          runWithParquetSink(df1, outputDir, checkpointDir, /* trigger= */ null);

          sql("RESTORE TABLE %s TO VERSION AS OF 1", tableName);
          spark()
              .range(100, 102)
              .selectExpr("cast(id as int) as id")
              .write()
              .format("delta")
              .mode("append")
              .saveAsTable(tableName);

          Dataset<Row> df2 =
              spark().readStream().option("skipChangeCommits", "true").table(tableName);
          List<Row> rows = runWithParquetSink(df2, outputDir, checkpointDir, /* trigger= */ null);
          // Run 1: 10 rows. Run 2: RESTORE skipped, post-RESTORE append of 2 rows surfaces.
          assertEquals(
              12, rows.size(), () -> "Expected 12 rows after RESTORE + restart; got: " + rows);
        });
  }

  /** 15. INSERT OVERWRITE between runs, skipChangeCommits=true on restart. */
  @TestAllTableTypes
  public void testRestart_acrossInsertOverwrite_skipChangeCommits(TableType tableType)
      throws Exception {
    withNewTable(
        "uc_rs_iover",
        "id INT",
        tableType,
        tableName -> {
          sql("INSERT INTO %s VALUES (1), (2), (3)", tableName);

          Path checkpointDir = freshCheckpointDir();
          Path outputDir = freshSinkDir();

          Dataset<Row> df1 = spark().readStream().table(tableName);
          runWithParquetSink(df1, outputDir, checkpointDir, /* trigger= */ null);

          sql("INSERT OVERWRITE TABLE %s VALUES (10), (11)", tableName);

          Dataset<Row> df2 =
              spark().readStream().option("skipChangeCommits", "true").table(tableName);
          List<Row> rows = runWithParquetSink(df2, outputDir, checkpointDir, /* trigger= */ null);
          // Run 1: 3 rows. Run 2: OVERWRITE skipped. Total = 3.
          assertEquals(
              3, rows.size(), () -> "Expected 3 rows after OVERWRITE + restart; got: " + rows);
        });
  }

  /** 16. maxFilesPerTrigger value changes between runs. */
  @TestAllTableTypes
  public void testRestart_withMaxFilesPerTrigger_changeBetweenRuns(TableType tableType)
      throws Exception {
    withNewTable(
        "uc_rs_mfpt_change",
        "id INT",
        tableType,
        tableName -> {
          sql("INSERT INTO %s VALUES (1)", tableName);
          sql("INSERT INTO %s VALUES (2)", tableName);
          sql("INSERT INTO %s VALUES (3)", tableName);

          Path checkpointDir = freshCheckpointDir();
          Path outputDir = freshSinkDir();

          Dataset<Row> df1 =
              spark().readStream().option("maxFilesPerTrigger", "1").table(tableName);
          runWithParquetSink(df1, outputDir, checkpointDir, /* trigger= */ null);

          sql("INSERT INTO %s VALUES (4)", tableName);
          sql("INSERT INTO %s VALUES (5)", tableName);

          Dataset<Row> df2 =
              spark().readStream().option("maxFilesPerTrigger", "10").table(tableName);
          List<Row> rows = runWithParquetSink(df2, outputDir, checkpointDir, /* trigger= */ null);
          assertEquals(
              5, rows.size(), () -> "Expected 5 rows after rate-limit change; got: " + rows);
        });
  }

  /** 17. maxBytesPerTrigger value changes between runs. */
  @TestAllTableTypes
  public void testRestart_withMaxBytesPerTrigger_changeBetweenRuns(TableType tableType)
      throws Exception {
    withNewTable(
        "uc_rs_mbpt_change",
        "id INT",
        tableType,
        tableName -> {
          sql("INSERT INTO %s VALUES (1)", tableName);
          sql("INSERT INTO %s VALUES (2)", tableName);

          Path checkpointDir = freshCheckpointDir();
          Path outputDir = freshSinkDir();

          Dataset<Row> df1 =
              spark().readStream().option("maxBytesPerTrigger", "1k").table(tableName);
          runWithParquetSink(df1, outputDir, checkpointDir, /* trigger= */ null);

          sql("INSERT INTO %s VALUES (3)", tableName);

          Dataset<Row> df2 =
              spark().readStream().option("maxBytesPerTrigger", "100m").table(tableName);
          List<Row> rows = runWithParquetSink(df2, outputDir, checkpointDir, /* trigger= */ null);
          assertEquals(
              3, rows.size(), () -> "Expected 3 rows after byte-limit change; got: " + rows);
        });
  }

  /** 18. startingVersion option on the restart is ignored - checkpoint is authoritative. */
  @TestAllTableTypes
  public void testRestart_withStartingVersion_optionIgnoredOnRestart(TableType tableType)
      throws Exception {
    withNewTable(
        "uc_rs_sv_ignored",
        "id INT",
        tableType,
        tableName -> {
          sql("INSERT INTO %s VALUES (1), (2)", tableName);

          Path checkpointDir = freshCheckpointDir();
          Path outputDir = freshSinkDir();

          Dataset<Row> df1 = spark().readStream().table(tableName);
          runWithParquetSink(df1, outputDir, checkpointDir, /* trigger= */ null);

          sql("INSERT INTO %s VALUES (3)", tableName);

          // Restart with startingVersion=0 - should be ignored.
          Dataset<Row> df2 = spark().readStream().option("startingVersion", "0").table(tableName);
          List<Row> rows = runWithParquetSink(df2, outputDir, checkpointDir, /* trigger= */ null);
          assertEquals(
              3,
              rows.size(),
              () ->
                  "startingVersion must be ignored on restart; expected 3 total rows, got: "
                      + rows);
        });
  }

  /**
   * 19. failOnDataLoss=false; prune log between runs. Requires {@code
   * DeltaLog.forTable(path).checkpoint()} and {@code cleanUpExpiredLogs}; both need direct access
   * to the table path. UC MANAGED hides that path.
   */
  @TestAllTableTypes
  public void testRestart_withFailOnDataLossFalse_pruneBetweenRuns(TableType tableType)
      throws Exception {
    skipManaged(
        tableType,
        "log-pruning between runs requires DeltaLog.forTable(path).checkpoint() +"
            + " cleanUpExpiredLogs; UC MANAGED hides the path.");
    withNewTable(
        "uc_rs_fnl_prune",
        "id INT",
        null,
        tableType,
        "'delta.logRetentionDuration' = 'interval 0 hours', "
            + "'delta.deletedFileRetentionDuration' = 'interval 0 hours'",
        tableName -> {
          sql("INSERT INTO %s VALUES (1)", tableName);

          Path checkpointDir = freshCheckpointDir();
          Path outputDir = freshSinkDir();

          Dataset<Row> df1 = spark().readStream().table(tableName);
          runWithParquetSink(df1, outputDir, checkpointDir, /* trigger= */ null);

          // Force a checkpoint and metadata cleanup to "prune" older log entries.
          sql("INSERT INTO %s VALUES (2)", tableName);
          String location = describeLocation(tableName);
          org.apache.spark.sql.delta.DeltaLog log =
              org.apache.spark.sql.delta.DeltaLog.forTable(spark(), location);
          log.checkpoint();
          log.cleanUpExpiredLogs(log.snapshot());

          sql("INSERT INTO %s VALUES (3)", tableName);

          Dataset<Row> df2 =
              spark().readStream().option("failOnDataLoss", "false").table(tableName);
          List<Row> rows = runWithParquetSink(df2, outputDir, checkpointDir, /* trigger= */ null);
          assertEquals(
              3, rows.size(), () -> "Expected 3 rows after log pruning + restart; got: " + rows);
        });
  }

  /** Returns the table's storage location from {@code DESCRIBE EXTENDED}. */
  private String describeLocation(String tableName) {
    return sql("DESCRIBE EXTENDED %s", tableName).stream()
        .filter(row -> row.size() >= 2 && "Location".equals(row.get(0)))
        .map(row -> row.get(1))
        .findFirst()
        .orElseThrow(() -> new AssertionError("No Location row in DESCRIBE EXTENDED output"));
  }
}
