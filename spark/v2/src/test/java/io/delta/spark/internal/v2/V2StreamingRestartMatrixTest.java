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

package io.delta.spark.internal.v2;

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.delta.DeltaLog;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Restart-from-checkpoint matrix for DSv2 streaming. Each test writes initial commits, drains a
 * stream to a parquet sink + checkpoint, then writes more commits and restarts the stream from the
 * same checkpoint. The total parquet sink contents at the end pins the row delta across runs.
 *
 * <p>Other restart cases live in feature-specific files ({@link
 * V2StreamingClusteredTest#testStreamingReadClusteredRestart}, {@link
 * V2StreamingIctTest#case6_restartIgnoresStartingTimestamp}, {@link
 * V2StreamingRowTrackingTest#testRowTrackingAcrossStreamRestart}, {@link
 * V2StreamingVariantScenarioTest#testVariant_restart}, {@link
 * V2StreamingOptimizeTest#testOptimize_restart}, {@link
 * V2StreamingV2CheckpointTest#case3_restartWithCheckpointBetween}, {@link
 * V2StreamingTimestampNtzTest#case6_restart}, {@link
 * V2StreamingAppendOnlyTest#testAppendOnlyRestart}, {@link
 * V2StreamingInsertOverwriteTest#testOverwrite_restart}). This file fills in the missing feature
 * combinations and adds smoke tests that cross-reference those existing ones.
 *
 * <p>Parquet sink is required because the memory sink does not recover from a checkpoint in append
 * mode.
 */
public class V2StreamingRestartMatrixTest extends V2TestBase {

  /**
   * Runs a streaming query that writes to a parquet sink at {@code outputDir} with the given
   * checkpoint, drains all available data, and stops. Returns the parquet sink's current contents.
   */
  private List<Row> runWithParquetSink(
      Dataset<Row> streamingDF, File outputDir, File checkpointDir, Trigger trigger)
      throws Exception {
    StreamingQuery query = null;
    try {
      DataStreamWriter<Row> writer =
          streamingDF
              .writeStream()
              .format("parquet")
              .outputMode("append")
              .option("path", outputDir.getAbsolutePath())
              .option("checkpointLocation", checkpointDir.getAbsolutePath());
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
    return spark.read().parquet(outputDir.getAbsolutePath()).collectAsList();
  }

  /** Format epoch millis as the session-timezone "yyyy-MM-dd HH:mm:ss.SSS" string Spark parses. */
  private String formatTs(long millis) {
    String tz = spark.sessionState().conf().sessionLocalTimeZone();
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    sdf.setTimeZone(TimeZone.getTimeZone(tz));
    return sdf.format(new Date(millis));
  }

  /**
   * 1. DV table; DV-only DELETE between runs; restart with skipChangeCommits.
   *
   * <p>Run 1 drains v0 (10 rows) with a bare stream. Between runs, a DV-only DELETE rewrites the
   * AddFile-with-DV (dataChange=true on the AddFile). Run 2 restarts with {@code
   * skipChangeCommits=true} so the DELETE commit is dropped. End-state sink = v0's 10 rows.
   */
  @Test
  public void testRestart_onDvTable_withDvDeleteBetweenRuns(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT) USING delta "
                + "TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')",
            tablePath));
    spark
        .range(0, 10)
        .selectExpr("cast(id as int) as id")
        .coalesce(1)
        .write()
        .format("delta")
        .mode("append")
        .save(tablePath);

    File checkpointDir = new File(deltaTablePath, "_ckpt");
    File outputDir = new File(deltaTablePath, "_out");

    Dataset<Row> df1 = spark.readStream().table(dsv2Ref);
    runWithParquetSink(df1, outputDir, checkpointDir, /* trigger= */ null);
    DeltaLog.clearCache();

    // DV-only DELETE: rewrites a single file with a deletion vector attached.
    spark.sql(str("DELETE FROM delta.`%s` WHERE id %% 2 = 0", tablePath));

    Dataset<Row> df2 = spark.readStream().option("skipChangeCommits", "true").table(dsv2Ref);
    try {
      List<Row> rows = runWithParquetSink(df2, outputDir, checkpointDir, /* trigger= */ null);
      assertEquals(
          10, rows.size(), () -> "Run 1 emitted 10 rows; restart drops DV DELETE. Got: " + rows);
    } finally {
      DeltaLog.clearCache();
    }
  }

  /**
   * 2. CM-name; ADD COLUMN (additive) between runs.
   *
   * <p>Additive ADD COLUMN on a column-mapped name-mode table is metadata-only. Restart must
   * surface the post-ADD COLUMN inserts without a schema-mismatch error and without re-emitting v0
   * rows.
   */
  @Test
  public void testRestart_onColMappedNameTable_addColumnBetweenRuns(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, name STRING) USING delta "
                + "TBLPROPERTIES ('delta.columnMapping.mode' = 'name')",
            tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'Alice'), (2, 'Bob')", tablePath));

    File checkpointDir = new File(deltaTablePath, "_ckpt");
    File outputDir = new File(deltaTablePath, "_out");

    Dataset<Row> df1 = spark.readStream().table(dsv2Ref);
    runWithParquetSink(df1, outputDir, checkpointDir, /* trigger= */ null);
    DeltaLog.clearCache();

    // Additive schema change; the existing rows project the new column as NULL.
    spark.sql(str("ALTER TABLE delta.`%s` ADD COLUMN extra STRING", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (3, 'Carol', 'x')", tablePath));

    Dataset<Row> df2 = spark.readStream().table(dsv2Ref);
    try {
      List<Row> rows = runWithParquetSink(df2, outputDir, checkpointDir, /* trigger= */ null);
      // 2 rows from run 1 + 1 row from run 2 = 3.
      assertEquals(3, rows.size(), () -> "Expected 3 rows across runs; got: " + rows);
    } finally {
      DeltaLog.clearCache();
    }
  }

  /** 3. CM-id; ADD COLUMN (additive) between runs. Same shape as case 2 but id-mode. */
  @Test
  public void testRestart_onColMappedIdTable_addColumnBetweenRuns(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, name STRING) USING delta "
                + "TBLPROPERTIES ('delta.columnMapping.mode' = 'id', "
                + "  'delta.minReaderVersion' = '2', "
                + "  'delta.minWriterVersion' = '5')",
            tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'Alice'), (2, 'Bob')", tablePath));

    File checkpointDir = new File(deltaTablePath, "_ckpt");
    File outputDir = new File(deltaTablePath, "_out");

    Dataset<Row> df1 = spark.readStream().table(dsv2Ref);
    runWithParquetSink(df1, outputDir, checkpointDir, /* trigger= */ null);
    DeltaLog.clearCache();

    spark.sql(str("ALTER TABLE delta.`%s` ADD COLUMN extra STRING", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (3, 'Carol', 'x')", tablePath));

    Dataset<Row> df2 = spark.readStream().table(dsv2Ref);
    try {
      List<Row> rows = runWithParquetSink(df2, outputDir, checkpointDir, /* trigger= */ null);
      assertEquals(3, rows.size(), () -> "Expected 3 rows across runs; got: " + rows);
    } finally {
      DeltaLog.clearCache();
    }
  }

  /**
   * 4. CM-name; RENAME COLUMN (non-additive) between runs.
   *
   * <p>Non-additive schema change. DSv2 streaming does not yet route through {@code
   * schemaTrackingLocation}, so the restart cannot reconcile the rename. Tracked alongside the
   * other Cluster S-3 cases.
   */
  @Test
  public void testRestart_onColMappedNameTable_renameColumnBetweenRuns(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, name STRING) USING delta "
                + "TBLPROPERTIES ('delta.columnMapping.mode' = 'name')",
            tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'Alice'), (2, 'Bob')", tablePath));

    File checkpointDir = new File(deltaTablePath, "_ckpt");
    File outputDir = new File(deltaTablePath, "_out");
    File schemaTrackingDir = new File(checkpointDir, "_schema_tracking");

    Dataset<Row> df1 =
        spark
            .readStream()
            .option("schemaTrackingLocation", schemaTrackingDir.getAbsolutePath())
            .table(dsv2Ref);
    runWithParquetSink(df1, outputDir, checkpointDir, /* trigger= */ null);
    DeltaLog.clearCache();

    spark.sql(str("ALTER TABLE delta.`%s` RENAME COLUMN name TO display_name", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (3, 'Carol')", tablePath));

    Dataset<Row> df2 =
        spark
            .readStream()
            .option("schemaTrackingLocation", schemaTrackingDir.getAbsolutePath())
            .table(dsv2Ref);
    try {
      List<Row> rows = runWithParquetSink(df2, outputDir, checkpointDir, /* trigger= */ null);
      assertEquals(3, rows.size(), () -> "Expected 3 rows after rename + restart; got: " + rows);
    } finally {
      DeltaLog.clearCache();
    }
  }

  /**
   * 5. Row tracking; project _metadata.row_id across restart.
   *
   * <p>Cross-references {@link V2StreamingRowTrackingTest#testRowTrackingAcrossStreamRestart} which
   * already pins the row_id mapping across restart. This smoke test just confirms a {@code
   * row_id}-projecting stream survives the restart at all from this matrix's vantage point.
   */
  @Test
  public void testRestart_onRowTrackedTable_projectRowId(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id LONG, name STRING) USING delta "
                + "TBLPROPERTIES ('delta.enableRowTracking' = 'true')",
            tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'A'), (2, 'B')", tablePath));

    File checkpointDir = new File(deltaTablePath, "_ckpt");
    File outputDir = new File(deltaTablePath, "_out");

    Dataset<Row> df1 =
        spark.readStream().table(dsv2Ref).selectExpr("id", "_metadata.row_id AS row_id");
    runWithParquetSink(df1, outputDir, checkpointDir, /* trigger= */ null);
    DeltaLog.clearCache();

    spark.sql(str("INSERT INTO delta.`%s` VALUES (3, 'C')", tablePath));

    Dataset<Row> df2 =
        spark.readStream().table(dsv2Ref).selectExpr("id", "_metadata.row_id AS row_id");
    try {
      List<Row> rows = runWithParquetSink(df2, outputDir, checkpointDir, /* trigger= */ null);
      assertEquals(3, rows.size(), () -> "Expected 3 rows across restart; got: " + rows);
    } finally {
      DeltaLog.clearCache();
    }
  }

  /**
   * 6. ICT table with startingTimestamp on a fresh stream (no checkpoint yet).
   *
   * <p>Smoke test: ICT table, drain initial commits with a fresh stream that uses {@code
   * startingTimestamp}, then restart from the same checkpoint after a new commit. The restart-time
   * behaviour of {@code startingTimestamp} on ICT tables is pinned by {@link
   * V2StreamingIctTest#case6_restartIgnoresStartingTimestamp}; this case just confirms ICT doesn't
   * change the basic restart path.
   */
  @Test
  public void testRestart_onIctTable_withStartingTimestamp(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT) USING delta TBLPROPERTIES ("
                + "'delta.enableInCommitTimestamps' = 'true')",
            tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1), (2)", tablePath));

    File checkpointDir = new File(deltaTablePath, "_ckpt");
    File outputDir = new File(deltaTablePath, "_out");

    String farPast = formatTs(0L);
    Dataset<Row> df1 = spark.readStream().option("startingTimestamp", farPast).table(dsv2Ref);
    runWithParquetSink(df1, outputDir, checkpointDir, /* trigger= */ null);
    DeltaLog.clearCache();

    spark.sql(str("INSERT INTO delta.`%s` VALUES (3)", tablePath));

    // Restart re-applies startingTimestamp option but the checkpoint should ignore it (per case6).
    Dataset<Row> df2 = spark.readStream().option("startingTimestamp", farPast).table(dsv2Ref);
    try {
      List<Row> rows = runWithParquetSink(df2, outputDir, checkpointDir, /* trigger= */ null);
      assertEquals(3, rows.size(), () -> "Expected 3 rows total; got: " + rows);
    } finally {
      DeltaLog.clearCache();
    }
  }

  /**
   * 7. v2-checkpoint table; force a v2 checkpoint between runs.
   *
   * <p>v2-checkpoint table. Run 1 drains v0+v1. Force a v2 checkpoint between runs. Run 2 restarts
   * and consumes v2. Cross-reference: {@link
   * V2StreamingV2CheckpointTest#case3_restartWithCheckpointBetween} which covers the same scenario
   * at a different vantage; this one stays on a single column (BIGINT) and is intentionally
   * minimal.
   */
  @Test
  public void testRestart_onV2CheckpointTable_forceCheckpointBetweenRuns(
      @TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id BIGINT) USING delta TBLPROPERTIES ("
                + "'delta.checkpointPolicy' = 'v2')",
            tablePath));
    spark.range(0, 5).write().format("delta").mode("append").save(tablePath);

    File checkpointDir = new File(deltaTablePath, "_ckpt");
    File outputDir = new File(deltaTablePath, "_out");

    Dataset<Row> df1 = spark.readStream().table(dsv2Ref);
    runWithParquetSink(df1, outputDir, checkpointDir, /* trigger= */ null);
    DeltaLog.clearCache();

    // Force a v2 checkpoint between runs. The streaming restart should not be tripped up by the
    // new checkpoint file appearing in _delta_log.
    spark.range(5, 10).write().format("delta").mode("append").save(tablePath);
    DeltaLog.forTable(spark, tablePath).checkpoint();
    spark.range(10, 15).write().format("delta").mode("append").save(tablePath);

    Dataset<Row> df2 = spark.readStream().table(dsv2Ref);
    try {
      List<Row> rows = runWithParquetSink(df2, outputDir, checkpointDir, /* trigger= */ null);
      assertEquals(15, rows.size(), () -> "Expected 15 rows after restart; got: " + rows);
    } finally {
      DeltaLog.clearCache();
    }
  }

  /**
   * 8. Clustered table; ALTER CLUSTER BY between runs.
   *
   * <p>Clustered table; clustering keys change mid-stream. Clustering is metadata-only with respect
   * to a streaming reader (no row content change), so the restart must continue to emit only the
   * new commits.
   */
  @Test
  public void testRestart_onClusteredTable_alterClusterBetweenRuns(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, name STRING) USING delta CLUSTER BY (id)",
            tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'A'), (2, 'B')", tablePath));

    File checkpointDir = new File(deltaTablePath, "_ckpt");
    File outputDir = new File(deltaTablePath, "_out");

    Dataset<Row> df1 = spark.readStream().table(dsv2Ref);
    runWithParquetSink(df1, outputDir, checkpointDir, /* trigger= */ null);
    DeltaLog.clearCache();

    spark.sql(str("ALTER TABLE delta.`%s` CLUSTER BY (name)", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (3, 'C')", tablePath));

    Dataset<Row> df2 = spark.readStream().table(dsv2Ref);
    try {
      List<Row> rows = runWithParquetSink(df2, outputDir, checkpointDir, /* trigger= */ null);
      assertEquals(3, rows.size(), () -> "Expected 3 rows after ALTER CLUSTER BY; got: " + rows);
    } finally {
      DeltaLog.clearCache();
    }
  }

  /**
   * 9. TimestampNtz partition value crossing a DST boundary.
   *
   * <p>TimestampNtz partition values straddling the US/Pacific spring-forward DST boundary
   * (2024-03-10 02:00 local; TIMESTAMP_NTZ has no time-zone so the partition path is encoded as the
   * wall-clock string). The restart must continue to read the partition correctly after the
   * boundary commit.
   */
  @Test
  public void testRestart_onTimestampNtzTable_withDstBoundary(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, ts TIMESTAMP_NTZ) USING delta PARTITIONED BY (ts)",
            tablePath));
    // Pre-DST commit.
    spark.sql(
        str("INSERT INTO delta.`%s` VALUES (1, TIMESTAMP_NTZ'2024-03-10 01:30:00')", tablePath));

    File checkpointDir = new File(deltaTablePath, "_ckpt");
    File outputDir = new File(deltaTablePath, "_out");

    Dataset<Row> df1 = spark.readStream().table(dsv2Ref);
    runWithParquetSink(df1, outputDir, checkpointDir, /* trigger= */ null);
    DeltaLog.clearCache();

    // Wall-clock 02:30 doesn't exist in US/Pacific local time on this date; for TIMESTAMP_NTZ the
    // value is stored verbatim, so this still round-trips. After-DST commit:
    spark.sql(
        str("INSERT INTO delta.`%s` VALUES (2, TIMESTAMP_NTZ'2024-03-10 03:30:00')", tablePath));

    Dataset<Row> df2 = spark.readStream().table(dsv2Ref);
    try {
      List<Row> rows = runWithParquetSink(df2, outputDir, checkpointDir, /* trigger= */ null);
      assertEquals(2, rows.size(), () -> "Expected 2 rows across DST boundary; got: " + rows);
    } finally {
      DeltaLog.clearCache();
    }
  }

  /** 10. appendOnly + restart - smoke coverage alongside the existing append-only file. */
  @Test
  public void testRestart_onAppendOnlyTable_basic(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, name STRING) USING delta "
                + "TBLPROPERTIES ('delta.appendOnly' = 'true')",
            tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'A')", tablePath));

    File checkpointDir = new File(deltaTablePath, "_ckpt");
    File outputDir = new File(deltaTablePath, "_out");

    Dataset<Row> df1 = spark.readStream().table(dsv2Ref);
    runWithParquetSink(df1, outputDir, checkpointDir, /* trigger= */ null);
    DeltaLog.clearCache();

    spark.sql(str("INSERT INTO delta.`%s` VALUES (2, 'B'), (3, 'C')", tablePath));

    Dataset<Row> df2 = spark.readStream().table(dsv2Ref);
    try {
      List<Row> rows = runWithParquetSink(df2, outputDir, checkpointDir, /* trigger= */ null);
      assertEquals(3, rows.size(), () -> "Expected 3 rows across restart; got: " + rows);
    } finally {
      DeltaLog.clearCache();
    }
  }

  /**
   * 11. Partitioned table; partition column listed last in the schema.
   *
   * <p>Partition-last DDL: the partition column is declared after the data column. Restart must
   * preserve partition-column ordering across the resumed stream.
   */
  @Test
  public void testRestart_onPartitionedTable_partLast(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, part INT) USING delta PARTITIONED BY (part)",
            tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 0), (2, 0), (3, 1)", tablePath));

    File checkpointDir = new File(deltaTablePath, "_ckpt");
    File outputDir = new File(deltaTablePath, "_out");

    Dataset<Row> df1 = spark.readStream().table(dsv2Ref);
    runWithParquetSink(df1, outputDir, checkpointDir, /* trigger= */ null);
    DeltaLog.clearCache();

    spark.sql(str("INSERT INTO delta.`%s` VALUES (4, 1), (5, 2)", tablePath));

    Dataset<Row> df2 = spark.readStream().table(dsv2Ref);
    try {
      List<Row> rows = runWithParquetSink(df2, outputDir, checkpointDir, /* trigger= */ null);
      assertEquals(5, rows.size(), () -> "Expected 5 rows across restart; got: " + rows);
    } finally {
      DeltaLog.clearCache();
    }
  }

  /**
   * 12. Variant; smoke restart.
   *
   * <p>Cross-references {@link V2StreamingVariantScenarioTest#testVariant_restart} which pins
   * per-row alignment. This case just confirms VARIANT columns survive the restart path from this
   * matrix's vantage point.
   */
  @Test
  public void testRestart_onVariantTable_basic(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, v VARIANT) USING delta TBLPROPERTIES ("
                + "'delta.feature.variantType' = 'supported')",
            tablePath));
    spark.sql(str("INSERT INTO delta.`%s` SELECT 1, parse_json('{\"a\": 1}')", tablePath));

    File checkpointDir = new File(deltaTablePath, "_ckpt");
    File outputDir = new File(deltaTablePath, "_out");

    Dataset<Row> df1 = spark.readStream().table(dsv2Ref).selectExpr("id", "to_json(v) AS v_json");
    runWithParquetSink(df1, outputDir, checkpointDir, /* trigger= */ null);
    DeltaLog.clearCache();

    spark.sql(str("INSERT INTO delta.`%s` SELECT 2, parse_json('{\"b\": 2}')", tablePath));

    Dataset<Row> df2 = spark.readStream().table(dsv2Ref).selectExpr("id", "to_json(v) AS v_json");
    try {
      List<Row> rows = runWithParquetSink(df2, outputDir, checkpointDir, /* trigger= */ null);
      assertEquals(2, rows.size(), () -> "Expected 2 variant rows across restart; got: " + rows);
    } finally {
      DeltaLog.clearCache();
    }
  }

  /**
   * 13. OPTIMIZE between runs.
   *
   * <p>Smoke restart over OPTIMIZE; the detailed restart-after-OPTIMIZE assertion lives in {@link
   * V2StreamingOptimizeTest#testOptimize_restart}. We just confirm restart survives an OPTIMIZE in
   * history without errors.
   */
  @Test
  public void testRestart_acrossOptimize(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);
    spark.sql(str("CREATE TABLE delta.`%s` (id INT) USING delta", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1)", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (2)", tablePath));

    File checkpointDir = new File(deltaTablePath, "_ckpt");
    File outputDir = new File(deltaTablePath, "_out");

    Dataset<Row> df1 = spark.readStream().table(dsv2Ref);
    runWithParquetSink(df1, outputDir, checkpointDir, /* trigger= */ null);
    DeltaLog.clearCache();

    spark.sql(str("OPTIMIZE delta.`%s`", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (3)", tablePath));

    Dataset<Row> df2 = spark.readStream().table(dsv2Ref);
    try {
      List<Row> rows = runWithParquetSink(df2, outputDir, checkpointDir, /* trigger= */ null);
      // 2 from run 1 + 1 from run 2. OPTIMIZE is dataChange=false so it contributes nothing.
      assertEquals(3, rows.size(), () -> "Expected 3 rows across OPTIMIZE + restart; got: " + rows);
    } finally {
      DeltaLog.clearCache();
    }
  }

  /**
   * 14. RESTORE between runs, skipChangeCommits=true on restart.
   *
   * <p>Smoke restart over RESTORE; deeper coverage lives in {@link V2StreamingRestoreTest}. With
   * {@code skipChangeCommits=true} on restart, the RESTORE commit is dropped, so the parquet sink
   * grows only by post-restart appends.
   */
  @Test
  public void testRestart_acrossRestore_skipChangeCommits(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);
    spark.sql(str("CREATE TABLE delta.`%s` (id INT) USING delta", tablePath));
    spark
        .range(0, 5)
        .selectExpr("cast(id as int) as id")
        .write()
        .format("delta")
        .mode("append")
        .save(tablePath); // v1
    spark
        .range(5, 10)
        .selectExpr("cast(id as int) as id")
        .write()
        .format("delta")
        .mode("append")
        .save(tablePath); // v2

    File checkpointDir = new File(deltaTablePath, "_ckpt");
    File outputDir = new File(deltaTablePath, "_out");

    Dataset<Row> df1 = spark.readStream().table(dsv2Ref);
    runWithParquetSink(df1, outputDir, checkpointDir, /* trigger= */ null);
    DeltaLog.clearCache();

    // RESTORE to v1, then append.
    spark.sql(str("RESTORE TABLE delta.`%s` TO VERSION AS OF 1", tablePath));
    spark
        .range(100, 102)
        .selectExpr("cast(id as int) as id")
        .write()
        .format("delta")
        .mode("append")
        .save(tablePath);

    Dataset<Row> df2 = spark.readStream().option("skipChangeCommits", "true").table(dsv2Ref);
    try {
      List<Row> rows = runWithParquetSink(df2, outputDir, checkpointDir, /* trigger= */ null);
      // Run 1: 10 rows. Run 2: RESTORE skipped, post-RESTORE append of 2 rows surfaces. Total 12.
      assertEquals(12, rows.size(), () -> "Expected 12 rows after RESTORE + restart; got: " + rows);
    } finally {
      DeltaLog.clearCache();
    }
  }

  /**
   * 15. INSERT OVERWRITE between runs, skipChangeCommits=true on restart.
   *
   * <p>Smoke restart over INSERT OVERWRITE; detailed coverage lives in {@link
   * V2StreamingInsertOverwriteTest#testOverwrite_restart}. With {@code skipChangeCommits} on
   * restart the OVERWRITE commit is dropped.
   */
  @Test
  public void testRestart_acrossInsertOverwrite_skipChangeCommits(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);
    spark.sql(str("CREATE TABLE delta.`%s` (id INT) USING delta", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1), (2), (3)", tablePath));

    File checkpointDir = new File(deltaTablePath, "_ckpt");
    File outputDir = new File(deltaTablePath, "_out");

    Dataset<Row> df1 = spark.readStream().table(dsv2Ref);
    runWithParquetSink(df1, outputDir, checkpointDir, /* trigger= */ null);
    DeltaLog.clearCache();

    spark.sql(str("INSERT OVERWRITE TABLE delta.`%s` VALUES (10), (11)", tablePath));

    Dataset<Row> df2 = spark.readStream().option("skipChangeCommits", "true").table(dsv2Ref);
    try {
      List<Row> rows = runWithParquetSink(df2, outputDir, checkpointDir, /* trigger= */ null);
      // Run 1 emitted 3 rows. Run 2 skips the OVERWRITE entirely. Total = 3.
      assertEquals(3, rows.size(), () -> "Expected 3 rows after OVERWRITE + restart; got: " + rows);
    } finally {
      DeltaLog.clearCache();
    }
  }

  /**
   * 16. maxFilesPerTrigger value changes between runs.
   *
   * <p>Rate-limit option changes across restart. The new option value is honoured by run 2 (the
   * checkpoint persists offsets, not source options). End-state row count must equal the table's
   * full contents.
   */
  @Test
  public void testRestart_withMaxFilesPerTrigger_changeBetweenRuns(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);
    spark.sql(str("CREATE TABLE delta.`%s` (id INT) USING delta", tablePath));
    // Three single-file commits.
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1)", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (2)", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (3)", tablePath));

    File checkpointDir = new File(deltaTablePath, "_ckpt");
    File outputDir = new File(deltaTablePath, "_out");

    Dataset<Row> df1 = spark.readStream().option("maxFilesPerTrigger", "1").table(dsv2Ref);
    runWithParquetSink(df1, outputDir, checkpointDir, /* trigger= */ null);
    DeltaLog.clearCache();

    spark.sql(str("INSERT INTO delta.`%s` VALUES (4)", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (5)", tablePath));

    // Restart with a different rate-limit value.
    Dataset<Row> df2 = spark.readStream().option("maxFilesPerTrigger", "10").table(dsv2Ref);
    try {
      List<Row> rows = runWithParquetSink(df2, outputDir, checkpointDir, /* trigger= */ null);
      assertEquals(5, rows.size(), () -> "Expected 5 rows after rate-limit change; got: " + rows);
    } finally {
      DeltaLog.clearCache();
    }
  }

  /** 17. maxBytesPerTrigger value changes between runs. Same shape as case 16. */
  @Test
  public void testRestart_withMaxBytesPerTrigger_changeBetweenRuns(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);
    spark.sql(str("CREATE TABLE delta.`%s` (id INT) USING delta", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1)", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (2)", tablePath));

    File checkpointDir = new File(deltaTablePath, "_ckpt");
    File outputDir = new File(deltaTablePath, "_out");

    Dataset<Row> df1 = spark.readStream().option("maxBytesPerTrigger", "1k").table(dsv2Ref);
    runWithParquetSink(df1, outputDir, checkpointDir, /* trigger= */ null);
    DeltaLog.clearCache();

    spark.sql(str("INSERT INTO delta.`%s` VALUES (3)", tablePath));

    Dataset<Row> df2 = spark.readStream().option("maxBytesPerTrigger", "100m").table(dsv2Ref);
    try {
      List<Row> rows = runWithParquetSink(df2, outputDir, checkpointDir, /* trigger= */ null);
      assertEquals(3, rows.size(), () -> "Expected 3 rows after byte-limit change; got: " + rows);
    } finally {
      DeltaLog.clearCache();
    }
  }

  /**
   * 18. startingVersion option on the restart is ignored - checkpoint is authoritative.
   *
   * <p>On restart, {@code startingVersion} is ignored - the checkpoint is the source of truth.
   * Setting {@code startingVersion=0} on the restart of a checkpoint that already passed v0 must
   * NOT re-emit v0 rows.
   */
  @Test
  public void testRestart_withStartingVersion_optionIgnoredOnRestart(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);
    spark.sql(str("CREATE TABLE delta.`%s` (id INT) USING delta", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1), (2)", tablePath));

    File checkpointDir = new File(deltaTablePath, "_ckpt");
    File outputDir = new File(deltaTablePath, "_out");

    Dataset<Row> df1 = spark.readStream().table(dsv2Ref);
    runWithParquetSink(df1, outputDir, checkpointDir, /* trigger= */ null);
    DeltaLog.clearCache();

    spark.sql(str("INSERT INTO delta.`%s` VALUES (3)", tablePath));

    // Restart with startingVersion=0 - should be ignored.
    Dataset<Row> df2 = spark.readStream().option("startingVersion", "0").table(dsv2Ref);
    try {
      List<Row> rows = runWithParquetSink(df2, outputDir, checkpointDir, /* trigger= */ null);
      // If startingVersion were honoured, run 2 would re-emit v0+v1+v2 = 5 rows; total = 7.
      // Correct behaviour: checkpoint wins, run 2 emits only v2 = 1 row; total = 3.
      assertEquals(
          3,
          rows.size(),
          () -> "startingVersion must be ignored on restart; expected 3 total rows, got: " + rows);
    } finally {
      DeltaLog.clearCache();
    }
  }

  /**
   * 19. failOnDataLoss=false; prune log between runs.
   *
   * <p>With {@code failOnDataLoss=false}, the stream tolerates missing commits at the head of the
   * pending range. We do not physically delete log files here (it is brittle on the local
   * filesystem); instead we set {@code logRetentionDuration} and {@code
   * deletedFileRetentionDuration} to {@code interval 0 hours} and force a checkpoint + cleanup so
   * older log files are eligible for deletion, exercising the same option path.
   */
  @Test
  public void testRestart_withFailOnDataLossFalse_pruneBetweenRuns(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT) USING delta TBLPROPERTIES ("
                + "'delta.logRetentionDuration' = 'interval 0 hours', "
                + "'delta.deletedFileRetentionDuration' = 'interval 0 hours')",
            tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1)", tablePath));

    File checkpointDir = new File(deltaTablePath, "_ckpt");
    File outputDir = new File(deltaTablePath, "_out");

    Dataset<Row> df1 = spark.readStream().table(dsv2Ref);
    runWithParquetSink(df1, outputDir, checkpointDir, /* trigger= */ null);
    DeltaLog.clearCache();

    // Force a checkpoint between drain runs. cleanUpExpiredLogs is private[delta] and not callable
    // from Java; the existing failOnDataLoss tests in V2StreamingFailOnDataLossMatrixTest cover
    // the actual prune-and-skip semantics by Files.delete'ing commit JSONs directly.
    spark.sql(str("INSERT INTO delta.`%s` VALUES (2)", tablePath));
    DeltaLog log = DeltaLog.forTable(spark, tablePath);
    log.checkpoint();

    spark.sql(str("INSERT INTO delta.`%s` VALUES (3)", tablePath));

    Dataset<Row> df2 = spark.readStream().option("failOnDataLoss", "false").table(dsv2Ref);
    try {
      List<Row> rows = runWithParquetSink(df2, outputDir, checkpointDir, /* trigger= */ null);
      assertEquals(
          3, rows.size(), () -> "Expected 3 rows after log pruning + restart; got: " + rows);
    } finally {
      DeltaLog.clearCache();
    }
  }
}
