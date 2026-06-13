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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.delta.DeltaLog;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * End-to-end DSv2 streaming tests for tables with {@code delta.checkpointPolicy = 'v2'}.
 *
 * <p>Per the cross-product matrix in {@code true_cross_product.md}, v2Checkpoint has 100% uncovered
 * cells across streaming scenarios - there is no DSv1 nor DSv2 streaming test that exercises a
 * table whose log uses v2 checkpoints (sidecars + checkpoint metadata top-level file).
 *
 * <p>Each test:
 *
 * <ol>
 *   <li>Basic + AvailableNow on a v2-checkpoint table.
 *   <li>Stream after a v2 checkpoint is written (cross checkpoint boundary).
 *   <li>Restart with a v2 checkpoint written between runs.
 *   <li>v2Checkpoint x DV (DELETE writes a DV; checkpoint after).
 *   <li>v2Checkpoint x column mapping name.
 *   <li>v2Checkpoint x maxFilesPerTrigger=1 (multi-batch progression across the checkpoint).
 *   <li>v2Checkpoint x startingVersion at the checkpoint version.
 * </ol>
 */
public class V2StreamingV2CheckpointTest extends V2TestBase {

  /** Force a checkpoint at the current snapshot via the public DeltaLog API. */
  @SuppressWarnings("deprecation")
  private void triggerCheckpoint(String tablePath) {
    DeltaLog log = DeltaLog.forTable(spark, tablePath);
    log.checkpoint();
  }

  /** Run the streaming DF with the given checkpoint dir, no trigger. */
  private List<Row> runWithParquetSink(
      Dataset<Row> streamingDF, File outputDir, File checkpointDir, Trigger trigger)
      throws Exception {
    StreamingQuery query = null;
    try {
      org.apache.spark.sql.streaming.DataStreamWriter<Row> writer =
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
        query.awaitTermination(60_000);
      }
    } finally {
      if (query != null) {
        query.stop();
      }
    }
    return spark.read().parquet(outputDir.getAbsolutePath()).collectAsList();
  }

  /** Create a v2-checkpoint table with id BIGINT, return the path. */
  private void createV2CheckpointTable(String tablePath) {
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id BIGINT) USING delta TBLPROPERTIES ("
                + "'delta.checkpointPolicy' = 'v2')",
            tablePath));
  }

  /** 1a. Basic stream from a v2-checkpoint table. */
  @Test
  public void case1a_basicStream(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    createV2CheckpointTable(tablePath);
    spark.range(0, 5).write().format("delta").mode("append").save(tablePath);

    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> df = spark.readStream().table(dsv2Ref);
    assertTrue(df.isStreaming());

    List<Row> rows = processStreamingQuery(df, "v2ckpt_case1a");
    assertEquals(5, rows.size(), () -> "Expected 5 rows; got: " + rows);
  }

  /** 1b. AvailableNow on a v2-checkpoint table. */
  @Test
  public void case1b_availableNow(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    createV2CheckpointTable(tablePath);
    spark.range(0, 5).write().format("delta").mode("append").save(tablePath);

    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> df = spark.readStream().table(dsv2Ref);

    File checkpointDir = new File(deltaTablePath, "_ckpt");
    File outputDir = new File(deltaTablePath, "_out");
    try {
      List<Row> rows = runWithParquetSink(df, outputDir, checkpointDir, Trigger.AvailableNow());
      assertEquals(5, rows.size(), () -> "Expected 5 rows; got: " + rows);
    } finally {
      DeltaLog.clearCache();
    }
  }

  /**
   * 2. Stream after a v2 checkpoint has been written. We write some data, force a v2 checkpoint at
   * version K, then start streaming from {@code startingVersion=K-1} so the source must read across
   * the checkpoint boundary (v2 sidecar + JSON).
   */
  @Test
  public void case2_streamAcrossV2CheckpointBoundary(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    createV2CheckpointTable(tablePath);
    // v1, v2, v3 commits.
    spark.range(0, 10).write().format("delta").mode("append").save(tablePath);
    spark.range(10, 20).write().format("delta").mode("append").save(tablePath);
    spark.range(20, 30).write().format("delta").mode("append").save(tablePath);
    // Force v2 checkpoint at v3.
    triggerCheckpoint(tablePath);
    // Continue with v4.
    spark.range(30, 40).write().format("delta").mode("append").save(tablePath);

    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);
    // startingVersion = 2 (one before the checkpoint at v3); replay must cross the checkpoint.
    Dataset<Row> df = spark.readStream().option("startingVersion", "2").table(dsv2Ref);

    List<Row> rows = processStreamingQuery(df, "v2ckpt_case2");
    // Versions read: 2 (10..19), 3 (20..29), 4 (30..39) = 30 rows.
    assertEquals(30, rows.size(), () -> "Expected 30 rows from v2/v3/v4; got: " + rows.size());
  }

  /** 3. Stream + restart, with a v2 checkpoint forced between the two runs. */
  @Test
  public void case3_restartWithCheckpointBetween(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    createV2CheckpointTable(tablePath);
    spark.range(0, 5).write().format("delta").mode("append").save(tablePath);

    File checkpointDir = new File(deltaTablePath, "_ckpt");
    File outputDir = new File(deltaTablePath, "_out");
    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> df1 = spark.readStream().table(dsv2Ref);
    runWithParquetSink(df1, outputDir, checkpointDir, /* trigger= */ null);

    // Add more commits and force a v2 checkpoint between the runs.
    spark.range(5, 10).write().format("delta").mode("append").save(tablePath);
    triggerCheckpoint(tablePath);
    spark.range(10, 15).write().format("delta").mode("append").save(tablePath);

    try {
      Dataset<Row> df2 = spark.readStream().table(dsv2Ref);
      List<Row> rows = runWithParquetSink(df2, outputDir, checkpointDir, /* trigger= */ null);
      assertEquals(
          15, rows.size(), () -> "Expected 15 total rows after restart; got: " + rows.size());
    } finally {
      DeltaLog.clearCache();
    }
  }

  /** 4. v2Checkpoint x DV (DELETE writes a DV; force a v2 checkpoint after). */
  @Test
  public void case4_v2CheckpointWithDV(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id BIGINT) USING delta TBLPROPERTIES ("
                + "'delta.checkpointPolicy' = 'v2', "
                + "'delta.enableDeletionVectors' = 'true')",
            tablePath));
    spark.range(0, 10).coalesce(1).write().format("delta").mode("append").save(tablePath);
    spark.sql(str("DELETE FROM delta.`%s` WHERE id < 3", tablePath));
    triggerCheckpoint(tablePath);

    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> df = spark.readStream().table(dsv2Ref);
    List<Row> rows = processStreamingQuery(df, "v2ckpt_case4");
    assertEquals(7, rows.size(), () -> "DV survivors expected 7 rows; got " + rows);
  }

  /** 5. v2Checkpoint x column mapping name. */
  @Test
  public void case5_v2CheckpointWithColumnMapping(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id BIGINT, name STRING) USING delta TBLPROPERTIES ("
                + "'delta.checkpointPolicy' = 'v2', "
                + "'delta.columnMapping.mode' = 'name', "
                + "'delta.minReaderVersion' = '2', "
                + "'delta.minWriterVersion' = '5')",
            tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'A'), (2, 'B')", tablePath));
    triggerCheckpoint(tablePath);
    spark.sql(str("INSERT INTO delta.`%s` VALUES (3, 'C')", tablePath));

    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> df = spark.readStream().table(dsv2Ref);
    List<Row> rows = processStreamingQuery(df, "v2ckpt_case5");
    List<Row> expected =
        Arrays.asList(
            RowFactory.create(1L, "A"), RowFactory.create(2L, "B"), RowFactory.create(3L, "C"));
    assertDataEquals(rows, expected);
  }

  /** 6. v2Checkpoint x maxFilesPerTrigger=1 (multi-batch progression across the checkpoint). */
  @Test
  public void case6_v2CheckpointWithMaxFilesPerTrigger(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    createV2CheckpointTable(tablePath);
    // 4 separate single-row commits.
    spark.range(0, 1).write().format("delta").mode("append").save(tablePath);
    spark.range(1, 2).write().format("delta").mode("append").save(tablePath);
    triggerCheckpoint(tablePath);
    spark.range(2, 3).write().format("delta").mode("append").save(tablePath);
    spark.range(3, 4).write().format("delta").mode("append").save(tablePath);

    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> df = spark.readStream().option("maxFilesPerTrigger", "1").table(dsv2Ref);
    List<Row> rows = processStreamingQuery(df, "v2ckpt_case6");
    List<Row> expected = new ArrayList<>();
    for (long i = 0; i < 4; i++) expected.add(RowFactory.create(i));
    assertDataEquals(rows, expected);
  }

  /** 7. v2Checkpoint x startingVersion = the exact version at which the checkpoint exists. */
  @Test
  public void case7_v2CheckpointWithStartingVersionAtCheckpoint(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    createV2CheckpointTable(tablePath);
    // v1, v2, v3 commits.
    spark.range(0, 10).write().format("delta").mode("append").save(tablePath);
    spark.range(10, 20).write().format("delta").mode("append").save(tablePath);
    spark.range(20, 30).write().format("delta").mode("append").save(tablePath);
    // v2 checkpoint at v3.
    triggerCheckpoint(tablePath);
    spark.range(30, 40).write().format("delta").mode("append").save(tablePath);

    // The current snapshot is at v4. Find the checkpoint version (v3).
    DeltaLog log = DeltaLog.forTable(spark, tablePath);
    long checkpointVersion =
        log.update(false, scala.Option.empty(), scala.Option.empty())
            .checkpointProvider()
            .version();
    assertEquals(3L, checkpointVersion, "Expected checkpoint at v3");

    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> df =
        spark
            .readStream()
            .option("startingVersion", String.valueOf(checkpointVersion))
            .table(dsv2Ref);
    List<Row> rows = processStreamingQuery(df, "v2ckpt_case7");
    // From v3 (20..29) + v4 (30..39) = 20 rows.
    assertEquals(20, rows.size(), () -> "Expected 20 rows from v3+v4; got " + rows.size());
  }
}
