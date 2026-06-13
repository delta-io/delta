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
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.delta.DeltaLog;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryProgress;
import org.apache.spark.sql.streaming.Trigger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * DSv2 streaming option-combination coverage for two clusters that were uncovered after the Phase-0
 * matrix:
 *
 * <ul>
 *   <li><b>Cluster D - Trigger type x restart.</b> Trigger.AvailableNow and the deprecated
 *       Trigger.Once both terminate after draining the source, but their interaction with
 *       checkpoint restart, option composition (ignoreDeletes), and rate-limiting
 *       (maxFilesPerTrigger) is not exercised elsewhere.
 *   <li><b>Cluster E - Starting-offset x options.</b> startingVersion and startingTimestamp compose
 *       with maxFilesPerTrigger, excludeRegex, AvailableNow, and checkpoint restart in ways that
 *       are pinned by individual feature tests but not the cross-product.
 * </ul>
 */
public class V2StreamingOptionCombinationsTriggerStartingTest extends V2TestBase {

  /**
   * Drain a streaming query into a parquet sink at {@code outputDir} with the given checkpoint and
   * optional trigger, then return the sink's current contents. Mirrors the helper in {@link
   * V2StreamingRestartMatrixTest} so restart semantics behave identically.
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

  /** Run an AvailableNow query against a memory sink and return the sink's row count. */
  private long runAvailableNowMemory(Dataset<Row> streamingDF, String queryName) throws Exception {
    StreamingQuery query = null;
    try {
      query =
          streamingDF
              .writeStream()
              .format("memory")
              .queryName(queryName)
              .outputMode("append")
              .trigger(Trigger.AvailableNow())
              .start();
      query.processAllAvailable();
      assertTrue(query.awaitTermination(60_000L), "AvailableNow did not terminate within 60s");
      return spark.sql("SELECT COUNT(*) FROM " + queryName).collectAsList().get(0).getLong(0);
    } finally {
      if (query != null) {
        query.stop();
        DeltaLog.clearCache();
      }
    }
  }

  /** Append {@code n} single-row commits with id = base..base+n-1 to a (id INT) table. */
  private void appendIntCommits(String tablePath, int base, int n) {
    for (int i = 0; i < n; i++) {
      spark
          .range(base + i, base + i + 1)
          .selectExpr("cast(id as int) as id")
          .write()
          .format("delta")
          .mode("append")
          .save(tablePath);
    }
  }

  /** Format epoch millis as the session-timezone "yyyy-MM-dd HH:mm:ss.SSS" string Spark parses. */
  private String formatTs(long millis) {
    String tz = spark.sessionState().conf().sessionLocalTimeZone();
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    sdf.setTimeZone(TimeZone.getTimeZone(tz));
    return sdf.format(new Date(millis));
  }

  /**
   * D1. AvailableNow drains all committed rows on the first run, then a restart from the same
   * checkpoint with AvailableNow consumes only the rows added between the two runs (no duplicates).
   */
  @Test
  public void testAvailableNow_thenRestart(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);
    spark.sql(str("CREATE TABLE delta.`%s` (id INT) USING delta", tablePath));
    appendIntCommits(tablePath, 0, 3);

    File checkpointDir = new File(deltaTablePath, "_ckpt");
    File outputDir = new File(deltaTablePath, "_out");

    List<Row> firstRun =
        runWithParquetSink(
            spark.readStream().table(dsv2Ref), outputDir, checkpointDir, Trigger.AvailableNow());
    assertEquals(
        3, firstRun.size(), () -> "First AvailableNow run should drain 3 rows; got: " + firstRun);
    DeltaLog.clearCache();

    appendIntCommits(tablePath, 3, 3);

    List<Row> total =
        runWithParquetSink(
            spark.readStream().table(dsv2Ref), outputDir, checkpointDir, Trigger.AvailableNow());
    assertEquals(
        6,
        total.size(),
        () ->
            "Restart with AvailableNow should add exactly 3 new rows (no duplicates from first run); got: "
                + total);
    DeltaLog.clearCache();
  }

  /**
   * D2. AvailableNow + ignoreDeletes=true: whole-file DELETE between INSERTs must not error the
   * stream, and only INSERT rows are visible at the sink. AvailableNow self-terminates.
   */
  @Test
  public void testAvailableNow_withIgnoreDeletes(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, part INT) USING delta PARTITIONED BY (part)",
            tablePath));
    // INSERT, whole-file DELETE (by partition), then more INSERTs. With ignoreDeletes=true the
    // stream sees v1 (3 rows in part=0) and v3 (3 rows in part=1), but not the DELETE.
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 0), (2, 0), (3, 0)", tablePath)); // v=1
    spark.sql(str("DELETE FROM delta.`%s` WHERE part = 0", tablePath)); // v=2 (whole-file)
    spark.sql(str("INSERT INTO delta.`%s` VALUES (10, 1), (11, 1), (12, 1)", tablePath)); // v=3

    long rows =
        runAvailableNowMemory(
            spark.readStream().option("ignoreDeletes", "true").table(dsv2Ref),
            "avail_ignore_deletes");
    // 3 rows from v1 + 3 rows from v3 = 6. The DELETE is silently dropped by ignoreDeletes.
    assertEquals(
        6L, rows, () -> "AvailableNow + ignoreDeletes should see 6 INSERT rows; got: " + rows);
  }

  /**
   * D3. Trigger.Once drains the table once, then a restart with AvailableNow from the same
   * checkpoint picks up only the new rows. Pins offset continuity across trigger-type switch.
   */
  @SuppressWarnings("deprecation")
  @Test
  public void testTriggerOnce_thenRestartWithAvailableNow(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);
    spark.sql(str("CREATE TABLE delta.`%s` (id INT) USING delta", tablePath));
    appendIntCommits(tablePath, 0, 3);

    File checkpointDir = new File(deltaTablePath, "_ckpt");
    File outputDir = new File(deltaTablePath, "_out");

    // First run: Trigger.Once consumes everything visible at start.
    List<Row> firstRun =
        runWithParquetSink(
            spark.readStream().table(dsv2Ref), outputDir, checkpointDir, Trigger.Once());
    assertEquals(3, firstRun.size(), () -> "Trigger.Once should drain 3 rows; got: " + firstRun);
    DeltaLog.clearCache();

    appendIntCommits(tablePath, 3, 4);

    // Restart with AvailableNow: must only consume the 4 new rows.
    List<Row> total =
        runWithParquetSink(
            spark.readStream().table(dsv2Ref), outputDir, checkpointDir, Trigger.AvailableNow());
    assertEquals(
        7,
        total.size(),
        () ->
            "AvailableNow restart should pick up exactly the 4 new rows on top of 3 old; got: "
                + total);
    DeltaLog.clearCache();
  }

  /**
   * D4. AvailableNow + maxFilesPerTrigger=2 across 6 single-file commits must produce exactly 3
   * non-empty batches, all rows present, query self-terminates.
   */
  @Test
  public void testAvailableNow_withMaxFilesPerTrigger_fullDrain(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);
    spark.sql(str("CREATE TABLE delta.`%s` (id INT) USING delta", tablePath));
    // 6 single-file commits -> 6 AddFile entries, paced 2-at-a-time -> 3 batches.
    appendIntCommits(tablePath, 0, 6);

    StreamingQuery q =
        spark
            .readStream()
            .option("maxFilesPerTrigger", "2")
            .table(dsv2Ref)
            .writeStream()
            .format("memory")
            .queryName("avail_max_files")
            .outputMode("append")
            .trigger(Trigger.AvailableNow())
            .start();
    try {
      q.processAllAvailable();
      assertTrue(q.awaitTermination(60_000L), "AvailableNow did not terminate within 60s");
      long batches =
          Arrays.stream(q.recentProgress())
              .filter((StreamingQueryProgress p) -> p.numInputRows() != 0L)
              .count();
      assertEquals(3L, batches, () -> "Expected exactly 3 non-empty batches; got: " + batches);
      long rows =
          spark.sql("SELECT COUNT(*) FROM avail_max_files").collectAsList().get(0).getLong(0);
      assertEquals(6L, rows, () -> "Expected all 6 rows; got: " + rows);
    } finally {
      q.stop();
      DeltaLog.clearCache();
    }
  }

  /**
   * E1. startingVersion=1 + maxFilesPerTrigger=1 across 3 commits (v0=CREATE, v1, v2, v3). Must
   * read only v1+v2+v3, each commit's single file in its own batch.
   */
  @Test
  public void testStartingVersion_withMaxFilesPerTrigger(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);
    spark.sql(str("CREATE TABLE delta.`%s` (id INT) USING delta", tablePath));
    // v1, v2, v3 - three single-file appends.
    appendIntCommits(tablePath, 0, 3);

    StreamingQuery q =
        spark
            .readStream()
            .option("startingVersion", "2")
            .option("maxFilesPerTrigger", "1")
            .table(dsv2Ref)
            .writeStream()
            .format("memory")
            .queryName("start_ver_max_files")
            .outputMode("append")
            .start();
    try {
      q.processAllAvailable();
      long rows =
          spark.sql("SELECT COUNT(*) FROM start_ver_max_files").collectAsList().get(0).getLong(0);
      // startingVersion=2 -> read v2 and v3 (2 rows total).
      assertEquals(2L, rows, () -> "Expected 2 rows from v2+v3; got: " + rows);
      long batches =
          Arrays.stream(q.recentProgress())
              .filter((StreamingQueryProgress p) -> p.numInputRows() != 0L)
              .count();
      // maxFilesPerTrigger=1: each of the 2 files lands in its own batch.
      assertEquals(
          2L, batches, () -> "Expected 2 non-empty batches (rate limit honoured); got: " + batches);
    } finally {
      q.stop();
      DeltaLog.clearCache();
    }
  }

  /**
   * E2. startingVersion=2 + excludeRegex filtering partition p=2 path component. Must only see p=1
   * rows from versions at or after the requested start.
   */
  @Test
  public void testStartingVersion_withExcludeRegex(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);
    spark.sql(
        str("CREATE TABLE delta.`%s` (id INT, p INT) USING delta PARTITIONED BY (p)", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 1)", tablePath)); // v=1 (skipped by version)
    spark.sql(str("INSERT INTO delta.`%s` VALUES (2, 1), (20, 2)", tablePath)); // v=2
    spark.sql(str("INSERT INTO delta.`%s` VALUES (3, 1), (30, 2)", tablePath)); // v=3

    Dataset<Row> df =
        spark
            .readStream()
            .option("startingVersion", "2")
            .option("excludeRegex", "p=2")
            .table(dsv2Ref);

    List<Row> rows = processStreamingQuery(df, "start_ver_exclude_regex");
    // From v2 and v3 the p=1 files contribute id=2 and id=3; p=2 files are excluded.
    assertEquals(2, rows.size(), () -> "Expected 2 rows (only p=1 from v2+v3); got: " + rows);
    rows.forEach(
        r -> assertEquals(1, r.getInt(1), () -> "Expected partition p=1 only; got row: " + r));
  }

  /**
   * E3. startingTimestamp at the midpoint + parquet sink, then restart from the same checkpoint
   * after another append. Restart must ignore startingTimestamp and continue from the checkpointed
   * offset (no re-emit of rows before the midpoint).
   */
  @Test
  public void testStartingTimestamp_withRestart(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);
    spark.sql(str("CREATE TABLE delta.`%s` (id INT) USING delta", tablePath));
    // v1 commit comes first; then v2. Use real wall-clock so that picking "midpoint = before v2"
    // resolves to v2 under next-commit semantics.
    appendIntCommits(tablePath, 0, 1); // v=1, 1 row
    long midpoint = System.currentTimeMillis();
    // Ensure v2's commit timestamp is strictly later than midpoint.
    Thread.sleep(50L);
    appendIntCommits(tablePath, 1, 1); // v=2, 1 row

    File checkpointDir = new File(deltaTablePath, "_ckpt");
    File outputDir = new File(deltaTablePath, "_out");

    String midpointStr = formatTs(midpoint);
    List<Row> firstRun =
        runWithParquetSink(
            spark.readStream().option("startingTimestamp", midpointStr).table(dsv2Ref),
            outputDir,
            checkpointDir,
            /* trigger= */ null);
    // startingTimestamp at midpoint -> next-commit semantics resolves to v2 (1 row).
    assertEquals(
        1, firstRun.size(), () -> "Initial run should see exactly v2's 1 row; got: " + firstRun);
    DeltaLog.clearCache();

    // Add v3 after the first run.
    appendIntCommits(tablePath, 2, 1);

    List<Row> total =
        runWithParquetSink(
            spark.readStream().option("startingTimestamp", midpointStr).table(dsv2Ref),
            outputDir,
            checkpointDir,
            /* trigger= */ null);
    // Restart ignores startingTimestamp and continues from the checkpointed offset:
    // first run = 1 row, restart adds v3 = 1 row, total = 2. Pre-midpoint v1 is NOT re-emitted.
    assertEquals(
        2,
        total.size(),
        () -> "Restart should ignore startingTimestamp and only add v3 (1 new row); got: " + total);
    DeltaLog.clearCache();
  }

  /**
   * E4. startingVersion=1 + Trigger.AvailableNow across 3 appends. Must terminate and emit only
   * versions at or after the requested start.
   */
  @Test
  public void testStartingVersion_withAvailableNow(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);
    spark.sql(str("CREATE TABLE delta.`%s` (id INT) USING delta", tablePath));
    // v1, v2, v3 - three single-row appends.
    appendIntCommits(tablePath, 0, 3);

    long rows =
        runAvailableNowMemory(
            spark.readStream().option("startingVersion", "2").table(dsv2Ref),
            "start_ver_avail_now");
    // startingVersion=2 -> read v2 and v3 (2 rows).
    assertEquals(2L, rows, () -> "Expected 2 rows from v2+v3 with AvailableNow; got: " + rows);
  }
}
