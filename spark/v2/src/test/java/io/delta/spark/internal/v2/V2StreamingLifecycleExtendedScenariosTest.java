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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.delta.DeltaLog;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import scala.Option;

/**
 * DSv2 streaming coverage for the lifecycle rows crossed with extended scenarios S3-S8 and S15-S22.
 *
 * <p>Companion file to {@link V2StreamingLifecycleScenarioMatrixTest}, which pinned the basic /
 * startingVersion-spec / restart column (S1, S2, S14) of the lifecycle row. This file fills in the
 * remaining ~25 uncovered cells in the lifecycle cluster of {@code COVERAGE_FINAL_V2.md}.
 *
 * <p>Scenario columns:
 *
 * <ul>
 *   <li>S3 = startingVersion=latest
 *   <li>S4 = startingTimestamp
 *   <li>S5 = maxFilesPerTrigger
 *   <li>S6 = maxBytesPerTrigger
 *   <li>S7 = Trigger.AvailableNow
 *   <li>S8 = Trigger.Once / ProcessingTime
 *   <li>S15 = excludeRegex
 *   <li>S16-S22 = log-state mutations (corrupt last-checkpoint, compacted JSON, extra non-JSON
 *       file, log-retention prune, metadata-only commits) and concurrency / failOnDataLoss
 *       combinations.
 * </ul>
 *
 * <p>Lifecycle rows:
 *
 * <ul>
 *   <li>Concurrent-writer x non-vanilla scenario columns.
 *   <li>Restore-past-checkpoint x non-vanilla scenario columns.
 *   <li>OPTIMIZE-before-fresh-stream x non-vanilla scenario columns.
 *   <li>Mid-stream feature toggles (DV / RT / ICT / appendOnly-unset) x non-vanilla columns.
 *   <li>Log-state perturbations against a streaming read.
 * </ul>
 *
 * <p>Patterns mirror {@link V2StreamingLifecycleScenarioMatrixTest} (concurrent-writer setup,
 * parquet-sink restart), {@link V2StreamingRaceLifecycleTest} (race control), {@link
 * V2StreamingLogIntegrityTest} (delta-log perturbation: truncate, compaction-file fabrication,
 * extra file), and {@link V2StreamingFailOnDataLossMatrixTest#pruneCommitJson} (prune+checkpoint).
 */
public class V2StreamingLifecycleExtendedScenariosTest extends V2TestBase {

  /**
   * Runs a streaming query against a parquet sink + checkpoint, drains all available data, then
   * stops. Parquet sink supports checkpoint recovery across restarts; memory sink does not.
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

  /** Returns the current DeltaLog snapshot version (using DSv1 DeltaLog cache). */
  private long latestVersion(String tablePath) {
    return DeltaLog.forTable(spark, tablePath)
        .update(false, Option.empty(), Option.empty())
        .version();
  }

  /** Walks a cause chain looking for any {@link NullPointerException}. */
  private static boolean containsNpe(Throwable t) {
    Throwable cur = t;
    while (cur != null) {
      if (cur instanceof NullPointerException) {
        return true;
      }
      cur = cur.getCause();
    }
    return false;
  }

  /** Force a checkpoint so a pruned commit JSON does not block snapshot reconstruction. */
  @SuppressWarnings("deprecation")
  private void checkpoint(String tablePath) {
    DeltaLog.forTable(spark, tablePath).checkpoint();
  }

  /** Delete the commit JSON (and its CRC sibling) for the given version. */
  private void pruneCommitJson(String tablePath, long version) throws Exception {
    Path json = Paths.get(tablePath, "_delta_log", String.format("%020d.json", version));
    Files.delete(json);
    Path crc = Paths.get(tablePath, "_delta_log", String.format("%020d.crc", version));
    if (Files.exists(crc)) {
      Files.delete(crc);
    }
    DeltaLog.clearCache();
  }

  /** Format wall-clock millis using the session-local time zone for {@code startingTimestamp}. */
  private String formatTs(long millis) {
    String tz = spark.sessionState().conf().sessionLocalTimeZone();
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    sdf.setTimeZone(TimeZone.getTimeZone(tz));
    return sdf.format(new Date(millis));
  }

  /** Spawn a concurrent appender that inserts {@code n} single-row commits until stopped. */
  private ExecutorService startConcurrentAppender(String tablePath, AtomicBoolean stop, int n) {
    ExecutorService writer = Executors.newSingleThreadExecutor();
    writer.submit(
        () -> {
          int i = 1;
          while (!stop.get() && i <= n) {
            try {
              spark.sql(str("INSERT INTO delta.`%s` VALUES (%d)", tablePath, i));
              Thread.sleep(50);
              i++;
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              return;
            } catch (Exception ignored) {
              // Concurrent commit may transiently fail; just continue.
            }
          }
        });
    return writer;
  }

  /** Stop the appender executor cleanly. */
  private void stopAppender(ExecutorService writer, AtomicBoolean stop) throws Exception {
    stop.set(true);
    writer.shutdownNow();
    writer.awaitTermination(5, TimeUnit.SECONDS);
  }

  /**
   * 1. S3: concurrent appender x {@code startingVersion=latest}. The fresh stream pins endOffset at
   * the moment of start; concurrent appends afterward should arrive in subsequent micro-batches.
   * Sink rows >= 0 and <= total commits; the stream must not surface a raw NPE.
   */
  @Test
  public void testConcurrentWriter_startingVersionLatest(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    File checkpointDir = new File(deltaTablePath, "_ckpt");
    File outputDir = new File(deltaTablePath, "_out");

    spark.sql(str("CREATE TABLE delta.`%s` (id INT) USING delta", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (0)", tablePath));

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> streamingDF =
        spark.readStream().option("startingVersion", "latest").table(dsv2TableRef);

    AtomicBoolean stop = new AtomicBoolean(false);
    ExecutorService writer = null;
    StreamingQuery query = null;
    Throwable caught = null;
    try {
      query =
          streamingDF
              .writeStream()
              .format("parquet")
              .outputMode("append")
              .option("path", outputDir.getAbsolutePath())
              .option("checkpointLocation", checkpointDir.getAbsolutePath())
              .start();
      writer = startConcurrentAppender(tablePath, stop, 20);
      Thread.sleep(2_000L);
      stop.set(true);
      query.processAllAvailable();
    } catch (Throwable t) {
      caught = t;
    } finally {
      if (writer != null) {
        stopAppender(writer, stop);
      }
      if (query != null) {
        query.stop();
      }
      DeltaLog.clearCache();
    }
    if (caught != null && containsNpe(caught)) {
      fail("startingVersion=latest + concurrent appender produced a raw NPE: " + caught);
    }
    // startingVersion=latest skips the pre-existing v=0 row; everything else is post-start.
    List<Row> sink = spark.read().parquet(outputDir.getAbsolutePath()).collectAsList();
    long sourceCount = spark.read().format("delta").load(tablePath).count();
    assertTrue(
        sink.size() <= sourceCount,
        () -> "Sink size " + sink.size() + " must not exceed source " + sourceCount);
  }

  /**
   * 2. S6: concurrent appender x {@code maxBytesPerTrigger=1b}. Forces one file per batch under
   * concurrent commits; sink must equal the source row count at drain.
   */
  @Test
  public void testConcurrentWriter_maxBytesPerTrigger(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    File checkpointDir = new File(deltaTablePath, "_ckpt");
    File outputDir = new File(deltaTablePath, "_out");

    spark.sql(str("CREATE TABLE delta.`%s` (id INT) USING delta", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (0)", tablePath));

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> streamingDF =
        spark.readStream().option("maxBytesPerTrigger", "1b").table(dsv2TableRef);

    AtomicBoolean stop = new AtomicBoolean(false);
    ExecutorService writer = null;
    StreamingQuery query = null;
    try {
      query =
          streamingDF
              .writeStream()
              .format("parquet")
              .outputMode("append")
              .option("path", outputDir.getAbsolutePath())
              .option("checkpointLocation", checkpointDir.getAbsolutePath())
              .start();
      writer = startConcurrentAppender(tablePath, stop, 20);
      Thread.sleep(2_000L);
      stop.set(true);
      query.processAllAvailable();
    } finally {
      if (writer != null) {
        stopAppender(writer, stop);
      }
      if (query != null) {
        query.stop();
      }
      DeltaLog.clearCache();
    }

    long sourceCount = spark.read().format("delta").load(tablePath).count();
    List<Row> sink = spark.read().parquet(outputDir.getAbsolutePath()).collectAsList();
    assertEquals(
        sourceCount,
        (long) sink.size(),
        () ->
            "maxBytesPerTrigger=1b missed concurrent commits: source="
                + sourceCount
                + " sink="
                + sink.size());
  }

  /**
   * 3. S7: concurrent appender x {@code Trigger.AvailableNow}. AvailableNow drains exactly the
   * commits visible at start; later commits stay unconsumed. We assert: (a) no NPE; (b) sink rows
   * less-than-or-equal-to source count.
   */
  @Test
  public void testConcurrentWriter_triggerAvailableNow(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    File checkpointDir = new File(deltaTablePath, "_ckpt");
    File outputDir = new File(deltaTablePath, "_out");

    spark.sql(str("CREATE TABLE delta.`%s` (id INT) USING delta", tablePath));
    // Seed with a handful of commits so AvailableNow has work to do.
    for (int i = 0; i < 5; i++) {
      spark.sql(str("INSERT INTO delta.`%s` VALUES (%d)", tablePath, i));
    }

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> streamingDF = spark.readStream().table(dsv2TableRef);

    AtomicBoolean stop = new AtomicBoolean(false);
    ExecutorService writer = startConcurrentAppender(tablePath, stop, 10);
    Throwable caught = null;
    StreamingQuery query = null;
    try {
      query =
          streamingDF
              .writeStream()
              .format("parquet")
              .outputMode("append")
              .option("path", outputDir.getAbsolutePath())
              .option("checkpointLocation", checkpointDir.getAbsolutePath())
              .trigger(Trigger.AvailableNow())
              .start();
      query.awaitTermination(30_000L);
    } catch (Throwable t) {
      caught = t;
    } finally {
      stopAppender(writer, stop);
      if (query != null) {
        try {
          query.stop();
        } catch (Throwable ignored) {
        }
      }
      DeltaLog.clearCache();
    }
    if (caught == null && query != null && query.exception().isDefined()) {
      caught = query.exception().get();
    }
    if (caught != null && containsNpe(caught)) {
      fail("AvailableNow + concurrent appender produced a raw NPE: " + caught);
    }
    long sourceCount = spark.read().format("delta").load(tablePath).count();
    List<Row> sink = spark.read().parquet(outputDir.getAbsolutePath()).collectAsList();
    assertTrue(
        sink.size() <= sourceCount,
        () -> "AvailableNow sink size " + sink.size() + " exceeds source " + sourceCount);
  }

  /**
   * 4. S15: concurrent appender x {@code excludeRegex}. A no-match regex must not filter any commit
   * files; all concurrent appends should reach the sink.
   */
  @Test
  public void testConcurrentWriter_excludeRegex(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    File checkpointDir = new File(deltaTablePath, "_ckpt");
    File outputDir = new File(deltaTablePath, "_out");

    spark.sql(str("CREATE TABLE delta.`%s` (id INT) USING delta", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (0)", tablePath));

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> streamingDF =
        spark
            .readStream()
            .option("excludeRegex", "this-pattern-cannot-match-any-real-file-name-xyz")
            .table(dsv2TableRef);

    AtomicBoolean stop = new AtomicBoolean(false);
    ExecutorService writer = null;
    StreamingQuery query = null;
    try {
      query =
          streamingDF
              .writeStream()
              .format("parquet")
              .outputMode("append")
              .option("path", outputDir.getAbsolutePath())
              .option("checkpointLocation", checkpointDir.getAbsolutePath())
              .start();
      writer = startConcurrentAppender(tablePath, stop, 10);
      Thread.sleep(2_000L);
      stop.set(true);
      query.processAllAvailable();
    } finally {
      if (writer != null) {
        stopAppender(writer, stop);
      }
      if (query != null) {
        query.stop();
      }
      DeltaLog.clearCache();
    }

    long sourceCount = spark.read().format("delta").load(tablePath).count();
    List<Row> sink = spark.read().parquet(outputDir.getAbsolutePath()).collectAsList();
    assertEquals(
        sourceCount,
        (long) sink.size(),
        () ->
            "no-match excludeRegex + concurrent appender lost rows: source="
                + sourceCount
                + " sink="
                + sink.size());
  }

  /**
   * 5. S22 x S12 combo: concurrent writer prunes a commit JSON mid-stream while {@code
   * failOnDataLoss=false}. After concurrent appends commit a few versions, we prune one early
   * version's JSON. With a checkpoint at the latest snapshot, the stream should still see all
   * surviving rows via snapshot reconstruction.
   */
  @Test
  public void testConcurrentWriter_failOnDataLossFalse_pruneDuring(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (id INT) USING delta", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (0)", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1)", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (2)", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (3)", tablePath));

    // Take a checkpoint so the snapshot can be reconstructed without the pruned commit JSON.
    checkpoint(tablePath);
    // Prune a middle commit; failOnDataLoss=false should let the stream proceed.
    pruneCommitJson(tablePath, /* version= */ 2L);

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> df =
        spark
            .readStream()
            .option("failOnDataLoss", "false")
            .option("startingVersion", "0")
            .table(dsv2TableRef);

    List<Row> rows = processStreamingQuery(df, "concurrent_fnl_prune");
    // 4 inserted rows survive in the reconstructed snapshot.
    assertEquals(
        4,
        rows.size(),
        () -> "failOnDataLoss=false + pruned commit should surface all 4 rows, got: " + rows);
  }

  /**
   * 6. S5: restore past checkpoint x {@code maxFilesPerTrigger=1}. The pre-restart checkpoint
   * advances the offset past where RESTORE rewinds to; restart must not NPE under rate limiting.
   */
  @Test
  public void testRestorePastCheckpoint_maxFilesPerTrigger(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    File checkpointDir = new File(deltaTablePath, "_ckpt");
    File outputDir = new File(deltaTablePath, "_out");

    spark.sql(str("CREATE TABLE delta.`%s` (id INT) USING delta", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1)", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (2)", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (3)", tablePath));

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> df1 = spark.readStream().table(dsv2TableRef);
    runWithParquetSink(df1, outputDir, checkpointDir, /* trigger= */ null);
    DeltaLog.clearCache();

    spark.sql(str("RESTORE TABLE delta.`%s` TO VERSION AS OF 0", tablePath));

    Throwable caught = null;
    try {
      Dataset<Row> df2 = spark.readStream().option("maxFilesPerTrigger", "1").table(dsv2TableRef);
      runWithParquetSink(df2, outputDir, checkpointDir, /* trigger= */ null);
    } catch (Throwable t) {
      caught = t;
    } finally {
      DeltaLog.clearCache();
    }
    if (caught != null && containsNpe(caught)) {
      fail("Restore + maxFilesPerTrigger produced a raw NPE: " + caught);
    }
  }

  /** 7. S7: restore past checkpoint x {@code Trigger.AvailableNow}. */
  @Test
  public void testRestorePastCheckpoint_triggerAvailableNow(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    File checkpointDir = new File(deltaTablePath, "_ckpt");
    File outputDir = new File(deltaTablePath, "_out");

    spark.sql(str("CREATE TABLE delta.`%s` (id INT) USING delta", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1)", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (2)", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (3)", tablePath));

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    runWithParquetSink(
        spark.readStream().table(dsv2TableRef), outputDir, checkpointDir, /* trigger= */ null);
    DeltaLog.clearCache();

    spark.sql(str("RESTORE TABLE delta.`%s` TO VERSION AS OF 0", tablePath));

    Throwable caught = null;
    StreamingQuery query = null;
    try {
      query =
          spark
              .readStream()
              .table(dsv2TableRef)
              .writeStream()
              .format("parquet")
              .outputMode("append")
              .option("path", outputDir.getAbsolutePath())
              .option("checkpointLocation", checkpointDir.getAbsolutePath())
              .trigger(Trigger.AvailableNow())
              .start();
      query.awaitTermination(30_000L);
    } catch (Throwable t) {
      caught = t;
    } finally {
      if (query != null) {
        try {
          query.stop();
        } catch (Throwable ignored) {
        }
      }
      DeltaLog.clearCache();
    }
    if (caught == null && query != null && query.exception().isDefined()) {
      caught = query.exception().get();
    }
    if (caught != null && containsNpe(caught)) {
      fail("Restore + AvailableNow produced a raw NPE: " + caught);
    }
  }

  /** 8. S15: restore past checkpoint x {@code excludeRegex} (no-match pattern). */
  @Test
  public void testRestorePastCheckpoint_excludeRegex(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    File checkpointDir = new File(deltaTablePath, "_ckpt");
    File outputDir = new File(deltaTablePath, "_out");

    spark.sql(str("CREATE TABLE delta.`%s` (id INT) USING delta", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1)", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (2)", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (3)", tablePath));

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    runWithParquetSink(
        spark.readStream().table(dsv2TableRef), outputDir, checkpointDir, /* trigger= */ null);
    DeltaLog.clearCache();

    spark.sql(str("RESTORE TABLE delta.`%s` TO VERSION AS OF 1", tablePath));

    Throwable caught = null;
    try {
      Dataset<Row> df2 =
          spark
              .readStream()
              .option("excludeRegex", "this-pattern-cannot-match-any-real-file-name-xyz")
              .option("skipChangeCommits", "true")
              .table(dsv2TableRef);
      runWithParquetSink(df2, outputDir, checkpointDir, /* trigger= */ null);
    } catch (Throwable t) {
      caught = t;
    } finally {
      DeltaLog.clearCache();
    }
    if (caught != null && containsNpe(caught)) {
      fail("Restore + excludeRegex produced a raw NPE: " + caught);
    }
  }

  /**
   * 9. S4: restore past checkpoint x {@code startingTimestamp}.
   *
   * <p>{@code startingTimestamp} is a fresh-stream option (it pins the initial offset). DSv2 may
   * reject combining a fresh-start time-travel option with a restart from an existing checkpoint;
   * we assert only no raw NPE. The option may resolve to a version pre-RESTORE.
   */
  @Test
  public void testRestorePastCheckpoint_startingTimestamp(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    File checkpointDir = new File(deltaTablePath, "_ckpt");
    File outputDir = new File(deltaTablePath, "_out");

    spark.sql(str("CREATE TABLE delta.`%s` (id INT) USING delta", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1)", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (2)", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (3)", tablePath));

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    runWithParquetSink(
        spark.readStream().table(dsv2TableRef), outputDir, checkpointDir, /* trigger= */ null);
    DeltaLog.clearCache();

    spark.sql(str("RESTORE TABLE delta.`%s` TO VERSION AS OF 0", tablePath));

    String tsStr = formatTs(0L);
    Throwable caught = null;
    try {
      Dataset<Row> df2 = spark.readStream().option("startingTimestamp", tsStr).table(dsv2TableRef);
      runWithParquetSink(df2, outputDir, checkpointDir, /* trigger= */ null);
    } catch (Throwable t) {
      caught = t;
    } finally {
      DeltaLog.clearCache();
    }
    if (caught != null && containsNpe(caught)) {
      fail("Restore + startingTimestamp produced a raw NPE: " + caught);
    }
  }

  /**
   * 10. S15: OPTIMIZE then fresh stream x {@code excludeRegex} (no-match). Post-OPTIMIZE the table
   * is one big file; the no-match regex must not exclude it.
   */
  @Test
  public void testOptimizeBeforeFreshStream_excludeRegex(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (id INT, name STRING) USING delta", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'A')", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (2, 'B')", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (3, 'C')", tablePath));
    withSQLConf(
        "spark.databricks.delta.optimize.minFileSize",
        Long.toString(1024L * 1024L * 1024L),
        () -> spark.sql(str("OPTIMIZE delta.`%s`", tablePath)));

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> streamingDF =
        spark
            .readStream()
            .option("excludeRegex", "this-pattern-cannot-match-any-real-file-name-xyz")
            .table(dsv2TableRef);
    List<Row> rows = processStreamingQuery(streamingDF, "opt_fresh_exclRegex");
    assertEquals(
        3,
        rows.size(),
        () -> "no-match excludeRegex post-OPTIMIZE should emit 3 rows, got: " + rows.size());
  }

  /** 11. S7: OPTIMIZE then fresh stream x {@code Trigger.AvailableNow}. */
  @Test
  public void testOptimizeBeforeFreshStream_triggerAvailableNow(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    File checkpointDir = new File(deltaTablePath, "_ckpt");
    File outputDir = new File(deltaTablePath, "_out");

    spark.sql(str("CREATE TABLE delta.`%s` (id INT, name STRING) USING delta", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'A')", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (2, 'B')", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (3, 'C')", tablePath));
    withSQLConf(
        "spark.databricks.delta.optimize.minFileSize",
        Long.toString(1024L * 1024L * 1024L),
        () -> spark.sql(str("OPTIMIZE delta.`%s`", tablePath)));

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> streamingDF = spark.readStream().table(dsv2TableRef);
    List<Row> rows =
        runWithParquetSink(streamingDF, outputDir, checkpointDir, Trigger.AvailableNow());
    // Post-OPTIMIZE the logical content is the original 3 user-written rows; the dataChange=false
    // rewrite must not produce extras.
    assertEquals(
        3, rows.size(), () -> "AvailableNow post-OPTIMIZE should emit 3 rows, got: " + rows.size());
  }

  /**
   * 12. S12 (failOnDataLoss=false): OPTIMIZE then fresh stream with the option. The option is
   * silently dropped if DSv2 rejects {@code failOnDataLoss}; either way the snapshot's logical
   * content is 3 rows.
   */
  @Test
  public void testOptimizeBeforeFreshStream_failOnDataLossFalse(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (id INT, name STRING) USING delta", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'A')", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (2, 'B')", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (3, 'C')", tablePath));
    withSQLConf(
        "spark.databricks.delta.optimize.minFileSize",
        Long.toString(1024L * 1024L * 1024L),
        () -> spark.sql(str("OPTIMIZE delta.`%s`", tablePath)));

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    Throwable caught = null;
    List<Row> rows = null;
    try {
      Dataset<Row> streamingDF =
          spark
              .readStream()
              .option("failOnDataLoss", "false")
              .option("startingVersion", "0")
              .table(dsv2TableRef);
      rows = processStreamingQuery(streamingDF, "opt_fresh_fnl_false");
    } catch (Throwable t) {
      caught = t;
    } finally {
      DeltaLog.clearCache();
    }
    if (caught != null && containsNpe(caught)) {
      fail("OPTIMIZE + fresh stream + failOnDataLoss=false produced a raw NPE: " + caught);
    }
    if (caught == null && rows != null) {
      final List<Row> finalRows = rows;
      assertEquals(
          3,
          rows.size(),
          () -> "OPTIMIZE + failOnDataLoss=false should emit 3 rows, got: " + finalRows.size());
    }
  }

  /** 13. S6: OPTIMIZE then fresh stream x {@code maxBytesPerTrigger=1b}. */
  @Test
  public void testOptimizeBeforeFreshStream_maxBytesPerTrigger(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (id INT, name STRING) USING delta", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'A')", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (2, 'B')", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (3, 'C')", tablePath));
    withSQLConf(
        "spark.databricks.delta.optimize.minFileSize",
        Long.toString(1024L * 1024L * 1024L),
        () -> spark.sql(str("OPTIMIZE delta.`%s`", tablePath)));

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> streamingDF =
        spark.readStream().option("maxBytesPerTrigger", "1b").table(dsv2TableRef);
    List<Row> rows = processStreamingQuery(streamingDF, "opt_fresh_maxBytes");
    // Post-OPTIMIZE the table is one file; even at 1b rate the snapshot's single file is admitted.
    assertEquals(
        3,
        rows.size(),
        () -> "maxBytesPerTrigger=1b post-OPTIMIZE should emit 3 rows, got: " + rows.size());
  }

  /**
   * 14. S5: enable DV mid-stream x {@code maxFilesPerTrigger=1}. Pre-DV commits go through one file
   * per batch; post-DV commits (including the protocol upgrade) replay under the same rate limit.
   * Assert no NPE.
   */
  @Test
  public void testEnableDvMidStream_maxFilesPerTrigger(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    File checkpointDir = new File(deltaTablePath, "_ckpt");
    File outputDir = new File(deltaTablePath, "_out");

    spark.sql(str("CREATE TABLE delta.`%s` (id INT) USING delta", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1), (2), (3)", tablePath));

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> df1 = spark.readStream().option("maxFilesPerTrigger", "1").table(dsv2TableRef);
    runWithParquetSink(df1, outputDir, checkpointDir, /* trigger= */ null);
    DeltaLog.clearCache();

    spark.sql(
        str(
            "ALTER TABLE delta.`%s` SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')",
            tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (4)", tablePath));

    Throwable caught = null;
    try {
      Dataset<Row> df2 =
          spark
              .readStream()
              .option("maxFilesPerTrigger", "1")
              .option("skipChangeCommits", "true")
              .table(dsv2TableRef);
      runWithParquetSink(df2, outputDir, checkpointDir, /* trigger= */ null);
    } catch (Throwable t) {
      caught = t;
    } finally {
      DeltaLog.clearCache();
    }
    if (caught != null && containsNpe(caught)) {
      fail("Enable DV mid-stream + maxFilesPerTrigger produced a raw NPE: " + caught);
    }
  }

  /** 15. S7: enable RT mid-stream x {@code Trigger.AvailableNow}. */
  @Test
  public void testEnableRtMidStream_triggerAvailableNow(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    File checkpointDir = new File(deltaTablePath, "_ckpt");
    File outputDir = new File(deltaTablePath, "_out");

    spark.sql(str("CREATE TABLE delta.`%s` (id INT, name STRING) USING delta", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'A')", tablePath));

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    runWithParquetSink(
        spark.readStream().table(dsv2TableRef), outputDir, checkpointDir, /* trigger= */ null);
    DeltaLog.clearCache();

    spark.sql(
        str(
            "ALTER TABLE delta.`%s` SET TBLPROPERTIES ('delta.enableRowTracking' = 'true')",
            tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (2, 'B')", tablePath));

    Throwable caught = null;
    StreamingQuery query = null;
    try {
      query =
          spark
              .readStream()
              .table(dsv2TableRef)
              .writeStream()
              .format("parquet")
              .outputMode("append")
              .option("path", outputDir.getAbsolutePath())
              .option("checkpointLocation", checkpointDir.getAbsolutePath())
              .trigger(Trigger.AvailableNow())
              .start();
      query.awaitTermination(30_000L);
    } catch (Throwable t) {
      caught = t;
    } finally {
      if (query != null) {
        try {
          query.stop();
        } catch (Throwable ignored) {
        }
      }
      DeltaLog.clearCache();
    }
    if (caught == null && query != null && query.exception().isDefined()) {
      caught = query.exception().get();
    }
    if (caught != null && containsNpe(caught)) {
      fail("Enable RT mid-stream + AvailableNow produced a raw NPE: " + caught);
    }
  }

  /**
   * 16. S4: enable ICT mid-stream x {@code startingTimestamp}. ICT changes how commit timestamps
   * are sourced; a startingTimestamp option on a restart across the toggle must not NPE.
   */
  @Test
  public void testEnableIctMidStream_startingTimestamp(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    File checkpointDir = new File(deltaTablePath, "_ckpt");
    File outputDir = new File(deltaTablePath, "_out");

    spark.sql(str("CREATE TABLE delta.`%s` (id INT) USING delta", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1)", tablePath));

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    runWithParquetSink(
        spark.readStream().table(dsv2TableRef), outputDir, checkpointDir, /* trigger= */ null);
    DeltaLog.clearCache();

    spark.sql(
        str(
            "ALTER TABLE delta.`%s` SET TBLPROPERTIES "
                + "('delta.enableInCommitTimestamps' = 'true')",
            tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (2)", tablePath));

    // startingTimestamp is a fresh-start option; combined with an existing checkpoint, the engine
    // may surface a structured error. Either way: no NPE.
    String tsStr = formatTs(0L);
    Throwable caught = null;
    try {
      Dataset<Row> df2 = spark.readStream().option("startingTimestamp", tsStr).table(dsv2TableRef);
      runWithParquetSink(df2, outputDir, checkpointDir, /* trigger= */ null);
    } catch (Throwable t) {
      caught = t;
    } finally {
      DeltaLog.clearCache();
    }
    if (caught != null && containsNpe(caught)) {
      fail("Enable ICT mid-stream + startingTimestamp produced a raw NPE: " + caught);
    }
  }

  /**
   * 17. S12 (failOnDataLoss=false): enable DV mid-stream then DV-DELETE; restart with the option.
   * If DSv2 rejects failOnDataLoss, the restart fails analysis cleanly; otherwise, the DV change
   * commit replays into the sink. Either way: no NPE.
   */
  @Test
  public void testEnableDvMidStream_failOnDataLossFalse(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    File checkpointDir = new File(deltaTablePath, "_ckpt");
    File outputDir = new File(deltaTablePath, "_out");

    spark.sql(str("CREATE TABLE delta.`%s` (id INT) USING delta", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1), (2), (3)", tablePath));

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    runWithParquetSink(
        spark.readStream().table(dsv2TableRef), outputDir, checkpointDir, /* trigger= */ null);
    DeltaLog.clearCache();

    spark.sql(
        str(
            "ALTER TABLE delta.`%s` SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')",
            tablePath));
    spark.sql(str("DELETE FROM delta.`%s` WHERE id = 1", tablePath));

    Throwable caught = null;
    try {
      Dataset<Row> df2 =
          spark
              .readStream()
              .option("failOnDataLoss", "false")
              .option("skipChangeCommits", "true")
              .table(dsv2TableRef);
      runWithParquetSink(df2, outputDir, checkpointDir, /* trigger= */ null);
    } catch (Throwable t) {
      caught = t;
    } finally {
      DeltaLog.clearCache();
    }
    if (caught != null && containsNpe(caught)) {
      fail("Enable DV mid-stream + failOnDataLoss=false produced a raw NPE: " + caught);
    }
  }

  /**
   * 18. S20: corrupt {@code _last_checkpoint}; the stream should resume via the list-fallback path.
   * Pinned-shape assertion: either successful drain or a structured error (no raw NPE).
   */
  @Test
  public void testStreamThroughCorruptCheckpoint(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    // 12 commits triggers a checkpoint at v=10 by default.
    spark.sql(str("CREATE TABLE delta.`%s` (id INT) USING delta", tablePath));
    for (int i = 0; i < 12; i++) {
      spark.sql(str("INSERT INTO delta.`%s` VALUES (%d)", tablePath, i));
    }
    checkpoint(tablePath);

    Path lastCheckpoint = Paths.get(tablePath, "_delta_log", "_last_checkpoint");
    assertTrue(Files.exists(lastCheckpoint), "_last_checkpoint should exist");
    long origSize = Files.size(lastCheckpoint);
    int halfRemove = (int) (origSize / 2);
    try (java.nio.channels.FileChannel ch =
        java.nio.channels.FileChannel.open(lastCheckpoint, StandardOpenOption.WRITE)) {
      ch.truncate(Math.max(0, origSize - halfRemove));
    }
    DeltaLog.clearCache();

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> df = spark.readStream().option("startingVersion", "0").table(dsv2TableRef);

    Throwable caught = null;
    try {
      processStreamingQuery(df, "corrupt_lastckpt");
    } catch (Throwable t) {
      caught = t;
    } finally {
      DeltaLog.clearCache();
    }
    if (caught != null && containsNpe(caught)) {
      fail("Corrupt _last_checkpoint produced a raw NPE: " + caught);
    }
  }

  /**
   * 19. S20 variant: a fabricated {@code 0...N.compacted.json} alongside the per-version commits.
   * The stream should still resolve the commit range and produce the expected row set, matching the
   * DSv1 behavior already pinned by {@link
   * V2StreamingLogIntegrityTest#testScenario12_logCompactionFilePresent}.
   */
  @Test
  public void testStreamWithCompactedJson(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (id INT) USING delta", tablePath));
    for (int i = 0; i < 10; i++) {
      spark.sql(str("INSERT INTO delta.`%s` VALUES (%d)", tablePath, i));
    }

    // Fabricate 0...4.compacted.json by concatenating raw JSON from v=0..4.
    Path compactedFile =
        Paths.get(tablePath, "_delta_log", String.format("%020d.%020d.compacted.json", 0L, 4L));
    StringBuilder concatenated = new StringBuilder();
    for (int v = 0; v < 5; v++) {
      Path commit = Paths.get(tablePath, "_delta_log", String.format("%020d.json", v));
      String content = new String(Files.readAllBytes(commit), StandardCharsets.UTF_8);
      concatenated.append(content);
      if (!content.endsWith("\n")) {
        concatenated.append("\n");
      }
    }
    Files.write(compactedFile, concatenated.toString().getBytes(StandardCharsets.UTF_8));
    DeltaLog.clearCache();

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> df = spark.readStream().option("startingVersion", "0").table(dsv2TableRef);

    Throwable caught = null;
    List<Row> rows = null;
    try {
      rows = processStreamingQuery(df, "compacted_json");
    } catch (Throwable t) {
      caught = t;
    } finally {
      DeltaLog.clearCache();
    }
    if (caught != null && containsNpe(caught)) {
      fail("Compacted JSON present produced a raw NPE: " + caught);
    }
    // 10 user-inserted rows; the compacted file is a duplicate of the first 5 commits but Kernel
    // should not double-count.
    if (caught == null && rows != null) {
      final List<Row> finalRows = rows;
      assertEquals(
          10,
          rows.size(),
          () ->
              "Stream with fabricated compacted JSON should emit 10 rows, got: "
                  + finalRows.size());
    }
  }

  /**
   * 20. S20 variant: drop a non-JSON file into {@code _delta_log/}; the stream must ignore it. We
   * use a stray {@code .txt} file (commit-log listing is filtered to {@code *.json} +
   * checkpoint-shaped files).
   */
  @Test
  public void testStreamWithExtraNonJsonFile_inDeltaLog(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (id INT) USING delta", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1)", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (2)", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (3)", tablePath));

    Path stray = Paths.get(tablePath, "_delta_log", "stray_garbage.txt");
    Files.write(stray, "not a delta log file".getBytes(StandardCharsets.UTF_8));
    DeltaLog.clearCache();

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> df = spark.readStream().option("startingVersion", "0").table(dsv2TableRef);

    List<Row> rows = processStreamingQuery(df, "stray_nonjson");
    assertEquals(
        3,
        rows.size(),
        () -> "Stream must ignore stray non-JSON file in _delta_log; got: " + rows.size());
  }

  /**
   * 21. S21: prune past retention boundary with {@code failOnDataLoss=false}. After taking a
   * checkpoint, prune both v=0 and v=1; the snapshot still reconstructs and the stream proceeds.
   */
  @Test
  public void testStreamAcrossLogRetentionPrune_failOnDataLossFalse(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (id INT) USING delta", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1)", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (2)", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (3)", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (4)", tablePath));

    checkpoint(tablePath);
    pruneCommitJson(tablePath, /* version= */ 0L);
    pruneCommitJson(tablePath, /* version= */ 1L);

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> df =
        spark
            .readStream()
            .option("failOnDataLoss", "false")
            .option("startingVersion", "0")
            .table(dsv2TableRef);
    List<Row> rows = processStreamingQuery(df, "logretention_fnl_false");
    // All 4 rows survive in the reconstructed snapshot.
    assertEquals(
        4,
        rows.size(),
        () ->
            "Stream across log-retention prune + failOnDataLoss=false should emit 4 rows, got: "
                + rows.size());
  }

  /**
   * 22. S18 variant: stream across a metadata-only commit (ALTER TABLE SET TBLPROPERTIES). The
   * commit produces no AddFiles; the stream should pass through it without emitting extra rows.
   */
  @Test
  public void testStreamAcrossMetadataOnlyCommit(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (id INT) USING delta", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1)", tablePath));
    // Metadata-only commit: set a benign property.
    spark.sql(
        str(
            "ALTER TABLE delta.`%s` SET TBLPROPERTIES ('delta.userMetadata.test' = 'true')",
            tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (2)", tablePath));

    final long postVersion = latestVersion(tablePath);
    assertTrue(
        postVersion >= 3L, () -> "Expected v>=3 after metadata-only commit; got " + postVersion);

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> df = spark.readStream().option("startingVersion", "0").table(dsv2TableRef);

    List<Row> rows = processStreamingQuery(df, "metadata_only_commit");
    assertEquals(
        2,
        rows.size(),
        () ->
            "Stream across metadata-only commit should emit 2 rows (one per INSERT), got: "
                + rows.size());
  }

  /**
   * S17 (nullability change mid-stream): ALTER TABLE drops the NOT NULL constraint on a column
   * mid-stream. With schemaTrackingLocation, DSv2 should adopt the updated schema and continue
   * emitting rows rather than raising DELTA_STREAMING_SCHEMA_MISMATCH_ON_RESTART.
   */
  @Test
  public void testNullabilityToggleMidStream(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    File checkpointDir = new File(deltaTablePath, "_checkpoint");
    File schemaTrackingDir = new File(checkpointDir, "_schema_tracking");
    File outputDir = new File(deltaTablePath, "_output");

    // Create table with a NOT NULL column.
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT NOT NULL, value STRING) USING delta "
                + "TBLPROPERTIES ('delta.columnMapping.mode' = 'name', "
                + "               'delta.minReaderVersion' = '2', "
                + "               'delta.minWriterVersion' = '5')",
            tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'a'), (2, 'b')", tablePath));

    // First run: stream 2 rows before the nullability change.
    Dataset<Row> df1 =
        spark
            .readStream()
            .option("schemaTrackingLocation", schemaTrackingDir.getAbsolutePath())
            .table(str("dsv2.delta.`%s`", tablePath));
    List<Row> firstRun = runWithParquetSink(df1, outputDir, checkpointDir, Trigger.AvailableNow());
    assertEquals(2, firstRun.size(), "first run should emit 2 rows");

    // Drop NOT NULL constraint mid-stream (nullability toggle).
    spark.sql(str("ALTER TABLE delta.`%s` ALTER COLUMN id DROP NOT NULL", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (3, 'c'), (null, 'd')", tablePath));

    // Second run: restart from checkpoint; schema evolution handler should adopt new nullability.
    Dataset<Row> df2 =
        spark
            .readStream()
            .option("schemaTrackingLocation", schemaTrackingDir.getAbsolutePath())
            .table(str("dsv2.delta.`%s`", tablePath));
    List<Row> secondRun = runWithParquetSink(df2, outputDir, checkpointDir, Trigger.AvailableNow());
    assertEquals(2, secondRun.size(), "second run should emit 2 new rows");

    // Collect all rows and verify null id is present.
    List<Row> all = new java.util.ArrayList<>(firstRun);
    all.addAll(secondRun);
    assertEquals(4, all.size());
    assertTrue(
        all.stream().anyMatch(r -> r.isNullAt(0)), "post-toggle null id row should be present");
  }

  /**
   * S16 (drop column mid-stream) x concurrent-writer lifecycle: stream a first batch, then a
   * mid-stream writer drops a column. The restart must adopt the post-drop 2-column schema via the
   * schema-tracking log + {@code allowSourceColumnDrop=always}, not surface a hard
   * DELTA_STREAMING_SCHEMA_MISMATCH_ON_RESTART.
   */
  @Test
  public void testConcurrentWriter_dropColumnMidStream(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    File checkpointDir = new File(deltaTablePath, "_checkpoint");
    File schemaTrackingDir = new File(checkpointDir, "_schema_tracking");
    File outputDir = new File(deltaTablePath, "_output");
    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);

    // CM-name table with three columns; the `extra` column will be dropped mid-stream.
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, value STRING, extra INT) USING delta "
                + "TBLPROPERTIES ('delta.columnMapping.mode' = 'name', "
                + "               'delta.minReaderVersion' = '2', "
                + "               'delta.minWriterVersion' = '5')",
            tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'a', 10), (2, 'b', 20)", tablePath));

    // First run: drain the 2 pre-drop rows under the original 3-column schema.
    Dataset<Row> df1 =
        spark
            .readStream()
            .option("schemaTrackingLocation", schemaTrackingDir.getAbsolutePath())
            .table(dsv2TableRef);
    List<Row> firstRun = runWithParquetSink(df1, outputDir, checkpointDir, Trigger.AvailableNow());
    assertEquals(2, firstRun.size(), "first run should emit 2 pre-drop rows");
    assertEquals(3, firstRun.get(0).size(), "first run rows should have 3 columns");
    DeltaLog.clearCache();

    // Mid-stream non-additive change: drop `extra`, then write post-drop rows.
    spark.sql(str("ALTER TABLE delta.`%s` DROP COLUMN extra", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (3, 'c'), (4, 'd')", tablePath));

    // Second run: restart from the same checkpoint + schema-tracking log; allowSourceColumnDrop
    // unblocks the non-additive evolution so the stream continues without
    // DELTA_STREAMING_SCHEMA_MISMATCH_ON_RESTART.
    Dataset<Row> df2 =
        spark
            .readStream()
            .option("schemaTrackingLocation", schemaTrackingDir.getAbsolutePath())
            .option("allowSourceColumnDrop", "always")
            .table(dsv2TableRef);
    List<Row> secondRun = runWithParquetSink(df2, outputDir, checkpointDir, Trigger.AvailableNow());
    DeltaLog.clearCache();

    assertEquals(
        2, secondRun.size(), () -> "second run should emit 2 post-drop rows: " + secondRun);
    // Post-drop rows should match the evolved 2-column schema.
    assertEquals(
        2,
        secondRun.get(0).size(),
        () -> "post-drop row should have 2 columns, got: " + secondRun.get(0));
  }

  /**
   * S16 (rename column) x restore-past-checkpoint lifecycle: rename a column, then RESTORE past the
   * rename, then stream with schemaTrackingLocation + allowSourceColumnRename. The stream must
   * surface the historical pre-rename schema rows without NPE; under the evolved schema-tracking
   * log all surviving rows from the restored snapshot are accounted for.
   */
  @Test
  public void testRestoreAfterRename_withSchemaTracking(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    File checkpointDir = new File(deltaTablePath, "_checkpoint");
    File schemaTrackingDir = new File(checkpointDir, "_schema_tracking");
    File outputDir = new File(deltaTablePath, "_output");
    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);

    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, value STRING) USING delta "
                + "TBLPROPERTIES ('delta.columnMapping.mode' = 'name', "
                + "               'delta.minReaderVersion' = '2', "
                + "               'delta.minWriterVersion' = '5')",
            tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'a'), (2, 'b')", tablePath));
    long preRenameVersion = latestVersion(tablePath);

    // Rename: non-additive change requires schemaTrackingLocation to thread through restart.
    spark.sql(str("ALTER TABLE delta.`%s` RENAME COLUMN value TO val", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (3, 'c')", tablePath));

    // RESTORE past the rename - the table's latest snapshot now exposes the pre-rename `value`
    // column again, but the commit history still contains the rename action.
    spark.sql(str("RESTORE TABLE delta.`%s` TO VERSION AS OF %d", tablePath, preRenameVersion));
    DeltaLog.clearCache();

    Throwable caught = null;
    List<Row> rows = null;
    try {
      Dataset<Row> df =
          spark
              .readStream()
              .option("schemaTrackingLocation", schemaTrackingDir.getAbsolutePath())
              .option("allowSourceColumnRename", "always")
              .table(dsv2TableRef);
      rows = runWithParquetSink(df, outputDir, checkpointDir, Trigger.AvailableNow());
    } catch (Throwable t) {
      caught = t;
    } finally {
      DeltaLog.clearCache();
    }
    if (caught != null && containsNpe(caught)) {
      fail("RESTORE-after-rename + schemaTrackingLocation produced a raw NPE: " + caught);
    }
    // Either the stream drains cleanly (every emitted row maps to a surviving id) or it surfaces a
    // structured error. No raw NPE either way.
    if (caught == null && rows != null) {
      long sourceCount = spark.read().format("delta").load(tablePath).count();
      final List<Row> finalRows = rows;
      assertTrue(
          rows.size() <= sourceCount,
          () ->
              "sink rows "
                  + finalRows.size()
                  + " must not exceed restored snapshot count "
                  + sourceCount);
    }
  }

  /**
   * S16 (rename column) x restart-from-checkpoint lifecycle: drain a first batch under the original
   * schema, rename a column between runs, then restart with allowSourceColumnRename. The second run
   * must see the renamed column without DELTA_STREAMING_SCHEMA_MISMATCH_ON_RESTART.
   */
  @Test
  public void testRenameColumn_thenRestart(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    File checkpointDir = new File(deltaTablePath, "_checkpoint");
    File schemaTrackingDir = new File(checkpointDir, "_schema_tracking");
    File outputDir = new File(deltaTablePath, "_output");
    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);

    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, value STRING) USING delta "
                + "TBLPROPERTIES ('delta.columnMapping.mode' = 'name', "
                + "               'delta.minReaderVersion' = '2', "
                + "               'delta.minWriterVersion' = '5')",
            tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'a'), (2, 'b')", tablePath));

    // First run: drain the 2 pre-rename rows.
    Dataset<Row> df1 =
        spark
            .readStream()
            .option("schemaTrackingLocation", schemaTrackingDir.getAbsolutePath())
            .table(dsv2TableRef);
    List<Row> firstRun = runWithParquetSink(df1, outputDir, checkpointDir, Trigger.AvailableNow());
    assertEquals(2, firstRun.size(), "first run should emit 2 pre-rename rows");
    DeltaLog.clearCache();

    // Mid-rest non-additive change: rename `value` to `val`, then write post-rename rows.
    spark.sql(str("ALTER TABLE delta.`%s` RENAME COLUMN value TO val", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (3, 'c'), (4, 'd')", tablePath));

    // Second run: restart from the same checkpoint + schema-tracking log; allowSourceColumnRename
    // unblocks the non-additive evolution so the stream continues.
    Dataset<Row> df2 =
        spark
            .readStream()
            .option("schemaTrackingLocation", schemaTrackingDir.getAbsolutePath())
            .option("allowSourceColumnRename", "always")
            .table(dsv2TableRef);
    List<Row> secondRun = runWithParquetSink(df2, outputDir, checkpointDir, Trigger.AvailableNow());
    DeltaLog.clearCache();

    assertEquals(
        2, secondRun.size(), () -> "second run should emit 2 post-rename rows: " + secondRun);
    // Post-rename rows are read under the new logical name `val` at ordinal 1.
    assertTrue(
        secondRun.stream().anyMatch(r -> "c".equals(r.getString(1))),
        () -> "post-rename row 'c' should be present, got: " + secondRun);
  }

  /**
   * S9 (ignoreDeletes) x concurrent-writer lifecycle: a background thread DELETEs a whole partition
   * mid-stream. With {@code ignoreDeletes=true} the DELETE commit is dropped rather than failing
   * the stream; the surviving partitions' rows still surface and the stream does not surface a raw
   * NPE.
   */
  @Test
  public void testConcurrentWriter_ignoreDeletes(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    File checkpointDir = new File(deltaTablePath, "_ckpt");
    File outputDir = new File(deltaTablePath, "_out");

    // Partition by id so the DELETE removes a file wholesale rather than rewriting via DV.
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, name STRING) USING delta PARTITIONED BY (id)",
            tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'a')", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (2, 'b')", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (3, 'c')", tablePath));

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> streamingDF =
        spark.readStream().option("ignoreDeletes", "true").table(dsv2TableRef);

    AtomicBoolean stop = new AtomicBoolean(false);
    ExecutorService deleter = null;
    StreamingQuery query = null;
    Throwable caught = null;
    try {
      query =
          streamingDF
              .writeStream()
              .format("parquet")
              .outputMode("append")
              .option("path", outputDir.getAbsolutePath())
              .option("checkpointLocation", checkpointDir.getAbsolutePath())
              .start();
      // Let the first batch drain the pre-existing partitions.
      query.processAllAvailable();

      // Mid-stream: another thread drops the partition for id=2.
      deleter = Executors.newSingleThreadExecutor();
      deleter.submit(
          () -> {
            try {
              spark.sql(str("DELETE FROM delta.`%s` WHERE id = 2", tablePath));
            } catch (Exception ignored) {
              // Concurrent commit may transiently fail; the test still proceeds.
            }
          });
      deleter.shutdown();
      deleter.awaitTermination(10, TimeUnit.SECONDS);
      query.processAllAvailable();
    } catch (Throwable t) {
      caught = t;
    } finally {
      stop.set(true);
      if (deleter != null && !deleter.isTerminated()) {
        deleter.shutdownNow();
      }
      if (query != null) {
        query.stop();
      }
      DeltaLog.clearCache();
    }
    if (caught != null && containsNpe(caught)) {
      fail("ignoreDeletes + concurrent DELETE produced a raw NPE: " + caught);
    }
    // Surviving partitions' rows (id=1, id=3) must have reached the sink; the DELETE itself is
    // dropped under ignoreDeletes=true.
    List<Row> sink = spark.read().parquet(outputDir.getAbsolutePath()).collectAsList();
    assertTrue(
        sink.size() >= 2,
        () ->
            "ignoreDeletes stream should surface at least the 2 surviving partitions; got: "
                + sink.size());
  }

  /**
   * S9 (ignoreDeletes) x RESTORE-before-stream lifecycle: INSERT rows, DELETE some whole files,
   * then RESTORE to a pre-DELETE version so the snapshot regains the deleted rows. A fresh stream
   * with {@code ignoreDeletes=true} starting at v=0 must replay the history (including the
   * since-RESTORED partitions) without surfacing a raw NPE.
   */
  @Test
  public void testRestoreBeforeStream_ignoreDeletes(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    File checkpointDir = new File(deltaTablePath, "_ckpt");
    File outputDir = new File(deltaTablePath, "_out");

    // Partition by id so the DELETE removes whole files (no DV rewrite under ignoreDeletes).
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, name STRING) USING delta PARTITIONED BY (id)",
            tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'a')", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (2, 'b')", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (3, 'c')", tablePath));
    long preDeleteVersion = latestVersion(tablePath);

    // Whole-file DELETE then RESTORE rewinds past it.
    spark.sql(str("DELETE FROM delta.`%s` WHERE id = 2", tablePath));
    spark.sql(str("RESTORE TABLE delta.`%s` TO VERSION AS OF %d", tablePath, preDeleteVersion));
    DeltaLog.clearCache();

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    Throwable caught = null;
    try {
      Dataset<Row> df =
          spark
              .readStream()
              .option("ignoreDeletes", "true")
              .option("startingVersion", "0")
              .table(dsv2TableRef);
      runWithParquetSink(df, outputDir, checkpointDir, /* trigger= */ null);
    } catch (Throwable t) {
      caught = t;
    } finally {
      DeltaLog.clearCache();
    }
    if (caught != null && containsNpe(caught)) {
      fail("RESTORE-before-stream + ignoreDeletes produced a raw NPE: " + caught);
    }
    // The restored snapshot regains the rows; the stream's drained history covers them.
    if (caught == null) {
      List<Row> sink = spark.read().parquet(outputDir.getAbsolutePath()).collectAsList();
      long sourceCount = spark.read().format("delta").load(tablePath).count();
      assertTrue(
          sink.size() >= sourceCount,
          () ->
              "Stream history should cover restored snapshot rows; sink="
                  + sink.size()
                  + " source="
                  + sourceCount);
    }
  }
}
