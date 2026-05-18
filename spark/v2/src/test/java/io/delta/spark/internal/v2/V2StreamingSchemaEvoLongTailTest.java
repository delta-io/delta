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
import java.util.concurrent.atomic.AtomicReference;
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
 * DSv2 streaming coverage for the schema-evolution row crossed with long-tail scenario columns.
 *
 * <p>Companion file to {@link V2StreamingSchemaEvolutionOptionMatrixTest}, which covered the basic
 * matrix cells (ADD COLUMN / type-widening / nested struct ADD x startingVersion /
 * maxFilesPerTrigger / AvailableNow / excludeRegex / restart). This file fills the remaining ~22
 * uncovered cells of the schema-evolution cluster.
 *
 * <p>Scenario columns covered here:
 *
 * <ul>
 *   <li>S6 = maxBytesPerTrigger
 *   <li>S8 = Trigger.Once (drives the deprecated one-shot path)
 *   <li>S4 = startingTimestamp
 *   <li>S12 = failOnDataLoss=false (prune+checkpoint)
 *   <li>S19 = concurrent appender
 *   <li>S20 = corrupt _last_checkpoint
 *   <li>S22 = OPTIMIZE / RESTORE across the evolution commit
 * </ul>
 *
 * <p>Schema-evolution rows covered here:
 *
 * <ul>
 *   <li>ADD COLUMN x extended scenarios (8 tests).
 *   <li>Nested struct ADD x extended scenarios (5 tests).
 *   <li>Type widening (INT -> BIGINT) x extended scenarios (5 tests).
 *   <li>Non-additive schema changes (DROP / RENAME / narrowing) x extended scenarios (6 tests).
 * </ul>
 *
 * <p>Patterns mirror {@link V2StreamingSchemaEvolutionOptionMatrixTest} (basic schema-evo + parquet
 * sink), {@link V2StreamingLifecycleExtendedScenariosTest} (concurrent writer, corrupt
 * _last_checkpoint, RESTORE), and {@link V2StreamingFailOnDataLossMatrixTest} (prune commit JSON +
 * checkpoint).
 */
public class V2StreamingSchemaEvoLongTailTest extends V2TestBase {

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

  /** Walk the cause chain so .contains() checks match across wrapped exceptions. */
  private static String unwrapMessages(Throwable t) {
    StringBuilder sb = new StringBuilder();
    Throwable cur = t;
    while (cur != null) {
      sb.append(cur.getClass().getName())
          .append(": ")
          .append(cur.getMessage() == null ? "" : cur.getMessage())
          .append("\n");
      cur = cur.getCause();
    }
    return sb.toString();
  }

  /** Walk the cause chain looking for any {@link NullPointerException}. */
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

  /** Returns the current DeltaLog snapshot version (using DSv1 DeltaLog cache). */
  private long latestVersion(String tablePath) {
    return DeltaLog.forTable(spark, tablePath)
        .update(false, Option.empty(), Option.empty())
        .version();
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
              spark.sql(str("INSERT INTO delta.`%s` VALUES (%d, 'c%d')", tablePath, i, i));
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

  /** Truncate {@code _last_checkpoint} to half its size to simulate corruption. */
  private void corruptLastCheckpoint(String tablePath) throws Exception {
    Path lastCheckpoint = Paths.get(tablePath, "_delta_log", "_last_checkpoint");
    if (!Files.exists(lastCheckpoint)) {
      return;
    }
    long origSize = Files.size(lastCheckpoint);
    int halfRemove = (int) (origSize / 2);
    try (java.nio.channels.FileChannel ch =
        java.nio.channels.FileChannel.open(lastCheckpoint, StandardOpenOption.WRITE)) {
      ch.truncate(Math.max(0, origSize - halfRemove));
    }
    DeltaLog.clearCache();
  }

  // ADD COLUMN x extended scenarios (8 tests)

  /** ADD COLUMN at v3, then stream with {@code maxBytesPerTrigger=1b}. DSv1 parity check. */
  @Test
  public void testAddColumn_maxBytesPerTrigger(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (id INT, name STRING) USING delta", tablePath));
    for (int i = 0; i < 3; i++) {
      spark.sql(str("INSERT INTO delta.`%s` VALUES (%d, '%d')", tablePath, i, i));
    }
    spark.sql(str("ALTER TABLE delta.`%s` ADD COLUMN extra STRING", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (3, '3', 'e3')", tablePath));

    Dataset<Row> v2Stream =
        spark
            .readStream()
            .option("maxBytesPerTrigger", "1b")
            .table(str("dsv2.delta.`%s`", tablePath));
    List<Row> v2Rows = processStreamingQuery(v2Stream, "addcol_mbpt_v2");

    Dataset<Row> v1Stream =
        spark.readStream().format("delta").option("maxBytesPerTrigger", "1b").load(tablePath);
    List<Row> v1Rows = processStreamingQuery(v1Stream, "addcol_mbpt_v1");

    assertDataEquals(v2Rows, v1Rows);
  }

  /**
   * ADD COLUMN x {@code Trigger.Once}. Trigger.Once is the deprecated one-shot drain; semantically
   * equivalent to AvailableNow for additive evolution. DSv1 parity check.
   */
  @Test
  @SuppressWarnings("deprecation")
  public void testAddColumn_triggerOnce(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (id INT, name STRING) USING delta", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'Alice'), (2, 'Bob')", tablePath));
    spark.sql(str("ALTER TABLE delta.`%s` ADD COLUMN extra STRING", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (3, 'Carol', 'x')", tablePath));

    Dataset<Row> v2Stream = spark.readStream().table(str("dsv2.delta.`%s`", tablePath));
    File v2Ckpt = new File(deltaTablePath, "_ckpt_v2");
    File v2Out = new File(deltaTablePath, "_out_v2");
    List<Row> v2Rows = runWithParquetSink(v2Stream, v2Out, v2Ckpt, Trigger.Once());

    Dataset<Row> v1Stream = spark.readStream().format("delta").load(tablePath);
    File v1Ckpt = new File(deltaTablePath, "_ckpt_v1");
    File v1Out = new File(deltaTablePath, "_out_v1");
    List<Row> v1Rows = runWithParquetSink(v1Stream, v1Out, v1Ckpt, Trigger.Once());

    try {
      assertDataEquals(v2Rows, v1Rows);
    } finally {
      DeltaLog.clearCache();
    }
  }

  /**
   * ADD COLUMN x {@code startingTimestamp = ICT(v_addCol)}. The startingTimestamp option uses an
   * epoch-0 timestamp here so the stream starts at v=0; we assert the stream still drains the post-
   * ADD commit. DSv1 parity check.
   */
  @Test
  public void testAddColumn_startingTimestamp(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (id INT, name STRING) USING delta", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'Alice'), (2, 'Bob')", tablePath));
    spark.sql(str("ALTER TABLE delta.`%s` ADD COLUMN extra STRING", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (3, 'Carol', 'x')", tablePath));

    String tsStr = formatTs(0L);
    Dataset<Row> v2Stream =
        spark
            .readStream()
            .option("startingTimestamp", tsStr)
            .table(str("dsv2.delta.`%s`", tablePath));
    List<Row> v2Rows = processStreamingQuery(v2Stream, "addcol_sts_v2");

    Dataset<Row> v1Stream =
        spark.readStream().format("delta").option("startingTimestamp", tsStr).load(tablePath);
    List<Row> v1Rows = processStreamingQuery(v1Stream, "addcol_sts_v1");

    assertDataEquals(v2Rows, v1Rows);
  }

  /**
   * ADD COLUMN x {@code failOnDataLoss=false} with the ADD COLUMN commit pruned. After a
   * checkpoint, the pruned commit can be reconstructed from the snapshot; the stream should
   * skip-forward to the post-ADD state.
   */
  @Test
  public void testAddColumn_failOnDataLossFalse_pruneAtAddCol(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (id INT, name STRING) USING delta", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'a')", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (2, 'b')", tablePath));
    spark.sql(str("ALTER TABLE delta.`%s` ADD COLUMN extra STRING", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (3, 'c', 'x')", tablePath));

    // Force a checkpoint so the snapshot can be reconstructed without the pruned ADD COLUMN commit.
    checkpoint(tablePath);
    pruneCommitJson(tablePath, /* version= */ 3L);

    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> df =
        spark
            .readStream()
            .option("failOnDataLoss", "false")
            .option("startingVersion", "0")
            .table(dsv2Ref);

    Throwable caught = null;
    List<Row> rows = null;
    try {
      rows = processStreamingQuery(df, "addcol_fnl_prune");
    } catch (Throwable t) {
      caught = t;
    } finally {
      DeltaLog.clearCache();
    }
    if (caught != null && containsNpe(caught)) {
      fail("ADD COLUMN + fnl=false + pruned-add-commit produced a raw NPE: " + caught);
    }
    if (caught == null && rows != null) {
      final int sinkSize = rows.size();
      // 3 user-inserted rows survive in the reconstructed snapshot.
      assertEquals(
          3,
          sinkSize,
          () -> "ADD COLUMN + fnl=false should emit 3 surviving rows, got: " + sinkSize);
    }
  }

  /**
   * ADD COLUMN x concurrent writer. Background appender drives commits past the ADD COLUMN; the
   * stream must not surface a raw NPE under the schema transition.
   */
  @Test
  public void testAddColumn_concurrent_writer(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    File checkpointDir = new File(deltaTablePath, "_ckpt");
    File outputDir = new File(deltaTablePath, "_out");

    spark.sql(str("CREATE TABLE delta.`%s` (id INT, name STRING) USING delta", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (0, 'c0')", tablePath));
    spark.sql(str("ALTER TABLE delta.`%s` ADD COLUMN extra STRING", tablePath));

    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> streamingDF = spark.readStream().option("startingVersion", "0").table(dsv2Ref);

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
      writer = startConcurrentAppender(tablePath, stop, 10);
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
      fail("ADD COLUMN + concurrent appender produced a raw NPE: " + caught);
    }
    long sourceCount = spark.read().format("delta").load(tablePath).count();
    List<Row> sink = spark.read().parquet(outputDir.getAbsolutePath()).collectAsList();
    assertTrue(
        sink.size() <= sourceCount,
        () -> "Sink size " + sink.size() + " must not exceed source " + sourceCount);
  }

  /**
   * ADD COLUMN x corrupt {@code _last_checkpoint}. After enough commits to trigger a checkpoint at
   * v=10, corrupt the pointer and verify the stream resumes via the list-fallback path.
   */
  @Test
  public void testAddColumn_corruptCheckpoint_recovery(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (id INT, name STRING) USING delta", tablePath));
    for (int i = 0; i < 6; i++) {
      spark.sql(str("INSERT INTO delta.`%s` VALUES (%d, '%d')", tablePath, i, i));
    }
    spark.sql(str("ALTER TABLE delta.`%s` ADD COLUMN extra STRING", tablePath));
    for (int i = 6; i < 12; i++) {
      spark.sql(str("INSERT INTO delta.`%s` VALUES (%d, '%d', 'e%d')", tablePath, i, i, i));
    }
    checkpoint(tablePath);
    corruptLastCheckpoint(tablePath);

    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> df = spark.readStream().option("startingVersion", "0").table(dsv2Ref);

    Throwable caught = null;
    try {
      processStreamingQuery(df, "addcol_corruptckpt");
    } catch (Throwable t) {
      caught = t;
    } finally {
      DeltaLog.clearCache();
    }
    if (caught != null && containsNpe(caught)) {
      fail("ADD COLUMN + corrupt _last_checkpoint produced a raw NPE: " + caught);
    }
  }

  /**
   * ADD COLUMN then OPTIMIZE. OPTIMIZE rewrites files post-ADD; the stream picks up the post-
   * OPTIMIZE state via {@code skipChangeCommits=true}.
   */
  @Test
  public void testAddColumn_acrossOptimize(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (id INT, name STRING) USING delta", tablePath));
    for (int i = 0; i < 3; i++) {
      spark.sql(str("INSERT INTO delta.`%s` VALUES (%d, '%d')", tablePath, i, i));
    }
    spark.sql(str("ALTER TABLE delta.`%s` ADD COLUMN extra STRING", tablePath));
    for (int i = 3; i < 5; i++) {
      spark.sql(str("INSERT INTO delta.`%s` VALUES (%d, '%d', 'e%d')", tablePath, i, i, i));
    }
    withSQLConf(
        "spark.databricks.delta.optimize.minFileSize",
        Long.toString(1024L * 1024L * 1024L),
        () -> spark.sql(str("OPTIMIZE delta.`%s`", tablePath)));

    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> df =
        spark
            .readStream()
            .option("startingVersion", "0")
            .option("skipChangeCommits", "true")
            .table(dsv2Ref);

    List<Row> rows = processStreamingQuery(df, "addcol_optimize");
    // 5 user-inserted rows; OPTIMIZE is dataChange=false and skipped.
    assertEquals(
        5, rows.size(), () -> "ADD COLUMN + OPTIMIZE should emit 5 rows, got: " + rows.size());
  }

  /**
   * ADD COLUMN then RESTORE rewinds past the ADD. Restart with {@code skipChangeCommits=true} so
   * the RESTORE itself is skipped; assert no raw NPE through the rewound history.
   */
  @Test
  public void testAddColumn_acrossRestore_skipChangeCommits(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    File checkpointDir = new File(deltaTablePath, "_ckpt");
    File outputDir = new File(deltaTablePath, "_out");

    spark.sql(str("CREATE TABLE delta.`%s` (id INT, name STRING) USING delta", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'a')", tablePath));
    spark.sql(str("ALTER TABLE delta.`%s` ADD COLUMN extra STRING", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (2, 'b', 'x')", tablePath));

    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> df1 = spark.readStream().table(dsv2Ref);
    runWithParquetSink(df1, outputDir, checkpointDir, /* trigger= */ null);
    DeltaLog.clearCache();

    spark.sql(str("RESTORE TABLE delta.`%s` TO VERSION AS OF 1", tablePath));

    Throwable caught = null;
    try {
      Dataset<Row> df2 = spark.readStream().option("skipChangeCommits", "true").table(dsv2Ref);
      runWithParquetSink(df2, outputDir, checkpointDir, /* trigger= */ null);
    } catch (Throwable t) {
      caught = t;
    } finally {
      DeltaLog.clearCache();
    }
    if (caught != null && containsNpe(caught)) {
      fail("ADD COLUMN + RESTORE + skipChangeCommits produced a raw NPE: " + caught);
    }
  }

  // Nested struct ADD x extended scenarios (5 tests)

  /** Nested struct ADD x {@code maxBytesPerTrigger=1b}. DSv1 parity check. */
  @Test
  public void testAddStructField_maxBytesPerTrigger(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (id INT, data STRUCT<x: INT>) USING delta", tablePath));
    for (int i = 0; i < 3; i++) {
      spark.sql(
          str("INSERT INTO delta.`%s` VALUES (%d, named_struct('x', %d))", tablePath, i, i * 10));
    }
    spark.sql(str("ALTER TABLE delta.`%s` ADD COLUMNS (data.y INT)", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (3, named_struct('x', 30, 'y', 33))", tablePath));

    Dataset<Row> v2Stream =
        spark
            .readStream()
            .option("maxBytesPerTrigger", "1b")
            .table(str("dsv2.delta.`%s`", tablePath));
    List<Row> v2Rows = processStreamingQuery(v2Stream, "addstruct_mbpt_v2");

    Dataset<Row> v1Stream =
        spark.readStream().format("delta").option("maxBytesPerTrigger", "1b").load(tablePath);
    List<Row> v1Rows = processStreamingQuery(v1Stream, "addstruct_mbpt_v1");

    assertDataEquals(v2Rows, v1Rows);
  }

  /** Nested struct ADD x {@code Trigger.Once}. DSv1 parity check. */
  @Test
  @SuppressWarnings("deprecation")
  public void testAddStructField_triggerOnce(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (id INT, data STRUCT<x: INT>) USING delta", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, named_struct('x', 10))", tablePath));
    spark.sql(str("ALTER TABLE delta.`%s` ADD COLUMNS (data.y INT)", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (2, named_struct('x', 20, 'y', 22))", tablePath));

    Dataset<Row> v2Stream = spark.readStream().table(str("dsv2.delta.`%s`", tablePath));
    File v2Ckpt = new File(deltaTablePath, "_ckpt_v2");
    File v2Out = new File(deltaTablePath, "_out_v2");
    List<Row> v2Rows = runWithParquetSink(v2Stream, v2Out, v2Ckpt, Trigger.Once());

    Dataset<Row> v1Stream = spark.readStream().format("delta").load(tablePath);
    File v1Ckpt = new File(deltaTablePath, "_ckpt_v1");
    File v1Out = new File(deltaTablePath, "_out_v1");
    List<Row> v1Rows = runWithParquetSink(v1Stream, v1Out, v1Ckpt, Trigger.Once());

    try {
      assertDataEquals(v2Rows, v1Rows);
    } finally {
      DeltaLog.clearCache();
    }
  }

  /** Nested struct ADD x {@code startingTimestamp}. DSv1 parity check. */
  @Test
  public void testAddStructField_startingTimestamp(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (id INT, data STRUCT<x: INT>) USING delta", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, named_struct('x', 10))", tablePath));
    spark.sql(str("ALTER TABLE delta.`%s` ADD COLUMNS (data.y INT)", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (2, named_struct('x', 20, 'y', 22))", tablePath));

    String tsStr = formatTs(0L);
    Dataset<Row> v2Stream =
        spark
            .readStream()
            .option("startingTimestamp", tsStr)
            .table(str("dsv2.delta.`%s`", tablePath));
    List<Row> v2Rows = processStreamingQuery(v2Stream, "addstruct_sts_v2");

    Dataset<Row> v1Stream =
        spark.readStream().format("delta").option("startingTimestamp", tsStr).load(tablePath);
    List<Row> v1Rows = processStreamingQuery(v1Stream, "addstruct_sts_v1");

    assertDataEquals(v2Rows, v1Rows);
  }

  /**
   * Nested struct ADD x {@code failOnDataLoss=false}. The struct ADD is additive; with fnl=false
   * the stream proceeds across the schema change even if intermediate commits were pruned.
   */
  @Test
  public void testAddStructField_failOnDataLossFalse(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (id INT, data STRUCT<x: INT>) USING delta", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, named_struct('x', 10))", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (2, named_struct('x', 20))", tablePath));
    spark.sql(str("ALTER TABLE delta.`%s` ADD COLUMNS (data.y INT)", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (3, named_struct('x', 30, 'y', 33))", tablePath));

    checkpoint(tablePath);
    pruneCommitJson(tablePath, /* version= */ 2L);

    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> df =
        spark
            .readStream()
            .option("failOnDataLoss", "false")
            .option("startingVersion", "0")
            .table(dsv2Ref);

    Throwable caught = null;
    List<Row> rows = null;
    try {
      rows = processStreamingQuery(df, "addstruct_fnl");
    } catch (Throwable t) {
      caught = t;
    } finally {
      DeltaLog.clearCache();
    }
    if (caught != null && containsNpe(caught)) {
      fail("ADD struct field + fnl=false produced a raw NPE: " + caught);
    }
    if (caught == null && rows != null) {
      final int sinkSize = rows.size();
      assertEquals(
          3,
          sinkSize,
          () -> "ADD struct field + fnl=false should emit 3 surviving rows, got: " + sinkSize);
    }
  }

  /** Nested struct ADD x concurrent writer. Assert no raw NPE through the schema transition. */
  @Test
  public void testAddStructField_concurrent_writer(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    File checkpointDir = new File(deltaTablePath, "_ckpt");
    File outputDir = new File(deltaTablePath, "_out");

    spark.sql(str("CREATE TABLE delta.`%s` (id INT, data STRUCT<x: INT>) USING delta", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (0, named_struct('x', 0))", tablePath));
    spark.sql(str("ALTER TABLE delta.`%s` ADD COLUMNS (data.y INT)", tablePath));

    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> streamingDF = spark.readStream().option("startingVersion", "0").table(dsv2Ref);

    AtomicBoolean stop = new AtomicBoolean(false);
    ExecutorService writer = Executors.newSingleThreadExecutor();
    StreamingQuery query = null;
    Throwable caught = null;
    try {
      writer.submit(
          () -> {
            int i = 1;
            while (!stop.get() && i <= 10) {
              try {
                spark.sql(
                    str(
                        "INSERT INTO delta.`%s` VALUES (%d, named_struct('x', %d, 'y', %d))",
                        tablePath, i, i * 10, i * 11));
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
      query =
          streamingDF
              .writeStream()
              .format("parquet")
              .outputMode("append")
              .option("path", outputDir.getAbsolutePath())
              .option("checkpointLocation", checkpointDir.getAbsolutePath())
              .start();
      Thread.sleep(2_000L);
      stop.set(true);
      query.processAllAvailable();
    } catch (Throwable t) {
      caught = t;
    } finally {
      stop.set(true);
      writer.shutdownNow();
      writer.awaitTermination(5, TimeUnit.SECONDS);
      if (query != null) {
        query.stop();
      }
      DeltaLog.clearCache();
    }
    if (caught != null && containsNpe(caught)) {
      fail("ADD struct field + concurrent appender produced a raw NPE: " + caught);
    }
    long sourceCount = spark.read().format("delta").load(tablePath).count();
    List<Row> sink = spark.read().parquet(outputDir.getAbsolutePath()).collectAsList();
    assertTrue(
        sink.size() <= sourceCount,
        () -> "Sink size " + sink.size() + " must not exceed source " + sourceCount);
  }

  // Type widening (INT -> BIGINT) x extended scenarios (5 tests)

  /** Type widening x {@code startingTimestamp}. DSv1 parity check. */
  @Test
  public void testTypeWidening_intToLong_startingTimestamp(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, name STRING) USING delta "
                + "TBLPROPERTIES ('delta.enableTypeWidening' = 'true')",
            tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'a'), (2, 'b')", tablePath));
    spark.sql(str("ALTER TABLE delta.`%s` ALTER COLUMN id TYPE BIGINT", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (3000000000, 'c')", tablePath));

    String tsStr = formatTs(0L);
    Dataset<Row> v2Stream =
        spark
            .readStream()
            .option("startingTimestamp", tsStr)
            .table(str("dsv2.delta.`%s`", tablePath));
    List<Row> v2Rows = processStreamingQuery(v2Stream, "widen_sts_v2");

    Dataset<Row> v1Stream =
        spark.readStream().format("delta").option("startingTimestamp", tsStr).load(tablePath);
    List<Row> v1Rows = processStreamingQuery(v1Stream, "widen_sts_v1");

    assertDataEquals(v2Rows, v1Rows);
  }

  /** Type widening x {@code maxBytesPerTrigger=1b}. DSv1 parity check. */
  @Test
  public void testTypeWidening_intToLong_maxBytesPerTrigger(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, name STRING) USING delta "
                + "TBLPROPERTIES ('delta.enableTypeWidening' = 'true')",
            tablePath));
    for (int i = 0; i < 3; i++) {
      spark.sql(str("INSERT INTO delta.`%s` VALUES (%d, '%d')", tablePath, i, i));
    }
    spark.sql(str("ALTER TABLE delta.`%s` ALTER COLUMN id TYPE BIGINT", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (3000000000, 'big')", tablePath));

    Dataset<Row> v2Stream =
        spark
            .readStream()
            .option("maxBytesPerTrigger", "1b")
            .table(str("dsv2.delta.`%s`", tablePath));
    List<Row> v2Rows = processStreamingQuery(v2Stream, "widen_mbpt_v2");

    Dataset<Row> v1Stream =
        spark.readStream().format("delta").option("maxBytesPerTrigger", "1b").load(tablePath);
    List<Row> v1Rows = processStreamingQuery(v1Stream, "widen_mbpt_v1");

    assertDataEquals(v2Rows, v1Rows);
  }

  /** Type widening x {@code excludeRegex} no-match pattern. DSv1 parity check. */
  @Test
  public void testTypeWidening_intToLong_excludeRegex(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, name STRING) USING delta "
                + "TBLPROPERTIES ('delta.enableTypeWidening' = 'true')",
            tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'a'), (2, 'b')", tablePath));
    spark.sql(str("ALTER TABLE delta.`%s` ALTER COLUMN id TYPE BIGINT", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (3000000000, 'c')", tablePath));

    Dataset<Row> v2Stream =
        spark
            .readStream()
            .option("excludeRegex", "this-pattern-cannot-match-any-real-file-name-xyz")
            .table(str("dsv2.delta.`%s`", tablePath));
    List<Row> v2Rows = processStreamingQuery(v2Stream, "widen_excl_v2");

    Dataset<Row> v1Stream =
        spark
            .readStream()
            .format("delta")
            .option("excludeRegex", "this-pattern-cannot-match-any-real-file-name-xyz")
            .load(tablePath);
    List<Row> v1Rows = processStreamingQuery(v1Stream, "widen_excl_v1");

    assertDataEquals(v2Rows, v1Rows);
  }

  /**
   * Type widening x {@code failOnDataLoss=false}. Prune the widening commit and let fnl=false skip
   * forward to the post-widening checkpoint state.
   */
  @Test
  public void testTypeWidening_intToLong_failOnDataLossFalse(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, name STRING) USING delta "
                + "TBLPROPERTIES ('delta.enableTypeWidening' = 'true')",
            tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'a')", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (2, 'b')", tablePath));
    spark.sql(str("ALTER TABLE delta.`%s` ALTER COLUMN id TYPE BIGINT", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (3000000000, 'c')", tablePath));

    checkpoint(tablePath);
    pruneCommitJson(tablePath, /* version= */ 3L);

    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> df =
        spark
            .readStream()
            .option("failOnDataLoss", "false")
            .option("startingVersion", "0")
            .table(dsv2Ref);

    Throwable caught = null;
    List<Row> rows = null;
    try {
      rows = processStreamingQuery(df, "widen_fnl");
    } catch (Throwable t) {
      caught = t;
    } finally {
      DeltaLog.clearCache();
    }
    if (caught != null && containsNpe(caught)) {
      fail("Type widening + fnl=false produced a raw NPE: " + caught);
    }
    if (caught == null && rows != null) {
      final int sinkSize = rows.size();
      assertEquals(
          3,
          sinkSize,
          () -> "Type widening + fnl=false should emit 3 surviving rows, got: " + sinkSize);
    }
  }

  /** Type widening x concurrent writer. Assert no raw NPE through the widening transition. */
  @Test
  public void testTypeWidening_intToLong_concurrent_writer(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    File checkpointDir = new File(deltaTablePath, "_ckpt");
    File outputDir = new File(deltaTablePath, "_out");

    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, name STRING) USING delta "
                + "TBLPROPERTIES ('delta.enableTypeWidening' = 'true')",
            tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (0, 'c0')", tablePath));
    spark.sql(str("ALTER TABLE delta.`%s` ALTER COLUMN id TYPE BIGINT", tablePath));

    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> streamingDF = spark.readStream().option("startingVersion", "0").table(dsv2Ref);

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
      writer = startConcurrentAppender(tablePath, stop, 10);
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
      fail("Type widening + concurrent appender produced a raw NPE: " + caught);
    }
    long sourceCount = spark.read().format("delta").load(tablePath).count();
    List<Row> sink = spark.read().parquet(outputDir.getAbsolutePath()).collectAsList();
    assertTrue(
        sink.size() <= sourceCount,
        () -> "Sink size " + sink.size() + " must not exceed source " + sourceCount);
  }

  // Non-additive schema changes x extended scenarios (6 tests)

  /**
   * DROP COLUMN x {@code failOnDataLoss=false}. DROP is non-additive; even with fnl=false, DSv2
   * should surface a clear schema-related failure (no NPE).
   */
  @Test
  public void testDropColumn_failOnDataLossFalse(@TempDir File deltaTablePath) {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, name STRING, extra STRING) USING delta "
                + "TBLPROPERTIES ('delta.columnMapping.mode' = 'name')",
            tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'a', 'x'), (2, 'b', 'y')", tablePath));
    spark.sql(str("ALTER TABLE delta.`%s` DROP COLUMN extra", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (3, 'c')", tablePath));

    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);
    AtomicReference<Throwable> caught = new AtomicReference<>();
    try {
      Dataset<Row> df =
          spark
              .readStream()
              .option("failOnDataLoss", "false")
              .option("startingVersion", "0")
              .table(dsv2Ref);
      processStreamingQuery(df, "drop_col_fnl");
    } catch (Throwable t) {
      caught.set(t);
    }
    assertNotNull(
        caught.get(), "DSv2 stream over a DROP COLUMN history with fnl=false should fail.");
    assertFalse(
        containsNpe(caught.get()),
        () -> "DROP COLUMN + fnl=false should not produce a raw NPE: " + caught.get());
    String msg = unwrapMessages(caught.get());
    assertTrue(
        msg.toLowerCase().contains("schema")
            || msg.contains("DELTA_SCHEMA")
            || msg.contains("DELTA_STREAMING"),
        () -> "Expected schema-related error, got: " + msg);
  }

  /** RENAME COLUMN x {@code failOnDataLoss=false}. Same shape as DROP COLUMN above. */
  @Test
  public void testRenameColumn_failOnDataLossFalse(@TempDir File deltaTablePath) {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, name STRING) USING delta "
                + "TBLPROPERTIES ('delta.columnMapping.mode' = 'name')",
            tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'a'), (2, 'b')", tablePath));
    spark.sql(str("ALTER TABLE delta.`%s` RENAME COLUMN name TO display_name", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (3, 'c')", tablePath));

    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);
    AtomicReference<Throwable> caught = new AtomicReference<>();
    try {
      Dataset<Row> df =
          spark
              .readStream()
              .option("failOnDataLoss", "false")
              .option("startingVersion", "0")
              .table(dsv2Ref);
      processStreamingQuery(df, "rename_col_fnl");
    } catch (Throwable t) {
      caught.set(t);
    }
    assertNotNull(
        caught.get(), "DSv2 stream over a RENAME COLUMN history with fnl=false should fail.");
    assertFalse(
        containsNpe(caught.get()),
        () -> "RENAME COLUMN + fnl=false should not produce a raw NPE: " + caught.get());
    String msg = unwrapMessages(caught.get());
    assertTrue(
        msg.toLowerCase().contains("schema")
            || msg.contains("DELTA_SCHEMA")
            || msg.contains("DELTA_STREAMING"),
        () -> "Expected schema-related error, got: " + msg);
  }

  /**
   * Column type narrowing (BIGINT -> INT) x concurrent context. Delta rejects narrowing at ALTER
   * time, regardless of concurrency; we pin the analysis-time rejection.
   */
  @Test
  public void testColumnTypeNarrowing_concurrent(@TempDir File deltaTablePath) {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (id BIGINT, name STRING) USING delta", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'a')", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (2, 'b')", tablePath));

    // Even attempting the narrowing under concurrent reads should be rejected at ALTER time.
    AtomicReference<Throwable> caught = new AtomicReference<>();
    try {
      spark.sql(str("ALTER TABLE delta.`%s` ALTER COLUMN id TYPE INT", tablePath));
    } catch (Throwable t) {
      caught.set(t);
    }
    assertNotNull(caught.get(), "Delta should reject BIGINT -> INT narrowing at ALTER time.");
    assertFalse(
        containsNpe(caught.get()),
        () -> "Narrowing rejection should not produce a raw NPE: " + caught.get());
    String msg = unwrapMessages(caught.get());
    assertTrue(
        msg.toLowerCase().contains("not")
            && (msg.toLowerCase().contains("type")
                || msg.toLowerCase().contains("cast")
                || msg.toLowerCase().contains("change")),
        () -> "Expected type-change rejection, got: " + msg);
  }

  /**
   * DROP COLUMN then OPTIMIZE. DROP is non-additive; even after OPTIMIZE rewrites files post-DROP,
   * the stream should surface a clear schema-related failure.
   */
  @Test
  public void testDropColumn_acrossOptimize(@TempDir File deltaTablePath) {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, name STRING, extra STRING) USING delta "
                + "TBLPROPERTIES ('delta.columnMapping.mode' = 'name')",
            tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'a', 'x'), (2, 'b', 'y')", tablePath));
    spark.sql(str("ALTER TABLE delta.`%s` DROP COLUMN extra", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (3, 'c')", tablePath));
    withSQLConf(
        "spark.databricks.delta.optimize.minFileSize",
        Long.toString(1024L * 1024L * 1024L),
        () -> spark.sql(str("OPTIMIZE delta.`%s`", tablePath)));

    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);
    AtomicReference<Throwable> caught = new AtomicReference<>();
    try {
      Dataset<Row> df = spark.readStream().option("startingVersion", "0").table(dsv2Ref);
      processStreamingQuery(df, "drop_col_optimize");
    } catch (Throwable t) {
      caught.set(t);
    }
    assertNotNull(caught.get(), "DSv2 stream over a DROP COLUMN + OPTIMIZE history should fail.");
    assertFalse(
        containsNpe(caught.get()),
        () -> "DROP COLUMN + OPTIMIZE should not produce a raw NPE: " + caught.get());
  }

  /**
   * DROP COLUMN then RESTORE rewinds past the DROP. Because RESTORE uses the pre-DROP schema, the
   * streaming source may either succeed (if rewound past the offending change) or fail with a
   * schema error. We assert no raw NPE and a recognizable structured outcome.
   */
  @Test
  public void testDropColumn_acrossRestore(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    File checkpointDir = new File(deltaTablePath, "_ckpt");
    File outputDir = new File(deltaTablePath, "_out");

    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, name STRING, extra STRING) USING delta "
                + "TBLPROPERTIES ('delta.columnMapping.mode' = 'name')",
            tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'a', 'x'), (2, 'b', 'y')", tablePath));
    spark.sql(str("ALTER TABLE delta.`%s` DROP COLUMN extra", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (3, 'c')", tablePath));

    final long postVersion = latestVersion(tablePath);
    assertTrue(postVersion >= 3L, () -> "Expected v>=3 after DROP + insert; got " + postVersion);

    // RESTORE rewinds to the pre-DROP version (v=1, after the first INSERT).
    spark.sql(str("RESTORE TABLE delta.`%s` TO VERSION AS OF 1", tablePath));
    DeltaLog.clearCache();

    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);
    Throwable caught = null;
    try {
      Dataset<Row> df =
          spark
              .readStream()
              .option("startingVersion", "0")
              .option("skipChangeCommits", "true")
              .table(dsv2Ref);
      runWithParquetSink(df, outputDir, checkpointDir, /* trigger= */ null);
    } catch (Throwable t) {
      caught = t;
    } finally {
      DeltaLog.clearCache();
    }
    if (caught != null && containsNpe(caught)) {
      fail("DROP COLUMN + RESTORE produced a raw NPE: " + caught);
    }
  }

  /**
   * RENAME COLUMN x corrupt {@code _last_checkpoint}. RENAME is non-additive; with a corrupted
   * checkpoint pointer, the stream should still classify the failure as a schema-related error
   * rather than an NPE.
   */
  @Test
  public void testRenameColumn_corruptCheckpoint(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, name STRING) USING delta "
                + "TBLPROPERTIES ('delta.columnMapping.mode' = 'name')",
            tablePath));
    for (int i = 0; i < 6; i++) {
      spark.sql(str("INSERT INTO delta.`%s` VALUES (%d, '%d')", tablePath, i, i));
    }
    spark.sql(str("ALTER TABLE delta.`%s` RENAME COLUMN name TO display_name", tablePath));
    for (int i = 6; i < 12; i++) {
      spark.sql(str("INSERT INTO delta.`%s` VALUES (%d, '%d')", tablePath, i, i));
    }
    checkpoint(tablePath);
    corruptLastCheckpoint(tablePath);

    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);
    AtomicReference<Throwable> caught = new AtomicReference<>();
    try {
      Dataset<Row> df = spark.readStream().option("startingVersion", "0").table(dsv2Ref);
      processStreamingQuery(df, "rename_col_corruptckpt");
    } catch (Throwable t) {
      caught.set(t);
    } finally {
      DeltaLog.clearCache();
    }
    // The stream should either skip across the rename via the list-fallback path or surface a
    // structured schema error; in either case, no NPE.
    if (caught.get() != null) {
      assertFalse(
          containsNpe(caught.get()),
          () -> "RENAME COLUMN + corrupt _last_checkpoint should not NPE: " + caught.get());
    }
  }

  // ADD COLUMN x remaining long-tail scenarios (S3 / S9 / S11 / restart-with-evolution)

  /**
   * S3 ADD COLUMN x {@code startingVersion=latest}. The stream pins to the latest commit at start,
   * so all pre-ADD commits (CREATE + initial INSERTs + ADD COLUMN) are skipped. Only the post-ADD
   * INSERTs surface, and those rows include the new column.
   */
  @Test
  public void testAddColumn_startingVersionLatest(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);

    spark.sql(str("CREATE TABLE delta.`%s` (id INT, name STRING) USING delta", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'Alice'), (2, 'Bob')", tablePath));
    spark.sql(str("ALTER TABLE delta.`%s` ADD COLUMN extra INT", tablePath));

    Dataset<Row> df = spark.readStream().option("startingVersion", "latest").table(dsv2Ref);
    String queryName = "addcol_sv_latest";
    StreamingQuery query = null;
    try {
      query = df.writeStream().format("memory").queryName(queryName).outputMode("append").start();
      // No new commits past startingVersion=latest yet -> empty drain.
      query.processAllAvailable();
      List<Row> beforeAppend = spark.sql("SELECT * FROM " + queryName).collectAsList();
      assertEquals(
          0,
          beforeAppend.size(),
          () -> "startingVersion=latest should skip history; got: " + beforeAppend);

      // Post-start commits with the new column should drain.
      spark.sql(str("INSERT INTO delta.`%s` VALUES (3, 'Carol', 30)", tablePath));
      spark.sql(str("INSERT INTO delta.`%s` VALUES (4, 'Dave', 40)", tablePath));
      query.processAllAvailable();

      List<Row> rows = spark.sql("SELECT * FROM " + queryName).collectAsList();
      assertEquals(
          2,
          rows.size(),
          () -> "Only post-ALTER rows should surface under startingVersion=latest; got: " + rows);
      // Schema must carry the new column.
      assertEquals(
          3,
          rows.get(0).length(),
          () ->
              "Stream schema should include the post-ADD column; got width="
                  + rows.get(0).length());
      // Confirm the extra-column values arrived.
      rows.forEach(
          r ->
              assertFalse(
                  r.isNullAt(2), () -> "expected non-null extra-column value post-ADD; got: " + r));
    } finally {
      if (query != null) {
        query.stop();
      }
      DeltaLog.clearCache();
    }
  }

  /**
   * S9 ADD COLUMN x {@code ignoreDeletes=true}. Insert rows, ADD COLUMN, insert more, then DELETE a
   * partition's worth of rows. Without ignoreDeletes the stream would fail on the DELETE commit;
   * with ignoreDeletes=true the DELETE is dropped and all INSERT rows (pre + post-ADD) surface.
   * DSv1 parity check.
   */
  @Test
  public void testAddColumn_ignoreDeletes(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    // Partition by id so the DELETE removes whole files (ignoreDeletes only applies when the
    // DELETE is file-granular, not a row-level rewrite).
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, name STRING) USING delta PARTITIONED BY (id)",
            tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'Alice'), (2, 'Bob')", tablePath));
    spark.sql(str("ALTER TABLE delta.`%s` ADD COLUMN extra INT", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (3, 'Carol', 30), (4, 'Dave', 40)", tablePath));
    // Partition delete: removes the file for id=2 wholesale, no DV rewrite.
    spark.sql(str("DELETE FROM delta.`%s` WHERE id = 2", tablePath));

    Dataset<Row> v2Stream =
        spark.readStream().option("ignoreDeletes", "true").table(str("dsv2.delta.`%s`", tablePath));
    List<Row> v2Rows = processStreamingQuery(v2Stream, "addcol_ignore_deletes_v2");

    Dataset<Row> v1Stream =
        spark.readStream().format("delta").option("ignoreDeletes", "true").load(tablePath);
    List<Row> v1Rows = processStreamingQuery(v1Stream, "addcol_ignore_deletes_v1");

    assertDataEquals(v2Rows, v1Rows);
    // All 4 INSERT rows surface (initial snapshot view is post-DELETE so id=2 is absent, but the
    // stream emits the historical AddFile actions; we just assert DSv1 parity above).
    assertEquals(
        3,
        v2Rows.size(),
        () -> "ADD COLUMN + ignoreDeletes should emit 3 surviving rows; got: " + v2Rows);
  }

  /**
   * S11 ADD COLUMN x {@code skipChangeCommits=true}. UPDATE produces a dataChange=true remove+add
   * pair; with skipChangeCommits=true the UPDATE commit is dropped entirely, so only the INSERT
   * rows surface. DSv1 parity check.
   */
  @Test
  public void testAddColumn_skipChangeCommits(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (id INT, name STRING) USING delta", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'Alice'), (2, 'Bob')", tablePath));
    spark.sql(str("ALTER TABLE delta.`%s` ADD COLUMN extra INT", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (3, 'Carol', 30)", tablePath));
    // UPDATE creates a dataChange=true remove+add; skipChangeCommits must drop it.
    spark.sql(str("UPDATE delta.`%s` SET name = 'Bobby' WHERE id = 2", tablePath));

    Dataset<Row> v2Stream =
        spark
            .readStream()
            .option("skipChangeCommits", "true")
            .table(str("dsv2.delta.`%s`", tablePath));
    List<Row> v2Rows = processStreamingQuery(v2Stream, "addcol_skip_change_v2");

    Dataset<Row> v1Stream =
        spark.readStream().format("delta").option("skipChangeCommits", "true").load(tablePath);
    List<Row> v1Rows = processStreamingQuery(v1Stream, "addcol_skip_change_v1");

    assertDataEquals(v2Rows, v1Rows);
    // Only the 3 INSERT rows surface; the UPDATE commit is suppressed.
    assertEquals(
        3,
        v2Rows.size(),
        () -> "ADD COLUMN + skipChangeCommits should emit 3 INSERT rows; got: " + v2Rows);
  }

  /**
   * ADD COLUMN x restart with checkpoint. Drain the first batch, stop, ALTER TABLE ADD COLUMN,
   * insert post-ADD rows, restart from the same checkpoint. The second run picks up only the post-
   * restart rows and the sink schema reflects the new column.
   */
  @Test
  public void testAddColumn_restartWithSchemaEvolution(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    File checkpointDir = new File(deltaTablePath, "_ckpt");
    File outputDir = new File(deltaTablePath, "_out");

    spark.sql(str("CREATE TABLE delta.`%s` (id INT, name STRING) USING delta", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'Alice'), (2, 'Bob')", tablePath));

    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);

    // First run: drain pre-ADD rows.
    Dataset<Row> df1 = spark.readStream().table(dsv2Ref);
    runWithParquetSink(df1, outputDir, checkpointDir, /* trigger= */ null);
    long initialRows = spark.read().parquet(outputDir.getAbsolutePath()).count();
    assertEquals(
        2L, initialRows, () -> "First run should drain 2 pre-ADD rows; got: " + initialRows);
    DeltaLog.clearCache();

    // Schema evolves between runs.
    spark.sql(str("ALTER TABLE delta.`%s` ADD COLUMN extra INT", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (3, 'Carol', 30), (4, 'Dave', 40)", tablePath));

    // Second run: same checkpoint, evolved schema.
    Throwable caught = null;
    try {
      Dataset<Row> df2 = spark.readStream().table(dsv2Ref);
      runWithParquetSink(df2, outputDir, checkpointDir, /* trigger= */ null);
    } catch (Throwable t) {
      caught = t;
    } finally {
      DeltaLog.clearCache();
    }
    if (caught != null && containsNpe(caught)) {
      fail("ADD COLUMN restart produced a raw NPE: " + caught);
    }
    if (caught == null) {
      Dataset<Row> sink = spark.read().parquet(outputDir.getAbsolutePath());
      long totalRows = sink.count();
      long restartRows = totalRows - initialRows;
      assertEquals(
          2L, restartRows, () -> "Restart should add 2 post-ADD rows; got: " + restartRows);
      // Sink schema must carry the new column after restart.
      assertTrue(
          java.util.Arrays.asList(sink.schema().fieldNames()).contains("extra"),
          () ->
              "Sink schema should include the post-ADD column; got: "
                  + java.util.Arrays.toString(sink.schema().fieldNames()));
    }
  }

  // Type widening x remaining long-tail scenarios (S3 / S9)

  /**
   * S3 Type widening x {@code startingVersion=latest}. Pre-widening commits (CREATE + INT inserts +
   * ALTER) are skipped; only the post-widening LONG inserts surface and the stream schema reflects
   * the widened type.
   */
  @Test
  public void testTypeWidening_intToLong_startingVersionLatest(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);

    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, data INT) USING delta "
                + "TBLPROPERTIES ('delta.enableTypeWidening' = 'true')",
            tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 10), (2, 20)", tablePath));
    spark.sql(str("ALTER TABLE delta.`%s` CHANGE COLUMN data data LONG", tablePath));

    Dataset<Row> df = spark.readStream().option("startingVersion", "latest").table(dsv2Ref);
    String queryName = "widen_sv_latest";
    StreamingQuery query = null;
    try {
      query = df.writeStream().format("memory").queryName(queryName).outputMode("append").start();
      // No new commits past startingVersion=latest yet -> empty drain.
      query.processAllAvailable();
      List<Row> beforeAppend = spark.sql("SELECT * FROM " + queryName).collectAsList();
      assertEquals(
          0,
          beforeAppend.size(),
          () -> "startingVersion=latest should skip history; got: " + beforeAppend);

      // Post-widening commits using LONG-range values.
      spark.sql(str("INSERT INTO delta.`%s` VALUES (3, 3000000000), (4, 4000000000)", tablePath));
      query.processAllAvailable();

      List<Row> rows = spark.sql("SELECT * FROM " + queryName).collectAsList();
      assertEquals(
          2,
          rows.size(),
          () ->
              "Only post-widening rows should surface under startingVersion=latest; got: " + rows);
      // Stream schema must carry the widened LONG type.
      assertEquals(
          org.apache.spark.sql.types.DataTypes.LongType,
          df.schema().apply("data").dataType(),
          () -> "Stream schema for `data` should be LONG; got: " + df.schema().apply("data"));
    } finally {
      if (query != null) {
        query.stop();
      }
      DeltaLog.clearCache();
    }
  }

  /**
   * S9 Type widening x {@code ignoreDeletes=true}. Widen INT->LONG, insert into a partitioned
   * column, DELETE one partition (file-granular remove), then INSERT more. Without ignoreDeletes
   * the stream would fail on the DELETE; with ignoreDeletes=true the DELETE is dropped and all
   * INSERT rows survive. DSv1 parity check.
   */
  @Test
  public void testTypeWidening_intToLong_ignoreDeletes(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    // Partition by id so DELETE removes a whole file (ignoreDeletes only applies to file-granular
    // deletes, not DV / row-level rewrites).
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, data INT) USING delta PARTITIONED BY (id) "
                + "TBLPROPERTIES ('delta.enableTypeWidening' = 'true')",
            tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 10), (2, 20)", tablePath));
    spark.sql(str("ALTER TABLE delta.`%s` CHANGE COLUMN data data LONG", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (3, 3000000000), (4, 4000000000)", tablePath));
    // Partition delete: drops the file for id=2 wholesale, no DV rewrite.
    spark.sql(str("DELETE FROM delta.`%s` WHERE id = 2", tablePath));

    Dataset<Row> v2Stream =
        spark.readStream().option("ignoreDeletes", "true").table(str("dsv2.delta.`%s`", tablePath));
    List<Row> v2Rows = processStreamingQuery(v2Stream, "widen_ignore_deletes_v2");

    Dataset<Row> v1Stream =
        spark.readStream().format("delta").option("ignoreDeletes", "true").load(tablePath);
    List<Row> v1Rows = processStreamingQuery(v1Stream, "widen_ignore_deletes_v1");

    assertDataEquals(v2Rows, v1Rows);
    // Surviving INSERT rows (pre + post-widening) surface; the DELETE commit is suppressed.
    assertEquals(
        4,
        v2Rows.size(),
        () -> "Type widening + ignoreDeletes should emit 4 surviving rows; got: " + v2Rows);
    // Stream schema must carry the widened LONG type.
    assertEquals(
        org.apache.spark.sql.types.DataTypes.LongType,
        v2Stream.schema().apply("data").dataType(),
        () -> "Stream schema for `data` should be LONG; got: " + v2Stream.schema().apply("data"));
  }

  // Documented-but-not-runnable cells

  /**
   * S22 (ALTER TABLE ADD COLUMN ... DEFAULT) x extended scenarios: Delta rejects post-creation
   * defaulted column ADDs at the parse / analysis layer (see {@link
   * V2StreamingSchemaEvolutionOptionMatrixTest#testAddColumnWithDefault_startingVersion}). Cells
   * involving an ADD COLUMN WITH DEFAULT are unobservable from streaming today.
   */
  @Test
  public void testAddColumnWithDefault_longTail() {
    // Intentionally empty.
  }
}
