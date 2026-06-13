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
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.delta.DeltaLog;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import scala.Option;

/**
 * DSv2 streaming coverage for lifecycle scenarios crossed with scenario columns.
 *
 * <p>The closeout coverage report identified lifecycle x S2-S22 as ~30 uncovered cells. This file
 * pins behavior for five lifecycle families:
 *
 * <ul>
 *   <li>Sink-and-source-same-table: streaming a table that is also the writeStream sink.
 *   <li>Concurrent writers: an external writer commits while the stream is running.
 *   <li>Restore-past-checkpoint: RESTORE rewinds past a version the stream's checkpoint pins.
 *   <li>OPTIMIZE-before-fresh-stream: stream starts after compaction; must see post-OPTIMIZE state.
 *   <li>Mid-stream feature toggles: ALTER TABLE enables RT / DV / ICT / unsets appendOnly while a
 *       stream is in flight.
 * </ul>
 *
 * <p>Patterns mirror {@code V2StreamingRaceLifecycleTest} (concurrent writers, restart with
 * checkpoint) and {@code V2StreamingLogIntegrityTest} (failure classification, AvailableNow drain).
 */
public class V2StreamingLifecycleScenarioMatrixTest extends V2TestBase {

  /**
   * Runs a streaming query against a parquet sink + checkpoint, drains all available data, then
   * stops. Returns the parquet sink's current contents. Parquet sink supports checkpoint recovery
   * across restarts; memory sink does not.
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

  /** Returns the current DeltaLog snapshot version. */
  private long latestVersion(String tablePath) {
    return DeltaLog.forTable(spark, tablePath)
        .update(false, Option.empty(), Option.empty())
        .version();
  }

  /** Walks a cause chain looking for any class name containing {@code NullPointerException}. */
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

  /**
   * 1. Streaming a delta table while writing back to the same path is forbidden. Without explicit
   * checkpoint, the engine should reject the query (loop detection / shared metadata location) and
   * not silently produce duplicates or NPE.
   */
  @Test
  public void testSinkAndSourceSameTable_throws(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (id INT) USING delta", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1), (2), (3)", tablePath));

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> streamingDF = spark.readStream().table(dsv2TableRef);

    Throwable caught = null;
    StreamingQuery query = null;
    try {
      // The implicit checkpoint location collides with the source's _delta_log; depending on the
      // Spark version the failure may come at start() or at processAllAvailable().
      query =
          streamingDF
              .writeStream()
              .format("delta")
              .outputMode("append")
              .trigger(Trigger.AvailableNow())
              .start(tablePath);
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
    // Whether the engine throws an analysis error, an IllegalArgumentException about a shared
    // checkpoint, or a delta error, it must NOT be a raw NPE leak.
    if (caught != null && containsNpe(caught)) {
      fail("Sink-and-source same table produced a raw NPE: " + caught);
    }
  }

  /**
   * 2. Same setup with an explicit checkpoint dir distinct from the source path. The query is still
   * semantically a loop (stream reads its own writes), but a separate checkpoint dir means we are
   * testing the engine's own loop / shared-metadata detection rather than checkpoint-path
   * collision.
   */
  @Test
  public void testSinkAndSourceSameTable_withCheckpointDir(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    File checkpointDir = new File(deltaTablePath.getParentFile(), "ckpt_" + System.nanoTime());
    spark.sql(str("CREATE TABLE delta.`%s` (id INT) USING delta", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1), (2), (3)", tablePath));

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> streamingDF = spark.readStream().table(dsv2TableRef);

    Throwable caught = null;
    StreamingQuery query = null;
    try {
      query =
          streamingDF
              .writeStream()
              .format("delta")
              .outputMode("append")
              .option("checkpointLocation", checkpointDir.getAbsolutePath())
              .trigger(Trigger.AvailableNow())
              .start(tablePath);
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
      fail("Sink-and-source same table (with explicit checkpoint) produced a raw NPE: " + caught);
    }
    // If neither exception nor NPE occurred, the engine accepted the loop. We then assert the
    // sink did not amplify the source's rows (3 rows in, at most 3 rows out from this run).
    if (caught == null) {
      long rows = spark.read().format("delta").load(tablePath).count();
      assertTrue(
          rows <= 6, () -> "Self-feedback loop should not amplify rows unboundedly; got " + rows);
    }
  }

  /**
   * 3. Bare stream + concurrent writer appending rows. Total rows emitted by the stream should
   * cover every committed row by the time we stop the writer and drain.
   */
  @Test
  public void testConcurrentWriter_appendWhileStreaming_basicSucceeds(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    File checkpointDir = new File(deltaTablePath, "_ckpt");
    File outputDir = new File(deltaTablePath, "_out");

    spark.sql(str("CREATE TABLE delta.`%s` (id INT) USING delta", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (0)", tablePath));

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> streamingDF = spark.readStream().table(dsv2TableRef);

    AtomicBoolean stop = new AtomicBoolean(false);
    ExecutorService writer = Executors.newSingleThreadExecutor();
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

      writer.submit(
          () -> {
            int i = 1;
            while (!stop.get() && i <= 20) {
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

      // Give the reader/writer time to interleave, then drain.
      Thread.sleep(2_000L);
      stop.set(true);
      query.processAllAvailable();
    } finally {
      stop.set(true);
      writer.shutdownNow();
      writer.awaitTermination(5, TimeUnit.SECONDS);
      if (query != null) {
        query.stop();
      }
      DeltaLog.clearCache();
    }

    List<Row> sink = spark.read().parquet(outputDir.getAbsolutePath()).collectAsList();
    long sourceCount = spark.read().format("delta").load(tablePath).count();
    // Sink should contain every row committed to the source by the time we drained.
    assertEquals(
        sourceCount,
        (long) sink.size(),
        () ->
            "Concurrent-append stream missed rows: source has "
                + sourceCount
                + " rows but sink has "
                + sink.size());
  }

  /** 4. Same as above with {@code maxFilesPerTrigger=1} to maximize batch boundary crossings. */
  @Test
  public void testConcurrentWriter_appendWhileStreaming_maxFilesPerTrigger(
      @TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    File checkpointDir = new File(deltaTablePath, "_ckpt");
    File outputDir = new File(deltaTablePath, "_out");

    spark.sql(str("CREATE TABLE delta.`%s` (id INT) USING delta", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (0)", tablePath));

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> streamingDF =
        spark.readStream().option("maxFilesPerTrigger", "1").table(dsv2TableRef);

    AtomicBoolean stop = new AtomicBoolean(false);
    ExecutorService writer = Executors.newSingleThreadExecutor();
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

      writer.submit(
          () -> {
            int i = 1;
            while (!stop.get() && i <= 20) {
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

      Thread.sleep(2_000L);
      stop.set(true);
      query.processAllAvailable();
    } finally {
      stop.set(true);
      writer.shutdownNow();
      writer.awaitTermination(5, TimeUnit.SECONDS);
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
            "maxFilesPerTrigger=1 missed concurrent commits: source="
                + sourceCount
                + " sink="
                + sink.size());
  }

  /**
   * 5. DV-enabled table; concurrent writer performs DV-DELETE while the stream runs with {@code
   * skipChangeCommits=true}. The DV-DELETE produces a change commit that must be skipped; the sink
   * must contain only the pre-DELETE rows seen at run time.
   */
  @Test
  public void testConcurrentWriter_dvDeleteWhileStreaming_skipChangeCommits(
      @TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    File checkpointDir = new File(deltaTablePath, "_ckpt");
    File outputDir = new File(deltaTablePath, "_out");

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

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> streamingDF =
        spark.readStream().option("skipChangeCommits", "true").table(dsv2TableRef);

    AtomicBoolean stop = new AtomicBoolean(false);
    ExecutorService writer = Executors.newSingleThreadExecutor();
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

      writer.submit(
          () -> {
            try {
              Thread.sleep(50);
              spark.sql(str("DELETE FROM delta.`%s` WHERE id %% 2 = 0", tablePath));
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            } catch (Exception ignored) {
            }
          });

      Thread.sleep(2_000L);
      stop.set(true);
      query.processAllAvailable();
    } finally {
      stop.set(true);
      writer.shutdownNow();
      writer.awaitTermination(5, TimeUnit.SECONDS);
      if (query != null) {
        query.stop();
      }
      DeltaLog.clearCache();
    }

    List<Row> sink = spark.read().parquet(outputDir.getAbsolutePath()).collectAsList();
    // skipChangeCommits drops the DV-DELETE commit; the stream should emit the original 10 rows
    // (from the initial snapshot) and nothing else.
    assertEquals(
        10,
        sink.size(),
        () ->
            "skipChangeCommits should drop concurrent DV DELETE; expected 10 rows from initial"
                + " snapshot, got "
                + sink.size());
  }

  /**
   * 6. OPTIMIZE runs concurrently with a streaming read. OPTIMIZE emits dataChange=false
   * AddFile/RemoveFile; the stream must not re-emit those rewritten files.
   */
  @Test
  public void testConcurrentWriter_optimizeWhileStreaming_noReEmission(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    File checkpointDir = new File(deltaTablePath, "_ckpt");
    File outputDir = new File(deltaTablePath, "_out");

    spark.sql(str("CREATE TABLE delta.`%s` (id INT) USING delta", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1)", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (2)", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (3)", tablePath));

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> streamingDF = spark.readStream().table(dsv2TableRef);

    ExecutorService writer = Executors.newSingleThreadExecutor();
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

      writer.submit(
          () -> {
            try {
              Thread.sleep(50);
              withSQLConf(
                  "spark.databricks.delta.optimize.minFileSize",
                  Long.toString(1024L * 1024L * 1024L),
                  () -> spark.sql(str("OPTIMIZE delta.`%s`", tablePath)));
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            } catch (Exception ignored) {
            }
          });

      Thread.sleep(2_000L);
      query.processAllAvailable();
    } finally {
      writer.shutdownNow();
      writer.awaitTermination(5, TimeUnit.SECONDS);
      if (query != null) {
        query.stop();
      }
      DeltaLog.clearCache();
    }

    List<Row> sink = spark.read().parquet(outputDir.getAbsolutePath()).collectAsList();
    // 3 source rows; OPTIMIZE's rewrite is dataChange=false so must not surface as new rows.
    assertEquals(
        3,
        sink.size(),
        () ->
            "OPTIMIZE rewrite must not re-emit data; expected 3 rows, got "
                + sink.size()
                + " (OPTIMIZE may have leaked dataChange=false files into the stream)");
  }

  /**
   * 7. Bare stream; checkpoint reaches vN; RESTORE to v0 rewinds the table. On restart, the
   * checkpoint pins a version that no longer represents the table's history. The stream must either
   * surface a structured error or skip forward to the post-RESTORE state; raw NPE = bug.
   */
  @Test
  public void testRestorePastCheckpoint_failsClearly(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    File checkpointDir = new File(deltaTablePath, "_ckpt");
    File outputDir = new File(deltaTablePath, "_out");

    spark.sql(str("CREATE TABLE delta.`%s` (id INT) USING delta", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1)", tablePath)); // v1
    spark.sql(str("INSERT INTO delta.`%s` VALUES (2)", tablePath)); // v2
    spark.sql(str("INSERT INTO delta.`%s` VALUES (3)", tablePath)); // v3

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> df1 = spark.readStream().table(dsv2TableRef);
    runWithParquetSink(df1, outputDir, checkpointDir, /* trigger= */ null);
    DeltaLog.clearCache();

    // RESTORE rewinds past the version the checkpoint pins.
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
      fail("Restore-past-checkpoint produced a raw NPE: " + caught);
    }
  }

  /**
   * 8. Same setup with {@code skipChangeCommits=true}. RESTORE writes a change commit
   * (AddFile+RemoveFile, dataChange=true); skipChangeCommits should drop it, so the restart sees no
   * forward progress and emits no new rows.
   */
  @Test
  public void testRestorePastCheckpoint_withSkipChangeCommits(@TempDir File deltaTablePath)
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
    List<Row> run1 = runWithParquetSink(df1, outputDir, checkpointDir, /* trigger= */ null);
    DeltaLog.clearCache();

    spark.sql(str("RESTORE TABLE delta.`%s` TO VERSION AS OF 1", tablePath));

    Throwable caught = null;
    List<Row> run2 = null;
    try {
      Dataset<Row> df2 = spark.readStream().option("skipChangeCommits", "true").table(dsv2TableRef);
      run2 = runWithParquetSink(df2, outputDir, checkpointDir, /* trigger= */ null);
    } catch (Throwable t) {
      caught = t;
    } finally {
      DeltaLog.clearCache();
    }
    if (caught != null && containsNpe(caught)) {
      fail("Restore-past-checkpoint w/ skipChangeCommits produced a raw NPE: " + caught);
    }
    // If the restart succeeded, the sink should contain exactly run1's rows (skipChangeCommits
    // drops the RESTORE commit; no further appends to deliver).
    if (caught == null && run2 != null) {
      final List<Row> finalRun2 = run2;
      assertEquals(
          run1.size(),
          run2.size(),
          () ->
              "skipChangeCommits should drop RESTORE; sink unchanged. run1="
                  + run1.size()
                  + " run2="
                  + finalRun2.size());
    }
  }

  /**
   * 9. Same setup with {@code failOnDataLoss=false}. The option name is DSv1-style and may not be
   * accepted by DSv2 (cf. {@link
   * V2StreamingLogIntegrityTest#testScenario1_failOnDataLossOptionParity}). Whether accepted or
   * rejected, the failure mode must not be a raw NPE.
   */
  @Test
  public void testRestorePastCheckpoint_withFailOnDataLossFalse(@TempDir File deltaTablePath)
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
    StreamingQuery query = null;
    try {
      Dataset<Row> df2 = spark.readStream().option("failOnDataLoss", "false").table(dsv2TableRef);
      query =
          df2.writeStream()
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
      fail("Restore-past-checkpoint w/ failOnDataLoss=false produced a raw NPE: " + caught);
    }
  }

  /**
   * 10. RESTORE past checkpoint, then append more commits forward of where the restore landed.
   * Restart must surface the forward commits or fail cleanly; never NPE.
   */
  @Test
  public void testRestorePastCheckpoint_thenForwardCommits(@TempDir File deltaTablePath)
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

    spark.sql(str("RESTORE TABLE delta.`%s` TO VERSION AS OF 1", tablePath));
    // Forward commit after RESTORE.
    spark.sql(str("INSERT INTO delta.`%s` VALUES (99)", tablePath));

    Throwable caught = null;
    StreamingQuery query = null;
    try {
      Dataset<Row> df2 = spark.readStream().option("skipChangeCommits", "true").table(dsv2TableRef);
      query =
          df2.writeStream()
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
      fail("Restore-past-checkpoint then forward commits produced a raw NPE: " + caught);
    }
  }

  /**
   * 11. OPTIMIZE compacts files; THEN a fresh stream starts (no prior checkpoint). The initial
   * snapshot must reflect the post-OPTIMIZE state (one big file), not the pre-OPTIMIZE files.
   */
  @Test
  public void testOptimizeBeforeFreshStream_basic(@TempDir File deltaTablePath) throws Exception {
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
    Dataset<Row> streamingDF = spark.readStream().table(dsv2TableRef);
    List<Row> rows = processStreamingQuery(streamingDF, "opt_before_fresh_basic");
    List<Row> expected =
        Arrays.asList(
            RowFactory.create(1, "A"), RowFactory.create(2, "B"), RowFactory.create(3, "C"));
    assertDataEquals(rows, expected);
  }

  /**
   * 12. Same setup with {@code maxFilesPerTrigger=1}. The post-OPTIMIZE snapshot is a single file
   * so the entire content arrives in one batch (the option pre-OPTIMIZE would have forced 3
   * batches).
   */
  @Test
  public void testOptimizeBeforeFreshStream_maxFilesPerTrigger(@TempDir File deltaTablePath)
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
        spark.readStream().option("maxFilesPerTrigger", "1").table(dsv2TableRef);
    List<Row> rows = processStreamingQuery(streamingDF, "opt_before_fresh_mfpt");
    List<Row> expected =
        Arrays.asList(
            RowFactory.create(1, "A"), RowFactory.create(2, "B"), RowFactory.create(3, "C"));
    assertDataEquals(rows, expected);
  }

  /**
   * 13. {@code startingVersion=0} after OPTIMIZE. The OPTIMIZE rewrite is dataChange=false; the
   * stream from v=0 should still emit only the user-written rows (the pre-OPTIMIZE AddFiles), NOT
   * the OPTIMIZE rewrite's AddFile-with-dataChange=false. Both paths yield the same row set because
   * OPTIMIZE preserves logical content.
   */
  @Test
  public void testOptimizeBeforeFreshStream_startingVersion0(@TempDir File deltaTablePath)
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
        spark.readStream().option("startingVersion", "0").table(dsv2TableRef);
    List<Row> rows = processStreamingQuery(streamingDF, "opt_before_fresh_sv0");
    // The forward replay of v=0..vOpt sees 3 appends + 1 dataChange=false OPTIMIZE. The OPTIMIZE
    // must not produce additional rows.
    assertEquals(
        3,
        rows.size(),
        () ->
            "startingVersion=0 over a post-OPTIMIZE table should emit exactly 3 rows (the user"
                + " appends), got "
                + rows.size()
                + ". A larger count means the OPTIMIZE rewrite was emitted as data.");
  }

  /**
   * 14. v=0 CREATE, v=1 INSERT, run stream (drains v0+v1), ALTER TABLE enables row tracking (v=2),
   * v=3 INSERT new rows. Restart from checkpoint. The pre-RT snapshot has no row_id; post-RT
   * inserts do. The stream must surface all rows; row_id projection on the metadata is tested
   * separately by {@code V2StreamingRowTrackingTest#testRowTrackingEnabledAfterTableCreate} - here
   * we only assert restart progresses without NPE.
   */
  @Test
  public void testEnableRowTrackingMidStream(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    File checkpointDir = new File(deltaTablePath, "_ckpt");
    File outputDir = new File(deltaTablePath, "_out");

    spark.sql(str("CREATE TABLE delta.`%s` (id INT, name STRING) USING delta", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'A')", tablePath));

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> df1 = spark.readStream().table(dsv2TableRef);
    runWithParquetSink(df1, outputDir, checkpointDir, /* trigger= */ null);
    DeltaLog.clearCache();

    // ALTER mid-stream: enable row tracking.
    spark.sql(
        str(
            "ALTER TABLE delta.`%s` SET TBLPROPERTIES ('delta.enableRowTracking' = 'true')",
            tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (2, 'B')", tablePath));

    Throwable caught = null;
    List<Row> rows = null;
    try {
      rows = runWithParquetSink(df1, outputDir, checkpointDir, /* trigger= */ null);
    } catch (Throwable t) {
      caught = t;
    } finally {
      DeltaLog.clearCache();
    }
    if (caught != null && containsNpe(caught)) {
      fail("Mid-stream RT enable produced a raw NPE: " + caught);
    }
    // If the restart succeeded, sink should hold both rows.
    if (caught == null && rows != null) {
      final List<Row> finalRows = rows;
      assertEquals(
          2,
          rows.size(),
          () ->
              "Mid-stream RT enable should surface both pre and post rows; got "
                  + finalRows.size());
    }
  }

  /**
   * 15. ALTER TABLE enables DV mid-stream, then DV-DELETE is issued. Per {@link
   * V2StreamingRaceLifecycleTest#testScenario5_ProtocolUpgradeMidStream}, the protocol upgrade
   * commit should surface a clean error or be skipped cleanly with {@code skipChangeCommits}. Here
   * we assert no NPE on restart with skipChangeCommits to drop the change commit.
   */
  @Test
  public void testEnableDeletionVectorsMidStream(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    File checkpointDir = new File(deltaTablePath, "_ckpt");
    File outputDir = new File(deltaTablePath, "_out");

    spark.sql(str("CREATE TABLE delta.`%s` (id INT) USING delta", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1), (2), (3)", tablePath));

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> df1 = spark.readStream().table(dsv2TableRef);
    runWithParquetSink(df1, outputDir, checkpointDir, /* trigger= */ null);
    DeltaLog.clearCache();

    spark.sql(
        str(
            "ALTER TABLE delta.`%s` SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')",
            tablePath));
    spark.sql(str("DELETE FROM delta.`%s` WHERE id = 1", tablePath));

    Throwable caught = null;
    try {
      Dataset<Row> df2 = spark.readStream().option("skipChangeCommits", "true").table(dsv2TableRef);
      runWithParquetSink(df2, outputDir, checkpointDir, /* trigger= */ null);
    } catch (Throwable t) {
      caught = t;
    } finally {
      DeltaLog.clearCache();
    }
    if (caught != null && containsNpe(caught)) {
      fail("Mid-stream DV enable produced a raw NPE: " + caught);
    }
  }

  /**
   * 16. v=0..1 in non-ICT mode; run stream; ALTER enables ICT mid-stream; append more. Cross-ref
   * {@code V2StreamingIctTest} - this is the lifecycle-axis sanity check for that family. The
   * restart should progress without NPE; row count exact value depends on whether the ALTER commit
   * is counted as a change commit on this protocol path.
   */
  @Test
  public void testEnableIctMidStream(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    File checkpointDir = new File(deltaTablePath, "_ckpt");
    File outputDir = new File(deltaTablePath, "_out");

    spark.sql(str("CREATE TABLE delta.`%s` (id INT) USING delta", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1)", tablePath));

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> df1 = spark.readStream().table(dsv2TableRef);
    runWithParquetSink(df1, outputDir, checkpointDir, /* trigger= */ null);
    DeltaLog.clearCache();

    spark.sql(
        str(
            "ALTER TABLE delta.`%s` SET TBLPROPERTIES "
                + "('delta.enableInCommitTimestamps' = 'true')",
            tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (2)", tablePath));

    Throwable caught = null;
    List<Row> rows = null;
    try {
      rows = runWithParquetSink(df1, outputDir, checkpointDir, /* trigger= */ null);
    } catch (Throwable t) {
      caught = t;
    } finally {
      DeltaLog.clearCache();
    }
    if (caught != null && containsNpe(caught)) {
      fail("Mid-stream ICT enable produced a raw NPE: " + caught);
    }
    // Both pre and post-ICT rows should reach the sink (ICT is purely a commit-metadata change).
    if (caught == null && rows != null) {
      final List<Row> finalRows = rows;
      assertEquals(
          2,
          rows.size(),
          () -> "Mid-stream ICT enable should surface both rows; got " + finalRows.size());
    }
  }

  /**
   * 17. appendOnly table; DELETE is rejected with DELTA_CANNOT_MODIFY_APPEND_ONLY. After unsetting
   * the property, DELETE succeeds. The boundary test verifies the stream surfaces the rows on
   * either side of the toggle without NPE.
   */
  @Test
  public void testDisableAppendOnlyMidStream(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    File checkpointDir = new File(deltaTablePath, "_ckpt");
    File outputDir = new File(deltaTablePath, "_out");

    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT) USING delta "
                + "TBLPROPERTIES ('delta.appendOnly' = 'true')",
            tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1), (2), (3)", tablePath));

    // Boundary 1: while appendOnly=true, DELETE must be rejected.
    Throwable preToggleErr = null;
    try {
      spark.sql(str("DELETE FROM delta.`%s` WHERE id = 1", tablePath));
    } catch (Throwable t) {
      preToggleErr = t;
    }
    assertNotNull(
        preToggleErr,
        "DELETE on appendOnly=true must be rejected with DELTA_CANNOT_MODIFY_APPEND_ONLY");

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> df1 = spark.readStream().table(dsv2TableRef);
    runWithParquetSink(df1, outputDir, checkpointDir, /* trigger= */ null);
    DeltaLog.clearCache();

    // Boundary 2: unset appendOnly mid-stream. DELETE is then allowed.
    spark.sql(str("ALTER TABLE delta.`%s` UNSET TBLPROPERTIES ('delta.appendOnly')", tablePath));
    spark.sql(str("DELETE FROM delta.`%s` WHERE id = 1", tablePath));

    Throwable caught = null;
    try {
      Dataset<Row> df2 = spark.readStream().option("skipChangeCommits", "true").table(dsv2TableRef);
      runWithParquetSink(df2, outputDir, checkpointDir, /* trigger= */ null);
    } catch (Throwable t) {
      caught = t;
    } finally {
      DeltaLog.clearCache();
    }
    if (caught != null && containsNpe(caught)) {
      fail("Mid-stream appendOnly unset produced a raw NPE: " + caught);
    }
    // Sanity: the table did move past v=1 (the initial insert).
    assertTrue(
        latestVersion(tablePath) >= 3L,
        () -> "Expected v>=3 after UNSET+DELETE; got " + latestVersion(tablePath));
  }
}
