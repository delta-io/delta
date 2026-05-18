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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
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
 * Streaming-option x streaming-option matrix for DSv2: the rate-limit ({@code maxFilesPerTrigger},
 * {@code maxBytesPerTrigger}) cluster crossed with the lifecycle options ({@code AvailableNow},
 * restart, {@code ignoreDeletes}, {@code excludeRegex}).
 *
 * <p>The existing matrices in {@link V2RateLimitStreamingTest} and {@link
 * V2StreamingOptionMatrixTest} cross table states with streaming options; this file covers option x
 * option combinations that would otherwise fall in the gaps between those files.
 */
public class V2StreamingOptionCombinationsRateLimitTest extends V2TestBase {

  /** Writes {@code n} single-row commits with values {@code start..start+n-1}. */
  private void writeNSingleRowVersions(String tablePath, int start, int n) {
    for (int i = start; i < start + n; i++) {
      spark
          .createDataFrame(
              Arrays.asList(org.apache.spark.sql.RowFactory.create(String.valueOf(i))),
              org.apache.spark.sql.types.DataTypes.createStructType(
                  Arrays.asList(
                      org.apache.spark.sql.types.DataTypes.createStructField(
                          "value", org.apache.spark.sql.types.DataTypes.StringType, true))))
          .write()
          .format("delta")
          .mode("append")
          .save(tablePath);
    }
  }

  /** Returns recent progress entries that produced rows. */
  private static StreamingQueryProgress[] nonEmptyProgress(StreamingQuery q) {
    return Arrays.stream(q.recentProgress())
        .filter(p -> p.numInputRows() != 0L)
        .toArray(StreamingQueryProgress[]::new);
  }

  /**
   * Runs a streaming query that writes to a parquet sink + checkpoint and drains all available
   * data. Required for restart cases because the memory sink does not recover from checkpoints in
   * append mode.
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

  /**
   * 6 single-file commits, run 1 with {@code maxFilesPerTrigger=2} stopped after the first 2
   * batches, then restart with the same option. Assert no duplicates across the restart boundary
   * and all 6 rows present after full drain.
   *
   * <p>Bug risk: the checkpoint offset format interacts with the rate-limit admission state - if
   * the resume offset is recomputed incorrectly, the restart can either re-emit already-committed
   * files or skip files that were admitted but not yet committed.
   */
  @Test
  public void testMaxFilesPerTrigger_withRestart(@TempDir File baseDir) throws Exception {
    File inputDir = new File(baseDir, "input");
    File outputDir = new File(baseDir, "output");
    File checkpointDir = new File(baseDir, "checkpoint");
    String tablePath = inputDir.getAbsolutePath();
    writeNSingleRowVersions(tablePath, 0, 6);

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);

    // Run 1: maxFilesPerTrigger=2, stop after the first 2 batches (4 rows).
    StreamingQuery q1 =
        spark
            .readStream()
            .option("maxFilesPerTrigger", "2")
            .table(dsv2TableRef)
            .writeStream()
            .format("parquet")
            .outputMode("append")
            .option("path", outputDir.getAbsolutePath())
            .option("checkpointLocation", checkpointDir.getAbsolutePath())
            .start();
    try {
      // Wait until 2 non-empty batches have committed, then stop.
      long deadline = System.currentTimeMillis() + 60_000L;
      while (nonEmptyProgress(q1).length < 2 && System.currentTimeMillis() < deadline) {
        Thread.sleep(50);
      }
      assertTrue(
          nonEmptyProgress(q1).length >= 2, "expected at least 2 batches before stopping run 1");
    } finally {
      q1.stop();
      DeltaLog.clearCache();
    }

    // Run 2: restart from the same checkpoint, same maxFilesPerTrigger.
    List<Row> finalRows;
    StreamingQuery q2 =
        spark
            .readStream()
            .option("maxFilesPerTrigger", "2")
            .table(dsv2TableRef)
            .writeStream()
            .format("parquet")
            .outputMode("append")
            .option("path", outputDir.getAbsolutePath())
            .option("checkpointLocation", checkpointDir.getAbsolutePath())
            .start();
    try {
      q2.processAllAvailable();
      finalRows = spark.read().parquet(outputDir.getAbsolutePath()).collectAsList();
    } finally {
      q2.stop();
      DeltaLog.clearCache();
    }

    // No duplicates: every row value appears exactly once.
    Set<String> seen = new HashSet<>();
    for (Row r : finalRows) {
      String v = r.getString(0);
      assertTrue(seen.add(v), () -> "duplicate row across restart boundary: " + v);
    }
    // All 6 rows present.
    assertEquals(
        6, finalRows.size(), () -> "expected 6 rows after restart drain, got: " + finalRows);
  }

  /**
   * Same shape as {@link #testMaxFilesPerTrigger_withRestart} but uses {@code maxBytesPerTrigger}
   * sized roughly to one file. Assert no duplicates and correct total row count across the restart
   * boundary.
   */
  @Test
  public void testMaxBytesPerTrigger_withRestart(@TempDir File baseDir) throws Exception {
    File inputDir = new File(baseDir, "input");
    File outputDir = new File(baseDir, "output");
    File checkpointDir = new File(baseDir, "checkpoint");
    String tablePath = inputDir.getAbsolutePath();
    writeNSingleRowVersions(tablePath, 0, 6);

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);

    // Run 1: 1b per trigger (the engine still admits at least one file per batch), stop early.
    StreamingQuery q1 =
        spark
            .readStream()
            .option("maxBytesPerTrigger", "1b")
            .table(dsv2TableRef)
            .writeStream()
            .format("parquet")
            .outputMode("append")
            .option("path", outputDir.getAbsolutePath())
            .option("checkpointLocation", checkpointDir.getAbsolutePath())
            .start();
    try {
      long deadline = System.currentTimeMillis() + 60_000L;
      while (nonEmptyProgress(q1).length < 2 && System.currentTimeMillis() < deadline) {
        Thread.sleep(50);
      }
      assertTrue(
          nonEmptyProgress(q1).length >= 2, "expected at least 2 batches before stopping run 1");
    } finally {
      q1.stop();
      DeltaLog.clearCache();
    }

    // Run 2: restart with the same byte limit and drain.
    List<Row> finalRows;
    StreamingQuery q2 =
        spark
            .readStream()
            .option("maxBytesPerTrigger", "1b")
            .table(dsv2TableRef)
            .writeStream()
            .format("parquet")
            .outputMode("append")
            .option("path", outputDir.getAbsolutePath())
            .option("checkpointLocation", checkpointDir.getAbsolutePath())
            .start();
    try {
      q2.processAllAvailable();
      finalRows = spark.read().parquet(outputDir.getAbsolutePath()).collectAsList();
    } finally {
      q2.stop();
      DeltaLog.clearCache();
    }

    Set<String> seen = new HashSet<>();
    for (Row r : finalRows) {
      String v = r.getString(0);
      assertTrue(seen.add(v), () -> "duplicate row across restart boundary: " + v);
    }
    assertEquals(
        6, finalRows.size(), () -> "expected 6 rows after restart drain, got: " + finalRows);
  }

  /**
   * 6 single-file commits with {@code maxFilesPerTrigger=2 + Trigger.AvailableNow}. The query must
   * terminate naturally, emit all 6 rows, and produce exactly 3 non-empty batches.
   */
  @Test
  public void testMaxFilesPerTrigger_withAvailableNow(@TempDir File baseDir) throws Exception {
    File inputDir = new File(baseDir, "input");
    File outputDir = new File(baseDir, "output");
    File checkpointDir = new File(baseDir, "checkpoint");
    String tablePath = inputDir.getAbsolutePath();
    writeNSingleRowVersions(tablePath, 0, 6);

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);

    StreamingQuery q =
        spark
            .readStream()
            .option("maxFilesPerTrigger", "2")
            .table(dsv2TableRef)
            .writeStream()
            .format("parquet")
            .outputMode("append")
            .option("path", outputDir.getAbsolutePath())
            .option("checkpointLocation", checkpointDir.getAbsolutePath())
            .trigger(Trigger.AvailableNow())
            .start();
    try {
      assertTrue(q.awaitTermination(60_000L), "AvailableNow query should terminate");
      StreamingQueryProgress[] progress = nonEmptyProgress(q);
      assertEquals(
          3,
          progress.length,
          () ->
              "expected 3 non-empty batches with maxFilesPerTrigger=2 + AvailableNow over 6 files");
      for (StreamingQueryProgress p : progress) {
        assertEquals(2L, p.numInputRows(), "each batch should carry 2 rows");
      }
      List<Row> all = spark.read().parquet(outputDir.getAbsolutePath()).collectAsList();
      assertEquals(6, all.size(), () -> "expected all 6 rows in sink, got: " + all);
    } finally {
      q.stop();
      DeltaLog.clearCache();
    }
  }

  /**
   * 6 single-file commits with {@code maxBytesPerTrigger=1b + Trigger.AvailableNow}. The 1b limit
   * still admits at least one file per batch (DSv1 parity), so we get exactly 6 non-empty batches
   * of 1 row each. Query must terminate naturally.
   *
   * <p>This pairs with {@link #testMaxFilesPerTrigger_withAvailableNow}: same shape with a byte cap
   * so coarse it forces 1-file batches.
   */
  @Test
  public void testMaxBytesPerTrigger_withAvailableNow(@TempDir File baseDir) throws Exception {
    File inputDir = new File(baseDir, "input");
    File outputDir = new File(baseDir, "output");
    File checkpointDir = new File(baseDir, "checkpoint");
    String tablePath = inputDir.getAbsolutePath();
    writeNSingleRowVersions(tablePath, 0, 6);

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);

    StreamingQuery q =
        spark
            .readStream()
            .option("maxBytesPerTrigger", "1b")
            .table(dsv2TableRef)
            .writeStream()
            .format("parquet")
            .outputMode("append")
            .option("path", outputDir.getAbsolutePath())
            .option("checkpointLocation", checkpointDir.getAbsolutePath())
            .trigger(Trigger.AvailableNow())
            .start();
    try {
      assertTrue(q.awaitTermination(60_000L), "AvailableNow query should terminate");
      StreamingQueryProgress[] progress = nonEmptyProgress(q);
      assertEquals(
          6,
          progress.length,
          () -> "expected 6 non-empty batches with 1b + AvailableNow over 6 files");
      for (StreamingQueryProgress p : progress) {
        assertEquals(1L, p.numInputRows(), "each batch should carry 1 row");
      }
      List<Row> all = spark.read().parquet(outputDir.getAbsolutePath()).collectAsList();
      assertEquals(6, all.size(), () -> "expected all 6 rows in sink, got: " + all);
    } finally {
      q.stop();
      DeltaLog.clearCache();
    }
  }

  /**
   * INSERT, whole-file DELETE, INSERT - stream with {@code maxFilesPerTrigger=1 + ignoreDeletes=
   * true}. Partition by id so the DELETE removes whole files (ignoreDeletes only applies to
   * file-granular deletes, not row-level rewrites). Assert only INSERT rows surface, the rate limit
   * is respected, and the stream does not crash on the DELETE commit.
   */
  @Test
  public void testMaxFilesPerTrigger_withIgnoreDeletes(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, name STRING) USING delta PARTITIONED BY (id)",
            tablePath));
    // Initial INSERTs across separate partitions (one file per partition).
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'Alice')", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (2, 'Bob')", tablePath));
    // Whole-file DELETE on partition id=2.
    spark.sql(str("DELETE FROM delta.`%s` WHERE id = 2", tablePath));
    // More INSERTs after the DELETE, into separate partitions.
    spark.sql(str("INSERT INTO delta.`%s` VALUES (3, 'Carol')", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (4, 'Dave')", tablePath));

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> streamingDF =
        spark
            .readStream()
            .option("maxFilesPerTrigger", "1")
            .option("ignoreDeletes", "true")
            .table(dsv2TableRef);

    StreamingQuery q =
        streamingDF
            .writeStream()
            .format("memory")
            .queryName("maxFiles_ignoreDeletes")
            .outputMode("append")
            .start();
    try {
      q.processAllAvailable();
      StreamingQueryProgress[] progress = nonEmptyProgress(q);
      // 4 INSERTs -> 4 files admitted; ignoreDeletes drops the DELETE commit entirely.
      assertEquals(
          4, progress.length, () -> "expected 4 batches (one per INSERT file), got: " + progress);
      for (StreamingQueryProgress p : progress) {
        assertEquals(1L, p.numInputRows(), "maxFilesPerTrigger=1 should admit exactly 1 row");
      }
      List<Row> rows = spark.sql("SELECT * FROM maxFiles_ignoreDeletes").collectAsList();
      assertEquals(4, rows.size(), () -> "expected 4 INSERT rows, got: " + rows);
      // Sanity check: only the inserted ids surface, never id=2's deleted file rewritten.
      Set<Integer> ids = new HashSet<>();
      for (Row r : rows) {
        ids.add(r.getInt(0));
      }
      assertEquals(
          new HashSet<>(Arrays.asList(1, 2, 3, 4)),
          ids,
          () -> "expected ids {1,2,3,4} from the INSERT commits; got: " + ids);
    } finally {
      q.stop();
      DeltaLog.clearCache();
    }
  }

  /**
   * Insert into two partitions {@code p=1} and {@code p=2}. Stream with {@code maxFilesPerTrigger=1
   * + excludeRegex} filtering out p=2 files. Assert only p=1 rows surface and the per-file rate
   * limit is respected on the remaining files.
   */
  @Test
  public void testMaxFilesPerTrigger_withExcludeRegex(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(
        str("CREATE TABLE delta.`%s` (id INT, p INT) USING delta PARTITIONED BY (p)", tablePath));
    // 3 single-row commits into p=1 and 2 single-row commits into p=2.
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 1)", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (2, 1)", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (3, 1)", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (10, 2)", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (11, 2)", tablePath));

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> streamingDF =
        spark
            .readStream()
            .option("maxFilesPerTrigger", "1")
            .option("excludeRegex", "p=2")
            .table(dsv2TableRef);

    StreamingQuery q =
        streamingDF
            .writeStream()
            .format("memory")
            .queryName("maxFiles_excludeRegex")
            .outputMode("append")
            .start();
    try {
      q.processAllAvailable();
      StreamingQueryProgress[] progress = nonEmptyProgress(q);
      // Only the 3 p=1 files should be admitted, one per batch.
      assertEquals(
          3, progress.length, () -> "expected 3 batches (one per p=1 file), got: " + progress);
      for (StreamingQueryProgress p : progress) {
        assertEquals(1L, p.numInputRows(), "maxFilesPerTrigger=1 should admit exactly 1 row");
      }
      List<Row> rows = spark.sql("SELECT * FROM maxFiles_excludeRegex").collectAsList();
      assertEquals(3, rows.size(), () -> "expected 3 p=1 rows, got: " + rows);
      Set<Integer> partitions = new HashSet<>();
      for (Row r : rows) {
        partitions.add(r.getInt(1));
      }
      assertEquals(
          new HashSet<>(Arrays.asList(1)),
          partitions,
          () -> "expected only p=1 rows after excludeRegex=p=2; got partitions: " + partitions);
    } finally {
      q.stop();
      DeltaLog.clearCache();
    }
  }
}
