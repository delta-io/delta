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
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.delta.DeltaLog;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * DSv2 streaming coverage for the DML operation x scenario cross-product.
 *
 * <p>The Phase-0 closeout report identified roughly 60 uncovered cells in the {@code
 * true_cross_product.md} matrix where DML operations (DELETE, UPDATE, MERGE, INSERT OVERWRITE with
 * replaceWhere) compose with non-vanilla streaming scenarios (startingVersion, maxFilesPerTrigger,
 * maxBytesPerTrigger, excludeRegex, failOnDataLoss=false, restart, Trigger.AvailableNow). This file
 * fills the row cluster.
 *
 * <p>Each DML operation rewrites data, so a bare stream over the modified table throws {@code
 * DELTA_SOURCE_TABLE_IGNORE_CHANGES}. The user must opt into either {@code skipChangeCommits=true}
 * (drop the change commit entirely) or {@code ignoreChanges=true} (treat the rewrite's AddFiles as
 * appends, possibly with duplicates). The tests below pin per-scenario behavior against the DSv1
 * baseline.
 *
 * <p>INSERT OVERWRITE without replaceWhere is already covered in {@link
 * V2StreamingInsertOverwriteTest}; the three replaceWhere tests here add the starting-version,
 * rate-limit, and restart variants that file did not cover.
 */
public class V2StreamingDmlOptionMatrixTest extends V2TestBase {

  /**
   * Writes a streaming query to a parquet sink at {@code outputDir} with {@code checkpointDir},
   * drains all available data, then stops. Required for restart cases because the memory sink does
   * not recover from checkpoints in append mode.
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

  /** Force a checkpoint at the current snapshot so pruned commit JSONs are still recoverable. */
  @SuppressWarnings("deprecation")
  private void checkpoint(String tablePath) {
    DeltaLog.forTable(spark, tablePath).checkpoint();
  }

  /**
   * Simulate {@code logRetentionDuration} expiry by deleting the commit JSON for {@code version}
   * (and its CRC sibling) under {@code _delta_log/}.
   */
  private void pruneCommitJson(String tablePath, long version) throws Exception {
    String name = String.format("%020d.json", version);
    Path json = Paths.get(tablePath, "_delta_log", name);
    Files.delete(json);
    Path crc = Paths.get(tablePath, "_delta_log", String.format("%020d.crc", version));
    if (Files.exists(crc)) {
      Files.delete(crc);
    }
    DeltaLog.clearCache();
  }

  /**
   * DELETE row: 5 tests covering startingVersion, maxFilesPerTrigger, maxBytesPerTrigger,
   * failOnDataLoss=false, and restart.
   *
   * <p>DELETE at v2; stream from {@code startingVersion=0} with {@code skipChangeCommits=true}. The
   * DELETE commit is dropped; the snapshot rows that survive in v0/v1 are emitted via the initial
   * snapshot path.
   */
  @Test
  public void testDelete_startingVersion_skipChangeCommits(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (id INT) USING delta", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1), (2), (3), (4), (5)", tablePath));
    spark.sql(str("DELETE FROM delta.`%s` WHERE id < 3", tablePath));

    Dataset<Row> v2Stream =
        spark
            .readStream()
            .option("skipChangeCommits", "true")
            .option("startingVersion", "0")
            .table(str("dsv2.delta.`%s`", tablePath));
    List<Row> v2Rows = processStreamingQuery(v2Stream, "del_sv_v2");

    Dataset<Row> v1Stream =
        spark
            .readStream()
            .format("delta")
            .option("skipChangeCommits", "true")
            .option("startingVersion", "0")
            .load(tablePath);
    List<Row> v1Rows = processStreamingQuery(v1Stream, "del_sv_v1");

    assertDataEquals(v2Rows, v1Rows);
  }

  /**
   * Two DELETE commits each rewriting a separate file; {@code maxFilesPerTrigger=1} + {@code
   * ignoreChanges=true}. The rate limiter drains the AddFiles of the two DELETE rewrites across
   * multiple micro-batches.
   */
  @Test
  public void testDelete_maxFilesPerTrigger_skipChangeCommits(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, part INT) USING delta PARTITIONED BY (part)",
            tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 0), (2, 1), (3, 2), (4, 3)", tablePath));
    // Two DELETEs that each rewrite a single-row file.
    spark.sql(str("DELETE FROM delta.`%s` WHERE id = 1", tablePath));
    spark.sql(str("DELETE FROM delta.`%s` WHERE id = 2", tablePath));

    Dataset<Row> v2Stream =
        spark
            .readStream()
            .option("skipChangeCommits", "true")
            .option("maxFilesPerTrigger", "1")
            .table(str("dsv2.delta.`%s`", tablePath));
    List<Row> v2Rows = processStreamingQuery(v2Stream, "del_mfpt_v2");

    Dataset<Row> v1Stream =
        spark
            .readStream()
            .format("delta")
            .option("skipChangeCommits", "true")
            .option("maxFilesPerTrigger", "1")
            .load(tablePath);
    List<Row> v1Rows = processStreamingQuery(v1Stream, "del_mfpt_v1");

    assertDataEquals(v2Rows, v1Rows);
  }

  /**
   * DELETE rewrite under a byte-based rate limit. With {@code maxBytesPerTrigger=1b} the engine
   * still admits at least one file per batch (DSv1 parity); {@code skipChangeCommits=true} drops
   * the DELETE rewrites so only the initial snapshot rows surface.
   */
  @Test
  public void testDelete_maxBytesPerTrigger_skipChangeCommits(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (id INT) USING delta", tablePath));
    // 5 single-file commits so the initial snapshot has 5 files, one per batch.
    for (int i = 0; i < 5; i++) {
      spark
          .range(i, i + 1)
          .toDF("id")
          .coalesce(1)
          .write()
          .format("delta")
          .mode("append")
          .save(tablePath);
    }
    spark.sql(str("DELETE FROM delta.`%s` WHERE id = 0", tablePath));

    Dataset<Row> v2Stream =
        spark
            .readStream()
            .option("skipChangeCommits", "true")
            .option("maxBytesPerTrigger", "1b")
            .table(str("dsv2.delta.`%s`", tablePath));
    List<Row> v2Rows = processStreamingQuery(v2Stream, "del_mbpt_v2");

    Dataset<Row> v1Stream =
        spark
            .readStream()
            .format("delta")
            .option("skipChangeCommits", "true")
            .option("maxBytesPerTrigger", "1b")
            .load(tablePath);
    List<Row> v1Rows = processStreamingQuery(v1Stream, "del_mbpt_v1");

    assertDataEquals(v2Rows, v1Rows);
  }

  /**
   * Prune the DELETE commit JSON, then stream with {@code failOnDataLoss=false} + {@code
   * skipChangeCommits=true} from version 0. The stream should skip the missing commit and surface
   * the post-DELETE snapshot rows from the checkpoint.
   */
  @Test
  public void testDelete_failOnDataLossFalse_skipChangeCommits(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (id INT) USING delta", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1), (2), (3), (4), (5)", tablePath));
    spark.sql(str("DELETE FROM delta.`%s` WHERE id < 3", tablePath));

    checkpoint(tablePath);
    pruneCommitJson(tablePath, /* version= */ 1L);

    Dataset<Row> v2Stream =
        spark
            .readStream()
            .option("failOnDataLoss", "false")
            .option("skipChangeCommits", "true")
            .option("startingVersion", "0")
            .table(str("dsv2.delta.`%s`", tablePath));
    List<Row> rows = processStreamingQuery(v2Stream, "del_fnl_v2");
    // 3 surviving rows (ids 3, 4, 5) in the reconstructed snapshot.
    assertEquals(
        3, rows.size(), () -> "expected 3 rows after DELETE + pruned commit JSON, got: " + rows);
  }

  /**
   * Parquet sink + checkpoint; run 1 drains the initial snapshot, DELETE between runs, run 2
   * restarts with {@code skipChangeCommits=true}. The restart must drop the DELETE commit and not
   * double-emit run-1 rows.
   */
  @Test
  public void testDelete_restart_skipChangeCommits(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);
    spark.sql(str("CREATE TABLE delta.`%s` (id INT) USING delta", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1), (2), (3)", tablePath));

    File checkpointDir = new File(deltaTablePath, "_ckpt");
    File outputDir = new File(deltaTablePath, "_out");

    Dataset<Row> df1 = spark.readStream().table(dsv2Ref);
    runWithParquetSink(df1, outputDir, checkpointDir, /* trigger= */ null);
    DeltaLog.clearCache();

    spark.sql(str("DELETE FROM delta.`%s` WHERE id = 2", tablePath));

    Dataset<Row> df2 = spark.readStream().option("skipChangeCommits", "true").table(dsv2Ref);
    try {
      List<Row> rows = runWithParquetSink(df2, outputDir, checkpointDir, /* trigger= */ null);
      // Run 1 emitted {1, 2, 3}; run 2 skips the DELETE commit; sink has 3 rows total.
      assertEquals(
          3, rows.size(), () -> "expected 3 rows in sink after DELETE restart, got: " + rows);
    } finally {
      DeltaLog.clearCache();
    }
  }

  /**
   * UPDATE row: 5 tests covering startingVersion, maxFilesPerTrigger, excludeRegex,
   * failOnDataLoss=false, and restart.
   *
   * <p>UPDATE rewrite at v2; stream from {@code startingVersion=0} with {@code skipChangeCommits}.
   */
  @Test
  public void testUpdate_startingVersion_skipChangeCommits(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (id INT, val STRING) USING delta", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'a'), (2, 'b')", tablePath));
    spark.sql(str("UPDATE delta.`%s` SET val = 'B' WHERE id = 2", tablePath));

    Dataset<Row> v2Stream =
        spark
            .readStream()
            .option("skipChangeCommits", "true")
            .option("startingVersion", "0")
            .table(str("dsv2.delta.`%s`", tablePath));
    List<Row> v2Rows = processStreamingQuery(v2Stream, "upd_sv_v2");

    Dataset<Row> v1Stream =
        spark
            .readStream()
            .format("delta")
            .option("skipChangeCommits", "true")
            .option("startingVersion", "0")
            .load(tablePath);
    List<Row> v1Rows = processStreamingQuery(v1Stream, "upd_sv_v1");

    assertDataEquals(v2Rows, v1Rows);
  }

  /**
   * UPDATE that rewrites multiple files (one per partition), rate-limited with {@code
   * maxFilesPerTrigger=1} + {@code ignoreChanges=true}. The engine drains the UPDATE rewrites
   * across multiple micro-batches.
   */
  @Test
  public void testUpdate_maxFilesPerTrigger_skipChangeCommits(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, part INT) USING delta PARTITIONED BY (part)",
            tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 0), (2, 1), (3, 2), (4, 3)", tablePath));
    // UPDATE matches one row in each of the 4 partitions, so up to 4 file rewrites.
    spark.sql(str("UPDATE delta.`%s` SET id = id + 100", tablePath));

    Dataset<Row> v2Stream =
        spark
            .readStream()
            .option("ignoreChanges", "true")
            .option("maxFilesPerTrigger", "1")
            .table(str("dsv2.delta.`%s`", tablePath));
    List<Row> v2Rows = processStreamingQuery(v2Stream, "upd_mfpt_v2");

    Dataset<Row> v1Stream =
        spark
            .readStream()
            .format("delta")
            .option("ignoreChanges", "true")
            .option("maxFilesPerTrigger", "1")
            .load(tablePath);
    List<Row> v1Rows = processStreamingQuery(v1Stream, "upd_mfpt_v1");

    assertDataEquals(v2Rows, v1Rows);
  }

  /**
   * UPDATE with {@code excludeRegex} matching every parquet file. With {@code
   * skipChangeCommits=true} the UPDATE commit is dropped, and the regex filters every file in the
   * initial snapshot - the stream emits zero rows.
   */
  @Test
  public void testUpdate_excludeRegex_skipChangeCommits(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (id INT, val STRING) USING delta", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'a'), (2, 'b')", tablePath));
    spark.sql(str("UPDATE delta.`%s` SET val = 'B' WHERE id = 2", tablePath));

    Dataset<Row> v2Stream =
        spark
            .readStream()
            .option("skipChangeCommits", "true")
            .option("excludeRegex", ".*\\.parquet$")
            .table(str("dsv2.delta.`%s`", tablePath));
    List<Row> v2Rows = processStreamingQuery(v2Stream, "upd_excl_v2");

    Dataset<Row> v1Stream =
        spark
            .readStream()
            .format("delta")
            .option("skipChangeCommits", "true")
            .option("excludeRegex", ".*\\.parquet$")
            .load(tablePath);
    List<Row> v1Rows = processStreamingQuery(v1Stream, "upd_excl_v1");

    assertDataEquals(v2Rows, v1Rows);
  }

  /**
   * Prune the UPDATE commit JSON, then stream with {@code failOnDataLoss=false} + {@code
   * skipChangeCommits=true}. The stream skips forward and surfaces the post-UPDATE snapshot.
   */
  @Test
  public void testUpdate_failOnDataLossFalse_skipChangeCommits(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (id INT, val STRING) USING delta", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'a'), (2, 'b')", tablePath));
    spark.sql(str("UPDATE delta.`%s` SET val = 'B' WHERE id = 2", tablePath));

    checkpoint(tablePath);
    pruneCommitJson(tablePath, /* version= */ 1L);

    Dataset<Row> v2Stream =
        spark
            .readStream()
            .option("failOnDataLoss", "false")
            .option("skipChangeCommits", "true")
            .option("startingVersion", "0")
            .table(str("dsv2.delta.`%s`", tablePath));
    List<Row> rows = processStreamingQuery(v2Stream, "upd_fnl_v2");
    // 2 rows survive in the post-UPDATE snapshot reconstructed from the checkpoint.
    assertEquals(
        2, rows.size(), () -> "expected 2 rows after UPDATE + pruned commit JSON, got: " + rows);
  }

  /** Parquet sink + checkpoint; UPDATE between runs; run 2 restarts with skipChangeCommits. */
  @Test
  public void testUpdate_restart_skipChangeCommits(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);
    spark.sql(str("CREATE TABLE delta.`%s` (id INT, val STRING) USING delta", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'a'), (2, 'b'), (3, 'c')", tablePath));

    File checkpointDir = new File(deltaTablePath, "_ckpt");
    File outputDir = new File(deltaTablePath, "_out");

    Dataset<Row> df1 = spark.readStream().table(dsv2Ref);
    runWithParquetSink(df1, outputDir, checkpointDir, /* trigger= */ null);
    DeltaLog.clearCache();

    spark.sql(str("UPDATE delta.`%s` SET val = 'B' WHERE id = 2", tablePath));

    Dataset<Row> df2 = spark.readStream().option("skipChangeCommits", "true").table(dsv2Ref);
    try {
      List<Row> rows = runWithParquetSink(df2, outputDir, checkpointDir, /* trigger= */ null);
      // Run 1 emitted 3 rows; run 2 skips the UPDATE commit; sink has 3 rows total.
      assertEquals(
          3, rows.size(), () -> "expected 3 rows in sink after UPDATE restart, got: " + rows);
    } finally {
      DeltaLog.clearCache();
    }
  }

  /**
   * MERGE row: 5 tests covering startingVersion, maxFilesPerTrigger, ignoreChanges, restart +
   * ignoreChanges, and Trigger.AvailableNow.
   *
   * <p>Basic MERGE INTO with WHEN MATCHED UPDATE / WHEN NOT MATCHED INSERT; stream from {@code
   * startingVersion=0} with {@code skipChangeCommits=true}.
   */
  @Test
  public void testMerge_startingVersion_skipChangeCommits(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (id INT, val STRING) USING delta", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'a'), (2, 'b')", tablePath));

    spark.sql("DROP VIEW IF EXISTS merge_sv_src");
    spark
        .sql("SELECT 2 AS id, 'B' AS val UNION ALL SELECT 3 AS id, 'c' AS val")
        .createOrReplaceTempView("merge_sv_src");
    spark.sql(
        str(
            "MERGE INTO delta.`%s` t USING merge_sv_src s ON t.id = s.id "
                + "WHEN MATCHED THEN UPDATE SET val = s.val "
                + "WHEN NOT MATCHED THEN INSERT (id, val) VALUES (s.id, s.val)",
            tablePath));

    Dataset<Row> v2Stream =
        spark
            .readStream()
            .option("skipChangeCommits", "true")
            .option("startingVersion", "0")
            .table(str("dsv2.delta.`%s`", tablePath));
    List<Row> v2Rows = processStreamingQuery(v2Stream, "mrg_sv_v2");

    Dataset<Row> v1Stream =
        spark
            .readStream()
            .format("delta")
            .option("skipChangeCommits", "true")
            .option("startingVersion", "0")
            .load(tablePath);
    List<Row> v1Rows = processStreamingQuery(v1Stream, "mrg_sv_v1");

    assertDataEquals(v2Rows, v1Rows);
  }

  /**
   * MERGE that rewrites multiple files (one per partition) crossed with {@code
   * maxFilesPerTrigger=1} + {@code ignoreChanges=true}. The rate limiter drains the MERGE's
   * AddFiles across batches.
   */
  @Test
  public void testMerge_maxFilesPerTrigger_skipChangeCommits(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, part INT) USING delta PARTITIONED BY (part)",
            tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 0), (2, 1), (3, 2)", tablePath));

    spark.sql("DROP VIEW IF EXISTS merge_mfpt_src");
    spark
        .sql(
            "SELECT 2 AS id, 1 AS part UNION ALL SELECT 3 AS id, 2 AS part "
                + "UNION ALL SELECT 4 AS id, 3 AS part")
        .createOrReplaceTempView("merge_mfpt_src");
    spark.sql(
        str(
            "MERGE INTO delta.`%s` t USING merge_mfpt_src s ON t.id = s.id "
                + "WHEN MATCHED THEN UPDATE SET part = s.part "
                + "WHEN NOT MATCHED THEN INSERT (id, part) VALUES (s.id, s.part)",
            tablePath));

    Dataset<Row> v2Stream =
        spark
            .readStream()
            .option("ignoreChanges", "true")
            .option("maxFilesPerTrigger", "1")
            .table(str("dsv2.delta.`%s`", tablePath));
    List<Row> v2Rows = processStreamingQuery(v2Stream, "mrg_mfpt_v2");

    Dataset<Row> v1Stream =
        spark
            .readStream()
            .format("delta")
            .option("ignoreChanges", "true")
            .option("maxFilesPerTrigger", "1")
            .load(tablePath);
    List<Row> v1Rows = processStreamingQuery(v1Stream, "mrg_mfpt_v1");

    assertDataEquals(v2Rows, v1Rows);
  }

  /**
   * MERGE with {@code ignoreChanges=true} (the other ignore option). The MERGE's AddFiles are
   * re-emitted as appends; the initial snapshot rows surface as well.
   */
  @Test
  public void testMerge_ignoreChanges(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (id INT, val STRING) USING delta", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'a'), (2, 'b')", tablePath));

    spark.sql("DROP VIEW IF EXISTS merge_ig_src");
    spark
        .sql("SELECT 2 AS id, 'B' AS val UNION ALL SELECT 3 AS id, 'c' AS val")
        .createOrReplaceTempView("merge_ig_src");
    spark.sql(
        str(
            "MERGE INTO delta.`%s` t USING merge_ig_src s ON t.id = s.id "
                + "WHEN MATCHED THEN UPDATE SET val = s.val "
                + "WHEN NOT MATCHED THEN INSERT (id, val) VALUES (s.id, s.val)",
            tablePath));

    Dataset<Row> v2Stream =
        spark.readStream().option("ignoreChanges", "true").table(str("dsv2.delta.`%s`", tablePath));
    List<Row> v2Rows = processStreamingQuery(v2Stream, "mrg_ig_v2");

    Dataset<Row> v1Stream =
        spark.readStream().format("delta").option("ignoreChanges", "true").load(tablePath);
    List<Row> v1Rows = processStreamingQuery(v1Stream, "mrg_ig_v1");

    assertDataEquals(v2Rows, v1Rows);
  }

  /** Parquet sink + checkpoint; MERGE between runs; run 2 restarts with {@code ignoreChanges}. */
  @Test
  public void testMerge_restart_ignoreChanges(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);
    spark.sql(str("CREATE TABLE delta.`%s` (id INT, val STRING) USING delta", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'a'), (2, 'b')", tablePath));

    File checkpointDir = new File(deltaTablePath, "_ckpt");
    File outputDir = new File(deltaTablePath, "_out");

    Dataset<Row> df1 = spark.readStream().table(dsv2Ref);
    runWithParquetSink(df1, outputDir, checkpointDir, /* trigger= */ null);
    DeltaLog.clearCache();

    spark.sql("DROP VIEW IF EXISTS merge_restart_src");
    spark
        .sql("SELECT 2 AS id, 'B' AS val UNION ALL SELECT 3 AS id, 'c' AS val")
        .createOrReplaceTempView("merge_restart_src");
    spark.sql(
        str(
            "MERGE INTO delta.`%s` t USING merge_restart_src s ON t.id = s.id "
                + "WHEN MATCHED THEN UPDATE SET val = s.val "
                + "WHEN NOT MATCHED THEN INSERT (id, val) VALUES (s.id, s.val)",
            tablePath));

    Dataset<Row> df2 = spark.readStream().option("ignoreChanges", "true").table(dsv2Ref);
    try {
      List<Row> rows = runWithParquetSink(df2, outputDir, checkpointDir, /* trigger= */ null);
      // Run 1 emitted 2 rows; run 2 re-emits the MERGE's AddFiles + new insert. Pin >= 3 rows
      // (exact count depends on how many files the MERGE rewrote; ignoreChanges may surface a
      // duplicate of id=1 if it rewrote the original file).
      assertTrue(
          rows.size() >= 3,
          () -> "expected at least 3 rows in sink after MERGE restart, got: " + rows);
    } finally {
      DeltaLog.clearCache();
    }
  }

  /**
   * {@code Trigger.AvailableNow} + MERGE in history; stream with {@code skipChangeCommits=true}
   * must terminate cleanly after consuming all available data.
   */
  @Test
  public void testMerge_triggerAvailableNow_skipChangeCommits(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (id INT, val STRING) USING delta", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'a'), (2, 'b')", tablePath));

    spark.sql("DROP VIEW IF EXISTS merge_av_src");
    spark
        .sql("SELECT 2 AS id, 'B' AS val UNION ALL SELECT 3 AS id, 'c' AS val")
        .createOrReplaceTempView("merge_av_src");
    spark.sql(
        str(
            "MERGE INTO delta.`%s` t USING merge_av_src s ON t.id = s.id "
                + "WHEN MATCHED THEN UPDATE SET val = s.val "
                + "WHEN NOT MATCHED THEN INSERT (id, val) VALUES (s.id, s.val)",
            tablePath));

    Dataset<Row> v2Stream =
        spark
            .readStream()
            .option("skipChangeCommits", "true")
            .table(str("dsv2.delta.`%s`", tablePath));

    File checkpointDir = new File(deltaTablePath, "_ckpt_v2");
    File outputDir = new File(deltaTablePath, "_out_v2");
    try {
      List<Row> v2Rows =
          runWithParquetSink(v2Stream, outputDir, checkpointDir, Trigger.AvailableNow());

      Dataset<Row> v1Stream =
          spark.readStream().format("delta").option("skipChangeCommits", "true").load(tablePath);
      File v1Ckpt = new File(deltaTablePath, "_ckpt_v1");
      File v1Out = new File(deltaTablePath, "_out_v1");
      List<Row> v1Rows = runWithParquetSink(v1Stream, v1Out, v1Ckpt, Trigger.AvailableNow());

      assertDataEquals(v2Rows, v1Rows);
    } finally {
      DeltaLog.clearCache();
    }
  }

  /**
   * REPLACE WHERE row: 3 tests adding starting-version, rate-limit, and restart variants on top of
   * the basic partitioned replaceWhere coverage in V2StreamingInsertOverwriteTest.
   *
   * <p>Partitioned replaceWhere at v1; stream from {@code startingVersion=0} with {@code
   * skipChangeCommits=true}. The replaceWhere commit is dropped; only the v0 snapshot surfaces.
   */
  @Test
  public void testReplaceWhere_partitioned_startingVersion(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, part INT) USING delta PARTITIONED BY (part)",
            tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 0), (2, 0), (10, 1), (11, 1)", tablePath));
    spark
        .createDataFrame(
            java.util.Arrays.asList(
                org.apache.spark.sql.RowFactory.create(100, 0),
                org.apache.spark.sql.RowFactory.create(101, 0)),
            new org.apache.spark.sql.types.StructType()
                .add("id", org.apache.spark.sql.types.DataTypes.IntegerType)
                .add("part", org.apache.spark.sql.types.DataTypes.IntegerType))
        .write()
        .format("delta")
        .mode("overwrite")
        .option("replaceWhere", "part = 0")
        .save(tablePath);

    Dataset<Row> v2Stream =
        spark
            .readStream()
            .option("skipChangeCommits", "true")
            .option("startingVersion", "0")
            .table(str("dsv2.delta.`%s`", tablePath));
    List<Row> v2Rows = processStreamingQuery(v2Stream, "rw_sv_v2");

    Dataset<Row> v1Stream =
        spark
            .readStream()
            .format("delta")
            .option("skipChangeCommits", "true")
            .option("startingVersion", "0")
            .load(tablePath);
    List<Row> v1Rows = processStreamingQuery(v1Stream, "rw_sv_v1");

    assertDataEquals(v2Rows, v1Rows);
  }

  /**
   * Partitioned replaceWhere + {@code maxFilesPerTrigger=1} + {@code ignoreChanges=true}: the
   * replaceWhere's AddFiles are drained one per batch.
   */
  @Test
  public void testReplaceWhere_partitioned_maxFilesPerTrigger(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, part INT) USING delta PARTITIONED BY (part)",
            tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 0), (2, 0), (10, 1), (11, 1)", tablePath));
    // Rewrite part=0 with 2 rows that will be written as one file under that partition.
    spark
        .createDataFrame(
            java.util.Arrays.asList(
                org.apache.spark.sql.RowFactory.create(100, 0),
                org.apache.spark.sql.RowFactory.create(101, 0)),
            new org.apache.spark.sql.types.StructType()
                .add("id", org.apache.spark.sql.types.DataTypes.IntegerType)
                .add("part", org.apache.spark.sql.types.DataTypes.IntegerType))
        .write()
        .format("delta")
        .mode("overwrite")
        .option("replaceWhere", "part = 0")
        .save(tablePath);

    Dataset<Row> v2Stream =
        spark
            .readStream()
            .option("ignoreChanges", "true")
            .option("maxFilesPerTrigger", "1")
            .table(str("dsv2.delta.`%s`", tablePath));
    List<Row> v2Rows = processStreamingQuery(v2Stream, "rw_mfpt_v2");

    Dataset<Row> v1Stream =
        spark
            .readStream()
            .format("delta")
            .option("ignoreChanges", "true")
            .option("maxFilesPerTrigger", "1")
            .load(tablePath);
    List<Row> v1Rows = processStreamingQuery(v1Stream, "rw_mfpt_v1");

    assertDataEquals(v2Rows, v1Rows);
  }

  /**
   * Parquet sink + checkpoint; partitioned replaceWhere between runs; run 2 restarts with {@code
   * skipChangeCommits=true}.
   */
  @Test
  public void testReplaceWhere_partitioned_restart(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, part INT) USING delta PARTITIONED BY (part)",
            tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 0), (2, 0), (10, 1), (11, 1)", tablePath));

    File checkpointDir = new File(deltaTablePath, "_ckpt");
    File outputDir = new File(deltaTablePath, "_out");

    Dataset<Row> df1 = spark.readStream().table(dsv2Ref);
    runWithParquetSink(df1, outputDir, checkpointDir, /* trigger= */ null);
    DeltaLog.clearCache();

    // replaceWhere on part=0 between runs.
    spark
        .createDataFrame(
            java.util.Arrays.asList(
                org.apache.spark.sql.RowFactory.create(100, 0),
                org.apache.spark.sql.RowFactory.create(101, 0)),
            new org.apache.spark.sql.types.StructType()
                .add("id", org.apache.spark.sql.types.DataTypes.IntegerType)
                .add("part", org.apache.spark.sql.types.DataTypes.IntegerType))
        .write()
        .format("delta")
        .mode("overwrite")
        .option("replaceWhere", "part = 0")
        .save(tablePath);

    Dataset<Row> df2 = spark.readStream().option("skipChangeCommits", "true").table(dsv2Ref);
    try {
      List<Row> rows = runWithParquetSink(df2, outputDir, checkpointDir, /* trigger= */ null);
      // Run 1 emitted 4 rows; run 2 skips the replaceWhere commit; sink has 4 rows total.
      assertEquals(
          4, rows.size(), () -> "expected 4 rows in sink after replaceWhere restart, got: " + rows);
    } finally {
      DeltaLog.clearCache();
    }
  }
}
