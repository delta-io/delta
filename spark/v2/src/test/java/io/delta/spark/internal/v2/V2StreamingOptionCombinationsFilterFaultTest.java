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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * DSv2 streaming coverage for option x option combinations not exercised elsewhere.
 *
 * <p>Two clusters are covered here:
 *
 * <ul>
 *   <li><b>Cluster B - Rate-limit x filter options</b>: maxFilesPerTrigger / maxBytesPerTrigger
 *       composed with ignoreChanges, skipChangeCommits, ignoreDeletes, and failOnDataLoss=false.
 *   <li><b>Cluster F - Fault-tolerance x options</b>: failOnDataLoss=false composed with
 *       excludeRegex / restart, plus skipChangeCommits and ignoreDeletes composed with excludeRegex
 *       on partitioned tables.
 * </ul>
 *
 * <p>Each test pins the observed DSv2 behavior for the specific option pair so any drift forces a
 * deliberate decision.
 */
public class V2StreamingOptionCombinationsFilterFaultTest extends V2TestBase {

  /** Force a checkpoint so the snapshot can be reconstructed without the pruned commit JSON. */
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
   * Run a parquet sink stream to drain all available input, then stop. Required for restart cases
   * because the memory sink does not recover from checkpoints in append mode.
   */
  private List<Row> runWithParquetSink(Dataset<Row> streamingDF, File outputDir, File checkpointDir)
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
      query = writer.start();
      query.processAllAvailable();
    } finally {
      if (query != null) {
        query.stop();
      }
    }
    return spark.read().parquet(outputDir.getAbsolutePath()).collectAsList();
  }

  /**
   * Cluster B - Rate-limit x filter options.
   *
   * <p>{@code maxFilesPerTrigger=1} x {@code ignoreChanges=true}.
   *
   * <p>v0 CREATE, v1 INSERT 2 rows, v2 UPDATE one row (data-rewrite change commit), v3 INSERT 2
   * more rows. With ignoreChanges, the UPDATE's AddFiles are re-emitted as appends; the rate limit
   * is respected and the stream does not crash on the UPDATE commit.
   */
  @Test
  public void testMaxFilesPerTrigger_withIgnoreChanges(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (id INT, val STRING) USING delta", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'a'), (2, 'b')", tablePath));
    spark.sql(str("UPDATE delta.`%s` SET val = 'B' WHERE id = 2", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (3, 'c'), (4, 'd')", tablePath));

    Dataset<Row> v2Stream =
        spark
            .readStream()
            .option("ignoreChanges", "true")
            .option("maxFilesPerTrigger", "1")
            .table(str("dsv2.delta.`%s`", tablePath));
    List<Row> v2Rows = processStreamingQuery(v2Stream, "mfpt_ic_v2");

    Dataset<Row> v1Stream =
        spark
            .readStream()
            .format("delta")
            .option("ignoreChanges", "true")
            .option("maxFilesPerTrigger", "1")
            .load(tablePath);
    List<Row> v1Rows = processStreamingQuery(v1Stream, "mfpt_ic_v1");

    // DSv1 parity is the strongest assertion here; the exact row count depends on how many files
    // the UPDATE rewrites, which is engine-internal.
    assertDataEquals(v2Rows, v1Rows);
    // INSERT rows must all be present; the rate limit must not have dropped any.
    long insertRows = v2Rows.stream().filter(r -> r.getInt(0) == 1 || r.getInt(0) >= 3).count();
    assertTrue(
        insertRows >= 3, () -> "expected all 3 INSERT rows (id=1,3,4) to surface; got: " + v2Rows);
  }

  /**
   * {@code maxFilesPerTrigger=1} x {@code skipChangeCommits=true}.
   *
   * <p>v0 CREATE, v1 INSERT 2 rows, v2 UPDATE (skipChangeCommits drops it), v3 INSERT 2 more. Only
   * INSERT rows surface; UPDATE commit is skipped entirely.
   */
  @Test
  public void testMaxFilesPerTrigger_withSkipChangeCommits(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (id INT, val STRING) USING delta", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'a'), (2, 'b')", tablePath));
    spark.sql(str("UPDATE delta.`%s` SET val = 'B' WHERE id = 2", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (3, 'c'), (4, 'd')", tablePath));

    Dataset<Row> v2Stream =
        spark
            .readStream()
            .option("skipChangeCommits", "true")
            .option("maxFilesPerTrigger", "1")
            .table(str("dsv2.delta.`%s`", tablePath));
    List<Row> v2Rows = processStreamingQuery(v2Stream, "mfpt_scc_v2");

    Dataset<Row> v1Stream =
        spark
            .readStream()
            .format("delta")
            .option("skipChangeCommits", "true")
            .option("maxFilesPerTrigger", "1")
            .load(tablePath);
    List<Row> v1Rows = processStreamingQuery(v1Stream, "mfpt_scc_v1");

    assertDataEquals(v2Rows, v1Rows);
    // skipChangeCommits drops the UPDATE commit entirely - exactly the 4 INSERT rows (id=1,2,3,4)
    // surface.
    assertEquals(
        4, v2Rows.size(), () -> "expected 4 INSERT rows with UPDATE skipped, got: " + v2Rows);
  }

  /**
   * {@code maxBytesPerTrigger=1b} x {@code ignoreDeletes=true}.
   *
   * <p>Partitioned table so DELETE is whole-file. v0 CREATE, v1 INSERT into two partitions, v2
   * DELETE one partition (whole-file delete), v3 INSERT more. ignoreDeletes drops the DELETE commit
   * and the byte-rate limit (at least one file per batch) admits the INSERT files correctly.
   */
  @Test
  public void testMaxBytesPerTrigger_withIgnoreDeletes(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    // Partition by p so DELETE removes whole files (ignoreDeletes only allows file-granular
    // deletes; a row-level rewrite would still error).
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, p STRING) USING delta PARTITIONED BY (p)",
            tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'x'), (2, 'y')", tablePath));
    // Whole-partition delete: removes the file for p='y'.
    spark.sql(str("DELETE FROM delta.`%s` WHERE p = 'y'", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (3, 'x'), (4, 'z')", tablePath));

    Dataset<Row> v2Stream =
        spark
            .readStream()
            .option("ignoreDeletes", "true")
            .option("maxBytesPerTrigger", "1b")
            .table(str("dsv2.delta.`%s`", tablePath));
    List<Row> v2Rows = processStreamingQuery(v2Stream, "mbpt_id_v2");

    Dataset<Row> v1Stream =
        spark
            .readStream()
            .format("delta")
            .option("ignoreDeletes", "true")
            .option("maxBytesPerTrigger", "1b")
            .load(tablePath);
    List<Row> v1Rows = processStreamingQuery(v1Stream, "mbpt_id_v1");

    assertDataEquals(v2Rows, v1Rows);
    // 4 INSERT rows (id=1,2,3,4) - DELETE commit is dropped.
    assertEquals(
        4, v2Rows.size(), () -> "expected 4 INSERT rows with DELETE skipped, got: " + v2Rows);
  }

  /**
   * {@code maxFilesPerTrigger=1} x {@code failOnDataLoss=false}.
   *
   * <p>4 single-row commits, prune the middle commit JSON (v=2). With failOnDataLoss=false the
   * stream skips the missing commit and reconstructs the snapshot from the checkpoint; the rate
   * limit is respected across the pruned region.
   */
  @Test
  public void testMaxFilesPerTrigger_withFailOnDataLossFalse(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    // 4 single-row commits: ids 0..3.
    for (int i = 0; i < 4; i++) {
      spark.range(i, i + 1).toDF("id").write().format("delta").mode("append").save(tablePath);
    }

    checkpoint(tablePath);
    pruneCommitJson(tablePath, /* version= */ 2L);

    Dataset<Row> df =
        spark
            .readStream()
            .option("failOnDataLoss", "false")
            .option("maxFilesPerTrigger", "1")
            .option("startingVersion", "0")
            .table(str("dsv2.delta.`%s`", tablePath));
    List<Row> rows = processStreamingQuery(df, "mfpt_fnl");
    // 4 rows survive in the reconstructed snapshot off the checkpoint; the pruned v2 commit must
    // not crash the stream.
    assertEquals(
        4,
        rows.size(),
        () ->
            "expected 4 rows from maxFilesPerTrigger=1 + failOnDataLoss=false across pruned region,"
                + " got: "
                + rows);
  }

  /**
   * Cluster F - Fault-tolerance x options.
   *
   * <p>{@code failOnDataLoss=false} x {@code excludeRegex} on a partitioned table.
   *
   * <p>Insert to partitions p=1 and p=2 across several commits, prune a middle commit JSON, then
   * stream with excludeRegex filtering p=2 path components. Only p=1 rows should surface and the
   * pruned commit must not error the stream.
   */
  @Test
  public void testFailOnDataLoss_false_withExcludeRegex(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(
        str("CREATE TABLE delta.`%s` (id INT, p INT) USING delta PARTITIONED BY (p)", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 1), (2, 1)", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (3, 2), (4, 2)", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (5, 1), (6, 2)", tablePath));

    checkpoint(tablePath);
    pruneCommitJson(tablePath, /* version= */ 2L);

    Dataset<Row> df =
        spark
            .readStream()
            .option("failOnDataLoss", "false")
            .option("excludeRegex", "p=2")
            .option("startingVersion", "0")
            .table(str("dsv2.delta.`%s`", tablePath));
    List<Row> rows = processStreamingQuery(df, "fnl_excl_partition");
    // p=1 rows: id=1, 2, 5. p=2 rows excluded by regex.
    assertEquals(
        3,
        rows.size(),
        () -> "expected 3 p=1 rows after excludeRegex + failOnDataLoss=false, got: " + rows);
    for (Row r : rows) {
      assertEquals(1, r.getInt(1), () -> "all surviving rows should have p=1, got: " + r);
    }
  }

  /**
   * {@code failOnDataLoss=false} x parquet sink + restart.
   *
   * <p>Run 1 drains 3 single-row commits to a parquet sink. Between runs, prune a middle commit
   * JSON and add more commits. Run 2 restarts from the checkpoint with failOnDataLoss=false: the
   * stream resumes without error and does not re-emit run-1 rows.
   */
  @Test
  public void testFailOnDataLoss_false_withRestart(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);
    // Run 1: 3 single-row commits, ids 0..2.
    for (int i = 0; i < 3; i++) {
      spark.range(i, i + 1).toDF("id").write().format("delta").mode("append").save(tablePath);
    }

    File checkpointDir = new File(deltaTablePath, "_ckpt");
    File outputDir = new File(deltaTablePath, "_out");

    Dataset<Row> df1 = spark.readStream().option("failOnDataLoss", "false").table(dsv2Ref);
    List<Row> rowsRun1 = runWithParquetSink(df1, outputDir, checkpointDir);
    assertEquals(3, rowsRun1.size(), () -> "run 1 should emit 3 rows, got: " + rowsRun1);
    DeltaLog.clearCache();

    // Add two more commits (v=3, v=4) so the source advances.
    for (int i = 3; i < 5; i++) {
      spark.range(i, i + 1).toDF("id").write().format("delta").mode("append").save(tablePath);
    }
    // Prune a middle commit (v=3) and force a checkpoint so the snapshot is reconstructable.
    checkpoint(tablePath);
    pruneCommitJson(tablePath, /* version= */ 3L);

    Dataset<Row> df2 = spark.readStream().option("failOnDataLoss", "false").table(dsv2Ref);
    try {
      List<Row> rowsRun2 = runWithParquetSink(df2, outputDir, checkpointDir);
      // Sink accumulates run 1's 3 rows + whatever run 2 admits. With failOnDataLoss=false the
      // restart succeeds; no duplicate of run-1 rows and the available run-2 rows surface.
      assertTrue(rowsRun2.size() >= 3, () -> "run 2 sink must retain run-1 rows, got: " + rowsRun2);
      assertTrue(
          rowsRun2.size() <= 5,
          () -> "run 2 must not emit more than the total 5 commits, got: " + rowsRun2);
      // No duplicate ids across the accumulated sink.
      long distinct = rowsRun2.stream().map(r -> r.get(0)).distinct().count();
      assertEquals(
          rowsRun2.size(), distinct, () -> "no duplicate rows across restart, got: " + rowsRun2);
    } finally {
      DeltaLog.clearCache();
    }
  }

  /**
   * {@code skipChangeCommits=true} x {@code excludeRegex} on a partitioned table.
   *
   * <p>v0 CREATE, v1 INSERT both partitions, v2 UPDATE one partition (change commit -
   * skipChangeCommits drops it), v3 INSERT both partitions. The excludeRegex filters one partition;
   * only INSERT rows in the non-excluded partition surface.
   */
  @Test
  public void testSkipChangeCommits_withExcludeRegex(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(
        str("CREATE TABLE delta.`%s` (id INT, p INT) USING delta PARTITIONED BY (p)", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 1), (2, 2)", tablePath));
    // UPDATE p=2 row: change commit, dropped by skipChangeCommits.
    spark.sql(str("UPDATE delta.`%s` SET id = id + 100 WHERE p = 2", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (3, 1), (4, 2)", tablePath));

    Dataset<Row> v2Stream =
        spark
            .readStream()
            .option("skipChangeCommits", "true")
            .option("excludeRegex", "p=2")
            .table(str("dsv2.delta.`%s`", tablePath));
    List<Row> v2Rows = processStreamingQuery(v2Stream, "scc_excl_v2");

    Dataset<Row> v1Stream =
        spark
            .readStream()
            .format("delta")
            .option("skipChangeCommits", "true")
            .option("excludeRegex", "p=2")
            .load(tablePath);
    List<Row> v1Rows = processStreamingQuery(v1Stream, "scc_excl_v1");

    assertDataEquals(v2Rows, v1Rows);
    // p=1 INSERT rows: id=1, 3. p=2 excluded by regex; UPDATE commit dropped by skipChangeCommits.
    assertEquals(
        2,
        v2Rows.size(),
        () -> "expected 2 p=1 INSERT rows after skipChangeCommits + excludeRegex, got: " + v2Rows);
    for (Row r : v2Rows) {
      assertEquals(1, r.getInt(1), () -> "all surviving rows should have p=1, got: " + r);
    }
  }

  /**
   * {@code ignoreDeletes=true} x {@code excludeRegex} on a partitioned table.
   *
   * <p>v0 CREATE, v1 INSERT both partitions, v2 DELETE one partition (whole-file -
   * ignoreDeletes-friendly), v3 INSERT both partitions. excludeRegex filters one partition; only
   * non-excluded INSERT rows surface; DELETE commit dropped by ignoreDeletes.
   */
  @Test
  public void testIgnoreDeletes_withExcludeRegex(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(
        str("CREATE TABLE delta.`%s` (id INT, p INT) USING delta PARTITIONED BY (p)", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 1), (2, 2)", tablePath));
    // Whole-partition delete on p=2: file-granular, allowed under ignoreDeletes.
    spark.sql(str("DELETE FROM delta.`%s` WHERE p = 2", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (3, 1), (4, 2)", tablePath));

    Dataset<Row> v2Stream =
        spark
            .readStream()
            .option("ignoreDeletes", "true")
            .option("excludeRegex", "p=2")
            .table(str("dsv2.delta.`%s`", tablePath));
    List<Row> v2Rows = processStreamingQuery(v2Stream, "id_excl_v2");

    Dataset<Row> v1Stream =
        spark
            .readStream()
            .format("delta")
            .option("ignoreDeletes", "true")
            .option("excludeRegex", "p=2")
            .load(tablePath);
    List<Row> v1Rows = processStreamingQuery(v1Stream, "id_excl_v1");

    assertDataEquals(v2Rows, v1Rows);
    // p=1 INSERT rows: id=1, 3. p=2 excluded by regex; DELETE dropped by ignoreDeletes.
    assertEquals(
        2,
        v2Rows.size(),
        () -> "expected 2 p=1 INSERT rows after ignoreDeletes + excludeRegex, got: " + v2Rows);
    for (Row r : v2Rows) {
      assertEquals(1, r.getInt(1), () -> "all surviving rows should have p=1, got: " + r);
    }
  }
}
