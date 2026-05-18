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
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.delta.DeltaLog;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryProgress;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Cross-product coverage for {@code maxBytesPerTrigger} x feature-row tables.
 *
 * <p>{@link V2RateLimitStreamingTest} exercises {@code maxBytesPerTrigger} on vanilla tables only.
 * This file fills the cross-product cells against each feature table that the Phase-0 re-baseline
 * flagged as uncovered (~37 features): DV, column mapping (name/id), row tracking, ICT, v2
 * checkpoint, TIMESTAMP_NTZ, clustering, appendOnly, partitioned (last vs first).
 *
 * <p>Pattern: write 5 single-file commits, set {@code maxBytesPerTrigger=1b} so the rate limiter
 * admits exactly one file per batch (the engine always admits at least one file even when its size
 * exceeds the limit; this is DSv1 parity behavior). Assert 5 non-empty batches.
 */
public class V2StreamingMaxBytesPerTriggerMatrixTest extends V2TestBase {

  /** Returns recent progress entries that produced rows. */
  private static StreamingQueryProgress[] nonEmptyProgress(StreamingQuery q) {
    return Arrays.stream(q.recentProgress())
        .filter(p -> p.numInputRows() != 0L)
        .toArray(StreamingQueryProgress[]::new);
  }

  /**
   * Runs a streaming query with {@code maxBytesPerTrigger=1b} against the given dsv2 table and
   * asserts exactly {@code expectedBatches} non-empty batches each with 1 row.
   */
  private void assertOneFilePerBatch(String tablePath, String queryName, int expectedBatches)
      throws Exception {
    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    StreamingQuery q =
        spark
            .readStream()
            .option("maxBytesPerTrigger", "1b")
            .table(dsv2TableRef)
            .writeStream()
            .format("memory")
            .queryName(queryName)
            .outputMode("append")
            .start();
    try {
      q.processAllAvailable();
      StreamingQueryProgress[] progress = nonEmptyProgress(q);
      assertEquals(
          expectedBatches,
          progress.length,
          () -> "Expected " + expectedBatches + " non-empty batches; got " + progress.length);
      for (StreamingQueryProgress p : progress) {
        assertEquals(1L, p.numInputRows(), "each batch should admit exactly one file");
      }
    } finally {
      q.stop();
      DeltaLog.clearCache();
    }
  }

  /** Appends a single-row Delta commit with INT id={@code id}. */
  private void appendIntRow(String tablePath, int id) {
    spark
        .createDataFrame(
            Arrays.asList(RowFactory.create(id)), new StructType().add("id", DataTypes.IntegerType))
        .coalesce(1)
        .write()
        .format("delta")
        .mode("append")
        .save(tablePath);
  }

  /** Appends a single-row commit with (id INT, name STRING). */
  private void appendIdName(String tablePath, int id, String name) {
    spark
        .createDataFrame(
            Arrays.asList(RowFactory.create(id, name)),
            new StructType().add("id", DataTypes.IntegerType).add("name", DataTypes.StringType))
        .coalesce(1)
        .write()
        .format("delta")
        .mode("append")
        .save(tablePath);
  }

  /**
   * DV-enabled table: 5 single-file commits + one DV delete commit that affects one of the files.
   * Stream from version 0 with {@code maxBytesPerTrigger=1b}; expect 5 non-empty batches (the DV
   * commit is a metadata-only change and does not surface as a new AddFile to the stream).
   */
  @Test
  public void testMaxBytes_onDvTable(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT) USING delta "
                + "TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')",
            tablePath));
    for (int i = 0; i < 5; i++) {
      appendIntRow(tablePath, i);
    }
    // DV-only delete on one file - does not introduce a new AddFile.
    spark.sql(str("DELETE FROM delta.`%s` WHERE id = 0", tablePath));

    assertOneFilePerBatch(tablePath, "maxBytes_dv", /* expectedBatches= */ 5);
  }

  @Test
  public void testMaxBytes_onColMappedNameTable(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, name STRING) USING delta "
                + "TBLPROPERTIES ('delta.columnMapping.mode' = 'name')",
            tablePath));
    for (int i = 0; i < 5; i++) {
      appendIdName(tablePath, i, "row" + i);
    }

    assertOneFilePerBatch(tablePath, "maxBytes_cm_name", /* expectedBatches= */ 5);
  }

  @Test
  public void testMaxBytes_onColMappedIdTable(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, name STRING) USING delta "
                + "TBLPROPERTIES ('delta.columnMapping.mode' = 'id', "
                + "  'delta.minReaderVersion' = '2', "
                + "  'delta.minWriterVersion' = '5')",
            tablePath));
    for (int i = 0; i < 5; i++) {
      appendIdName(tablePath, i, "row" + i);
    }

    assertOneFilePerBatch(tablePath, "maxBytes_cm_id", /* expectedBatches= */ 5);
  }

  @Test
  public void testMaxBytes_onRowTrackedTable(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT) USING delta "
                + "TBLPROPERTIES ('delta.enableRowTracking' = 'true')",
            tablePath));
    for (int i = 0; i < 5; i++) {
      appendIntRow(tablePath, i);
    }

    assertOneFilePerBatch(tablePath, "maxBytes_rt", /* expectedBatches= */ 5);
  }

  @Test
  public void testMaxBytes_onIctTable(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT) USING delta "
                + "TBLPROPERTIES ('delta.enableInCommitTimestamps' = 'true')",
            tablePath));
    for (int i = 0; i < 5; i++) {
      appendIntRow(tablePath, i);
    }

    assertOneFilePerBatch(tablePath, "maxBytes_ict", /* expectedBatches= */ 5);
  }

  @Test
  public void testMaxBytes_onV2CheckpointTable(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT) USING delta "
                + "TBLPROPERTIES ('delta.checkpointPolicy' = 'v2')",
            tablePath));
    for (int i = 0; i < 5; i++) {
      appendIntRow(tablePath, i);
    }

    assertOneFilePerBatch(tablePath, "maxBytes_v2ckpt", /* expectedBatches= */ 5);
  }

  @Test
  public void testMaxBytes_onTimestampNtzTable(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (id INT, ts TIMESTAMP_NTZ) USING delta", tablePath));
    for (int i = 0; i < 5; i++) {
      spark.sql(
          str(
              "INSERT INTO delta.`%s` VALUES (%d, TIMESTAMP_NTZ'2024-0%d-01 00:00:00')",
              tablePath, i, i + 1));
    }

    assertOneFilePerBatch(tablePath, "maxBytes_tsntz", /* expectedBatches= */ 5);
  }

  @Test
  public void testMaxBytes_onClusteredTable(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, name STRING) USING delta CLUSTER BY (id)",
            tablePath));
    for (int i = 0; i < 5; i++) {
      appendIdName(tablePath, i, "row" + i);
    }

    assertOneFilePerBatch(tablePath, "maxBytes_clustered", /* expectedBatches= */ 5);
  }

  @Test
  public void testMaxBytes_onAppendOnlyTable(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT) USING delta "
                + "TBLPROPERTIES ('delta.appendOnly' = 'true')",
            tablePath));
    for (int i = 0; i < 5; i++) {
      appendIntRow(tablePath, i);
    }

    assertOneFilePerBatch(tablePath, "maxBytes_appendonly", /* expectedBatches= */ 5);
  }

  /**
   * Partition column declared last in DDL (Spark's normalized form). 5 single-file commits, one row
   * per partition value.
   */
  @Test
  public void testMaxBytes_onPartitionedTable_partLast(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, part INT) USING delta PARTITIONED BY (part)",
            tablePath));
    for (int i = 0; i < 5; i++) {
      spark.sql(str("INSERT INTO delta.`%s` VALUES (%d, %d)", tablePath, i, i));
    }

    assertOneFilePerBatch(tablePath, "maxBytes_part_last", /* expectedBatches= */ 5);
  }

  /**
   * Partition column declared FIRST in schema (using DataFrame writer with partitionBy preserves
   * the ordering, unlike DDL which normalizes partition cols to the end). PR #6609's
   * V2StreamingSchemaReorder rule does not currently survive combined with rate limiting; see
   * {@code BUG_CATALOG.md} Bug #21.
   */
  @Test
  public void testMaxBytes_onPartitionedTable_partFirst(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    StructType schema =
        new StructType().add("part", DataTypes.IntegerType).add("id", DataTypes.IntegerType);
    for (int i = 0; i < 5; i++) {
      spark
          .createDataFrame(Arrays.asList(RowFactory.create(i, i)), schema)
          .coalesce(1)
          .write()
          .format("delta")
          .partitionBy("part")
          .mode("append")
          .save(tablePath);
    }

    assertOneFilePerBatch(tablePath, "maxBytes_part_first", /* expectedBatches= */ 5);
  }

  /**
   * DV-enabled table: 3 single-file commits, then DELETE half the rows (DV-only commit), then 2
   * more single-file commits. Rate-limited stream with {@code maxBytesPerTrigger=1b} reads the
   * latest snapshot's surviving files - the DV-only delete is metadata and doesn't surface as new
   * data on the source side. Expect 5 non-empty batches (one per AddFile).
   */
  @Test
  public void testMaxBytes_withDvDelete_acrossBatches(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT) USING delta "
                + "TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')",
            tablePath));
    // First batch of files
    for (int i = 0; i < 3; i++) {
      appendIntRow(tablePath, i);
    }
    // DV-only delete (does not rewrite files)
    spark.sql(str("DELETE FROM delta.`%s` WHERE id = 1", tablePath));
    // More files after the DV delete
    for (int i = 3; i < 5; i++) {
      appendIntRow(tablePath, i);
    }

    assertOneFilePerBatch(tablePath, "maxBytes_dv_delete_across", /* expectedBatches= */ 5);
  }

  /**
   * Single commit with 5 files (via {@code repartition(5)}), {@code maxBytesPerTrigger=1b}. The
   * rate limiter must split the within-commit AddFile sequence across batches - one file per batch
   * - even though all files came from the same Delta version. Expect 5 non-empty batches.
   */
  @Test
  public void testMaxBytes_withMultiFileCommit(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (id INT) USING delta", tablePath));
    // One commit producing 5 files via repartition.
    spark
        .range(0, 5)
        .selectExpr("cast(id as int) as id")
        .repartition(5)
        .write()
        .format("delta")
        .mode("append")
        .save(tablePath);

    assertOneFilePerBatch(tablePath, "maxBytes_multifile_commit", /* expectedBatches= */ 5);
  }

  /**
   * First run with {@code maxBytesPerTrigger=1b} drains 5 single-file commits across 5 batches.
   * After 5 more single-file commits, restart with {@code maxBytesPerTrigger=100gb}; the second run
   * should drain everything in a single batch. Uses parquet sink so the checkpoint can be replayed.
   */
  @Test
  public void testMaxBytes_changeBetweenRuns(@TempDir File baseDir) throws Exception {
    File inputDir = new File(baseDir, "input");
    File outputDir = new File(baseDir, "output");
    File checkpointDir = new File(baseDir, "checkpoint");
    String tablePath = inputDir.getAbsolutePath();
    for (int i = 0; i < 5; i++) {
      appendIntRow(tablePath, i);
    }

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);

    // First run: 1b -> 5 batches of 1 row each
    StreamingQuery q1 =
        spark
            .readStream()
            .option("maxBytesPerTrigger", "1b")
            .table(dsv2TableRef)
            .writeStream()
            .format("parquet")
            .option("path", outputDir.getAbsolutePath())
            .option("checkpointLocation", checkpointDir.getAbsolutePath())
            .start();
    try {
      q1.processAllAvailable();
      StreamingQueryProgress[] progress = nonEmptyProgress(q1);
      assertEquals(5, progress.length, "first run expected 5 batches with 1b limit");
      for (StreamingQueryProgress p : progress) {
        assertEquals(1L, p.numInputRows());
      }
    } finally {
      q1.stop();
      DeltaLog.clearCache();
    }

    // Add 5 more commits
    for (int i = 5; i < 10; i++) {
      appendIntRow(tablePath, i);
    }

    // Second run: 100gb -> all 5 new rows in 1 batch
    StreamingQuery q2 =
        spark
            .readStream()
            .option("maxBytesPerTrigger", "100gb")
            .table(dsv2TableRef)
            .writeStream()
            .format("parquet")
            .option("path", outputDir.getAbsolutePath())
            .option("checkpointLocation", checkpointDir.getAbsolutePath())
            .start();
    try {
      q2.processAllAvailable();
      StreamingQueryProgress[] progress = nonEmptyProgress(q2);
      assertEquals(1, progress.length, "second run expected 1 batch with 100gb limit");
      assertEquals(5L, progress[0].numInputRows());
      List<Row> all = spark.read().parquet(outputDir.getAbsolutePath()).collectAsList();
      assertEquals(10, all.size(), "all 10 rows should be present in the sink");
    } finally {
      q2.stop();
      DeltaLog.clearCache();
    }
  }

  /**
   * Both rate limits set: {@code maxFilesPerTrigger=5} and {@code maxBytesPerTrigger=1b}. The
   * tighter limit (bytes=1b) should win and admit exactly one file per batch.
   *
   * <p>Note: {@link V2RateLimitStreamingTest#testMaxBytesAndMaxFilesTogether} covers both
   * directions (files-wins and bytes-wins). This test pins the bytes-wins case under the
   * matrix-pattern style for symmetry with the other rows in this file; it is not duplicated.
   */
  @Test
  public void testMaxBytes_combinedWithMaxFiles(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (id INT) USING delta", tablePath));
    for (int i = 0; i < 5; i++) {
      appendIntRow(tablePath, i);
    }

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    StreamingQuery q =
        spark
            .readStream()
            .option("maxFilesPerTrigger", "5")
            .option("maxBytesPerTrigger", "1b")
            .table(dsv2TableRef)
            .writeStream()
            .format("memory")
            .queryName("maxBytes_combined_with_maxfiles")
            .outputMode("append")
            .start();
    try {
      q.processAllAvailable();
      StreamingQueryProgress[] progress = nonEmptyProgress(q);
      assertEquals(5, progress.length, "bytes=1b should dominate over files=5");
      for (StreamingQueryProgress p : progress) {
        assertEquals(1L, p.numInputRows());
      }
    } finally {
      q.stop();
      DeltaLog.clearCache();
    }
  }
}
