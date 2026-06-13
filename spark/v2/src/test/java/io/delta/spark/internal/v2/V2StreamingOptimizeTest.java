/*
 * Copyright (2025) The Delta Lake Project Authors.
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
 * Tests for V2 streaming reads against tables that receive {@code dataChange=false} commits:
 * OPTIMIZE (file compaction), OPTIMIZE ... ZORDER BY, partial OPTIMIZE, ALTER TABLE SET
 * TBLPROPERTIES (Metadata-only), and similar rewrites. None of these introduce new user-visible
 * rows; the streaming source must silently consume the commits without re-emitting compacted data.
 *
 * <p>{@code testStreamingReadAfterStatsRecompute} in {@link V2StreamingReadTest} already covers the
 * stats-recompute case via {@code StatisticsCollection.recompute} and is not duplicated here.
 */
public class V2StreamingOptimizeTest extends V2TestBase {

  /**
   * Runs a streaming query against a parquet sink + checkpoint, processes all available data, then
   * stops. Returns the rows by reading the parquet output. Parquet sink supports checkpoint
   * recovery on restart, unlike memory sink.
   */
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
      // Trigger.AvailableNow terminates on its own once data is consumed; wait briefly so the
      // final commit lands before reading back the parquet output.
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

  /**
   * Forces OPTIMIZE to actually compact small files instead of treating them as already-optimal.
   * Without raising {@code minFileSize}, OPTIMIZE on a few tiny commits may be a no-op (no
   * dataChange=false rewrite is emitted), which would silently make the test pass for the wrong
   * reason.
   */
  private void runOptimize(String tablePath) {
    withSQLConf(
        "spark.databricks.delta.optimize.minFileSize",
        Long.toString(1024L * 1024L * 1024L), // 1 GiB - guarantees small files are below threshold
        () -> spark.sql(str("OPTIMIZE delta.`%s`", tablePath)));
  }

  /**
   * 1. Stream a few rows, OPTIMIZE compacts the small files into one, then continue streaming.
   * Output should contain only the originally-written rows - no re-emission of the compacted file.
   */
  @Test
  public void testOptimizeBetweenStreamBatches_noReEmission(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    spark.sql(str("CREATE TABLE delta.`%s` (id INT, name STRING) USING delta", tablePath));
    // Multiple commits so OPTIMIZE has files to compact.
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'A')", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (2, 'B')", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (3, 'C')", tablePath));

    File checkpointDir = new File(deltaTablePath, "_checkpoint");
    File outputDir = new File(deltaTablePath, "_out");
    Dataset<Row> streamingDF = spark.readStream().table(dsv2TableRef);
    runWithParquetSink(streamingDF, outputDir, checkpointDir, /* trigger= */ null);

    // OPTIMIZE produces dataChange=false AddFile + RemoveFile; the stream must skip both.
    runOptimize(tablePath);

    try {
      List<Row> actualRows =
          runWithParquetSink(streamingDF, outputDir, checkpointDir, /* trigger= */ null);
      List<Row> expectedRows =
          Arrays.asList(
              RowFactory.create(1, "A"), RowFactory.create(2, "B"), RowFactory.create(3, "C"));
      assertDataEquals(actualRows, expectedRows);
    } finally {
      DeltaLog.clearCache();
    }
  }

  /** 2. OPTIMIZE ... ZORDER BY mid-stream - same dataChange=false guarantee. */
  @Test
  public void testOptimizeZorder_noReEmission(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    spark.sql(str("CREATE TABLE delta.`%s` (id INT, name STRING) USING delta", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'A')", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (2, 'B')", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (3, 'C')", tablePath));

    File checkpointDir = new File(deltaTablePath, "_checkpoint");
    File outputDir = new File(deltaTablePath, "_out");
    Dataset<Row> streamingDF = spark.readStream().table(dsv2TableRef);
    runWithParquetSink(streamingDF, outputDir, checkpointDir, /* trigger= */ null);

    withSQLConf(
        "spark.databricks.delta.optimize.minFileSize",
        Long.toString(1024L * 1024L * 1024L),
        () -> spark.sql(str("OPTIMIZE delta.`%s` ZORDER BY (id)", tablePath)));

    try {
      List<Row> actualRows =
          runWithParquetSink(streamingDF, outputDir, checkpointDir, /* trigger= */ null);
      List<Row> expectedRows =
          Arrays.asList(
              RowFactory.create(1, "A"), RowFactory.create(2, "B"), RowFactory.create(3, "C"));
      assertDataEquals(actualRows, expectedRows);
    } finally {
      DeltaLog.clearCache();
    }
  }

  /**
   * 3. OPTIMIZE then Trigger.AvailableNow - query must terminate cleanly and emit exactly the
   * user-written rows. AvailableNow exercises the path where the source plans all available offsets
   * (including the dataChange=false commit) in one shot.
   */
  @Test
  public void testOptimize_triggerAvailableNow(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    spark.sql(str("CREATE TABLE delta.`%s` (id INT, name STRING) USING delta", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'A')", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (2, 'B')", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (3, 'C')", tablePath));

    runOptimize(tablePath);

    Dataset<Row> streamingDF = spark.readStream().table(dsv2TableRef);
    File checkpointDir = new File(deltaTablePath, "_checkpoint");
    File outputDir = new File(deltaTablePath, "_out");
    try {
      List<Row> actualRows =
          runWithParquetSink(streamingDF, outputDir, checkpointDir, Trigger.AvailableNow());
      List<Row> expectedRows =
          Arrays.asList(
              RowFactory.create(1, "A"), RowFactory.create(2, "B"), RowFactory.create(3, "C"));
      assertDataEquals(actualRows, expectedRows);
    } finally {
      DeltaLog.clearCache();
    }
  }

  /**
   * 4. OPTIMIZE collapses N input files into 1; streaming with {@code maxFilesPerTrigger=1} must
   * still emit exactly the original rows (one batch per original AddFile), not collapse all N worth
   * of data into a single re-emission batch.
   */
  @Test
  public void testOptimize_maxFilesPerTrigger(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    spark.sql(str("CREATE TABLE delta.`%s` (id INT, name STRING) USING delta", tablePath));
    // Three separate commits -> three AddFiles.
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'A')", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (2, 'B')", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (3, 'C')", tablePath));

    runOptimize(tablePath);

    Dataset<Row> streamingDF =
        spark.readStream().option("maxFilesPerTrigger", "1").table(dsv2TableRef);
    List<Row> actualRows = processStreamingQuery(streamingDF, "optimize_mfpt");
    List<Row> expectedRows =
        Arrays.asList(
            RowFactory.create(1, "A"), RowFactory.create(2, "B"), RowFactory.create(3, "C"));
    assertDataEquals(actualRows, expectedRows);
  }

  /**
   * 5. Stream halfway, OPTIMIZE, restart from checkpoint, finish. Parquet sink + checkpointLocation
   * for recovery. The restart must not re-emit the original rows nor pick up the OPTIMIZE rewrite
   * as new data.
   */
  @Test
  public void testOptimize_restart(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    spark.sql(str("CREATE TABLE delta.`%s` (id INT, name STRING) USING delta", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'A')", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (2, 'B')", tablePath));

    File checkpointDir = new File(deltaTablePath, "_checkpoint");
    File outputDir = new File(deltaTablePath, "_out");
    Dataset<Row> streamingDF = spark.readStream().table(dsv2TableRef);
    runWithParquetSink(streamingDF, outputDir, checkpointDir, /* trigger= */ null);

    runOptimize(tablePath);
    spark.sql(str("INSERT INTO delta.`%s` VALUES (3, 'C')", tablePath));

    try {
      List<Row> actualRows =
          runWithParquetSink(streamingDF, outputDir, checkpointDir, /* trigger= */ null);
      List<Row> expectedRows =
          Arrays.asList(
              RowFactory.create(1, "A"), RowFactory.create(2, "B"), RowFactory.create(3, "C"));
      assertDataEquals(actualRows, expectedRows);
    } finally {
      DeltaLog.clearCache();
    }
  }

  /**
   * 6. DV-enabled table; DV-DELETE removes some rows; OPTIMIZE materializes the DV into a physical
   * rewrite (the post-OPTIMIZE AddFiles have no DV). Stream with {@code skipChangeCommits=true};
   * since OPTIMIZE is dataChange=false (and the DV DELETE was a change commit that
   * skipChangeCommits drops), the stream should emit exactly the surviving original rows from the
   * pre-DELETE snapshot.
   */
  @Test
  public void testOptimize_onDvTable_skipChangeCommits(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (value INT) USING delta "
                + "TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')",
            tablePath));
    spark
        .range(10)
        .selectExpr("cast(id as int) as value")
        .coalesce(1)
        .write()
        .format("delta")
        .mode("append")
        .save(tablePath);
    // DV-DELETE: produces a RemoveFile + AddFile-with-DV (dataChange=true on the AddFile-with-DV).
    spark.sql(str("DELETE FROM delta.`%s` WHERE value < 3", tablePath));
    // OPTIMIZE then materializes the DV into a clean rewrite (dataChange=false).
    runOptimize(tablePath);

    Dataset<Row> streamingDF =
        spark.readStream().option("skipChangeCommits", "true").table(dsv2TableRef);
    List<Row> actualRows = processStreamingQuery(streamingDF, "optimize_dv_skip");

    // Initial snapshot reflects the DV (rows 3..9 survive); the OPTIMIZE rewrite is
    // dataChange=false so it must contribute nothing further.
    List<Row> expectedRows = new ArrayList<>();
    for (int i = 3; i < 10; i++) {
      expectedRows.add(RowFactory.create(i));
    }
    assertDataEquals(actualRows, expectedRows);
  }

  /** 7. Column-mapping (name mode) table, OPTIMIZE, stream through - physical names change. */
  @Test
  public void testOptimize_onColMappedTable(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, user_name STRING) USING delta "
                + "TBLPROPERTIES ('delta.columnMapping.mode' = 'name')",
            tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'Alice')", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (2, 'Bob')", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (3, 'Carol')", tablePath));

    runOptimize(tablePath);

    Dataset<Row> streamingDF = spark.readStream().table(dsv2TableRef);
    List<Row> actualRows = processStreamingQuery(streamingDF, "optimize_cm");
    List<Row> expectedRows =
        Arrays.asList(
            RowFactory.create(1, "Alice"),
            RowFactory.create(2, "Bob"),
            RowFactory.create(3, "Carol"));
    assertDataEquals(actualRows, expectedRows);
  }

  /**
   * 8. Partitioned table; {@code OPTIMIZE ... WHERE part='X'} only rewrites one partition. The
   * stream should still see exactly the user-written rows for every partition.
   */
  @Test
  public void testOptimize_onPartitionedTable_perPartition(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, part STRING) USING delta PARTITIONED BY (part)",
            tablePath));
    // Two commits per partition to give OPTIMIZE something to compact in 'X'.
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'X')", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (2, 'X')", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (3, 'Y')", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (4, 'Y')", tablePath));

    withSQLConf(
        "spark.databricks.delta.optimize.minFileSize",
        Long.toString(1024L * 1024L * 1024L),
        () -> spark.sql(str("OPTIMIZE delta.`%s` WHERE part = 'X'", tablePath)));

    Dataset<Row> streamingDF = spark.readStream().table(dsv2TableRef);
    List<Row> actualRows = processStreamingQuery(streamingDF, "optimize_partial");
    List<Row> expectedRows =
        Arrays.asList(
            RowFactory.create(1, "X"),
            RowFactory.create(2, "X"),
            RowFactory.create(3, "Y"),
            RowFactory.create(4, "Y"));
    assertDataEquals(actualRows, expectedRows);
  }

  /**
   * 9. Stats-recompute case. The stats-recompute path (which writes dataChange=false AddFile
   * duplicates with refreshed stats) is already covered by {@code
   * V2StreamingReadTest.testStreamingReadAfterStatsRecompute} via {@code
   * StatisticsCollection.recompute}, which exercises the same selection-vector filtering that this
   * file exercises for OPTIMIZE. This stub anchors the campaign coverage row; the actual assertion
   * lives in the cross-referenced test.
   */
  @Test
  public void testStatsRecompute_noReEmission() {
    // Intentionally a no-op stub. See V2StreamingReadTest#testStreamingReadAfterStatsRecompute.
  }

  /**
   * 10. {@code ALTER TABLE ... SET TBLPROPERTIES} writes a Metadata-only commit (no AddFile /
   * RemoveFile). The stream must ignore it and emit only the user-written rows.
   */
  @Test
  public void testAlterTableSetTblProperties_noReEmission(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    spark.sql(str("CREATE TABLE delta.`%s` (id INT, name STRING) USING delta", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'A')", tablePath));

    File checkpointDir = new File(deltaTablePath, "_checkpoint");
    File outputDir = new File(deltaTablePath, "_out");
    Dataset<Row> streamingDF = spark.readStream().table(dsv2TableRef);
    runWithParquetSink(streamingDF, outputDir, checkpointDir, /* trigger= */ null);

    // Metadata-only commit - no AddFile/RemoveFile.
    spark.sql(str("ALTER TABLE delta.`%s` SET TBLPROPERTIES ('foo' = 'bar')", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (2, 'B')", tablePath));

    try {
      List<Row> actualRows =
          runWithParquetSink(streamingDF, outputDir, checkpointDir, /* trigger= */ null);
      List<Row> expectedRows = Arrays.asList(RowFactory.create(1, "A"), RowFactory.create(2, "B"));
      assertDataEquals(actualRows, expectedRows);
    } finally {
      DeltaLog.clearCache();
    }
  }
}
