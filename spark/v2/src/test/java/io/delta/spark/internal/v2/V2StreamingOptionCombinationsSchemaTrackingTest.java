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
 * Streaming-option x streaming-option matrix: {@code schemaTrackingLocation} crossed with the other
 * non-vanilla streaming option columns ({@code maxFilesPerTrigger}, {@code maxBytesPerTrigger},
 * restart, {@code Trigger.AvailableNow}, {@code excludeRegex}).
 *
 * <p>PR #6697 (kernel-spark: support non-additive schema evolution in v2 connector) wired {@code
 * schemaTrackingLocation} through {@link
 * io.delta.spark.internal.v2.read.SparkScan#SUPPORTED_STREAMING_OPTIONS} so the V2 connector can
 * read past a {@code RENAME COLUMN} (or {@code DROP COLUMN}). The single-option case is covered by
 * {@link V2StreamingColumnMappingTest#testColumnMapping_renameColumnUnsafe} (legacy unsafe-flag
 * path) and {@link V2StreamingCmRtDvTest#testRenameColumn_cmName_rt_dv} (schema-tracking path); the
 * cross combinations with the other non-vanilla streaming options were untested before this file.
 *
 * <p>Pattern: each test creates a CM-name table, applies a non-additive {@code RENAME COLUMN}, then
 * exercises a stream that combines {@code schemaTrackingLocation} with another streaming option.
 * {@code schemaTrackingLocation} points at a subdirectory under the checkpoint dir, which DSv2
 * requires.
 */
public class V2StreamingOptionCombinationsSchemaTrackingTest extends V2TestBase {

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

  /**
   * Creates a column-mapping (name mode) Delta table at {@code tablePath} with schema {@code (id
   * INT, name STRING)} and inserts {@code count} rows shaped {@code (i, "name-i")}.
   */
  private void createCmNameTableWithRows(String tablePath, int count) {
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, name STRING) USING delta "
                + "TBLPROPERTIES ('delta.columnMapping.mode' = 'name')",
            tablePath));
    for (int i = 0; i < count; i++) {
      spark.sql(str("INSERT INTO delta.`%s` VALUES (%d, 'name-%d')", tablePath, i, i));
    }
  }

  /** 1. schemaTrackingLocation x maxFilesPerTrigger=1. */
  @Test
  public void testSchemaTracking_withMaxFilesPerTrigger(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    File checkpointDir = new File(deltaTablePath, "_ckpt");
    File schemaTrackingDir = new File(checkpointDir, "_schema_tracking");
    File outputDir = new File(deltaTablePath, "_out");
    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);

    // 3 pre-rename single-file commits.
    createCmNameTableWithRows(tablePath, 3);
    // Non-additive change.
    spark.sql(str("ALTER TABLE delta.`%s` RENAME COLUMN name TO display_name", tablePath));
    // 2 post-rename single-file commits.
    for (int i = 3; i < 5; i++) {
      spark.sql(str("INSERT INTO delta.`%s` VALUES (%d, 'name-%d')", tablePath, i, i));
    }

    Dataset<Row> df =
        spark
            .readStream()
            .option("schemaTrackingLocation", schemaTrackingDir.getAbsolutePath())
            .option("maxFilesPerTrigger", "1")
            .table(dsv2Ref);
    List<Row> rows = runWithParquetSink(df, outputDir, checkpointDir, /* trigger= */ null);
    DeltaLog.clearCache();

    // All 5 rows should surface across multiple micro-batches; post-rename column carries the new
    // logical name `display_name` and is read at ordinal 1 in the parquet sink.
    assertEquals(5, rows.size(), () -> "Expected 5 rows across rate-limited drain; got: " + rows);
    assertTrue(
        rows.stream().anyMatch(r -> "name-4".equals(r.getString(1))),
        () -> "post-rename row should be present, got: " + rows);
  }

  /** 2. schemaTrackingLocation x maxBytesPerTrigger=1b. */
  @Test
  public void testSchemaTracking_withMaxBytesPerTrigger(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    File checkpointDir = new File(deltaTablePath, "_ckpt");
    File schemaTrackingDir = new File(checkpointDir, "_schema_tracking");
    File outputDir = new File(deltaTablePath, "_out");
    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);

    createCmNameTableWithRows(tablePath, 3);
    spark.sql(str("ALTER TABLE delta.`%s` RENAME COLUMN name TO display_name", tablePath));
    for (int i = 3; i < 5; i++) {
      spark.sql(str("INSERT INTO delta.`%s` VALUES (%d, 'name-%d')", tablePath, i, i));
    }

    Dataset<Row> df =
        spark
            .readStream()
            .option("schemaTrackingLocation", schemaTrackingDir.getAbsolutePath())
            .option("maxBytesPerTrigger", "1b")
            .table(dsv2Ref);
    List<Row> rows = runWithParquetSink(df, outputDir, checkpointDir, /* trigger= */ null);
    DeltaLog.clearCache();

    // maxBytesPerTrigger=1b still admits at least one file per batch (DSv1 parity behavior); the
    // assertion that matters is that all 5 rows surface across the rate-limited drain.
    assertEquals(
        5, rows.size(), () -> "Expected 5 rows across byte-rate-limited drain; got: " + rows);
    assertTrue(
        rows.stream().anyMatch(r -> "name-4".equals(r.getString(1))),
        () -> "post-rename row should be present, got: " + rows);
  }

  /** 3. schemaTrackingLocation x restart (RENAME COLUMN between runs). */
  @Test
  public void testSchemaTracking_withRestart(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    File checkpointDir = new File(deltaTablePath, "_ckpt");
    File schemaTrackingDir = new File(checkpointDir, "_schema_tracking");
    File outputDir = new File(deltaTablePath, "_out");
    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);

    createCmNameTableWithRows(tablePath, 3);

    // Run 1: drain the 3 pre-rename rows.
    Dataset<Row> df1 =
        spark
            .readStream()
            .option("schemaTrackingLocation", schemaTrackingDir.getAbsolutePath())
            .table(dsv2Ref);
    List<Row> firstRun = runWithParquetSink(df1, outputDir, checkpointDir, /* trigger= */ null);
    DeltaLog.clearCache();
    assertEquals(
        3, firstRun.size(), () -> "first run should drain 3 pre-rename rows, got: " + firstRun);

    // Non-additive change between runs + post-rename inserts.
    spark.sql(str("ALTER TABLE delta.`%s` RENAME COLUMN name TO display_name", tablePath));
    for (int i = 3; i < 5; i++) {
      spark.sql(str("INSERT INTO delta.`%s` VALUES (%d, 'name-%d')", tablePath, i, i));
    }

    // Run 2: restart from the same checkpoint and schema-tracking log; the schema log carries the
    // pre-rename entry from run 1, so the rename is treated as a non-additive evolution rather than
    // a hard DELTA_STREAMING_SCHEMA_MISMATCH_ON_RESTART.
    Dataset<Row> df2 =
        spark
            .readStream()
            .option("schemaTrackingLocation", schemaTrackingDir.getAbsolutePath())
            .table(dsv2Ref);
    List<Row> secondRun = runWithParquetSink(df2, outputDir, checkpointDir, /* trigger= */ null);
    DeltaLog.clearCache();
    assertEquals(
        2, secondRun.size(), () -> "second run should drain 2 post-rename rows, got: " + secondRun);

    // Sanity: across both runs the parquet sink holds 5 rows; the post-rename row name is read at
    // ordinal 1 under the evolved schema.
    List<Row> all = spark.read().parquet(outputDir.getAbsolutePath()).collectAsList();
    assertEquals(5, all.size(), () -> "Expected 5 rows total across restart; got: " + all);
    assertTrue(
        all.stream().anyMatch(r -> "name-4".equals(r.getString(1))),
        () -> "post-rename row should be present, got: " + all);
  }

  /** 4. schemaTrackingLocation x Trigger.AvailableNow. */
  @Test
  public void testSchemaTracking_withAvailableNow(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    File checkpointDir = new File(deltaTablePath, "_ckpt");
    File schemaTrackingDir = new File(checkpointDir, "_schema_tracking");
    File outputDir = new File(deltaTablePath, "_out");
    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);

    createCmNameTableWithRows(tablePath, 2);
    spark.sql(str("ALTER TABLE delta.`%s` RENAME COLUMN name TO display_name", tablePath));
    for (int i = 2; i < 4; i++) {
      spark.sql(str("INSERT INTO delta.`%s` VALUES (%d, 'name-%d')", tablePath, i, i));
    }

    Dataset<Row> df =
        spark
            .readStream()
            .option("schemaTrackingLocation", schemaTrackingDir.getAbsolutePath())
            .table(dsv2Ref);
    // Trigger.AvailableNow must terminate naturally after draining all available data.
    List<Row> rows = runWithParquetSink(df, outputDir, checkpointDir, Trigger.AvailableNow());
    DeltaLog.clearCache();

    assertEquals(4, rows.size(), () -> "Expected 4 rows from AvailableNow drain; got: " + rows);
    assertTrue(
        rows.stream().anyMatch(r -> "name-3".equals(r.getString(1))),
        () -> "post-rename row should be present, got: " + rows);
  }

  /**
   * 5. schemaTrackingLocation x excludeRegex on a partitioned CM-name table. {@code excludeRegex}
   * matches the file path so a {@code part=x/} prefix filters out that partition's rows.
   */
  @Test
  public void testSchemaTracking_withExcludeRegex(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    File checkpointDir = new File(deltaTablePath, "_ckpt");
    File schemaTrackingDir = new File(checkpointDir, "_schema_tracking");
    File outputDir = new File(deltaTablePath, "_out");
    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);

    // Partitioned CM-name table with two partitions: part=x and part=y.
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, name STRING, part STRING) USING delta "
                + "PARTITIONED BY (part) "
                + "TBLPROPERTIES ('delta.columnMapping.mode' = 'name')",
            tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'a', 'x'), (2, 'b', 'y')", tablePath));
    // Non-additive change.
    spark.sql(str("ALTER TABLE delta.`%s` RENAME COLUMN name TO display_name", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (3, 'c', 'x'), (4, 'd', 'y')", tablePath));

    Dataset<Row> df =
        spark
            .readStream()
            .option("schemaTrackingLocation", schemaTrackingDir.getAbsolutePath())
            // Exclude every file under partition `part=x/...`.
            .option("excludeRegex", ".*part=x/.*")
            .table(dsv2Ref);
    List<Row> rows = runWithParquetSink(df, outputDir, checkpointDir, /* trigger= */ null);
    DeltaLog.clearCache();

    // Only part=y rows survive: (2, 'b', 'y') and (4, 'd', 'y'). The post-rename row reads under
    // the renamed column at ordinal 1.
    assertEquals(2, rows.size(), () -> "Expected 2 rows from part=y only; got: " + rows);
    assertTrue(
        rows.stream().allMatch(r -> "y".equals(r.getString(2))),
        () -> "all surviving rows should be in part=y, got: " + rows);
    assertTrue(
        rows.stream().anyMatch(r -> "d".equals(r.getString(1))),
        () -> "post-rename row (4, 'd', 'y') should be present, got: " + rows);
  }
}
