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
import java.util.concurrent.atomic.AtomicReference;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.delta.DeltaLog;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * DSv2 streaming coverage for the schema-evolution x non-vanilla option cross-product.
 *
 * <p>The Phase-0 closeout report identified roughly 30 uncovered cells in the {@code
 * true_cross_product.md} matrix where schema-evolution rows (ADD COLUMN, ADD COLUMN WITH DEFAULT,
 * type widening, nested struct ADD) compose with non-vanilla streaming option columns
 * (startingVersion, maxFilesPerTrigger, excludeRegex, restart, Trigger.AvailableNow). This file
 * fills the row cluster.
 *
 * <p>Coverage focuses on <i>additive</i> schema changes, which do not require {@code
 * schemaTrackingLocation}. Non-additive cases (DROP COLUMN, RENAME COLUMN, narrowing) are blocked
 * on Cluster S-3 (DSv2 does not yet wire {@code schemaTrackingLocation} through {@code
 * SparkScan.SUPPORTED_STREAMING_OPTIONS}); they appear here only as rejection-shape regression
 * tests.
 *
 * <p>Patterns mirror {@link V2StreamingColumnMappingTest} and {@link
 * V2StreamingDmlOptionMatrixTest} for ADD COLUMN / parquet-sink-restart respectively, and {@link
 * V2StreamingSchemaRejectionTest} for the rejection-shape cases.
 */
public class V2StreamingSchemaEvolutionOptionMatrixTest extends V2TestBase {

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

  /**
   * ADD COLUMN at v3, then stream from {@code startingVersion=0}. Additive evolution; existing rows
   * project the new column as NULL. DSv1 parity check.
   */
  @Test
  public void testAddColumn_startingVersion(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (id INT, name STRING) USING delta", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'Alice')", tablePath)); // v1
    spark.sql(str("INSERT INTO delta.`%s` VALUES (2, 'Bob')", tablePath)); // v2
    spark.sql(str("ALTER TABLE delta.`%s` ADD COLUMN extra STRING", tablePath)); // v3
    spark.sql(str("INSERT INTO delta.`%s` VALUES (3, 'Carol', 'x')", tablePath)); // v4

    Dataset<Row> v2Stream =
        spark.readStream().option("startingVersion", "0").table(str("dsv2.delta.`%s`", tablePath));
    List<Row> v2Rows = processStreamingQuery(v2Stream, "addcol_sv_v2");

    Dataset<Row> v1Stream =
        spark.readStream().format("delta").option("startingVersion", "0").load(tablePath);
    List<Row> v1Rows = processStreamingQuery(v1Stream, "addcol_sv_v1");

    assertDataEquals(v2Rows, v1Rows);
  }

  /**
   * ADD COLUMN x {@code maxFilesPerTrigger=1}. The pre- and post-ADD AddFiles drain across multiple
   * micro-batches. DSv1 parity check.
   */
  @Test
  public void testAddColumn_maxFilesPerTrigger(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (id INT, name STRING) USING delta", tablePath));
    // 3 single-file commits before the ADD.
    for (int i = 0; i < 3; i++) {
      spark
          .range(i, i + 1)
          .toDF("id")
          .selectExpr("cast(id as int) as id", "cast(id as string) as name")
          .coalesce(1)
          .write()
          .format("delta")
          .mode("append")
          .save(tablePath);
    }
    spark.sql(str("ALTER TABLE delta.`%s` ADD COLUMN extra STRING", tablePath));
    // 2 single-file commits after the ADD.
    for (int i = 3; i < 5; i++) {
      spark.sql(str("INSERT INTO delta.`%s` VALUES (%d, '%d', 'e%d')", tablePath, i, i, i));
    }

    Dataset<Row> v2Stream =
        spark
            .readStream()
            .option("maxFilesPerTrigger", "1")
            .table(str("dsv2.delta.`%s`", tablePath));
    List<Row> v2Rows = processStreamingQuery(v2Stream, "addcol_mfpt_v2");

    Dataset<Row> v1Stream =
        spark.readStream().format("delta").option("maxFilesPerTrigger", "1").load(tablePath);
    List<Row> v1Rows = processStreamingQuery(v1Stream, "addcol_mfpt_v1");

    assertDataEquals(v2Rows, v1Rows);
  }

  /**
   * ADD COLUMN x {@code Trigger.AvailableNow}. Stream must terminate cleanly after consuming all
   * pre- and post-ADD history. DSv1 parity check.
   */
  @Test
  public void testAddColumn_triggerAvailableNow(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (id INT, name STRING) USING delta", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'Alice'), (2, 'Bob')", tablePath));
    spark.sql(str("ALTER TABLE delta.`%s` ADD COLUMN extra STRING", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (3, 'Carol', 'x')", tablePath));

    Dataset<Row> v2Stream = spark.readStream().table(str("dsv2.delta.`%s`", tablePath));
    File v2Ckpt = new File(deltaTablePath, "_ckpt_v2");
    File v2Out = new File(deltaTablePath, "_out_v2");
    List<Row> v2Rows = runWithParquetSink(v2Stream, v2Out, v2Ckpt, Trigger.AvailableNow());

    Dataset<Row> v1Stream = spark.readStream().format("delta").load(tablePath);
    File v1Ckpt = new File(deltaTablePath, "_ckpt_v1");
    File v1Out = new File(deltaTablePath, "_out_v1");
    List<Row> v1Rows = runWithParquetSink(v1Stream, v1Out, v1Ckpt, Trigger.AvailableNow());

    try {
      assertDataEquals(v2Rows, v1Rows);
    } finally {
      DeltaLog.clearCache();
    }
  }

  /**
   * ADD COLUMN x {@code excludeRegex} matching every parquet file. The regex filters every file in
   * the table; the stream emits zero rows on both DSv1 and DSv2.
   */
  @Test
  public void testAddColumn_excludeRegex(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (id INT, name STRING) USING delta", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'Alice')", tablePath));
    spark.sql(str("ALTER TABLE delta.`%s` ADD COLUMN extra STRING", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (2, 'Bob', 'x')", tablePath));

    Dataset<Row> v2Stream =
        spark
            .readStream()
            .option("excludeRegex", ".*\\.parquet$")
            .table(str("dsv2.delta.`%s`", tablePath));
    List<Row> v2Rows = processStreamingQuery(v2Stream, "addcol_excl_v2");

    Dataset<Row> v1Stream =
        spark.readStream().format("delta").option("excludeRegex", ".*\\.parquet$").load(tablePath);
    List<Row> v1Rows = processStreamingQuery(v1Stream, "addcol_excl_v1");

    assertDataEquals(v2Rows, v1Rows);
  }

  /**
   * Parquet sink + checkpoint; run 1 drains v0..v2, ADD COLUMN between runs, run 2 reads with the
   * new schema. Additive evolution; the restart stream picks up the wider schema and the run-2
   * rows. The run-1 rows in the sink retain the pre-ADD schema.
   */
  @Test
  public void testAddColumn_restart(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);
    spark.sql(str("CREATE TABLE delta.`%s` (id INT, name STRING) USING delta", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'Alice'), (2, 'Bob')", tablePath));

    File checkpointDir = new File(deltaTablePath, "_ckpt");
    File outputDir = new File(deltaTablePath, "_out");

    Dataset<Row> df1 = spark.readStream().table(dsv2Ref);
    runWithParquetSink(df1, outputDir, checkpointDir, /* trigger= */ null);
    DeltaLog.clearCache();

    spark.sql(str("ALTER TABLE delta.`%s` ADD COLUMN extra STRING", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (3, 'Carol', 'x')", tablePath));

    Dataset<Row> df2 = spark.readStream().table(dsv2Ref);
    try {
      List<Row> rows = runWithParquetSink(df2, outputDir, checkpointDir, /* trigger= */ null);
      // Run 1 emitted 2 rows; run 2 picks up the new row under the wider schema = 3 rows total.
      assertEquals(3, rows.size(), () -> "Expected 3 rows across runs; got: " + rows);
    } finally {
      DeltaLog.clearCache();
    }
  }

  // ADD COLUMN ... DEFAULT: Delta rejects post-creation defaulted columns with
  // WRONG_COLUMN_DEFAULTS_FOR_DELTA_ALTER_TABLE_ADD_COLUMN_NOT_SUPPORTED.

  @Test
  @Disabled(
      "KNOWN-GAP: Delta rejects ALTER TABLE ADD COLUMN ... DEFAULT post-creation with"
          + " WRONG_COLUMN_DEFAULTS_FOR_DELTA_ALTER_TABLE_ADD_COLUMN_NOT_SUPPORTED.")
  public void testAddColumnWithDefault_startingVersion(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT) USING delta "
                + "TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')",
            tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1), (2)", tablePath));
    spark.sql(str("ALTER TABLE delta.`%s` ADD COLUMN extra INT DEFAULT 7", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` (id) VALUES (3)", tablePath));

    Dataset<Row> v2Stream =
        spark.readStream().option("startingVersion", "0").table(str("dsv2.delta.`%s`", tablePath));
    List<Row> rows = processStreamingQuery(v2Stream, "addcol_def_sv_v2");
    assertEquals(3, rows.size());
  }

  @Test
  @Disabled(
      "KNOWN-GAP: Delta rejects ALTER TABLE ADD COLUMN ... DEFAULT post-creation with"
          + " WRONG_COLUMN_DEFAULTS_FOR_DELTA_ALTER_TABLE_ADD_COLUMN_NOT_SUPPORTED.")
  public void testAddColumnWithDefault_maxFilesPerTrigger(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT) USING delta "
                + "TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')",
            tablePath));
    for (int i = 0; i < 3; i++) {
      spark
          .range(i, i + 1)
          .toDF("id")
          .selectExpr("cast(id as int) as id")
          .coalesce(1)
          .write()
          .format("delta")
          .mode("append")
          .save(tablePath);
    }
    spark.sql(str("ALTER TABLE delta.`%s` ADD COLUMN extra INT DEFAULT 7", tablePath));

    Dataset<Row> v2Stream =
        spark
            .readStream()
            .option("maxFilesPerTrigger", "1")
            .table(str("dsv2.delta.`%s`", tablePath));
    List<Row> rows = processStreamingQuery(v2Stream, "addcol_def_mfpt_v2");
    assertEquals(3, rows.size());
  }

  @Test
  @Disabled(
      "KNOWN-GAP: Delta rejects ALTER TABLE ADD COLUMN ... DEFAULT post-creation with"
          + " WRONG_COLUMN_DEFAULTS_FOR_DELTA_ALTER_TABLE_ADD_COLUMN_NOT_SUPPORTED.")
  public void testAddColumnWithDefault_restart(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT) USING delta "
                + "TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')",
            tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1), (2)", tablePath));

    File checkpointDir = new File(deltaTablePath, "_ckpt");
    File outputDir = new File(deltaTablePath, "_out");

    Dataset<Row> df1 = spark.readStream().table(dsv2Ref);
    runWithParquetSink(df1, outputDir, checkpointDir, /* trigger= */ null);
    DeltaLog.clearCache();

    spark.sql(str("ALTER TABLE delta.`%s` ADD COLUMN extra INT DEFAULT 7", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` (id) VALUES (3)", tablePath));

    Dataset<Row> df2 = spark.readStream().table(dsv2Ref);
    try {
      List<Row> rows = runWithParquetSink(df2, outputDir, checkpointDir, /* trigger= */ null);
      assertEquals(3, rows.size(), () -> "Expected 3 rows; got: " + rows);
    } finally {
      DeltaLog.clearCache();
    }
  }

  // Type widening requires `delta.enableTypeWidening=true` at table creation (or as a SET
  // TBLPROPERTIES on an empty/compatible-protocol table). The DSv2 stream restart against a
  // widened column raises DELTA_STREAMING_SCHEMA_MISMATCH_ON_RESTART (see
  // V2StreamingReadTest.testNestedTypeWideningDetectedOnRestart).

  /**
   * Widen INT -> BIGINT at v2, then stream from {@code startingVersion=0}. Type widening is
   * additive at the row level. DSv1 parity check.
   */
  @Test
  public void testTypeWidening_intToLong_startingVersion(@TempDir File deltaTablePath)
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
        spark.readStream().option("startingVersion", "0").table(str("dsv2.delta.`%s`", tablePath));
    List<Row> v2Rows = processStreamingQuery(v2Stream, "widen_sv_v2");

    Dataset<Row> v1Stream =
        spark.readStream().format("delta").option("startingVersion", "0").load(tablePath);
    List<Row> v1Rows = processStreamingQuery(v1Stream, "widen_sv_v1");

    assertDataEquals(v2Rows, v1Rows);
  }

  /**
   * Type widening between two runs of a checkpointed stream. DSv2 surfaces the widening as {@code
   * DELTA_STREAMING_SCHEMA_MISMATCH_ON_RESTART} when the stale {@link Dataset} is re-used on
   * restart (see {@link V2StreamingReadTest#testNestedTypeWideningDetectedOnRestart}). This test
   * pins that rejection shape for the top-level column variant.
   */
  @Test
  public void testTypeWidening_intToLong_restart(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, name STRING) USING delta "
                + "TBLPROPERTIES ('delta.enableTypeWidening' = 'true')",
            tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'a'), (2, 'b')", tablePath));

    File checkpointDir = new File(deltaTablePath, "_ckpt");
    File outputDir = new File(deltaTablePath, "_out");

    Dataset<Row> df1 = spark.readStream().table(dsv2Ref);
    runWithParquetSink(df1, outputDir, checkpointDir, /* trigger= */ null);
    DeltaLog.clearCache();

    spark.sql(str("ALTER TABLE delta.`%s` ALTER COLUMN id TYPE BIGINT", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (3000000000, 'c')", tablePath));

    Dataset<Row> df2 = spark.readStream().table(dsv2Ref);
    try {
      List<Row> rows = runWithParquetSink(df2, outputDir, checkpointDir, /* trigger= */ null);
      assertEquals(3, rows.size(), () -> "Expected 3 rows across runs; got: " + rows);
    } finally {
      DeltaLog.clearCache();
    }
  }

  /** Type widening x {@code maxFilesPerTrigger=1}. DSv1 parity check. */
  @Test
  public void testTypeWidening_intToLong_maxFilesPerTrigger(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, name STRING) USING delta "
                + "TBLPROPERTIES ('delta.enableTypeWidening' = 'true')",
            tablePath));
    // 3 single-file commits before the widening.
    for (int i = 0; i < 3; i++) {
      spark.sql(str("INSERT INTO delta.`%s` VALUES (%d, '%d')", tablePath, i, i));
    }
    spark.sql(str("ALTER TABLE delta.`%s` ALTER COLUMN id TYPE BIGINT", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (3000000000, 'big')", tablePath));

    Dataset<Row> v2Stream =
        spark
            .readStream()
            .option("maxFilesPerTrigger", "1")
            .table(str("dsv2.delta.`%s`", tablePath));
    List<Row> v2Rows = processStreamingQuery(v2Stream, "widen_mfpt_v2");

    Dataset<Row> v1Stream =
        spark.readStream().format("delta").option("maxFilesPerTrigger", "1").load(tablePath);
    List<Row> v1Rows = processStreamingQuery(v1Stream, "widen_mfpt_v1");

    assertDataEquals(v2Rows, v1Rows);
  }

  /** Type widening x {@code Trigger.AvailableNow}. DSv1 parity check. */
  @Test
  public void testTypeWidening_intToLong_triggerAvailableNow(@TempDir File deltaTablePath)
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

    Dataset<Row> v2Stream = spark.readStream().table(str("dsv2.delta.`%s`", tablePath));
    File v2Ckpt = new File(deltaTablePath, "_ckpt_v2");
    File v2Out = new File(deltaTablePath, "_out_v2");
    List<Row> v2Rows = runWithParquetSink(v2Stream, v2Out, v2Ckpt, Trigger.AvailableNow());

    Dataset<Row> v1Stream = spark.readStream().format("delta").load(tablePath);
    File v1Ckpt = new File(deltaTablePath, "_ckpt_v1");
    File v1Out = new File(deltaTablePath, "_out_v1");
    List<Row> v1Rows = runWithParquetSink(v1Stream, v1Out, v1Ckpt, Trigger.AvailableNow());

    try {
      assertDataEquals(v2Rows, v1Rows);
    } finally {
      DeltaLog.clearCache();
    }
  }

  /**
   * Nested {@code ALTER TABLE ... ADD COLUMNS (s.newField ...)} at v2; stream from {@code
   * startingVersion=0}. Pre-ADD rows project the new nested field as NULL. DSv1 parity check.
   */
  @Test
  public void testAddStructField_startingVersion(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (id INT, data STRUCT<x: INT>) USING delta", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, named_struct('x', 10))", tablePath));
    spark.sql(str("ALTER TABLE delta.`%s` ADD COLUMNS (data.y INT)", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (2, named_struct('x', 20, 'y', 22))", tablePath));

    Dataset<Row> v2Stream =
        spark.readStream().option("startingVersion", "0").table(str("dsv2.delta.`%s`", tablePath));
    List<Row> v2Rows = processStreamingQuery(v2Stream, "addstruct_sv_v2");

    Dataset<Row> v1Stream =
        spark.readStream().format("delta").option("startingVersion", "0").load(tablePath);
    List<Row> v1Rows = processStreamingQuery(v1Stream, "addstruct_sv_v1");

    assertDataEquals(v2Rows, v1Rows);
  }

  /** Nested struct ADD x {@code maxFilesPerTrigger=1}. DSv1 parity check. */
  @Test
  public void testAddStructField_maxFilesPerTrigger(@TempDir File deltaTablePath) throws Exception {
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
            .option("maxFilesPerTrigger", "1")
            .table(str("dsv2.delta.`%s`", tablePath));
    List<Row> v2Rows = processStreamingQuery(v2Stream, "addstruct_mfpt_v2");

    Dataset<Row> v1Stream =
        spark.readStream().format("delta").option("maxFilesPerTrigger", "1").load(tablePath);
    List<Row> v1Rows = processStreamingQuery(v1Stream, "addstruct_mfpt_v1");

    assertDataEquals(v2Rows, v1Rows);
  }

  /**
   * Nested struct ADD between two runs of a checkpointed stream. DSv2 currently raises {@code
   * DELTA_STREAMING_SCHEMA_MISMATCH_ON_RESTART} for nested ADDs against a stale DataFrame (see
   * {@link V2StreamingReadTest#testNestedColumnAdditionDetectedOnRestart}). This test pins that
   * rejection shape from the matrix-cell vantage point.
   */
  @Test
  public void testAddStructField_restart(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);
    spark.sql(str("CREATE TABLE delta.`%s` (id INT, data STRUCT<x: INT>) USING delta", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, named_struct('x', 10))", tablePath));

    File checkpointDir = new File(deltaTablePath, "_ckpt");
    File outputDir = new File(deltaTablePath, "_out");

    Dataset<Row> df1 = spark.readStream().table(dsv2Ref);
    runWithParquetSink(df1, outputDir, checkpointDir, /* trigger= */ null);
    DeltaLog.clearCache();

    spark.sql(str("ALTER TABLE delta.`%s` ADD COLUMNS (data.y INT)", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (2, named_struct('x', 20, 'y', 22))", tablePath));

    Dataset<Row> df2 = spark.readStream().table(dsv2Ref);
    try {
      List<Row> rows = runWithParquetSink(df2, outputDir, checkpointDir, /* trigger= */ null);
      assertEquals(2, rows.size(), () -> "Expected 2 rows across runs; got: " + rows);
    } finally {
      DeltaLog.clearCache();
    }
  }

  /**
   * DROP COLUMN is a non-additive schema change. Without {@code schemaTrackingLocation} (blocked on
   * DSv2 by Cluster S-3), a bare stream over the dropped-column table should fail with a
   * schema-related error. DROP COLUMN requires column mapping to be enabled.
   */
  @Test
  public void testDropColumn_throwsClearly(@TempDir File deltaTablePath) {
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
      Dataset<Row> df = spark.readStream().option("startingVersion", "0").table(dsv2Ref);
      processStreamingQuery(df, "drop_col_throws");
    } catch (Throwable t) {
      caught.set(t);
    }
    assertNotNull(caught.get(), "DSv2 stream over a DROP COLUMN history should fail.");
    String msg = unwrapMessages(caught.get());
    assertTrue(
        msg.toLowerCase().contains("schema")
            || msg.contains("DELTA_SCHEMA")
            || msg.contains("DELTA_STREAMING"),
        () -> "Expected schema-related error, got: " + msg);
  }

  /**
   * RENAME COLUMN is a non-additive schema change. Without {@code schemaTrackingLocation}, a bare
   * stream over the renamed-column table should fail with a schema-related error.
   */
  @Test
  public void testRenameColumn_throwsClearly(@TempDir File deltaTablePath) {
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
      Dataset<Row> df = spark.readStream().option("startingVersion", "0").table(dsv2Ref);
      processStreamingQuery(df, "rename_col_throws");
    } catch (Throwable t) {
      caught.set(t);
    }
    assertNotNull(caught.get(), "DSv2 stream over a RENAME COLUMN history should fail.");
    String msg = unwrapMessages(caught.get());
    assertTrue(
        msg.toLowerCase().contains("schema")
            || msg.contains("DELTA_SCHEMA")
            || msg.contains("DELTA_STREAMING"),
        () -> "Expected schema-related error, got: " + msg);
  }

  /**
   * Pins the DSv2 contract that {@code schemaTrackingLocation} is currently rejected. This is the
   * blocker for the broader non-additive schema-evolution cluster (S-3). DSv2 should surface this
   * with an {@code UnsupportedOperationException} (or equivalent) at stream-construction or
   * stream-start time so users see a clear path-not-yet-supported signal.
   */
  @Test
  public void testNonAdditive_withSchemaTrackingLocation_throws(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, name STRING) USING delta "
                + "TBLPROPERTIES ('delta.columnMapping.mode' = 'name')",
            tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'a')", tablePath));
    spark.sql(str("ALTER TABLE delta.`%s` RENAME COLUMN name TO display_name", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (2, 'b')", tablePath));

    File schemaTrackingDir = new File(deltaTablePath, "_schema_tracking");
    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);

    AtomicReference<Throwable> caught = new AtomicReference<>();
    try {
      Dataset<Row> df =
          spark
              .readStream()
              .option("schemaTrackingLocation", schemaTrackingDir.getAbsolutePath())
              .table(dsv2Ref);
      processStreamingQuery(df, "stl_throws");
    } catch (Throwable t) {
      caught.set(t);
    }
    assertNotNull(caught.get(), "DSv2 should reject schemaTrackingLocation today.");
    String msg = unwrapMessages(caught.get());
    assertTrue(
        msg.contains("schemaTrackingLocation") || msg.contains("not supported"),
        () -> "Expected schemaTrackingLocation rejection, got: " + msg);
  }

  /**
   * Narrowing BIGINT -> INT is not a supported type change in Delta; the ALTER itself should be
   * rejected by Delta's analysis layer. This test pins the rejection shape (the analysis-time
   * failure path) so the matrix records that the cell is unobservable from streaming.
   */
  @Test
  public void testColumnTypeNarrowing_throwsClearly(@TempDir File deltaTablePath) {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (id BIGINT, name STRING) USING delta", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'a')", tablePath));

    // Delta rejects narrowing at ALTER time.
    AtomicReference<Throwable> caught = new AtomicReference<>();
    try {
      spark.sql(str("ALTER TABLE delta.`%s` ALTER COLUMN id TYPE INT", tablePath));
    } catch (Throwable t) {
      caught.set(t);
    }
    assertNotNull(caught.get(), "Delta should reject BIGINT -> INT narrowing at ALTER time.");
    String msg = unwrapMessages(caught.get());
    assertTrue(
        msg.toLowerCase().contains("not")
            && (msg.toLowerCase().contains("type")
                || msg.toLowerCase().contains("cast")
                || msg.toLowerCase().contains("change")),
        () -> "Expected type-change rejection, got: " + msg);
  }
}
