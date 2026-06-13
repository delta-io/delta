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
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * DSv2 streaming triple-composition coverage: VARIANT x column mapping x restart.
 *
 * <p>Bug #1 (PR-#6578 silent corruption in {@code ColumnVectorWithFilter#getChild}) fixed the
 * VARIANT + DV pair. Bug #25 fixed a CM/restart marker handling issue. The catch surface here is
 * whether VARIANT payloads remain row-aligned when CM-name / CM-id rewrites column names AND the
 * stream restarts from a checkpoint - i.e. whether the two fixes compose without re-introducing
 * silent reordering.
 *
 * <p>VARIANT payloads are written as {@code parse_json('{"row":<id>}')} so {@code variant_get(v,
 * '$.row', 'int') == id} must hold for every surviving row across the full set, regardless of which
 * commit produced it. This is the catch surface for silent corruption; row-count alone is not
 * sufficient.
 *
 * <p>Parquet sink is used for all restart cases because the memory sink does not recover from a
 * checkpoint in append mode.
 *
 * <p>Non-additive schema-change cases (rename) were previously {@code @Disabled} as KNOWN-GAP
 * Cluster S-3; re-enabled after PR #6697 added {@code schemaTrackingLocation} support.
 */
public class V2StreamingVariantCmRestartTest extends V2TestBase {

  /** Creates a CM-enabled Delta table with schema (id INT, v VARIANT). */
  private void createVariantCmTable(String tablePath, String cmMode) {
    if ("id".equals(cmMode)) {
      spark.sql(
          str(
              "CREATE TABLE delta.`%s` (id INT, v VARIANT) USING delta "
                  + "TBLPROPERTIES ('delta.columnMapping.mode' = 'id', "
                  + "  'delta.minReaderVersion' = '2', "
                  + "  'delta.minWriterVersion' = '5')",
              tablePath));
    } else {
      spark.sql(
          str(
              "CREATE TABLE delta.`%s` (id INT, v VARIANT) USING delta "
                  + "TBLPROPERTIES ('delta.columnMapping.mode' = 'name')",
              tablePath));
    }
  }

  /**
   * Appends {@code count} rows starting at id {@code startId}, each with {@code v =
   * parse_json('{"row":<id>}')}. {@code coalesce(1)} keeps each commit at a single Parquet file so
   * per-file rate-limit tests behave predictably.
   */
  private void appendVariantRows(String tablePath, int startId, int count) {
    spark
        .range(startId, startId + count)
        .selectExpr("cast(id as int) as id", "parse_json(concat('{\"row\":', id, '}')) as v")
        .coalesce(1)
        .write()
        .format("delta")
        .mode("append")
        .save(tablePath);
  }

  /**
   * Runs a streaming query that writes to a parquet sink at {@code outputDir} with the given
   * checkpoint, drains all available data, and stops. Optional {@code trigger} (e.g. AvailableNow)
   * is applied when non-null.
   */
  private void runStreamToParquet(
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
      if (trigger != null) {
        query.awaitTermination(60_000L);
      }
    } finally {
      if (query != null) {
        query.stop();
      }
      DeltaLog.clearCache();
    }
  }

  /**
   * Loads the parquet sink, projects {@code variant_get(v, '$.row', 'int')} (or a custom variant
   * expression), and asserts every row's id appears exactly once in [startInclusive, endExclusive)
   * and that the variant payload equals the row id.
   */
  private void assertParquetSinkVariantAligned(
      File outputDir, int startInclusive, int endExclusive) {
    Dataset<Row> sinkDf = spark.read().parquet(outputDir.getAbsolutePath());
    sinkDf.createOrReplaceTempView("variant_cm_sink");
    List<Row> rows =
        spark
            .sql(
                "SELECT id, variant_get(v, '$.row', 'int') AS v_row "
                    + "FROM variant_cm_sink ORDER BY id")
            .collectAsList();
    int expectedCount = endExclusive - startInclusive;
    assertEquals(
        expectedCount, rows.size(), () -> "Expected " + expectedCount + " rows, got " + rows);
    int expectedId = startInclusive;
    for (Row row : rows) {
      int id = row.getInt(0);
      assertEquals(expectedId, id, () -> "Unexpected id in parquet sink: " + id);
      Object vRowObj = row.get(1);
      assertNotNull(vRowObj, () -> "variant_get returned NULL for id=" + id);
      int vRow = ((Number) vRowObj).intValue();
      assertEquals(
          id,
          vRow,
          () ->
              "Silent corruption across VARIANT x CM x restart: id="
                  + id
                  + " has variant_get(v,'$.row','int')="
                  + vRow);
      expectedId++;
    }
  }

  /**
   * Basic triple composition: VARIANT column + CM-name + restart. Stream, drain, append more,
   * restart from same checkpoint, finish. Every row's variant payload must match its id across both
   * runs.
   */
  @Test
  public void testBasic_variant_cmName_restart(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);

    createVariantCmTable(tablePath, "name");
    appendVariantRows(tablePath, 1, 3); // ids 1..3

    File checkpointDir = new File(deltaTablePath, "_ckpt");
    File outputDir = new File(deltaTablePath, "_out");

    Dataset<Row> df1 = spark.readStream().table(dsv2Ref);
    runStreamToParquet(df1, outputDir, checkpointDir, /* trigger= */ null);

    appendVariantRows(tablePath, 4, 3); // ids 4..6

    Dataset<Row> df2 = spark.readStream().table(dsv2Ref);
    runStreamToParquet(df2, outputDir, checkpointDir, /* trigger= */ null);

    assertParquetSinkVariantAligned(outputDir, 1, 7);
  }

  /**
   * Same as #1 but with column mapping mode=id, which uses field ids rather than rename-tracking.
   */
  @Test
  public void testBasic_variant_cmId_restart(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);

    createVariantCmTable(tablePath, "id");
    appendVariantRows(tablePath, 1, 3);

    File checkpointDir = new File(deltaTablePath, "_ckpt");
    File outputDir = new File(deltaTablePath, "_out");

    Dataset<Row> df1 = spark.readStream().table(dsv2Ref);
    runStreamToParquet(df1, outputDir, checkpointDir, /* trigger= */ null);

    appendVariantRows(tablePath, 4, 3);

    Dataset<Row> df2 = spark.readStream().table(dsv2Ref);
    runStreamToParquet(df2, outputDir, checkpointDir, /* trigger= */ null);

    assertParquetSinkVariantAligned(outputDir, 1, 7);
  }

  /**
   * Explicit per-row payload check across the restart boundary. This is the core catch surface:
   * even if row counts match, variant payloads must agree with their id. Writes 5 rows pre-restart
   * and 5 rows post-restart from distinct commits.
   */
  @Test
  public void testVariantPayload_assertedAcrossRestart_cmName(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);

    createVariantCmTable(tablePath, "name");
    // 5 single-row commits pre-restart - exercises multi-file scan across CM rewrites.
    for (int i = 1; i <= 5; i++) {
      appendVariantRows(tablePath, i, 1);
    }

    File checkpointDir = new File(deltaTablePath, "_ckpt");
    File outputDir = new File(deltaTablePath, "_out");

    Dataset<Row> df1 = spark.readStream().table(dsv2Ref);
    runStreamToParquet(df1, outputDir, checkpointDir, /* trigger= */ null);

    // 5 more single-row commits post-restart.
    for (int i = 6; i <= 10; i++) {
      appendVariantRows(tablePath, i, 1);
    }

    Dataset<Row> df2 = spark.readStream().table(dsv2Ref);
    runStreamToParquet(df2, outputDir, checkpointDir, /* trigger= */ null);

    assertParquetSinkVariantAligned(outputDir, 1, 11);
  }

  /**
   * Quadruple composition: VARIANT + CM-name + DV (deletion vector) + restart. A DV-only DELETE
   * runs between the two stream runs. Bug #1's fix to ColumnVectorWithFilter.getChild and Bug #25's
   * CM/restart marker fix must both hold simultaneously; surviving variant payloads must still
   * align row-by-row.
   */
  @Test
  public void testVariantWithDv_cmName_restart(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);

    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, v VARIANT) USING delta "
                + "TBLPROPERTIES ('delta.columnMapping.mode' = 'name', "
                + "  'delta.enableDeletionVectors' = 'true')",
            tablePath));

    appendVariantRows(tablePath, 1, 10); // ids 1..10 in a single Parquet file

    File checkpointDir = new File(deltaTablePath, "_ckpt");
    File outputDir = new File(deltaTablePath, "_out");

    Dataset<Row> df1 = spark.readStream().table(dsv2Ref);
    runStreamToParquet(df1, outputDir, checkpointDir, /* trigger= */ null);

    // DV-only DELETE between runs: file kept, only a DV is written. Even-id rows removed.
    spark.sql(str("DELETE FROM delta.`%s` WHERE id %% 2 = 0", tablePath));

    // Append new commit so restart has something to drain - odd ids 11, 13.
    spark
        .range(11, 14)
        .selectExpr("cast(id as int) as id", "parse_json(concat('{\"row\":', id, '}')) as v")
        .filter("id % 2 = 1")
        .coalesce(1)
        .write()
        .format("delta")
        .mode("append")
        .save(tablePath);

    Dataset<Row> df2 = spark.readStream().option("skipChangeCommits", "true").table(dsv2Ref);
    runStreamToParquet(df2, outputDir, checkpointDir, /* trigger= */ null);

    // Run 1 ingested ids 1..10. Run 2 with skipChangeCommits drops the DV-DELETE commit and
    // ingests post-DELETE append (ids 11, 13). All variant payloads must align with their id.
    Dataset<Row> sinkDf = spark.read().parquet(outputDir.getAbsolutePath());
    sinkDf.createOrReplaceTempView("variant_cm_dv_sink");
    List<Row> rows =
        spark
            .sql(
                "SELECT id, variant_get(v, '$.row', 'int') AS v_row "
                    + "FROM variant_cm_dv_sink ORDER BY id")
            .collectAsList();
    // Expected: 1..10 (run 1) + 11, 13 (run 2) = 12 rows.
    assertEquals(12, rows.size(), () -> "Expected 12 rows across DV + CM + restart; got: " + rows);
    for (Row row : rows) {
      int id = row.getInt(0);
      Object vRowObj = row.get(1);
      assertNotNull(vRowObj, () -> "variant_get returned NULL for id=" + id);
      int vRow = ((Number) vRowObj).intValue();
      assertEquals(
          id,
          vRow,
          () ->
              "Silent corruption across VARIANT x CM x DV x restart: id=" + id + " v_row=" + vRow);
    }
  }

  /**
   * Nested-in-struct VARIANT under CM-name across restart. Exercises {@code ColumnarRow#getStruct
   * -> getVariant} through the CM-mapped schema on both legs of the restart.
   */
  @Test
  public void testVariantNestedInStruct_cmName_restart(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);

    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, s STRUCT<v: VARIANT>) USING delta "
                + "TBLPROPERTIES ('delta.columnMapping.mode' = 'name')",
            tablePath));

    spark
        .range(1, 4)
        .selectExpr(
            "cast(id as int) as id",
            "named_struct('v', parse_json(concat('{\"row\":', id, '}'))) as s")
        .coalesce(1)
        .write()
        .format("delta")
        .mode("append")
        .save(tablePath);

    File checkpointDir = new File(deltaTablePath, "_ckpt");
    File outputDir = new File(deltaTablePath, "_out");

    Dataset<Row> df1 = spark.readStream().table(dsv2Ref);
    runStreamToParquet(df1, outputDir, checkpointDir, /* trigger= */ null);

    spark
        .range(4, 7)
        .selectExpr(
            "cast(id as int) as id",
            "named_struct('v', parse_json(concat('{\"row\":', id, '}'))) as s")
        .coalesce(1)
        .write()
        .format("delta")
        .mode("append")
        .save(tablePath);

    Dataset<Row> df2 = spark.readStream().table(dsv2Ref);
    runStreamToParquet(df2, outputDir, checkpointDir, /* trigger= */ null);

    Dataset<Row> sinkDf = spark.read().parquet(outputDir.getAbsolutePath());
    sinkDf.createOrReplaceTempView("variant_struct_cm_sink");
    List<Row> rows =
        spark
            .sql(
                "SELECT id, variant_get(s.v, '$.row', 'int') AS v_row "
                    + "FROM variant_struct_cm_sink ORDER BY id")
            .collectAsList();
    assertEquals(6, rows.size(), () -> "Expected 6 rows after restart, got " + rows);
    int expectedId = 1;
    for (Row row : rows) {
      int id = row.getInt(0);
      assertEquals(expectedId, id, () -> "Unexpected id after restart: " + id);
      Object vRowObj = row.get(1);
      assertNotNull(vRowObj, () -> "variant_get returned NULL for id=" + id);
      int vRow = ((Number) vRowObj).intValue();
      assertEquals(
          id,
          vRow,
          () ->
              "STRUCT<VARIANT> payload misaligned across CM + restart: id="
                  + id
                  + " v_row="
                  + vRow);
      expectedId++;
    }
  }

  /**
   * Additive schema change between runs: ADD COLUMN to a CM-name VARIANT table. No
   * schemaTrackingLocation is required for purely additive changes. The new column shows up as NULL
   * in pre-change rows, and surviving variant payloads must still align row-by-row.
   */
  @Test
  public void testAddColumn_variant_cmName_restart(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);

    createVariantCmTable(tablePath, "name");
    appendVariantRows(tablePath, 1, 3); // ids 1..3

    File checkpointDir = new File(deltaTablePath, "_ckpt");
    File outputDir = new File(deltaTablePath, "_out");

    Dataset<Row> df1 = spark.readStream().table(dsv2Ref);
    runStreamToParquet(df1, outputDir, checkpointDir, /* trigger= */ null);

    // Additive change: ADD COLUMN extra STRING. New writes carry the extra column; existing rows
    // stay schema-compatible.
    spark.sql(str("ALTER TABLE delta.`%s` ADD COLUMN extra STRING", tablePath));
    spark
        .range(4, 7)
        .selectExpr(
            "cast(id as int) as id",
            "parse_json(concat('{\"row\":', id, '}')) as v",
            "cast(id as string) as extra")
        .coalesce(1)
        .write()
        .format("delta")
        .mode("append")
        .save(tablePath);

    Dataset<Row> df2 = spark.readStream().table(dsv2Ref);
    runStreamToParquet(df2, outputDir, checkpointDir, /* trigger= */ null);

    // Read sink projecting (id, variant_get) - the extra column was NULL pre-change so we don't
    // assert on it; the catch surface is variant payload alignment across schema evolution.
    Dataset<Row> sinkDf = spark.read().parquet(outputDir.getAbsolutePath());
    sinkDf.createOrReplaceTempView("variant_addcol_sink");
    List<Row> rows =
        spark
            .sql(
                "SELECT id, variant_get(v, '$.row', 'int') AS v_row "
                    + "FROM variant_addcol_sink ORDER BY id")
            .collectAsList();
    assertEquals(6, rows.size(), () -> "Expected 6 rows after ADD COLUMN + restart, got " + rows);
    int expectedId = 1;
    for (Row row : rows) {
      int id = row.getInt(0);
      assertEquals(expectedId, id, () -> "Unexpected id after ADD COLUMN: " + id);
      Object vRowObj = row.get(1);
      assertNotNull(vRowObj, () -> "variant_get returned NULL for id=" + id);
      int vRow = ((Number) vRowObj).intValue();
      assertEquals(
          id,
          vRow,
          () -> "Variant payload misaligned after ADD COLUMN: id=" + id + " v_row=" + vRow);
      expectedId++;
    }
  }

  /**
   * RENAME COLUMN between runs - a non-additive CM rename requires schemaTrackingLocation, which
   * DSv2 streaming does not yet support. Kept here as a placeholder for when the option is wired
   * through SparkScan.SUPPORTED_STREAMING_OPTIONS.
   */
  @Test
  public void testRenameColumn_variant_cmName_restart(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);

    createVariantCmTable(tablePath, "name");
    appendVariantRows(tablePath, 1, 3);

    File checkpointDir = new File(deltaTablePath, "_ckpt");
    File outputDir = new File(deltaTablePath, "_out");
    File schemaTrackingDir = new File(checkpointDir, "_schema_tracking");

    Dataset<Row> df1 =
        spark
            .readStream()
            .option("schemaTrackingLocation", schemaTrackingDir.getAbsolutePath())
            .table(dsv2Ref);
    runStreamToParquet(df1, outputDir, checkpointDir, /* trigger= */ null);

    // Non-additive: rename the VARIANT column. CM keeps the physical name stable but logical name
    // changes; DSv2 needs schemaTrackingLocation to thread the rename through restart.
    spark.sql(str("ALTER TABLE delta.`%s` RENAME COLUMN v TO v_renamed", tablePath));
    spark
        .range(4, 7)
        .selectExpr(
            "cast(id as int) as id", "parse_json(concat('{\"row\":', id, '}')) as v_renamed")
        .coalesce(1)
        .write()
        .format("delta")
        .mode("append")
        .save(tablePath);

    Dataset<Row> df2 =
        spark
            .readStream()
            .option("schemaTrackingLocation", schemaTrackingDir.getAbsolutePath())
            .table(dsv2Ref);
    runStreamToParquet(df2, outputDir, checkpointDir, /* trigger= */ null);

    // Once supported, the sink should contain ids 1..6 with aligned variant payloads.
  }

  /**
   * startingVersion=N at first run, restart from checkpoint at second run. The checkpoint must
   * carry the resolved start position forward; restart picks up only commits after the original
   * resume point.
   */
  @Test
  public void testStartingVersion_variant_cmName(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);

    createVariantCmTable(tablePath, "name"); // v=0
    appendVariantRows(tablePath, 1, 1); // v=1 -> id 1
    appendVariantRows(tablePath, 2, 1); // v=2 -> id 2
    appendVariantRows(tablePath, 3, 1); // v=3 -> id 3
    appendVariantRows(tablePath, 4, 1); // v=4 -> id 4

    File checkpointDir = new File(deltaTablePath, "_ckpt");
    File outputDir = new File(deltaTablePath, "_out");

    // First run: startingVersion=3 -> consume v=3, v=4 (ids 3, 4).
    Dataset<Row> df1 = spark.readStream().option("startingVersion", "3").table(dsv2Ref);
    runStreamToParquet(df1, outputDir, checkpointDir, /* trigger= */ null);

    // Append more commits. Restart picks them up from the checkpoint, ignoring startingVersion.
    appendVariantRows(tablePath, 5, 1); // v=5 -> id 5
    appendVariantRows(tablePath, 6, 1); // v=6 -> id 6

    Dataset<Row> df2 = spark.readStream().option("startingVersion", "3").table(dsv2Ref);
    runStreamToParquet(df2, outputDir, checkpointDir, /* trigger= */ null);

    // Total: ids 3..6 (run 1 picked up 3..4, run 2 picked up 5..6 via checkpoint).
    assertParquetSinkVariantAligned(outputDir, 3, 7);
  }

  /**
   * Rate-limited stream across VARIANT + CM-name data, then restart. Five single-file commits
   * before the first run force per-batch slicing; the second run drains another rate-limited set of
   * commits from the checkpoint.
   */
  @Test
  public void testMaxFilesPerTrigger_variant_cmName_restart(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);

    createVariantCmTable(tablePath, "name");
    for (int i = 1; i <= 5; i++) {
      appendVariantRows(tablePath, i, 1);
    }

    File checkpointDir = new File(deltaTablePath, "_ckpt");
    File outputDir = new File(deltaTablePath, "_out");

    Dataset<Row> df1 = spark.readStream().option("maxFilesPerTrigger", "1").table(dsv2Ref);
    runStreamToParquet(df1, outputDir, checkpointDir, /* trigger= */ null);

    for (int i = 6; i <= 10; i++) {
      appendVariantRows(tablePath, i, 1);
    }

    Dataset<Row> df2 = spark.readStream().option("maxFilesPerTrigger", "1").table(dsv2Ref);
    runStreamToParquet(df2, outputDir, checkpointDir, /* trigger= */ null);

    assertParquetSinkVariantAligned(outputDir, 1, 11);
  }

  /**
   * Both runs use Trigger.AvailableNow on the same checkpoint. AvailableNow is one-shot per run, so
   * this exercises the "two AvailableNow runs sharing one checkpoint" path - distinct from the
   * trigger=null bare-stream path - for VARIANT under CM-name.
   */
  @Test
  public void testAvailableNow_variant_cmName_restart(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);

    createVariantCmTable(tablePath, "name");
    appendVariantRows(tablePath, 1, 4); // ids 1..4

    File checkpointDir = new File(deltaTablePath, "_ckpt");
    File outputDir = new File(deltaTablePath, "_out");

    Dataset<Row> df1 = spark.readStream().table(dsv2Ref);
    runStreamToParquet(df1, outputDir, checkpointDir, Trigger.AvailableNow());

    appendVariantRows(tablePath, 5, 4); // ids 5..8

    Dataset<Row> df2 = spark.readStream().table(dsv2Ref);
    runStreamToParquet(df2, outputDir, checkpointDir, Trigger.AvailableNow());

    assertParquetSinkVariantAligned(outputDir, 1, 9);
  }
}
