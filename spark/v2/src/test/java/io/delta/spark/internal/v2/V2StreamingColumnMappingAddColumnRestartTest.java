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
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.delta.DeltaLog;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Failing tests for Bug #29: DSv2 streaming rejects checkpoint recovery after an additive (ADD
 * COLUMN) or non-additive (RENAME COLUMN) schema change on a column-mapped table.
 *
 * <p>Repro shape: create a CM-name table, drain a first micro-batch, alter the schema, write
 * post-change rows, then restart the same query from the same checkpoint with {@code
 * schemaTrackingLocation}. DSv1 handles this - additive changes are absorbed by the schema-tracking
 * log, non-additive changes are unblocked by {@code allowSourceColumnRename=always}. DSv2 raises
 * {@code AnalysisException: This query does not support recovering from checkpoint location}
 * because the {@link io.delta.spark.internal.v2.read.MetadataEvolutionHandler} path added in PR
 * #6697 only wires schemaTrackingLocation for the non-additive rename/drop branch; additive ADD
 * COLUMN (and the rename leg here, depending on Spark's resolution order) falls through to Spark's
 * {@code ResolveWriteToStream}, which rejects the checkpoint.
 *
 * <p>Each test runs the V1 leg first to (a) establish the expected behavior and (b) drive the
 * schema change so the shared on-disk table is in the right state for the V2 leg. The V2 leg is the
 * failing-test assertion: it captures the divergent current behavior so the test is RED until Bug
 * #29 is fixed. When DSv2 starts succeeding, the {@code assertThrows} will fail and force the test
 * to be re-classified to PASS.
 */
public class V2StreamingColumnMappingAddColumnRestartTest extends V2TestBase {

  /**
   * Bug #29 - ADD COLUMN mid-stream + restart from checkpoint.
   *
   * <p>V1 succeeds via schemaTrackingLocation; V2 throws "does not support recovering from
   * checkpoint location".
   */
  @Test
  public void testCmNameTable_addColumnMidStream_restartRejected(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);

    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id STRING, value STRING) USING delta "
                + "TBLPROPERTIES ('delta.columnMapping.mode' = 'name', "
                + "               'delta.minReaderVersion' = '2', "
                + "               'delta.minWriterVersion' = '5')",
            tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES ('0', '0'), ('1', '1')", tablePath));

    // V1 leg: drain, ADD COLUMN, write new rows, restart. Expect success.
    File v1Checkpoint = new File(deltaTablePath, "_v1_checkpoint");
    File v1SchemaTracking = new File(v1Checkpoint, "_schema_tracking");
    File v1Output = new File(deltaTablePath, "_v1_output");

    Dataset<Row> v1Df1 =
        spark
            .readStream()
            .format("delta")
            .option("schemaTrackingLocation", v1SchemaTracking.getAbsolutePath())
            .load(tablePath);
    runOnceWithParquetSink(v1Df1, v1Output, v1Checkpoint);
    DeltaLog.clearCache();

    // Additive schema change between V1 runs.
    spark.sql(str("ALTER TABLE delta.`%s` ADD COLUMN extra INT", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES ('2', '2', 2), ('3', '3', 3)", tablePath));

    Dataset<Row> v1Df2 =
        spark
            .readStream()
            .format("delta")
            .option("schemaTrackingLocation", v1SchemaTracking.getAbsolutePath())
            .load(tablePath);
    runOnceWithParquetSink(v1Df2, v1Output, v1Checkpoint);
    DeltaLog.clearCache();

    long v1Total = spark.read().parquet(v1Output.getAbsolutePath()).count();
    assertEquals(
        4L,
        v1Total,
        () -> "V1 should ingest 2 pre-add + 2 post-add rows across the restart, got " + v1Total);

    // V2 leg: drain a first batch on the already-evolved table, then attempt to restart from a
    // fresh checkpoint advanced past the ADD COLUMN. Currently expected to throw at restart time.
    File v2Checkpoint = new File(deltaTablePath, "_v2_checkpoint");
    File v2SchemaTracking = new File(v2Checkpoint, "_schema_tracking");
    File v2Output = new File(deltaTablePath, "_v2_output");

    // First V2 run: read from version 0 up through (and including) the ADD COLUMN commit and
    // the post-add inserts so the checkpoint is positioned past the schema change.
    Dataset<Row> v2Df1 =
        spark
            .readStream()
            .option("schemaTrackingLocation", v2SchemaTracking.getAbsolutePath())
            .table(dsv2TableRef);
    runOnceWithParquetSink(v2Df1, v2Output, v2Checkpoint);
    DeltaLog.clearCache();

    // Additional post-restart writes to give the second run something to do.
    spark.sql(str("INSERT INTO delta.`%s` VALUES ('4', '4', 4), ('5', '5', 5)", tablePath));

    // Second V2 run: restart from the same checkpoint + schema-tracking log. Bug #29 fires here.
    Throwable v2Err =
        assertThrows(
            Throwable.class,
            () -> {
              Dataset<Row> v2Df2 =
                  spark
                      .readStream()
                      .option("schemaTrackingLocation", v2SchemaTracking.getAbsolutePath())
                      .table(dsv2TableRef);
              runOnceWithParquetSink(v2Df2, v2Output, v2Checkpoint);
            },
            "DSv2 is currently expected to reject checkpoint recovery across an ADD COLUMN. "
                + "If this assertion fails, Bug #29 has been fixed - re-classify this test to "
                + "an end-to-end success assertion matching the V1 leg.");
    String v2Msg = unwrapMessages(v2Err);
    assertTrue(
        v2Msg.contains("does not support recovering from checkpoint location"),
        () ->
            "Expected DSv2 'does not support recovering from checkpoint location' error, got: "
                + v2Msg);
  }

  /**
   * Bug #29 (non-additive variant) - RENAME COLUMN mid-stream + restart from checkpoint.
   *
   * <p>V1 succeeds with schemaTrackingLocation + allowSourceColumnRename=always; V2 throws "does
   * not support recovering from checkpoint location".
   */
  @Test
  public void testCmNameTable_renameColumnMidStream_restartRejected(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);

    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id STRING, value STRING) USING delta "
                + "TBLPROPERTIES ('delta.columnMapping.mode' = 'name', "
                + "               'delta.minReaderVersion' = '2', "
                + "               'delta.minWriterVersion' = '5')",
            tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES ('0', '0'), ('1', '1')", tablePath));

    // V1 leg: drain, RENAME COLUMN, write new rows, restart with allowSourceColumnRename=always.
    File v1Checkpoint = new File(deltaTablePath, "_v1_checkpoint");
    File v1SchemaTracking = new File(v1Checkpoint, "_schema_tracking");
    File v1Output = new File(deltaTablePath, "_v1_output");

    Dataset<Row> v1Df1 =
        spark
            .readStream()
            .format("delta")
            .option("schemaTrackingLocation", v1SchemaTracking.getAbsolutePath())
            .load(tablePath);
    runOnceWithParquetSink(v1Df1, v1Output, v1Checkpoint);
    DeltaLog.clearCache();

    spark.sql(str("ALTER TABLE delta.`%s` RENAME COLUMN value TO value2", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES ('2', '2'), ('3', '3')", tablePath));

    Dataset<Row> v1Df2 =
        spark
            .readStream()
            .format("delta")
            .option("schemaTrackingLocation", v1SchemaTracking.getAbsolutePath())
            .option("allowSourceColumnRename", "always")
            .load(tablePath);
    runOnceWithParquetSink(v1Df2, v1Output, v1Checkpoint);
    DeltaLog.clearCache();

    long v1Total = spark.read().parquet(v1Output.getAbsolutePath()).count();
    assertEquals(
        4L,
        v1Total,
        () ->
            "V1 should ingest 2 pre-rename + 2 post-rename rows across the restart, got "
                + v1Total);

    // V2 leg: same shape, expected to throw at restart time.
    File v2Checkpoint = new File(deltaTablePath, "_v2_checkpoint");
    File v2SchemaTracking = new File(v2Checkpoint, "_schema_tracking");
    File v2Output = new File(deltaTablePath, "_v2_output");

    Dataset<Row> v2Df1 =
        spark
            .readStream()
            .option("schemaTrackingLocation", v2SchemaTracking.getAbsolutePath())
            .table(dsv2TableRef);
    runOnceWithParquetSink(v2Df1, v2Output, v2Checkpoint);
    DeltaLog.clearCache();

    spark.sql(str("INSERT INTO delta.`%s` VALUES ('4', '4'), ('5', '5')", tablePath));

    Throwable v2Err =
        assertThrows(
            Throwable.class,
            () -> {
              Dataset<Row> v2Df2 =
                  spark
                      .readStream()
                      .option("schemaTrackingLocation", v2SchemaTracking.getAbsolutePath())
                      .option("allowSourceColumnRename", "always")
                      .table(dsv2TableRef);
              runOnceWithParquetSink(v2Df2, v2Output, v2Checkpoint);
            },
            "DSv2 is currently expected to reject checkpoint recovery across a RENAME COLUMN, "
                + "even with allowSourceColumnRename=always. If this assertion fails, Bug #29 "
                + "(rename leg) has been fixed - re-classify this test to an end-to-end success "
                + "assertion matching the V1 leg.");
    String v2Msg = unwrapMessages(v2Err);
    assertTrue(
        v2Msg.contains("does not support recovering from checkpoint location"),
        () ->
            "Expected DSv2 'does not support recovering from checkpoint location' error, got: "
                + v2Msg);
  }

  /**
   * Runs a streaming query against a parquet sink + checkpoint with {@link Trigger#AvailableNow()},
   * drains everything, then stops. Parquet sink supports checkpoint recovery across restarts;
   * memory sink does not.
   */
  private void runOnceWithParquetSink(Dataset<Row> streamingDF, File outputDir, File checkpointDir)
      throws Exception {
    StreamingQuery query = null;
    try {
      DataStreamWriter<Row> writer =
          streamingDF
              .writeStream()
              .format("parquet")
              .outputMode("append")
              .option("path", outputDir.getAbsolutePath())
              .option("checkpointLocation", checkpointDir.getAbsolutePath())
              .trigger(Trigger.AvailableNow());
      query = writer.start();
      query.processAllAvailable();
      query.awaitTermination(60_000L);
    } finally {
      if (query != null) {
        query.stop();
      }
    }
  }

  /**
   * Walks the cause chain and concatenates messages so .contains() checks can match across wrapped
   * exceptions.
   */
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
}
