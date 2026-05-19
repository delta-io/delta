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
import java.util.Arrays;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
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
   * Bug #29 (CM name) - ADD COLUMN mid-stream + restart from checkpoint. Mirrors DSv1 "column
   * mapping + streaming - allowed workflows - column addition". DSv2 rejects the restart with "This
   * query does not support recovering from checkpoint location".
   */
  @Test
  public void testColumnMapping_addColumnMidStream(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);

    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id STRING, value STRING) USING delta "
                + "TBLPROPERTIES ('delta.columnMapping.mode' = 'name')",
            tablePath));
    for (int i = 0; i < 5; i++) {
      spark.sql(str("INSERT INTO delta.`%s` VALUES ('%d', '%d')", tablePath, i, i));
    }

    File checkpointDir = new File(deltaTablePath, "_checkpoint");
    Dataset<Row> streamingDF = spark.readStream().table(dsv2TableRef);

    StreamingQuery q1 =
        streamingDF
            .writeStream()
            .format("noop")
            .option("checkpointLocation", checkpointDir.getAbsolutePath())
            .start();
    q1.processAllAvailable();
    q1.stop();

    // Add column then write data in new schema
    spark.sql(str("ALTER TABLE delta.`%s` ADD COLUMN (value2 STRING)", tablePath));
    for (int i = 5; i < 10; i++) {
      spark.sql(str("INSERT INTO delta.`%s` VALUES ('%d', '%d', '%d')", tablePath, i, i, i));
    }

    // Restart with a fresh DataFrame matching new schema. DSv1 expects only 5..10 to be ingested
    // because the sink is reinitialized.
    Dataset<Row> streamingDF2 = spark.readStream().table(dsv2TableRef);
    List<Row> actualRows =
        processStreamingQueryFromCheckpoint(streamingDF2, "test_cm_add_column_mid", checkpointDir);

    List<Row> expected =
        Arrays.asList(
            RowFactory.create("5", "5", "5"),
            RowFactory.create("6", "6", "6"),
            RowFactory.create("7", "7", "7"),
            RowFactory.create("8", "8", "8"),
            RowFactory.create("9", "9", "9"));
    assertDataEquals(actualRows, expected);
  }

  /**
   * Bug #29 (CM name) - DROP COLUMN with unsafe flag + schema-tracking on. DSv2 rejects the restart
   * even with schemaTrackingLocation set.
   */
  @Test
  public void testColumnMapping_dropColumnUnsafe(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);

    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id STRING, value STRING) USING delta "
                + "TBLPROPERTIES ('delta.columnMapping.mode' = 'name')",
            tablePath));
    for (int i = 0; i < 5; i++) {
      spark.sql(str("INSERT INTO delta.`%s` VALUES ('%d', '%d')", tablePath, i, i));
    }

    File checkpointDir = new File(deltaTablePath, "_checkpoint");
    File schemaTrackingDir = new File(checkpointDir, "_schema_tracking");

    // First stream just to advance the checkpoint
    Dataset<Row> df1 =
        spark
            .readStream()
            .option("schemaTrackingLocation", schemaTrackingDir.getAbsolutePath())
            .table(dsv2TableRef);
    StreamingQuery q1 =
        df1.writeStream()
            .format("noop")
            .option("checkpointLocation", checkpointDir.getAbsolutePath())
            .start();
    q1.processAllAvailable();
    q1.stop();

    // Drop column
    spark.sql(str("ALTER TABLE delta.`%s` DROP COLUMN value", tablePath));
    for (int i = 5; i < 10; i++) {
      spark.sql(str("INSERT INTO delta.`%s` VALUES ('%d')", tablePath, i));
    }

    // Restart with unsafe flag enabled
    withSQLConf(
        "spark.databricks.delta.streaming.unsafeReadOnIncompatibleColumnMappingSchemaChanges.enabled",
        "true",
        () -> {
          try {
            Dataset<Row> df2 =
                spark
                    .readStream()
                    .option("schemaTrackingLocation", schemaTrackingDir.getAbsolutePath())
                    .table(dsv2TableRef);
            List<Row> actualRows =
                processStreamingQueryFromCheckpoint(df2, "test_cm_drop_col_unsafe", checkpointDir);
            // After drop, post-drop rows have only id; we expect 5..9 with single column
            List<Row> expected =
                Arrays.asList(
                    RowFactory.create("5"),
                    RowFactory.create("6"),
                    RowFactory.create("7"),
                    RowFactory.create("8"),
                    RowFactory.create("9"));
            assertDataEquals(actualRows, expected);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        });
  }

  /**
   * Bug #29 (CM name) - RENAME COLUMN with unsafe flag + schema-tracking on. DSv2 rejects the
   * restart even with schemaTrackingLocation set.
   */
  @Test
  public void testColumnMapping_renameColumnUnsafe(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);

    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id STRING, value STRING) USING delta "
                + "TBLPROPERTIES ('delta.columnMapping.mode' = 'name')",
            tablePath));
    for (int i = 0; i < 5; i++) {
      spark.sql(str("INSERT INTO delta.`%s` VALUES ('%d', '%d')", tablePath, i, i));
    }

    File checkpointDir = new File(deltaTablePath, "_checkpoint");
    File schemaTrackingDir = new File(checkpointDir, "_schema_tracking");

    Dataset<Row> df1 =
        spark
            .readStream()
            .option("schemaTrackingLocation", schemaTrackingDir.getAbsolutePath())
            .table(dsv2TableRef);
    StreamingQuery q1 =
        df1.writeStream()
            .format("noop")
            .option("checkpointLocation", checkpointDir.getAbsolutePath())
            .start();
    q1.processAllAvailable();
    q1.stop();

    // Rename column
    spark.sql(str("ALTER TABLE delta.`%s` RENAME COLUMN value TO value2", tablePath));
    for (int i = 5; i < 10; i++) {
      spark.sql(str("INSERT INTO delta.`%s` VALUES ('%d', '%d')", tablePath, i, i));
    }

    // Restart with unsafe flag enabled
    withSQLConf(
        "spark.databricks.delta.streaming.unsafeReadOnIncompatibleColumnMappingSchemaChanges.enabled",
        "true",
        () -> {
          try {
            Dataset<Row> df2 =
                spark
                    .readStream()
                    .option("schemaTrackingLocation", schemaTrackingDir.getAbsolutePath())
                    .table(dsv2TableRef);
            List<Row> actualRows =
                processStreamingQueryFromCheckpoint(
                    df2, "test_cm_rename_col_unsafe", checkpointDir);
            List<Row> expected =
                Arrays.asList(
                    RowFactory.create("5", "5"),
                    RowFactory.create("6", "6"),
                    RowFactory.create("7", "7"),
                    RowFactory.create("8", "8"),
                    RowFactory.create("9", "9"));
            assertDataEquals(actualRows, expected);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        });
  }

  /**
   * Bug #29 (CM name) - restart from checkpoint with no schema change. DSv2 rejects the restart
   * because the V2 read path does not advertise checkpoint-recovery support for CM tables.
   */
  @Test
  public void testColumnMapping_restart(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);

    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, name STRING) USING delta "
                + "TBLPROPERTIES ('delta.columnMapping.mode' = 'name')",
            tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'Alice'), (2, 'Bob')", tablePath));

    File checkpointDir = new File(deltaTablePath, "_checkpoint");

    // First stream - drains 1, 2
    Dataset<Row> df1 = spark.readStream().table(dsv2TableRef);
    StreamingQuery q1 =
        df1.writeStream()
            .format("memory")
            .queryName("test_cm_restart_1")
            .outputMode("append")
            .option("checkpointLocation", checkpointDir.getAbsolutePath())
            .start();
    q1.processAllAvailable();
    q1.stop();

    // New writes
    spark.sql(str("INSERT INTO delta.`%s` VALUES (3, 'Charlie'), (4, 'Dave')", tablePath));

    // Restart from same checkpoint with fresh DataFrame
    Dataset<Row> df2 = spark.readStream().table(dsv2TableRef);
    StreamingQuery q2 =
        df2.writeStream()
            .format("memory")
            .queryName("test_cm_restart_2")
            .outputMode("append")
            .option("checkpointLocation", checkpointDir.getAbsolutePath())
            .start();
    try {
      q2.processAllAvailable();
      List<Row> rows = spark.sql("SELECT * FROM test_cm_restart_2").collectAsList();
      List<Row> expected =
          Arrays.asList(RowFactory.create(3, "Charlie"), RowFactory.create(4, "Dave"));
      assertDataEquals(rows, expected);
    } finally {
      q2.stop();
    }
  }

  /**
   * Bug #29 (CM name) - ADD COLUMN then RENAME COLUMN (sequential schema changes). DSv2 rejects the
   * restart across the sequential schema changes.
   */
  @Test
  public void testColumnMapping_addThenRename(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);

    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id STRING, value STRING) USING delta "
                + "TBLPROPERTIES ('delta.columnMapping.mode' = 'name')",
            tablePath));
    for (int i = 0; i < 3; i++) {
      spark.sql(str("INSERT INTO delta.`%s` VALUES ('%d', '%d')", tablePath, i, i));
    }

    File checkpointDir = new File(deltaTablePath, "_checkpoint");
    File schemaTrackingDir = new File(checkpointDir, "_schema_tracking");

    Dataset<Row> df1 =
        spark
            .readStream()
            .option("schemaTrackingLocation", schemaTrackingDir.getAbsolutePath())
            .table(dsv2TableRef);
    StreamingQuery q1 =
        df1.writeStream()
            .format("noop")
            .option("checkpointLocation", checkpointDir.getAbsolutePath())
            .start();
    q1.processAllAvailable();
    q1.stop();

    // Sequential schema changes: add new col then rename it
    spark.sql(str("ALTER TABLE delta.`%s` ADD COLUMN extra STRING", tablePath));
    spark.sql(str("ALTER TABLE delta.`%s` RENAME COLUMN extra TO extra2", tablePath));
    for (int i = 3; i < 6; i++) {
      spark.sql(str("INSERT INTO delta.`%s` VALUES ('%d', '%d', '%d')", tablePath, i, i, i));
    }

    // With unsafe flag
    withSQLConf(
        "spark.databricks.delta.streaming.unsafeReadOnIncompatibleColumnMappingSchemaChanges.enabled",
        "true",
        () -> {
          try {
            Dataset<Row> df2 =
                spark
                    .readStream()
                    .option("schemaTrackingLocation", schemaTrackingDir.getAbsolutePath())
                    .table(dsv2TableRef);
            List<Row> rows =
                processStreamingQueryFromCheckpoint(df2, "test_cm_add_then_rename", checkpointDir);
            List<Row> expected =
                Arrays.asList(
                    RowFactory.create("3", "3", "3"),
                    RowFactory.create("4", "4", "4"),
                    RowFactory.create("5", "5", "5"));
            assertDataEquals(rows, expected);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        });
  }

  /**
   * Bug #29 (CM id) - ADD COLUMN mid-stream + restart from checkpoint on an id-mode table. Mirrors
   * the name-mode variant.
   */
  @Test
  public void testColumnMappingId_addColumnMidStream(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);

    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id STRING, value STRING) USING delta "
                + "TBLPROPERTIES ('delta.columnMapping.mode' = 'id', "
                + "  'delta.minReaderVersion' = '2', "
                + "  'delta.minWriterVersion' = '5')",
            tablePath));
    for (int i = 0; i < 5; i++) {
      spark.sql(str("INSERT INTO delta.`%s` VALUES ('%d', '%d')", tablePath, i, i));
    }

    File checkpointDir = new File(deltaTablePath, "_checkpoint");
    Dataset<Row> streamingDF = spark.readStream().table(dsv2TableRef);

    StreamingQuery q1 =
        streamingDF
            .writeStream()
            .format("noop")
            .option("checkpointLocation", checkpointDir.getAbsolutePath())
            .start();
    q1.processAllAvailable();
    q1.stop();

    spark.sql(str("ALTER TABLE delta.`%s` ADD COLUMN (value2 STRING)", tablePath));
    for (int i = 5; i < 10; i++) {
      spark.sql(str("INSERT INTO delta.`%s` VALUES ('%d', '%d', '%d')", tablePath, i, i, i));
    }

    Dataset<Row> streamingDF2 = spark.readStream().table(dsv2TableRef);
    List<Row> actualRows =
        processStreamingQueryFromCheckpoint(
            streamingDF2, "test_cm_id_add_column_mid", checkpointDir);

    List<Row> expected =
        Arrays.asList(
            RowFactory.create("5", "5", "5"),
            RowFactory.create("6", "6", "6"),
            RowFactory.create("7", "7", "7"),
            RowFactory.create("8", "8", "8"),
            RowFactory.create("9", "9", "9"));
    assertDataEquals(actualRows, expected);
  }

  /**
   * Bug #29 (CM id) - RENAME COLUMN with unsafe flag on an id-mode table. DSv2 rejects the restart
   * even with schemaTrackingLocation set.
   */
  @Test
  public void testColumnMappingId_rename(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);

    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id STRING, value STRING) USING delta "
                + "TBLPROPERTIES ('delta.columnMapping.mode' = 'id', "
                + "  'delta.minReaderVersion' = '2', "
                + "  'delta.minWriterVersion' = '5')",
            tablePath));
    for (int i = 0; i < 5; i++) {
      spark.sql(str("INSERT INTO delta.`%s` VALUES ('%d', '%d')", tablePath, i, i));
    }

    File checkpointDir = new File(deltaTablePath, "_checkpoint");
    File schemaTrackingDir = new File(checkpointDir, "_schema_tracking");

    Dataset<Row> df1 =
        spark
            .readStream()
            .option("schemaTrackingLocation", schemaTrackingDir.getAbsolutePath())
            .table(dsv2TableRef);
    StreamingQuery q1 =
        df1.writeStream()
            .format("noop")
            .option("checkpointLocation", checkpointDir.getAbsolutePath())
            .start();
    q1.processAllAvailable();
    q1.stop();

    spark.sql(str("ALTER TABLE delta.`%s` RENAME COLUMN value TO value2", tablePath));
    for (int i = 5; i < 10; i++) {
      spark.sql(str("INSERT INTO delta.`%s` VALUES ('%d', '%d')", tablePath, i, i));
    }

    withSQLConf(
        "spark.databricks.delta.streaming.unsafeReadOnIncompatibleColumnMappingSchemaChanges.enabled",
        "true",
        () -> {
          try {
            Dataset<Row> df2 =
                spark
                    .readStream()
                    .option("schemaTrackingLocation", schemaTrackingDir.getAbsolutePath())
                    .table(dsv2TableRef);
            List<Row> actualRows =
                processStreamingQueryFromCheckpoint(
                    df2, "test_cm_id_rename_col_unsafe", checkpointDir);
            List<Row> expected =
                Arrays.asList(
                    RowFactory.create("5", "5"),
                    RowFactory.create("6", "6"),
                    RowFactory.create("7", "7"),
                    RowFactory.create("8", "8"),
                    RowFactory.create("9", "9"));
            assertDataEquals(actualRows, expected);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        });
  }

  /**
   * Bug #29 (CM id) - nullability toggle (DROP NOT NULL) mid-stream. With schemaTrackingLocation,
   * the restart from checkpoint must adopt the relaxed nullability and surface the post-toggle
   * null-value row instead of raising DELTA_STREAMING_SCHEMA_MISMATCH_ON_RESTART.
   */
  @Test
  public void testColumnMappingId_nullabilityToggle(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    File checkpointDir = new File(deltaTablePath, "_checkpoint");
    File schemaTrackingDir = new File(checkpointDir, "_schema_tracking");
    File outputDir = new File(deltaTablePath, "_out");

    // CM-id table with a NOT NULL `value` column.
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, value STRING NOT NULL) USING delta "
                + "TBLPROPERTIES ('delta.columnMapping.mode' = 'id', "
                + "  'delta.minReaderVersion' = '2', "
                + "  'delta.minWriterVersion' = '5')",
            tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'a'), (2, 'b')", tablePath));

    // First run drains the 2 pre-toggle rows.
    Dataset<Row> df1 =
        spark
            .readStream()
            .option("schemaTrackingLocation", schemaTrackingDir.getAbsolutePath())
            .table(dsv2TableRef);
    StreamingQuery q1 =
        df1.writeStream()
            .format("parquet")
            .outputMode("append")
            .option("path", outputDir.getAbsolutePath())
            .option("checkpointLocation", checkpointDir.getAbsolutePath())
            .start();
    try {
      q1.processAllAvailable();
    } finally {
      q1.stop();
      DeltaLog.clearCache();
    }
    long firstRunRows = spark.read().parquet(outputDir.getAbsolutePath()).count();
    assertEquals(2L, firstRunRows, "first run should emit 2 pre-toggle rows");

    // Drop NOT NULL on `value`, then INSERT a row with NULL value.
    spark.sql(str("ALTER TABLE delta.`%s` ALTER COLUMN value DROP NOT NULL", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (3, NULL)", tablePath));

    // Restart from the same checkpoint + schema tracking log; the schema evolution handler must
    // adopt the relaxed nullability and not raise SCHEMA_MISMATCH_ON_RESTART.
    Dataset<Row> df2 =
        spark
            .readStream()
            .option("schemaTrackingLocation", schemaTrackingDir.getAbsolutePath())
            .table(dsv2TableRef);
    StreamingQuery q2 =
        df2.writeStream()
            .format("parquet")
            .outputMode("append")
            .option("path", outputDir.getAbsolutePath())
            .option("checkpointLocation", checkpointDir.getAbsolutePath())
            .start();
    try {
      q2.processAllAvailable();
      final StreamingQuery finalQ = q2;
      assertTrue(
          finalQ.exception().isEmpty(),
          () ->
              "Restart should not raise SCHEMA_MISMATCH_ON_RESTART: "
                  + (finalQ.exception().isDefined() ? finalQ.exception().get().toString() : ""));
    } finally {
      q2.stop();
      DeltaLog.clearCache();
    }

    // Sink should now contain all 3 rows including the post-toggle null-value row.
    List<Row> sinkRows = spark.read().parquet(outputDir.getAbsolutePath()).collectAsList();
    assertEquals(3, sinkRows.size(), () -> "expected 3 total rows, got " + sinkRows);
    assertTrue(
        sinkRows.stream().anyMatch(r -> r.isNullAt(1)),
        () -> "post-toggle null-value row must surface, got " + sinkRows);
  }

  /**
   * Process a streaming query that resumes from a specific checkpoint location, returning the rows
   * materialized into a memory sink.
   */
  private List<Row> processStreamingQueryFromCheckpoint(
      Dataset<Row> streamingDF, String queryName, File checkpointDir) throws Exception {
    StreamingQuery query = null;
    try {
      query =
          streamingDF
              .writeStream()
              .format("memory")
              .queryName(queryName)
              .outputMode("append")
              .option("checkpointLocation", checkpointDir.getAbsolutePath())
              .start();
      query.processAllAvailable();
      return spark.sql("SELECT * FROM " + queryName).collectAsList();
    } finally {
      if (query != null) {
        query.stop();
        DeltaLog.clearCache();
      }
    }
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
