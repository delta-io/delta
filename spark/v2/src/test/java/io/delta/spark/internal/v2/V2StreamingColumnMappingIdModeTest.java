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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests for V2 streaming reads on column-mapped Delta tables in {@code mode=id}.
 *
 * <p>Sibling of {@link V2StreamingColumnMappingTest}, which covers {@code mode=name}. The id-mode
 * cell of the F2/F3 ColMap x streaming cross-product had only S1 + S14 covered prior to this file;
 * this file closes the gap by mirroring the name-mode scenarios with {@code
 * delta.columnMapping.mode='id'} on every CREATE TABLE.
 *
 * <p>Both modes share the same physical-name plumbing in the read path, so behavior should be
 * identical except where flagged in javadoc.
 */
public class V2StreamingColumnMappingIdModeTest extends V2TestBase {

  /** Column mapping mode=id x stream basic. */
  @Test
  public void testStreamingBasic_columnMappingModeId(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, name STRING, value DOUBLE) "
                + "USING delta "
                + "TBLPROPERTIES ('delta.columnMapping.mode' = 'id', "
                + "  'delta.minReaderVersion' = '2', "
                + "  'delta.minWriterVersion' = '5')",
            tablePath));
    spark.sql(
        str("INSERT INTO delta.`%s` VALUES (1, 'Alice', 100.0), (2, 'Bob', 200.0)", tablePath));

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> streamingDF = spark.readStream().table(dsv2TableRef);
    assertTrue(streamingDF.isStreaming());

    List<Row> actualRows = processStreamingQuery(streamingDF, "test_cm_id_basic_v2");
    List<Row> expected =
        Arrays.asList(RowFactory.create(1, "Alice", 100.0), RowFactory.create(2, "Bob", 200.0));
    assertDataEquals(actualRows, expected);
  }

  /** Column mapping mode=id x DV. DELETE on a mode=id table, stream. */
  @Test
  public void testColumnMappingId_withDeletionVectors(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);

    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, name STRING) USING delta "
                + "TBLPROPERTIES ('delta.columnMapping.mode' = 'id', "
                + "  'delta.minReaderVersion' = '2', "
                + "  'delta.minWriterVersion' = '5', "
                + "  'delta.enableDeletionVectors' = 'true')",
            tablePath));
    spark
        .range(10)
        .selectExpr("cast(id as int) as id", "cast(id as string) as name")
        .coalesce(1)
        .write()
        .format("delta")
        .mode("append")
        .save(tablePath);

    spark.sql(str("DELETE FROM delta.`%s` WHERE id < 3", tablePath));

    Dataset<Row> streamingDF = spark.readStream().table(dsv2TableRef);
    List<Row> actualRows = processStreamingQuery(streamingDF, "test_cm_id_dv");

    List<Row> expected =
        Arrays.asList(
            RowFactory.create(3, "3"),
            RowFactory.create(4, "4"),
            RowFactory.create(5, "5"),
            RowFactory.create(6, "6"),
            RowFactory.create(7, "7"),
            RowFactory.create(8, "8"),
            RowFactory.create(9, "9"));
    assertDataEquals(actualRows, expected);
  }

  /** Column mapping mode=id x startingVersion. Stream from a version > 0. */
  @Test
  public void testColumnMappingId_startingVersion(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);

    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, name STRING) USING delta "
                + "TBLPROPERTIES ('delta.columnMapping.mode' = 'id', "
                + "  'delta.minReaderVersion' = '2', "
                + "  'delta.minWriterVersion' = '5')",
            tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'Alice')", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (2, 'Bob')", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (3, 'Charlie')", tablePath));

    Dataset<Row> streamingDF =
        spark.readStream().option("startingVersion", "2").table(dsv2TableRef);
    List<Row> actualRows = processStreamingQuery(streamingDF, "test_cm_id_starting_version");

    List<Row> expected =
        Arrays.asList(RowFactory.create(2, "Bob"), RowFactory.create(3, "Charlie"));
    assertDataEquals(actualRows, expected);
  }

  /** Column mapping mode=id x Trigger.AvailableNow. */
  @Test
  public void testColumnMappingId_triggerAvailableNow(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);

    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, name STRING) USING delta "
                + "TBLPROPERTIES ('delta.columnMapping.mode' = 'id', "
                + "  'delta.minReaderVersion' = '2', "
                + "  'delta.minWriterVersion' = '5')",
            tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'Alice'), (2, 'Bob')", tablePath));

    File checkpointDir = new File(deltaTablePath, "_checkpoint");
    Dataset<Row> streamingDF = spark.readStream().table(dsv2TableRef);

    StreamingQuery q =
        streamingDF
            .writeStream()
            .format("memory")
            .queryName("test_cm_id_avail_now")
            .outputMode("append")
            .trigger(Trigger.AvailableNow())
            .option("checkpointLocation", checkpointDir.getAbsolutePath())
            .start();
    try {
      q.processAllAvailable();
      assertTrue(q.awaitTermination(60_000));
      List<Row> rows = spark.sql("SELECT * FROM test_cm_id_avail_now").collectAsList();
      List<Row> expected =
          Arrays.asList(RowFactory.create(1, "Alice"), RowFactory.create(2, "Bob"));
      assertDataEquals(rows, expected);
    } finally {
      q.stop();
    }
  }

  /** Column mapping mode=id x maxFilesPerTrigger=1. Multi-batch admission on a CM-id table. */
  @Test
  public void testColumnMappingId_maxFilesPerTrigger(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);

    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, name STRING) USING delta "
                + "TBLPROPERTIES ('delta.columnMapping.mode' = 'id', "
                + "  'delta.minReaderVersion' = '2', "
                + "  'delta.minWriterVersion' = '5')",
            tablePath));
    // Four single-row commits -> four 1-file AddFiles.
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'A')", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (2, 'B')", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (3, 'C')", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (4, 'D')", tablePath));

    Dataset<Row> streamingDF =
        spark.readStream().option("maxFilesPerTrigger", "1").table(dsv2TableRef);
    List<Row> actualRows = processStreamingQuery(streamingDF, "test_cm_id_max_files");

    List<Row> expected =
        Arrays.asList(
            RowFactory.create(1, "A"),
            RowFactory.create(2, "B"),
            RowFactory.create(3, "C"),
            RowFactory.create(4, "D"));
    assertDataEquals(actualRows, expected);
  }

  /**
   * Column mapping mode=id x add column mid-stream. Mirrors the name-mode variant, which was
   * previously {@code @Disabled}; re-enabled after PR #6697 added schemaTrackingLocation support.
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
   * Column mapping mode=id x restart. Stop, restart, verify schema resolution and that the
   * checkpoint resumes correctly. Uses a parquet sink because the memory sink does not recover from
   * a checkpoint across query restarts.
   */
  @Test
  public void testColumnMappingId_restart(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);

    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, name STRING) USING delta "
                + "TBLPROPERTIES ('delta.columnMapping.mode' = 'id', "
                + "  'delta.minReaderVersion' = '2', "
                + "  'delta.minWriterVersion' = '5')",
            tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'Alice'), (2, 'Bob')", tablePath));

    File checkpointDir = new File(deltaTablePath, "_checkpoint");
    File outputDir = new File(deltaTablePath, "_out");

    // First stream - drains 1, 2.
    Dataset<Row> df1 = spark.readStream().table(dsv2TableRef);
    StreamingQuery q1 =
        df1.writeStream()
            .format("parquet")
            .option("path", outputDir.getAbsolutePath())
            .option("checkpointLocation", checkpointDir.getAbsolutePath())
            .outputMode("append")
            .start();
    q1.processAllAvailable();
    long initialRows = spark.read().parquet(outputDir.getAbsolutePath()).count();
    q1.stop();
    org.apache.spark.sql.delta.DeltaLog.clearCache();
    assertEquals(2L, initialRows, "Initial run should process the 2 pre-restart rows");

    // New writes after stopping the first stream.
    spark.sql(str("INSERT INTO delta.`%s` VALUES (3, 'Charlie'), (4, 'Dave')", tablePath));

    // Restart from the same checkpoint - should only see the post-restart rows.
    Dataset<Row> df2 = spark.readStream().table(dsv2TableRef);
    StreamingQuery q2 =
        df2.writeStream()
            .format("parquet")
            .option("path", outputDir.getAbsolutePath())
            .option("checkpointLocation", checkpointDir.getAbsolutePath())
            .outputMode("append")
            .start();
    try {
      q2.processAllAvailable();
      long totalRows = spark.read().parquet(outputDir.getAbsolutePath()).count();
      long restartRows = totalRows - initialRows;
      assertEquals(
          2L,
          restartRows,
          () -> "Restart should only process the 2 post-restart rows, got " + restartRows);
    } finally {
      q2.stop();
      org.apache.spark.sql.delta.DeltaLog.clearCache();
    }
  }

  /**
   * Column mapping mode=id x add column then rename column (sequential schema changes). Mirrors the
   * name-mode {@code testColumnMapping_addThenRename} variant.
   */
  @Test
  public void testColumnMappingId_addThenDrop(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);

    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id STRING, value STRING) USING delta "
                + "TBLPROPERTIES ('delta.columnMapping.mode' = 'id', "
                + "  'delta.minReaderVersion' = '2', "
                + "  'delta.minWriterVersion' = '5')",
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

    // Add a column, then drop it.
    spark.sql(str("ALTER TABLE delta.`%s` ADD COLUMN extra STRING", tablePath));
    spark.sql(str("ALTER TABLE delta.`%s` DROP COLUMN extra", tablePath));
    for (int i = 3; i < 6; i++) {
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
            List<Row> rows =
                processStreamingQueryFromCheckpoint(df2, "test_cm_id_add_then_drop", checkpointDir);
            List<Row> expected =
                Arrays.asList(
                    RowFactory.create("3", "3"),
                    RowFactory.create("4", "4"),
                    RowFactory.create("5", "5"));
            assertDataEquals(rows, expected);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        });
  }

  /**
   * Column mapping mode=id x rename column with unsafe flag. Currently blocked on
   * schemaTrackingLocation (Cluster S-3).
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
   * Column mapping mode=id x ignoreDeletes. Partitioned table so DELETE WHERE part=<x> removes
   * whole files (no rewrite), the case ignoreDeletes=true is designed to skip. Subsequent INSERT
   * rows must surface with their column names resolved through the id-mode physical mapping.
   */
  @Test
  public void testColumnMappingId_ignoreDeletes(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);

    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, name STRING, part INT) USING delta "
                + "PARTITIONED BY (part) "
                + "TBLPROPERTIES ('delta.columnMapping.mode' = 'id', "
                + "  'delta.minReaderVersion' = '2', "
                + "  'delta.minWriterVersion' = '5')",
            tablePath));
    spark.sql(
        str(
            "INSERT INTO delta.`%s` VALUES (1, 'Alice', 0), (2, 'Bob', 0), "
                + "(3, 'Charlie', 1), (4, 'Dave', 1)",
            tablePath));
    // Whole-partition DELETE - no file rewrite, so ignoreDeletes can skip the commit.
    spark.sql(str("DELETE FROM delta.`%s` WHERE part = 0", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (5, 'Eve', 2), (6, 'Frank', 2)", tablePath));

    Dataset<Row> streamingDF =
        spark.readStream().option("ignoreDeletes", "true").table(dsv2TableRef);
    List<Row> actualRows = processStreamingQuery(streamingDF, "test_cm_id_ignore_deletes");

    List<Row> expected =
        Arrays.asList(
            RowFactory.create(3, "Charlie", 1),
            RowFactory.create(4, "Dave", 1),
            RowFactory.create(5, "Eve", 2),
            RowFactory.create(6, "Frank", 2));
    assertDataEquals(actualRows, expected);
    // Column names must resolve correctly through the id-mode physical mapping.
    assertArrayEquals(
        new String[] {"id", "name", "part"},
        streamingDF.schema().fieldNames(),
        "Logical column names must round-trip through id-mode column mapping");
  }

  /**
   * Column mapping mode=id x ignoreChanges. UPDATE rewrites the touched file (an AddFile +
   * RemoveFile change commit); ignoreChanges=true accepts that commit as a re-emit of the file's
   * AddFiles, which under id-mode must still resolve column names via the physical mapping.
   */
  @Test
  public void testColumnMappingId_ignoreChanges(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);

    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, name STRING) USING delta "
                + "TBLPROPERTIES ('delta.columnMapping.mode' = 'id', "
                + "  'delta.minReaderVersion' = '2', "
                + "  'delta.minWriterVersion' = '5')",
            tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'Alice'), (2, 'Bob')", tablePath));
    // UPDATE rewrites the file containing id=1; ignoreChanges=true re-emits the rewritten AddFile.
    spark.sql(str("UPDATE delta.`%s` SET name = 'ALICE' WHERE id = 1", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (3, 'Charlie')", tablePath));

    Dataset<Row> streamingDF =
        spark.readStream().option("ignoreChanges", "true").table(dsv2TableRef);
    List<Row> actualRows = processStreamingQuery(streamingDF, "test_cm_id_ignore_changes");

    // The final INSERT row must be present. The UPDATE's rewritten file re-emits its surviving
    // rows under ignoreChanges, so id=1 (post-update) and id=2 (unchanged sibling in the same
    // file) appear in addition to the initial snapshot.
    java.util.Set<Integer> idsSeen = new java.util.HashSet<>();
    for (Row r : actualRows) {
      idsSeen.add(r.getInt(0));
    }
    assertTrue(idsSeen.contains(1), "id=1 must be present (initial snapshot or UPDATE re-emit)");
    assertTrue(idsSeen.contains(2), "id=2 must be present");
    assertTrue(idsSeen.contains(3), "Final INSERT row (id=3) must be present");
    // Column names must resolve correctly through the id-mode physical mapping.
    assertArrayEquals(
        new String[] {"id", "name"},
        streamingDF.schema().fieldNames(),
        "Logical column names must round-trip through id-mode column mapping");
  }

  /**
   * S17 x CM-id: nullability toggle (DROP NOT NULL) mid-stream. With schemaTrackingLocation, the
   * restart from checkpoint must adopt the relaxed nullability and surface the post-toggle null-
   * value row instead of raising DELTA_STREAMING_SCHEMA_MISMATCH_ON_RESTART.
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
      org.apache.spark.sql.delta.DeltaLog.clearCache();
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
      org.apache.spark.sql.delta.DeltaLog.clearCache();
    }

    // Sink should now contain all 3 rows including the post-toggle null-value row.
    List<Row> sinkRows = spark.read().parquet(outputDir.getAbsolutePath()).collectAsList();
    assertEquals(3, sinkRows.size(), () -> "expected 3 total rows, got " + sinkRows);
    assertTrue(
        sinkRows.stream().anyMatch(r -> r.isNullAt(1)),
        () -> "post-toggle null-value row must surface, got " + sinkRows);
  }

  /**
   * S21 x CM-id: simulate logRetentionDuration expiry by forcing a checkpoint and deleting an older
   * commit JSON. With {@code failOnDataLoss=false}, the stream must reconstruct the snapshot from
   * the checkpoint and resolve column names through the CM-id physical mapping.
   */
  @Test
  public void testColumnMappingId_logRetentionPrune(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);

    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, name STRING) USING delta "
                + "TBLPROPERTIES ('delta.columnMapping.mode' = 'id', "
                + "  'delta.minReaderVersion' = '2', "
                + "  'delta.minWriterVersion' = '5')",
            tablePath));
    // Four single-row commits at v=1..4.
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'Alice')", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (2, 'Bob')", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (3, 'Charlie')", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (4, 'Dave')", tablePath));

    // Force a checkpoint so the snapshot is reconstructable after pruning v=1's commit JSON.
    @SuppressWarnings("deprecation")
    org.apache.spark.sql.delta.DeltaLog log =
        org.apache.spark.sql.delta.DeltaLog.forTable(spark, tablePath);
    log.checkpoint();

    // Prune the v=1 commit JSON (and its CRC sibling). The checkpoint at v=4 covers the snapshot.
    Path json1 = Paths.get(tablePath, "_delta_log", String.format("%020d.json", 1L));
    Files.delete(json1);
    Path crc1 = Paths.get(tablePath, "_delta_log", String.format("%020d.crc", 1L));
    if (Files.exists(crc1)) {
      Files.delete(crc1);
    }
    org.apache.spark.sql.delta.DeltaLog.clearCache();

    Dataset<Row> streamingDF =
        spark.readStream().option("failOnDataLoss", "false").table(dsv2TableRef);
    List<Row> actualRows = processStreamingQuery(streamingDF, "test_cm_id_log_retention_prune");

    List<Row> expected =
        Arrays.asList(
            RowFactory.create(1, "Alice"),
            RowFactory.create(2, "Bob"),
            RowFactory.create(3, "Charlie"),
            RowFactory.create(4, "Dave"));
    assertDataEquals(actualRows, expected);
    // Column names must resolve correctly through the id-mode physical mapping after pruning.
    assertArrayEquals(
        new String[] {"id", "name"},
        streamingDF.schema().fieldNames(),
        "Logical column names must round-trip through id-mode column mapping after log prune");
  }

  /**
   * S22 x CM-id: concurrent appender writes new rows while the stream is running. The stream must
   * not error and the surfaced rows must all carry the correct CM-id logical column names.
   */
  @Test
  public void testColumnMappingId_concurrentWriter(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    File checkpointDir = new File(deltaTablePath, "_ckpt");
    File outputDir = new File(deltaTablePath, "_out");

    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, name STRING) USING delta "
                + "TBLPROPERTIES ('delta.columnMapping.mode' = 'id', "
                + "  'delta.minReaderVersion' = '2', "
                + "  'delta.minWriterVersion' = '5')",
            tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (0, 'seed')", tablePath));

    Dataset<Row> streamingDF = spark.readStream().table(dsv2TableRef);

    AtomicBoolean stop = new AtomicBoolean(false);
    ExecutorService writer = Executors.newSingleThreadExecutor();
    StreamingQuery query = null;
    try {
      query =
          streamingDF
              .writeStream()
              .format("parquet")
              .outputMode("append")
              .option("path", outputDir.getAbsolutePath())
              .option("checkpointLocation", checkpointDir.getAbsolutePath())
              .start();

      // Concurrent appender: 10 single-row commits with distinct ids.
      writer.submit(
          () -> {
            int i = 1;
            while (!stop.get() && i <= 10) {
              try {
                spark.sql(str("INSERT INTO delta.`%s` VALUES (%d, 'r%d')", tablePath, i, i));
                Thread.sleep(50);
                i++;
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
              } catch (Exception ignored) {
                // Concurrent commits may transiently fail; just continue.
              }
            }
          });

      // Give the appender time to land commits, then drain.
      Thread.sleep(2_000L);
      stop.set(true);
      query.processAllAvailable();

      final StreamingQuery finalQ = query;
      assertTrue(
          finalQ.exception().isEmpty(),
          () ->
              "Concurrent appender on CM-id table must not error: "
                  + (finalQ.exception().isDefined() ? finalQ.exception().get().toString() : ""));
    } finally {
      stop.set(true);
      writer.shutdownNow();
      writer.awaitTermination(5, TimeUnit.SECONDS);
      if (query != null) {
        query.stop();
      }
      org.apache.spark.sql.delta.DeltaLog.clearCache();
    }

    // All sink rows must carry the correct CM-id logical column names and have a non-null name.
    Dataset<Row> sinkDf = spark.read().parquet(outputDir.getAbsolutePath());
    assertArrayEquals(
        new String[] {"id", "name"},
        sinkDf.schema().fieldNames(),
        "Sink rows must carry the CM-id logical column names");

    // Sink must include the seed row plus at least some concurrently-appended rows. The exact
    // count depends on how many commits land before drain; assert seed presence + monotone size.
    long sourceCount = spark.read().format("delta").load(tablePath).count();
    List<Row> sinkRows = sinkDf.collectAsList();
    assertTrue(
        sinkRows.size() >= 1,
        () -> "stream should emit at least the seed row, got " + sinkRows.size());
    assertTrue(
        sinkRows.size() <= sourceCount,
        () -> "sink size " + sinkRows.size() + " must not exceed source " + sourceCount);
    // Every emitted row resolves the CM-id mapping correctly: name == "r" + id, or "seed" for id=0.
    for (Row r : sinkRows) {
      int id = r.getInt(0);
      String name = r.getString(1);
      assertNotNull(name, () -> "name column must be non-null for id=" + id);
      String expectedName = id == 0 ? "seed" : ("r" + id);
      assertEquals(
          expectedName,
          name,
          () -> "CM-id mapping garbled name for id=" + id + ": got '" + name + "'");
    }
  }

  /**
   * Process a streaming query that resumes from a specific checkpoint location, returning the rows
   * materialized into a memory sink.
   */
  protected List<Row> processStreamingQueryFromCheckpoint(
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
        org.apache.spark.sql.delta.DeltaLog.clearCache();
      }
    }
  }
}
