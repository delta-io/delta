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
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests for V2 streaming reads on column-mapped Delta tables.
 *
 * <p>These tests target the cross-product cell "F2/F3 ColMap × streaming" identified as the top-1
 * gap in the cross_product_matrix audit (2026-05-04). DSv1 has ~7 such tests in {@code
 * DeltaSourceColumnMappingSuite}; DSv2 had zero coverage prior to this file.
 *
 * <p>Patterns mirror {@code DeltaSourceColumnMappingSuite} and {@code
 * DeltaSourceSchemaEvolutionSuite} where applicable, adapted to JUnit 5 + the V2 catalog test
 * harness.
 */
public class V2StreamingColumnMappingTest extends V2TestBase {

  /** 1. Column mapping mode=name × stream basic. */
  @Test
  public void testStreamingBasic_columnMappingModeName(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, name STRING, value DOUBLE) "
                + "USING delta "
                + "TBLPROPERTIES ('delta.columnMapping.mode' = 'name')",
            tablePath));
    spark.sql(
        str("INSERT INTO delta.`%s` VALUES (1, 'Alice', 100.0), (2, 'Bob', 200.0)", tablePath));

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> streamingDF = spark.readStream().table(dsv2TableRef);
    assertTrue(streamingDF.isStreaming());

    List<Row> actualRows = processStreamingQuery(streamingDF, "test_cm_name_basic");
    List<Row> expected =
        Arrays.asList(RowFactory.create(1, "Alice", 100.0), RowFactory.create(2, "Bob", 200.0));
    assertDataEquals(actualRows, expected);
  }

  /** 2. Column mapping mode=id × stream basic. */
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

    List<Row> actualRows = processStreamingQuery(streamingDF, "test_cm_id_basic");
    List<Row> expected =
        Arrays.asList(RowFactory.create(1, "Alice", 100.0), RowFactory.create(2, "Bob", 200.0));
    assertDataEquals(actualRows, expected);
  }

  /**
   * 3. Column mapping × add column mid-stream. Mirrors DSv1 "column mapping + streaming - allowed
   * workflows - column addition".
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

  /** 4. Column mapping × drop column with unsafe flag + schema-tracking on. */
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

  /** 5. Column mapping × rename column with unsafe flag. */
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

  /** 6. Column mapping × DV. DELETE on a mode=name table, stream. */
  @Test
  public void testColumnMapping_withDeletionVectors(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);

    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, name STRING) USING delta "
                + "TBLPROPERTIES ('delta.columnMapping.mode' = 'name', "
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
    List<Row> actualRows = processStreamingQuery(streamingDF, "test_cm_dv");

    // Initial snapshot reads from latest version, after the DELETE → expect 3..9
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

  /** 7. Column mapping × startingVersion. Stream from a version > 0. */
  @Test
  public void testColumnMapping_startingVersion(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);

    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, name STRING) USING delta "
                + "TBLPROPERTIES ('delta.columnMapping.mode' = 'name')",
            tablePath));
    // version 1
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'Alice')", tablePath));
    // version 2
    spark.sql(str("INSERT INTO delta.`%s` VALUES (2, 'Bob')", tablePath));
    // version 3
    spark.sql(str("INSERT INTO delta.`%s` VALUES (3, 'Charlie')", tablePath));

    Dataset<Row> streamingDF =
        spark.readStream().option("startingVersion", "2").table(dsv2TableRef);
    List<Row> actualRows = processStreamingQuery(streamingDF, "test_cm_starting_version");

    List<Row> expected =
        Arrays.asList(RowFactory.create(2, "Bob"), RowFactory.create(3, "Charlie"));
    assertDataEquals(actualRows, expected);
  }

  /** 8. Column mapping × Trigger.AvailableNow. */
  @Test
  public void testColumnMapping_triggerAvailableNow(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);

    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, name STRING) USING delta "
                + "TBLPROPERTIES ('delta.columnMapping.mode' = 'name')",
            tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'Alice'), (2, 'Bob')", tablePath));

    File checkpointDir = new File(deltaTablePath, "_checkpoint");
    Dataset<Row> streamingDF = spark.readStream().table(dsv2TableRef);

    StreamingQuery q =
        streamingDF
            .writeStream()
            .format("memory")
            .queryName("test_cm_avail_now")
            .outputMode("append")
            .trigger(Trigger.AvailableNow())
            .option("checkpointLocation", checkpointDir.getAbsolutePath())
            .start();
    try {
      q.processAllAvailable();
      assertTrue(q.awaitTermination(60_000));
      List<Row> rows = spark.sql("SELECT * FROM test_cm_avail_now").collectAsList();
      List<Row> expected =
          Arrays.asList(RowFactory.create(1, "Alice"), RowFactory.create(2, "Bob"));
      assertDataEquals(rows, expected);
    } finally {
      q.stop();
    }
  }

  /** 9. Column mapping × restart. Stop, restart, verify schema resolution. */
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

    // First stream — drains 1, 2
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

  /** 10. Column mapping × add column then rename column (sequential schema changes). */
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

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

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
