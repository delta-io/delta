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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.delta.DeltaLog;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.Utils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import scala.Option;

/**
 * Integration tests for DSv2 row tracking read behavior.
 *
 * <p>This suite validates row-tracking metadata serving through DSv2 queries, including metadata
 * projection combinations (row_id only, row_commit_version only, both, and none), update evolution
 * semantics (commit version bump and stable row_id), deletion-vector interaction, and partitioned
 * table reads with correct column ordering around {@code _metadata}.
 *
 * <p>Uses real Delta tables with row tracking enabled -- no mocking.
 */
public class V2RowTrackingReadTest extends DeltaV2TestBase {

  private String tablePath;

  @BeforeEach
  public void setUpTable() {
    tablePath =
        java.nio.file.Paths.get(
                System.getProperty("java.io.tmpdir"), "delta-test-rt-" + System.nanoTime())
            .toString();
    spark.sql(
        String.format(
            "CREATE TABLE delta.`%s` (id LONG, name STRING) USING delta "
                + "TBLPROPERTIES ('delta.enableRowTracking' = 'true')",
            tablePath));
    spark.sql(
        String.format(
            "INSERT INTO delta.`%s` VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')", tablePath));
  }

  @AfterEach
  public void tearDownTable() {
    if (tablePath != null) {
      Utils.deleteRecursively(new File(tablePath));
    }
  }

  // ---------------------------------------------------------------------------
  //  Row tracking metadata fields (rowId, rowCommitVersion) reading matrix
  // ---------------------------------------------------------------------------

  @Test
  public void testReadOnlyRowIdMetadataField() {
    org.apache.spark.sql.Dataset<Row> df =
        spark.sql(
            String.format(
                "SELECT id, _metadata.row_id " + "FROM dsv2.delta.`%s` ORDER BY id", tablePath));
    assertArrayEquals(
        new String[] {"id", "row_id"},
        df.schema().fieldNames(),
        "Expected query output columns: id, row_id");

    List<Row> rows = df.collectAsList();
    assertEquals(3, rows.size());
    assertEquals(1L, rows.get(0).getLong(0));
    assertEquals(2L, rows.get(1).getLong(0));
    assertEquals(3L, rows.get(2).getLong(0));
    assertEquals(0L, rows.get(0).getLong(1));
    assertEquals(1L, rows.get(1).getLong(1));
    assertEquals(2L, rows.get(2).getLong(1));
  }

  @Test
  public void testReadOnlyRowCommitVersionMetadataField() {
    org.apache.spark.sql.Dataset<Row> df =
        spark.sql(
            String.format(
                "SELECT id, _metadata.row_commit_version " + "FROM dsv2.delta.`%s` ORDER BY id",
                tablePath));
    assertArrayEquals(
        new String[] {"id", "row_commit_version"},
        df.schema().fieldNames(),
        "Expected query output columns: id, row_commit_version");

    List<Row> rows = df.collectAsList();
    assertEquals(3, rows.size());
    assertEquals(1L, rows.get(0).getLong(1));
    assertEquals(1L, rows.get(1).getLong(1));
    assertEquals(1L, rows.get(2).getLong(1));
  }

  @Test
  public void testReadBothRowTrackingMetadataFields() {
    org.apache.spark.sql.Dataset<Row> df =
        spark.sql(
            String.format(
                "SELECT id, _metadata.row_id, _metadata.row_commit_version "
                    + "FROM dsv2.delta.`%s` ORDER BY id",
                tablePath));
    assertArrayEquals(
        new String[] {"id", "row_id", "row_commit_version"},
        df.schema().fieldNames(),
        "Expected query output columns: id, row_id, row_commit_version");

    List<Row> rows = df.collectAsList();
    assertEquals(3, rows.size());
    assertEquals(0L, rows.get(0).getLong(1));
    assertEquals(1L, rows.get(1).getLong(1));
    assertEquals(2L, rows.get(2).getLong(1));
    assertEquals(1L, rows.get(0).getLong(2));
    assertEquals(1L, rows.get(1).getLong(2));
    assertEquals(1L, rows.get(2).getLong(2));
  }

  @Test
  public void testReadWithoutRowTrackingMetadataFields() {
    org.apache.spark.sql.Dataset<Row> df =
        spark.sql(
            String.format("SELECT id, name " + "FROM dsv2.delta.`%s` ORDER BY id", tablePath));
    assertArrayEquals(
        new String[] {"id", "name"},
        df.schema().fieldNames(),
        "Expected query output columns: id, name");

    List<Row> rows = df.collectAsList();
    assertEquals(3, rows.size());
    assertEquals(1L, rows.get(0).getLong(0));
    assertEquals("Alice", rows.get(0).getString(1));
    assertEquals(2L, rows.get(1).getLong(0));
    assertEquals("Bob", rows.get(1).getString(1));
    assertEquals(3L, rows.get(2).getLong(0));
    assertEquals("Charlie", rows.get(2).getString(1));
  }

  @Test
  public void testReadWithRowTrackingDisabledMetadataFieldAccess(@TempDir File tempDir) {
    String noRtPath = tempDir.getAbsolutePath();
    spark.sql(
        String.format(
            "CREATE TABLE delta.`%s` (id LONG, name STRING) USING delta "
                + "TBLPROPERTIES ('delta.enableRowTracking' = 'false')",
            noRtPath));
    spark.sql(String.format("INSERT INTO delta.`%s` VALUES (1, 'Alice'), (2, 'Bob')", noRtPath));

    org.apache.spark.sql.Dataset<Row> metadataDf =
        spark.sql(String.format("SELECT _metadata FROM dsv2.delta.`%s`", noRtPath));
    StructType metadataSchema = (StructType) metadataDf.schema().apply("_metadata").dataType();
    assertEquals(
        0,
        metadataSchema.fields().length,
        "Expected _metadata to be an empty struct for row-tracking-disabled tables");

    List<Row> metadataRows = metadataDf.collectAsList();
    assertEquals(2, metadataRows.size());
    for (Row row : metadataRows) {
      assertNotNull(row.getStruct(0), "Expected _metadata struct value to be present");
      assertEquals(0, row.getStruct(0).size(), "Expected _metadata struct to have no fields");
    }

    AnalysisException e =
        assertThrows(
            AnalysisException.class,
            () ->
                spark
                    .sql(String.format("SELECT _metadata.row_id FROM dsv2.delta.`%s`", noRtPath))
                    .collectAsList());
    assertTrue(
        e.getMessage().contains("row_id"),
        "Expected analysis error mentioning missing _metadata.row_id");
  }

  // ---------------------------------------------------------------------------
  //  Second insert: rowId continues from high watermark
  // ---------------------------------------------------------------------------

  @Test
  public void testSecondInsertContinuesFromHighWatermark() {
    spark.sql(String.format("INSERT INTO delta.`%s` VALUES (4, 'Dave'), (5, 'Eve')", tablePath));

    List<Row> rows = queryRowTrackingWithRowTrackingMetadata(tablePath);
    assertEquals(5, rows.size());
    assertEquals(2L, rows.get(3).getLong(3), "Dave's commitVersion should be 2");
    assertEquals(2L, rows.get(4).getLong(3), "Eve's commitVersion should be 2");
    assertEquals(rows.get(3).getLong(2), 3L, "Dave's rowId should continue from watermark");
    assertEquals(rows.get(4).getLong(2), 4L, "Eve's rowId should continue from watermark");
  }

  // ---------------------------------------------------------------------------
  //  Update behaviour: commitVersion and rowId work correct
  // ---------------------------------------------------------------------------

  @Test
  public void testUpdateBumpsCommitVersion() {
    List<Row> beforeRows = queryRowTrackingWithRowTrackingMetadata(tablePath);
    assertEquals(3, beforeRows.size());
    assertEquals(1L, beforeRows.get(0).getLong(0), "ALICE's id");
    long previousCommitVersion = beforeRows.get(0).getLong(3);

    spark.sql(String.format("UPDATE delta.`%s` SET name = 'ALICE' WHERE id = 1", tablePath));

    List<Row> rows = queryRowTrackingWithRowTrackingMetadata(tablePath);
    assertEquals(1L, rows.get(0).getLong(0), "ALICE's id");
    assertEquals(
        previousCommitVersion + 1L,
        rows.get(0).getLong(3),
        "ALICE's commitVersion should be previous + 1 (original insert version)");
  }

  @Test
  public void testUpdateKeepsRowIdStable() {
    long originalUpdatedRowId =
        queryRowTrackingWithRowTrackingMetadata(tablePath).get(0).getLong(2);

    spark.sql(String.format("UPDATE delta.`%s` SET name = 'ALICE' WHERE id = 1", tablePath));

    List<Row> rows = queryRowTrackingWithRowTrackingMetadata(tablePath);
    assertEquals(3, rows.size());
    assertEquals(
        originalUpdatedRowId, rows.get(0).getLong(2), "Updated row should keep the same row_id");
  }

  // ---------------------------------------------------------------------------
  //  DV + row tracking: wrapper order keeps physical row IDs
  // ---------------------------------------------------------------------------

  @Test
  public void testDeletionVectorReadWithRowTrackingPreservesPhysicalRowIds(@TempDir File tempDir) {
    String dvRtPath = tempDir.getAbsolutePath();

    spark.sql(
        String.format(
            "CREATE TABLE delta.`%s` (id LONG, name STRING) USING delta "
                + "TBLPROPERTIES ("
                + "'delta.enableRowTracking' = 'true', "
                + "'delta.enableDeletionVectors' = 'true')",
            dvRtPath));

    // Insert enough rows to encourage DV-based deletes (instead of full-file rewrites).
    spark
        .range(1000)
        .selectExpr("id", "cast(id as string) as name")
        .write()
        .format("delta")
        .mode("append")
        .save(dvRtPath);

    spark.sql(String.format("DELETE FROM delta.`%s` WHERE id %% 2 = 0", dvRtPath));

    // Keep parity with existing DV tests: assert that DVs were actually created.
    DeltaLog deltaLog = DeltaLog.forTable(spark, dvRtPath);
    long numDVs =
        (long)
            deltaLog
                .update(false, Option.empty(), Option.empty())
                .numDeletionVectorsOpt()
                .getOrElse(() -> 0L);
    assertTrue(numDVs > 0, "Expected deletion vectors to be created, but none were found");

    List<Row> rows =
        spark
            .sql(
                String.format(
                    "SELECT id, _metadata.row_id, _metadata.row_commit_version "
                        + "FROM dsv2.delta.`%s` ORDER BY id",
                    dvRtPath))
            .collectAsList();

    assertEquals(500, rows.size(), "Expected only odd IDs after DV filtering");
    for (int i = 0; i < rows.size(); i++) {
      Row row = rows.get(i);
      long id = row.getLong(0);
      long rowId = row.getLong(1);
      long rowCommitVersion = row.getLong(2);
      assertEquals(2L * i + 1L, id, "Unexpected surviving row ID ordering");
      // If RT runs after DV filtering with stable physical row positions, row_id stays equal to id.
      assertEquals(id, rowId, "row_id should preserve physical row identity under DV filtering");
      assertEquals(1L, rowCommitVersion, "Unmodified rows should keep original commit version");
    }
  }

  // ---------------------------------------------------------------------------
  //  Partitioned table + row tracking: partition columns preserved
  // ---------------------------------------------------------------------------

  @Test
  public void testPartitionedTableReadWithRowTrackingPreservesPartitionColumns(
      @TempDir File tempDir) {
    String partitionedPath = tempDir.getAbsolutePath();
    spark.sql(
        String.format(
            "CREATE TABLE delta.`%s` "
                + "(id LONG, name STRING, value DOUBLE, date STRING, city STRING) USING delta "
                + "PARTITIONED BY (date, city) "
                + "TBLPROPERTIES ('delta.enableRowTracking' = 'true')",
            partitionedPath));
    spark.sql(
        String.format(
            "INSERT INTO delta.`%s` VALUES "
                + "(1, 'Alice', 10.5, '2026-01-01', 'SF'), "
                + "(2, 'Bob', 20.0, '2026-01-01', 'NY'), "
                + "(3, 'Carol', 30.25, '2026-01-02', 'SF'), "
                + "(4, 'Dave', 40.75, '2026-01-02', 'NY')",
            partitionedPath));

    org.apache.spark.sql.Dataset<Row> df =
        spark.sql(
            String.format(
                "SELECT id, name, value, _metadata, date, city "
                    + "FROM dsv2.delta.`%s` ORDER BY id",
                partitionedPath));
    assertArrayEquals(
        new String[] {"id", "name", "value", "_metadata", "date", "city"},
        df.schema().fieldNames(),
        "Expected output ordering: data columns, _metadata, partition columns");
    StructType metadataSchema = (StructType) df.schema().apply("_metadata").dataType();
    assertArrayEquals(
        new String[] {"row_id", "row_commit_version"},
        metadataSchema.fieldNames(),
        "Unexpected _metadata struct field order");

    List<Row> rows = df.collectAsList();
    assertEquals(4, rows.size());

    Set<Long> rowIds = new HashSet<>();
    for (int i = 0; i < rows.size(); i++) {
      Row row = rows.get(i);
      long id = row.getLong(0);
      String name = row.getString(1);
      double value = row.getDouble(2);
      Row metadata = row.getStruct(3);
      String date = row.getString(4);
      String city = row.getString(5);
      long rowId = metadata.getLong(0);
      long rowCommitVersion = metadata.getLong(1);

      if (id == 1L) {
        assertEquals("Alice", name);
        assertEquals(10.5d, value, 0.0d);
        assertEquals("2026-01-01", date);
        assertEquals("SF", city);
      } else if (id == 2L) {
        assertEquals("Bob", name);
        assertEquals(20.0d, value, 0.0d);
        assertEquals("2026-01-01", date);
        assertEquals("NY", city);
      } else if (id == 3L) {
        assertEquals("Carol", name);
        assertEquals(30.25d, value, 0.0d);
        assertEquals("2026-01-02", date);
        assertEquals("SF", city);
      } else if (id == 4L) {
        assertEquals("Dave", name);
        assertEquals(40.75d, value, 0.0d);
        assertEquals("2026-01-02", date);
        assertEquals("NY", city);
      } else {
        fail("Unexpected id in result set: " + id);
      }
      assertTrue(rowIds.add(rowId), "row_id should be unique per returned row");
      assertEquals(1L, rowCommitVersion, "Initial insert should have commit version 1");
    }
  }

  /**
   * Row tracking with a partition column declared in the MIDDLE of the DDL. Exercises the
   * interaction between {@code RowTrackingReadFunction} (splices {@code _metadata} between data and
   * partition columns) and {@code ColumnReorderReadFunction} (permutes data++partitions into DDL
   * order). Without correct layering the row-mode path would emit values from the wrong column.
   */
  @Test
  public void testReadWithRowTrackingAndPartitionColumnInMiddle(@TempDir File tempDir) {
    String path = tempDir.getAbsolutePath();
    spark.sql(
        String.format(
            "CREATE TABLE delta.`%s` (id LONG, part STRING, value DOUBLE) USING delta "
                + "PARTITIONED BY (part) "
                + "TBLPROPERTIES ('delta.enableRowTracking' = 'true')",
            path));
    spark.sql(
        String.format(
            "INSERT INTO delta.`%s` VALUES (1, 'a', 10.5), (2, 'b', 20.0), (3, 'a', 30.25)", path));

    org.apache.spark.sql.Dataset<Row> df =
        spark.sql(
            String.format(
                "SELECT id, part, value, _metadata.row_id FROM dsv2.delta.`%s` ORDER BY id", path));
    assertArrayEquals(new String[] {"id", "part", "value", "row_id"}, df.schema().fieldNames());

    List<Row> rows = df.collectAsList();
    assertEquals(3, rows.size());
    assertEquals(1L, rows.get(0).getLong(0));
    assertEquals("a", rows.get(0).getString(1));
    assertEquals(10.5d, rows.get(0).getDouble(2), 0.0d);
    assertEquals(2L, rows.get(1).getLong(0));
    assertEquals("b", rows.get(1).getString(1));
    assertEquals(20.0d, rows.get(1).getDouble(2), 0.0d);
    assertEquals(3L, rows.get(2).getLong(0));
    assertEquals("a", rows.get(2).getString(1));
    assertEquals(30.25d, rows.get(2).getDouble(2), 0.0d);
    Set<Long> rowIds = new HashSet<>();
    for (Row row : rows) {
      assertTrue(rowIds.add(row.getLong(3)), "row_id should be unique");
    }
  }

  @Test
  public void testReadWithRowTrackingAndDeletionVectorAndPartitionColumnInMiddle(
      @TempDir File tempDir) {
    String path = tempDir.getAbsolutePath();
    spark.sql(
        String.format(
            "CREATE TABLE delta.`%s` (id LONG, part LONG, value INT) USING delta "
                + "PARTITIONED BY (part) "
                + "TBLPROPERTIES ("
                + "'delta.enableRowTracking' = 'true', "
                + "'delta.enableDeletionVectors' = 'true')",
            path));
    // Insert enough rows so DELETE picks the DV path instead of rewriting whole files.
    spark
        .range(1000)
        .selectExpr("id", "id % 4 AS part", "cast(id * 10 AS INT) AS value")
        .write()
        .format("delta")
        .mode("append")
        .save(path);
    // Low-selectivity, non-partition predicate so Delta uses DVs.
    spark.sql(String.format("DELETE FROM delta.`%s` WHERE id < 100", path));

    DeltaLog deltaLog = DeltaLog.forTable(spark, path);
    long numDVs =
        (long)
            deltaLog
                .update(false, Option.empty(), Option.empty())
                .numDeletionVectorsOpt()
                .getOrElse(() -> 0L);
    assertTrue(numDVs > 0, "Expected deletion vectors to be produced");

    org.apache.spark.sql.Dataset<Row> df =
        spark.sql(
            String.format(
                "SELECT id, part, value, _metadata.row_id FROM dsv2.delta.`%s` ORDER BY id", path));
    assertArrayEquals(new String[] {"id", "part", "value", "row_id"}, df.schema().fieldNames());

    List<Row> rows = df.collectAsList();
    assertEquals(900, rows.size(), "Expected 900 surviving rows (id >= 100) after DV delete");
    Set<Long> rowIds = new HashSet<>();
    for (Row row : rows) {
      long id = row.getLong(0);
      long part = row.getLong(1);
      int value = row.getInt(2);
      assertEquals(id % 4, part, "Partition column should match insert formula");
      assertEquals((int) (id * 10), value, "Value column should match insert formula");
      assertTrue(rowIds.add(row.getLong(3)), "row_id should be unique under DV filtering");
    }
  }

  /**
   * Queries row tracking metadata via the DSv2 SQL path. V2 metadata columns are nested in
   * _metadata struct. Returns rows ordered by id with columns: id, name, row_id,
   * row_commit_version.
   */
  private List<Row> queryRowTrackingWithRowTrackingMetadata(String path) {
    return spark
        .sql(
            String.format(
                "SELECT id, name, _metadata.row_id, _metadata.row_commit_version "
                    + "FROM dsv2.delta.`%s` ORDER BY id",
                path))
        .collectAsList();
  }
}
