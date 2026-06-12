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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.Utils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Integration tests for DSv2 {@code _metadata} column reads that are not tied to row tracking.
 *
 * <p>This suite exercises generic metadata subfields against real Delta tables, including tables
 * where row tracking is disabled, to verify that DSv2 exposes metadata independently of
 * row-tracking machinery.
 */
public class V2MetadataReadTest extends DeltaV2TestBase {

  private String tablePath;

  @AfterEach
  public void tearDownTable() {
    if (tablePath != null) {
      Utils.deleteRecursively(new File(tablePath));
    }
  }

  // ---------------------------------------------------------------------------
  // Single-subfield projection
  // ---------------------------------------------------------------------------

  @ParameterizedTest(name = "rowTrackingEnabled={0}")
  @ValueSource(booleans = {true, false})
  public void testReadFilePathRegardlessOfRowTracking(boolean rowTrackingEnabled) {
    tablePath = newTablePath("delta-test-metadata-rt-" + (rowTrackingEnabled ? "on-" : "off-"));
    createTable(tablePath, rowTrackingEnabled);
    insertRows(tablePath);

    Dataset<Row> df =
        spark.sql(
            String.format(
                "SELECT id, _metadata.file_path FROM dsv2.delta.`%s` ORDER BY id", tablePath));
    assertArrayEquals(
        new String[] {"id", "file_path"},
        df.schema().fieldNames(),
        "Expected query output columns: id, file_path");

    assertFilePathRows(df.collectAsList(), tablePath);
  }

  /**
   * Round-trip every Spark base {@code _metadata} subfield. We assert each value's shape/range
   * matches what {@link
   * org.apache.spark.sql.execution.datasources.FileFormat.BASE_METADATA_EXTRACTORS} would derive
   * from the underlying {@code PartitionedFile}.
   */
  @ParameterizedTest(name = "subfield={0}")
  @ValueSource(
      strings = {
        "file_path",
        "file_name",
        "file_size",
        "file_block_start",
        "file_block_length",
        "file_modification_time"
      })
  public void testReadEachBaseMetadataSubfield(String subfield) {
    tablePath = newTablePath("delta-test-metadata-subfield-" + subfield + "-");
    createTable(tablePath, /* rowTrackingEnabled= */ false);
    insertRows(tablePath);

    Dataset<Row> df =
        spark.sql(
            String.format(
                "SELECT id, _metadata.%s AS m FROM dsv2.delta.`%s` ORDER BY id",
                subfield, tablePath));
    List<Row> rows = df.collectAsList();
    assertEquals(3, rows.size());
    for (Row row : rows) {
      Object value = row.get(1);
      assertNotNull(value, "Expected _metadata." + subfield + " to be populated");
      assertSubfieldValueShape(subfield, value, tablePath);
    }

    // All three rows came from a single INSERT -> same file -> identical subfield values.
    Object firstValue = rows.get(0).get(1);
    for (int i = 1; i < rows.size(); i++) {
      assertEquals(
          firstValue,
          rows.get(i).get(1),
          "Single-INSERT rows should share the same _metadata." + subfield);
    }
  }

  // ---------------------------------------------------------------------------
  // Whole-struct projection - `SELECT _metadata`, `SELECT _metadata, *`
  // ---------------------------------------------------------------------------

  @Test
  public void testSelectMetadataStructAlone() {
    tablePath = newTablePath("delta-test-metadata-whole-");
    createTable(tablePath, /* rowTrackingEnabled= */ false);
    insertRows(tablePath);

    Dataset<Row> df = spark.sql(String.format("SELECT _metadata FROM dsv2.delta.`%s`", tablePath));
    assertArrayEquals(
        new String[] {"_metadata"},
        df.schema().fieldNames(),
        "Output must have a single top-level _metadata column");

    StructField metadataField = df.schema().fields()[0];
    assertTrue(metadataField.dataType() instanceof StructType, "_metadata must be a struct");
    String[] metadataSubfieldNames = ((StructType) metadataField.dataType()).fieldNames();
    // Spark expands a bare `_metadata` reference to the full base set when row tracking is off.
    assertArrayEquals(
        new String[] {
          "file_path",
          "file_name",
          "file_size",
          "file_block_start",
          "file_block_length",
          "file_modification_time"
        },
        metadataSubfieldNames,
        "Bare _metadata should expand to all six Spark base fields");

    List<Row> rows = df.collectAsList();
    assertEquals(3, rows.size());
    for (Row row : rows) {
      Row metadata = row.getStruct(0);
      assertNotNull(metadata);
      assertSubfieldValueShape("file_path", metadata.get(0), tablePath);
      assertSubfieldValueShape("file_name", metadata.get(1), tablePath);
      assertSubfieldValueShape("file_size", metadata.get(2), tablePath);
      assertSubfieldValueShape("file_block_start", metadata.get(3), tablePath);
      assertSubfieldValueShape("file_block_length", metadata.get(4), tablePath);
      assertSubfieldValueShape("file_modification_time", metadata.get(5), tablePath);
    }
  }

  @Test
  public void testSelectMetadataAlongsideStar() {
    tablePath = newTablePath("delta-test-metadata-star-");
    createTable(tablePath, /* rowTrackingEnabled= */ false);
    insertRows(tablePath);

    Dataset<Row> df =
        spark.sql(String.format("SELECT _metadata, * FROM dsv2.delta.`%s` ORDER BY id", tablePath));
    // Output columns: _metadata + data columns (id, name). _metadata position is leading.
    String[] outputNames = df.schema().fieldNames();
    assertArrayEquals(
        new String[] {"_metadata", "id", "name"},
        outputNames,
        "Output should be [_metadata, id, name] in that order");

    List<Row> rows = df.collectAsList();
    assertEquals(3, rows.size());
    for (int i = 0; i < rows.size(); i++) {
      Row row = rows.get(i);
      Row metadata = row.getStruct(0);
      assertNotNull(metadata, "_metadata struct must be populated");
      assertSubfieldValueShape("file_path", metadata.get(0), tablePath);
      assertEquals(i + 1L, row.getLong(1), "data column `id` mismatch");
      assertNotNull(row.getString(2), "data column `name` should be populated");
    }
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private static String newTablePath(String prefix) {
    return Paths.get(System.getProperty("java.io.tmpdir"), prefix + System.nanoTime()).toString();
  }

  private void createTable(String path, boolean rowTrackingEnabled) {
    spark.sql(
        String.format(
            "CREATE TABLE delta.`%s` (id LONG, name STRING) USING delta "
                + "TBLPROPERTIES ('delta.enableRowTracking' = '%s')",
            path, rowTrackingEnabled ? "true" : "false"));
  }

  private void insertRows(String path) {
    spark.sql(
        String.format(
            "INSERT INTO delta.`%s` VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')", path));
  }

  /**
   * Per-subfield value-shape assertions. Each metadata subfield has a deterministic type and value
   * range; we sanity-check the field is plausible rather than re-computing the expected value from
   * the underlying file metadata.
   */
  private static void assertSubfieldValueShape(String subfield, Object value, String tablePath) {
    switch (subfield) {
      case "file_path":
        {
          String filePath = (String) value;
          assertTrue(filePath.startsWith("file:"), "Expected file:// URI, got: " + filePath);
          assertTrue(filePath.endsWith(".parquet"), "Expected Parquet extension, got: " + filePath);
          assertTrue(
              filePath.contains(tablePath),
              "file_path should reference the table path, got: " + filePath);
          break;
        }
      case "file_name":
        {
          String fileName = (String) value;
          assertTrue(
              fileName.endsWith(".parquet"),
              "Expected Parquet extension on file_name, got: " + fileName);
          assertTrue(
              !fileName.contains("/"),
              "file_name must be a leaf name, not a path, got: " + fileName);
          break;
        }
      case "file_size":
        {
          long size = ((Number) value).longValue();
          assertTrue(size > 0, "file_size must be positive, got: " + size);
          break;
        }
      case "file_block_start":
        {
          long blockStart = ((Number) value).longValue();
          assertTrue(blockStart >= 0, "file_block_start must be non-negative, got: " + blockStart);
          break;
        }
      case "file_block_length":
        {
          long blockLength = ((Number) value).longValue();
          assertTrue(blockLength > 0, "file_block_length must be positive, got: " + blockLength);
          break;
        }
      case "file_modification_time":
        {
          // Spark exposes this as a Timestamp with microsecond granularity.
          assertTrue(
              value instanceof Timestamp,
              "Expected Timestamp for file_modification_time, got: " + value.getClass());
          long millis = ((Timestamp) value).getTime();
          assertTrue(millis > 0, "file_modification_time must be a positive epoch, got: " + millis);
          break;
        }
      default:
        throw new IllegalArgumentException("Unknown subfield: " + subfield);
    }
  }

  private static void assertFilePathRows(List<Row> rows, String tablePath) {
    assertEquals(3, rows.size());
    for (int i = 0; i < rows.size(); i++) {
      assertEquals(i + 1L, rows.get(i).getLong(0));
      String filePath = rows.get(i).getString(1);
      assertSubfieldValueShape("file_path", filePath, tablePath);
    }
    // A single INSERT produces one parquet file, so every row must carry the same file_path.
    String firstFilePath = rows.get(0).getString(1);
    for (int i = 1; i < rows.size(); i++) {
      assertEquals(
          firstFilePath,
          rows.get(i).getString(1),
          "All rows from a single INSERT should share the same _metadata.file_path");
    }
  }
}
