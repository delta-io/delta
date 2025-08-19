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
package io.delta.spark.dsv2.scan.batch;

import static org.junit.jupiter.api.Assertions.*;

import io.delta.kernel.Scan;
import io.delta.kernel.TableManager;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.internal.json.JsonUtils;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.spark.dsv2.KernelSparkDsv2TestBase;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType$;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class KernelSparkPartitionReaderTest extends KernelSparkDsv2TestBase {

  @Test
  public void testConstructorWithNullScanState() {
    assertThrows(
        NullPointerException.class,
        () -> new KernelSparkPartitionReader(null, "{\"file\":\"data\"}"));
  }

  @Test
  public void testConstructorWithNullFileRow() {
    assertThrows(
        NullPointerException.class,
        () -> new KernelSparkPartitionReader("{\"scan\":\"data\"}", null));
  }

  @Test
  public void testConstructorValidInput() {
    String scanState = "{\"scan\":\"state\"}";
    String fileRow = "{\"file\":\"row\"}";

    KernelSparkPartitionReader reader = new KernelSparkPartitionReader(scanState, fileRow);
    assertNotNull(reader);
  }

  @Test
  public void testCloseWithoutInitialization() throws Exception {
    String scanState = "{\"scan\":\"state\"}";
    String fileRow = "{\"file\":\"row\"}";

    KernelSparkPartitionReader reader = new KernelSparkPartitionReader(scanState, fileRow);

    // Should not throw exception
    assertDoesNotThrow(() -> reader.close());
  }

  @Test
  public void testReadDataFromRealDeltaTable(@TempDir File deltaTablePath) throws Exception {
    // Create a real Delta table with test data
    String tablePath = deltaTablePath.getAbsolutePath();

    Dataset<org.apache.spark.sql.Row> testData =
        spark.createDataFrame(
            Arrays.asList(
                RowFactory.create(1, "Alice", 100.0),
                RowFactory.create(2, "Bob", 200.0),
                RowFactory.create(3, "Charlie", 300.0)),
            StructType$.MODULE$.apply(
                Arrays.asList(
                    DataTypes.createStructField("id", DataTypes.IntegerType, false),
                    DataTypes.createStructField("name", DataTypes.StringType, false),
                    DataTypes.createStructField("value", DataTypes.DoubleType, false))));

    testData.write().format("delta").save(tablePath);

    // Load table using Kernel API
    SnapshotImpl snapshot =
        (SnapshotImpl) TableManager.loadSnapshot(tablePath).build(defaultEngine);
    Scan scan = snapshot.getScanBuilder().build();

    // Get scan state and file row
    Row scanStateRow = scan.getScanState(defaultEngine);
    String serializedScanState = JsonUtils.rowToJson(scanStateRow);

    String serializedScanFileRow = null;
    try (CloseableIterator<FilteredColumnarBatch> scanFiles = scan.getScanFiles(defaultEngine)) {
      if (scanFiles.hasNext()) {
        FilteredColumnarBatch batch = scanFiles.next();
        try (CloseableIterator<Row> rows = batch.getRows()) {
          if (rows.hasNext()) {
            Row fileRow = rows.next();
            serializedScanFileRow = JsonUtils.rowToJson(fileRow);
          }
        }
      }
    }

    assertNotNull(serializedScanFileRow, "Should have file row data");

    // Create and test reader
    KernelSparkPartitionReader reader =
        new KernelSparkPartitionReader(serializedScanState, serializedScanFileRow);

    List<InternalRow> rows = new ArrayList<>();

    try {
      while (reader.next()) {
        InternalRow row = reader.get();
        assertNotNull(row);
        rows.add(row.copy()); // Copy because reader might reuse row objects
      }
    } finally {
      reader.close();
    }

    // Verify we read some data
    assertTrue(rows.size() > 0, "Should have read at least some rows");

    // Verify row structure (should have 3 columns: id, name, value)
    for (InternalRow row : rows) {
      assertNotNull(row);
      assertTrue(row.numFields() >= 3, "Row should have at least 3 fields");

      // Verify data types
      // ID should be integer
      assertFalse(row.isNullAt(0), "ID should not be null");

      // Name should be string (UTF8String)
      if (!row.isNullAt(1)) {
        Object nameValue = row.get(1, DataTypes.StringType);
        assertInstanceOf(UTF8String.class, nameValue);
      }

      // Value should be double
      if (!row.isNullAt(2)) {
        assertFalse(Double.isNaN(row.getDouble(2)), "Value should be a valid double");
      }
    }
  }

  @Test
  public void testReadEmptyPartition(@TempDir File deltaTablePath) throws Exception {
    // Create Delta table but don't add any data
    String tablePath = deltaTablePath.getAbsolutePath();

    Dataset<org.apache.spark.sql.Row> emptyData =
        spark.createDataFrame(
            Arrays.asList(), // Empty list
            StructType$.MODULE$.apply(
                Arrays.asList(
                    DataTypes.createStructField("id", DataTypes.IntegerType, false),
                    DataTypes.createStructField("name", DataTypes.StringType, false))));

    emptyData.write().format("delta").save(tablePath);

    // Load table
    SnapshotImpl snapshot =
        (SnapshotImpl) TableManager.loadSnapshot(tablePath).build(defaultEngine);
    Scan scan = snapshot.getScanBuilder().build();

    // Check if there are any scan files
    try (CloseableIterator<FilteredColumnarBatch> scanFiles = scan.getScanFiles(defaultEngine)) {
      if (scanFiles.hasNext()) {
        // If there are scan files, test with them
        FilteredColumnarBatch batch = scanFiles.next();
        try (CloseableIterator<Row> rows = batch.getRows()) {
          if (rows.hasNext()) {
            Row scanStateRow = scan.getScanState(defaultEngine);
            String serializedScanState = JsonUtils.rowToJson(scanStateRow);

            Row fileRow = rows.next();
            String serializedScanFileRow = JsonUtils.rowToJson(fileRow);

            KernelSparkPartitionReader reader =
                new KernelSparkPartitionReader(serializedScanState, serializedScanFileRow);

            try {
              // Should be able to iterate (even if no data)
              int rowCount = 0;
              while (reader.next()) {
                rowCount++;
                assertNotNull(reader.get());
              }
              // Empty partition should have 0 rows
              assertEquals(0, rowCount, "Empty partition should have no rows");
            } finally {
              reader.close();
            }
          }
        }
      }
      // If no scan files, that's also valid for empty table
    }
  }

  @Test
  public void testMultipleIterations(@TempDir File deltaTablePath) throws Exception {
    // Create table with single row
    String tablePath = deltaTablePath.getAbsolutePath();

    Dataset<org.apache.spark.sql.Row> testData =
        spark.createDataFrame(
            Arrays.asList(RowFactory.create(42, "test")),
            StructType$.MODULE$.apply(
                Arrays.asList(
                    DataTypes.createStructField("id", DataTypes.IntegerType, false),
                    DataTypes.createStructField("name", DataTypes.StringType, false))));

    testData.write().format("delta").save(tablePath);

    // Get serialized data
    SnapshotImpl snapshot =
        (SnapshotImpl) TableManager.loadSnapshot(tablePath).build(defaultEngine);
    Scan scan = snapshot.getScanBuilder().build();

    Row scanStateRow = scan.getScanState(defaultEngine);
    String serializedScanState = JsonUtils.rowToJson(scanStateRow);

    String serializedScanFileRow = null;
    try (CloseableIterator<FilteredColumnarBatch> scanFiles = scan.getScanFiles(defaultEngine)) {
      if (scanFiles.hasNext()) {
        FilteredColumnarBatch batch = scanFiles.next();
        try (CloseableIterator<Row> rows = batch.getRows()) {
          if (rows.hasNext()) {
            Row fileRow = rows.next();
            serializedScanFileRow = JsonUtils.rowToJson(fileRow);
          }
        }
      }
    }

    assertNotNull(serializedScanFileRow);

    // Test reader
    KernelSparkPartitionReader reader =
        new KernelSparkPartitionReader(serializedScanState, serializedScanFileRow);

    try {
      // First iteration should work
      assertTrue(reader.next(), "Should have at least one row");
      assertNotNull(reader.get());

      // Subsequent calls to next() should return false
      assertFalse(reader.next(), "Should not have more rows after first");

      // Multiple calls to next() should be safe
      assertFalse(reader.next());
      assertFalse(reader.next());
    } finally {
      reader.close();
    }
  }

  @Test
  public void testReaderWithDifferentDataTypes(@TempDir File deltaTablePath) throws Exception {
    // Create table with various data types
    String tablePath = deltaTablePath.getAbsolutePath();

    Dataset<org.apache.spark.sql.Row> testData =
        spark.createDataFrame(
            Arrays.asList(
                RowFactory.create(1, "string", 3.14, true, 123L),
                RowFactory.create(2, "another", 2.71, false, 456L)),
            StructType$.MODULE$.apply(
                Arrays.asList(
                    DataTypes.createStructField("int_col", DataTypes.IntegerType, false),
                    DataTypes.createStructField("string_col", DataTypes.StringType, false),
                    DataTypes.createStructField("double_col", DataTypes.DoubleType, false),
                    DataTypes.createStructField("bool_col", DataTypes.BooleanType, false),
                    DataTypes.createStructField("long_col", DataTypes.LongType, false))));

    testData.write().format("delta").save(tablePath);

    // Get serialized data
    SnapshotImpl snapshot =
        (SnapshotImpl) TableManager.loadSnapshot(tablePath).build(defaultEngine);
    Scan scan = snapshot.getScanBuilder().build();

    Row scanStateRow = scan.getScanState(defaultEngine);
    String serializedScanState = JsonUtils.rowToJson(scanStateRow);

    String serializedScanFileRow = null;
    try (CloseableIterator<FilteredColumnarBatch> scanFiles = scan.getScanFiles(defaultEngine)) {
      if (scanFiles.hasNext()) {
        FilteredColumnarBatch batch = scanFiles.next();
        try (CloseableIterator<Row> rows = batch.getRows()) {
          if (rows.hasNext()) {
            Row fileRow = rows.next();
            serializedScanFileRow = JsonUtils.rowToJson(fileRow);
          }
        }
      }
    }

    assertNotNull(serializedScanFileRow);

    // Test reader
    KernelSparkPartitionReader reader =
        new KernelSparkPartitionReader(serializedScanState, serializedScanFileRow);

    try {
      List<InternalRow> rows = new ArrayList<>();
      while (reader.next()) {
        rows.add(reader.get().copy());
      }

      assertTrue(rows.size() > 0, "Should have read some rows");

      // Test data types in first row
      InternalRow firstRow = rows.get(0);
      assertTrue(firstRow.numFields() >= 5, "Should have at least 5 fields");

      // Test integer
      if (!firstRow.isNullAt(0)) {
        assertDoesNotThrow(() -> firstRow.getInt(0));
      }

      // Test string (should be UTF8String)
      if (!firstRow.isNullAt(1)) {
        Object stringValue = firstRow.get(1, DataTypes.StringType);
        assertInstanceOf(UTF8String.class, stringValue);
      }

      // Test double
      if (!firstRow.isNullAt(2)) {
        assertDoesNotThrow(() -> firstRow.getDouble(2));
      }

      // Test boolean
      if (!firstRow.isNullAt(3)) {
        assertDoesNotThrow(() -> firstRow.getBoolean(3));
      }

      // Test long
      if (!firstRow.isNullAt(4)) {
        assertDoesNotThrow(() -> firstRow.getLong(4));
      }
    } finally {
      reader.close();
    }
  }
}
