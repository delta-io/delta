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
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.internal.json.JsonUtils;
import io.delta.kernel.internal.InternalScanFileUtils;
import io.delta.kernel.internal.actions.AddFile;
import io.delta.kernel.internal.data.GenericRow;
import io.delta.kernel.internal.util.VectorUtils;
import io.delta.kernel.types.StructType;
import io.delta.spark.dsv2.KernelSparkDsv2TestBase;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class KernelSparkInputPartitionTest extends KernelSparkDsv2TestBase {

  @Test
  public void testSerializationRoundTrip(@TempDir File tempDir) throws IOException {
    String path = tempDir.getAbsolutePath();
    String tableName = "test_serialization_roundtrip";
    createTestTable(path, tableName);

    Scan scan = TableManager.loadSnapshot(path).build(defaultEngine).getScanBuilder().build();

    // Get original scan state
    Row originalScanState = scan.getScanState(defaultEngine);
    StructType scanStateSchema = originalScanState.getSchema();
    String serializedScanState = JsonUtils.rowToJson(originalScanState);

    // Create a test file row using GenericRow
    StructType addFileSchema = AddFile.SCHEMA_WITHOUT_STATS;
    Map<Integer, Object> addFileData = new HashMap<>();
    addFileData.put(0, "file://test/path/data.parquet"); // path
    addFileData.put(
        1, VectorUtils.stringStringMapValue(new HashMap<>())); // partitionValues (empty MapValue)
    addFileData.put(2, 1024L); // size
    addFileData.put(3, 1234567890L); // modificationTime
    addFileData.put(4, true); // dataChange
    addFileData.put(5, null); // deletionVector
    Row addFileRow = new GenericRow(addFileSchema, addFileData);
    StructType scanFileSchema = InternalScanFileUtils.SCAN_FILE_SCHEMA;
    Map<Integer, Object> scanFileData = new HashMap<>();
    scanFileData.put(0, addFileRow); // add
    scanFileData.put(1, "/"); // tableRoot
    Row testFileRow = new GenericRow(scanFileSchema, scanFileData);

    String serializedFileRow = JsonUtils.rowToJson(testFileRow);

    // Create partition with the serialized data
    KernelSparkInputPartition partition =
        new KernelSparkInputPartition(serializedScanState, serializedFileRow);

    // Test scan state round trip
    String retrievedScanState = partition.getSerializedScanState();
    assertEquals(serializedScanState, retrievedScanState);
    Row deserializedScanState = JsonUtils.rowFromJson(retrievedScanState, scanStateSchema);
    assertNotNull(deserializedScanState);
    assertEquals(originalScanState.getSchema(), deserializedScanState.getSchema());

    // Test file row round trip
    String retrievedFileRow = partition.getSerializedScanFileRow();
    assertEquals(serializedFileRow, retrievedFileRow);
    // Test that we can deserialize it back using Kernel schema
    Row deserializedFileRow = JsonUtils.rowFromJson(retrievedFileRow, scanFileSchema);
    assertNotNull(deserializedFileRow);
    assertEquals(scanFileSchema, deserializedFileRow.getSchema());
    // Verify the structure matches our original testFileRow
    assertEquals(testFileRow.getSchema(), deserializedFileRow.getSchema());
  }

  @Test
  public void testConstructorWithNullScanState() {
    String fileRow = "{\"file\":\"test.parquet\"}";

    NullPointerException exception =
        assertThrows(
            NullPointerException.class,
            () -> {
              new KernelSparkInputPartition(null, fileRow);
            });

    assertEquals("serializedScanState", exception.getMessage());
  }

  @Test
  public void testConstructorWithNullFileRow() {
    String scanState = "{\"scanState\":\"test\"}";

    NullPointerException exception =
        assertThrows(
            NullPointerException.class,
            () -> {
              new KernelSparkInputPartition(scanState, null);
            });

    assertEquals("serializedScanFileRow", exception.getMessage());
  }

  //////////////////////
  // Private helpers //
  /////////////////////
  private void createTestTable(String path, String tableName) {
    spark.sql(
        String.format(
            "CREATE TABLE %s (id INT, name STRING, value DOUBLE) USING delta LOCATION '%s'",
            tableName, path));
    spark.sql(
        String.format("INSERT INTO %s VALUES (1, 'Alice', 10.5), (2, 'Bob', 20.5)", tableName));
  }
}
