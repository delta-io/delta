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
import io.delta.kernel.types.StructType;
import io.delta.spark.dsv2.KernelSparkDsv2TestBase;
import java.io.File;
import java.io.IOException;
import org.apache.spark.sql.connector.read.InputPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class KernelSparkInputPartitionTest extends KernelSparkDsv2TestBase {

  private void createTestTable(String path, String tableName) {
    spark.sql(
        String.format(
            "CREATE TABLE %s (id INT, name STRING, value DOUBLE) USING delta LOCATION '%s'",
            tableName, path));
    // Insert some test data
    spark.sql(
        String.format("INSERT INTO %s VALUES (1, 'Alice', 10.5), (2, 'Bob', 20.5)", tableName));
  }

  @Test
  public void testSerializationRoundTrip(@TempDir File tempDir) throws IOException {
    String path = tempDir.getAbsolutePath();
    String tableName = "test_serialization_roundtrip";
    createTestTable(path, tableName);

    Scan scan = TableManager.loadSnapshot(path).build(defaultEngine).getScanBuilder().build();

    // Get original scan state before creating scan context
    Row originalScanState = scan.getScanState(defaultEngine);
    StructType scanStateSchema = originalScanState.getSchema();

    // Create scan context which will consume the scan files
    KernelSparkScanContext scanContext = new KernelSparkScanContext(scan, defaultEngine);
    InputPartition[] partitions = scanContext.planPartitions();
    assertTrue(partitions.length > 0, "Should have at least one partition");

    KernelSparkInputPartition partition = (KernelSparkInputPartition) partitions[0];

    // Test scan state round trip
    String serializedScanState = partition.getSerializedScanState();
    assertNotNull(serializedScanState);
    Row deserializedScanState = JsonUtils.rowFromJson(serializedScanState, scanStateSchema);
    assertNotNull(deserializedScanState);
    assertEquals(originalScanState.getSchema(), deserializedScanState.getSchema());

    // Test file row round trip - use the predefined scan file schema
    StructType fileRowSchema = InternalScanFileUtils.SCAN_FILE_SCHEMA;
    String serializedFileRow = partition.getSerializedScanFileRow();
    assertNotNull(serializedFileRow);
    assertFalse(serializedFileRow.isEmpty());
    Row deserializedFileRow = JsonUtils.rowFromJson(serializedFileRow, fileRowSchema);
    assertNotNull(deserializedFileRow);
    assertEquals(fileRowSchema, deserializedFileRow.getSchema());
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
}
