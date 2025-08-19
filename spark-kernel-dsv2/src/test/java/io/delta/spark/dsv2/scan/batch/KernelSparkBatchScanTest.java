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
import io.delta.spark.dsv2.KernelSparkDsv2TestBase;
import java.io.File;
import org.apache.spark.sql.connector.read.InputPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class KernelSparkBatchScanTest extends KernelSparkDsv2TestBase {

  @Test
  public void testConstructorWithNullContext() {
    NullPointerException exception =
        assertThrows(
            NullPointerException.class,
            () -> {
              new KernelSparkBatchScan(null);
            });

    assertEquals("sharedContext is null", exception.getMessage());
  }

  @Test
  public void testPlanInputPartitions(@TempDir File tempDir) {
    String path = tempDir.getAbsolutePath();
    String tableName = "test_plan_input_partitions";
    createTestTable(path, tableName);
    Scan scan = TableManager.loadSnapshot(path).build(defaultEngine).getScanBuilder().build();
    KernelSparkScanContext scanContext = new KernelSparkScanContext(scan, defaultEngine);
    KernelSparkBatchScan batchScan = new KernelSparkBatchScan(scanContext);

    InputPartition[] partitions = batchScan.planInputPartitions();

    assertNotNull(partitions);
    assertTrue(partitions.length > 0, "Should have at least one partition");
    // All partitions should be KernelSparkInputPartition
    for (InputPartition partition : partitions) {
      assertTrue(partition instanceof KernelSparkInputPartition);
    }
  }

  @Test
  public void testPlanInputPartitionsMultipleCalls(@TempDir File tempDir) {
    String path = tempDir.getAbsolutePath();
    String tableName = "test_multiple_calls";
    createTestTable(path, tableName);

    Scan scan = TableManager.loadSnapshot(path).build(defaultEngine).getScanBuilder().build();
    KernelSparkScanContext scanContext = new KernelSparkScanContext(scan, defaultEngine);
    KernelSparkBatchScan batchScan = new KernelSparkBatchScan(scanContext);

    InputPartition[] result1 = batchScan.planInputPartitions();
    InputPartition[] result2 = batchScan.planInputPartitions();

    assertNotNull(result1);
    assertNotNull(result2);
    assertEquals(result1.length, result2.length);
    for (int i = 0; i < result1.length; i++) {
      KernelSparkInputPartition p1 = (KernelSparkInputPartition) result1[i];
      KernelSparkInputPartition p2 = (KernelSparkInputPartition) result2[i];
      assertEquals(p1.getSerializedScanState(), p2.getSerializedScanState());
      assertEquals(p1.getSerializedScanFileRow(), p2.getSerializedScanFileRow());
    }
  }

  @Test
  public void testCreateReaderFactory(@TempDir File tempDir) {
    String path = tempDir.getAbsolutePath();
    String tableName = "test_reader_factory";
    createTestTable(path, tableName);
    Scan scan = TableManager.loadSnapshot(path).build(defaultEngine).getScanBuilder().build();
    KernelSparkScanContext scanContext = new KernelSparkScanContext(scan, defaultEngine);
    KernelSparkBatchScan batchScan = new KernelSparkBatchScan(scanContext);

    UnsupportedOperationException exception =
        assertThrows(UnsupportedOperationException.class, batchScan::createReaderFactory);

    assertEquals("reader factory is not implemented", exception.getMessage());
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
        String.format(
            "INSERT INTO %s VALUES (1, 'Alice', 10.5), (2, 'Bob', 20.5), (3, 'Charlie', 30.5)",
            tableName));
  }
}
