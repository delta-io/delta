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
package io.delta.spark.dsv2.read;

import static org.junit.jupiter.api.Assertions.*;

import io.delta.kernel.Scan;
import io.delta.kernel.TableManager;
import io.delta.spark.dsv2.SparkDsv2TestBase;
import io.delta.spark.dsv2.read.batch.KernelSparkInputPartition;
import java.io.File;
import org.apache.spark.sql.connector.read.InputPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class SparkScanContextTest extends SparkDsv2TestBase {

  @Test
  public void testConstructorWithNullScan() {
    assertThrows(
        NullPointerException.class,
        () -> {
          new KernelSparkScanContext(null, spark.sessionState().newHadoopConf());
        });
  }

  @Test
  public void testConstructorWithNullHadoopConf(@TempDir File tempDir) {
    String path = tempDir.getAbsolutePath();
    String tableName = "test_null_hadoop_conf";
    createTestTableWithData(path, tableName);

    Scan scan = TableManager.loadSnapshot(path).build(defaultEngine).getScanBuilder().build();

    assertThrows(
        NullPointerException.class,
        () -> {
          new KernelSparkScanContext(scan, null);
        });
  }

  @Test
  public void testPlanPartitionsSuccess(@TempDir File tempDir) {
    String path = tempDir.getAbsolutePath();
    String tableName = "test_plan_partitions";
    createTestTableWithData(path, tableName);

    Scan scan = TableManager.loadSnapshot(path).build(defaultEngine).getScanBuilder().build();
    KernelSparkScanContext scanContext =
        new KernelSparkScanContext(scan, spark.sessionState().newHadoopConf());

    InputPartition[] partitions = scanContext.planPartitions();

    assertNotNull(partitions);
    assertTrue(partitions.length > 0, "Should have at least one partition");

    for (InputPartition partition : partitions) {
      assertTrue(partition instanceof KernelSparkInputPartition);
      KernelSparkInputPartition kernelPartition = (KernelSparkInputPartition) partition;
      assertNotNull(kernelPartition.getSerializedScanState());
      assertNotNull(kernelPartition.getSerializedScanFileRow());
    }
  }

  @Test
  public void testPlanPartitionsWithEmptyTable(@TempDir File tempDir) {
    String path = tempDir.getAbsolutePath();
    String tableName = "test_empty_table";
    createEmptyTestTable(path, tableName);

    Scan scan = TableManager.loadSnapshot(path).build(defaultEngine).getScanBuilder().build();
    KernelSparkScanContext scanContext =
        new KernelSparkScanContext(scan, spark.sessionState().newHadoopConf());

    InputPartition[] partitions = scanContext.planPartitions();

    assertNotNull(partitions);
    assertEquals(partitions.length, 0);
  }

  @Test
  public void testPlanPartitionsMultipleTime(@TempDir File tempDir) {
    String path = tempDir.getAbsolutePath();
    String tableName = "test_call_plan_partition_mutiple_time";
    createTestTableWithData(path, tableName);

    Scan scan = TableManager.loadSnapshot(path).build(defaultEngine).getScanBuilder().build();
    KernelSparkScanContext scanContext =
        new KernelSparkScanContext(scan, spark.sessionState().newHadoopConf());

    InputPartition[] partitions1 = scanContext.planPartitions();
    InputPartition[] partitions2 = scanContext.planPartitions();

    assertNotNull(partitions1);
    assertNotNull(partitions2);
    assertEquals(partitions1.length, partitions2.length);

    for (int i = 0; i < partitions1.length; i++) {
      KernelSparkInputPartition p1 = (KernelSparkInputPartition) partitions1[i];
      KernelSparkInputPartition p2 = (KernelSparkInputPartition) partitions2[i];
      assertEquals(p1.getSerializedScanState(), p2.getSerializedScanState());
      assertEquals(p1.getSerializedScanFileRow(), p2.getSerializedScanFileRow());
    }
  }

  // Test helper methods have been moved to KernelSparkDsv2TestBase
}
