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
package io.delta.spark.dsv2.scan;

import static org.junit.jupiter.api.Assertions.*;

import io.delta.kernel.Scan;
import io.delta.kernel.TableManager;
import io.delta.spark.dsv2.KernelSparkDsv2TestBase;
import io.delta.spark.dsv2.scan.batch.KernelSparkInputPartition;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.spark.sql.connector.read.InputPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class KernelSparkScanContextTest extends KernelSparkDsv2TestBase {

  @Test
  public void testConstructorWithNullScan() {
    assertThrows(
        NullPointerException.class,
        () -> {
          new KernelSparkScanContext(null, defaultEngine);
        });
  }

  @Test
  public void testConstructorWithNullEngine(@TempDir File tempDir) {
    String path = tempDir.getAbsolutePath();
    String tableName = "test_null_engine";
    createTestTable(path, tableName);

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
    createTestTable(path, tableName);

    Scan scan = TableManager.loadSnapshot(path).build(defaultEngine).getScanBuilder().build();
    KernelSparkScanContext scanContext = new KernelSparkScanContext(scan, defaultEngine);

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
    KernelSparkScanContext scanContext = new KernelSparkScanContext(scan, defaultEngine);

    InputPartition[] partitions = scanContext.planPartitions();

    assertNotNull(partitions);
    assertEquals(partitions.length, 0);
  }

  @Test
  public void testPlanPartitionsMultipleTime(@TempDir File tempDir) {
    String path = tempDir.getAbsolutePath();
    String tableName = "test_call_plan_partition_mutiple_time";
    createTestTable(path, tableName);

    Scan scan = TableManager.loadSnapshot(path).build(defaultEngine).getScanBuilder().build();
    KernelSparkScanContext scanContext = new KernelSparkScanContext(scan, defaultEngine);

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

  @Test
  public void testPlanPartitionsThreadSafety(@TempDir File tempDir) throws Exception {
    String path = tempDir.getAbsolutePath();
    String tableName = "test_thread_safety";
    createTestTable(path, tableName);

    Scan scan = TableManager.loadSnapshot(path).build(defaultEngine).getScanBuilder().build();
    KernelSparkScanContext scanContext = new KernelSparkScanContext(scan, defaultEngine);

    int numThreads = 5;
    ExecutorService executor = Executors.newFixedThreadPool(numThreads);
    CountDownLatch latch = new CountDownLatch(numThreads);
    List<Future<InputPartition[]>> futures = new ArrayList<>();

    for (int i = 0; i < numThreads; i++) {
      futures.add(
          executor.submit(
              () -> {
                try {
                  latch.countDown();
                  latch.await();
                  return scanContext.planPartitions();
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                  throw new RuntimeException(e);
                }
              }));
    }

    InputPartition[] firstResult = futures.get(0).get(5, TimeUnit.SECONDS);
    for (int i = 1; i < numThreads; i++) {
      InputPartition[] result = futures.get(i).get(5, TimeUnit.SECONDS);
      assertEquals(firstResult.length, result.length);

      for (int j = 0; j < firstResult.length; j++) {
        KernelSparkInputPartition expected = (KernelSparkInputPartition) firstResult[j];
        KernelSparkInputPartition actual = (KernelSparkInputPartition) result[j];
        assertEquals(expected.getSerializedScanState(), actual.getSerializedScanState());
        assertEquals(expected.getSerializedScanFileRow(), actual.getSerializedScanFileRow());
      }
    }

    executor.shutdown();
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

  private void createEmptyTestTable(String path, String tableName) {
    spark.sql(
        String.format(
            "CREATE TABLE %s (id INT, name STRING) USING delta LOCATION '%s'", tableName, path));
  }
}
