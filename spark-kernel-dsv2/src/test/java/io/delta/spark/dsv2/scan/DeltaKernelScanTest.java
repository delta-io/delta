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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import io.delta.kernel.Scan;
import io.delta.kernel.TableManager;
import io.delta.spark.dsv2.SparkKernelDsv2TestBase;
import java.io.File;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class DeltaKernelScanTest extends SparkKernelDsv2TestBase {

  @Test
  public void testReadSchema(@TempDir File tempDir) {
    String path = tempDir.getAbsolutePath();
    String tableName = "scan_test";
    spark.sql(
        String.format(
            "CREATE TABLE %s (id INT, name STRING) USING delta LOCATION '%s'", tableName, path));
    StructType expectedSparkSchema =
        DataTypes.createStructType(
            new StructField[] {
              DataTypes.createStructField("id", DataTypes.IntegerType, true /*nullable*/),
              DataTypes.createStructField("name", DataTypes.StringType, true /*nullable*/)
            });

    DeltaKernelScan scan =
        new DeltaKernelScan(
            TableManager.loadSnapshot(path).build(defaultEngine).getScanBuilder().build(),
            expectedSparkSchema);

    assertEquals(expectedSparkSchema, scan.readSchema());
  }

  @Test
  public void testScanWithNullParameters() {
    // Test null kernel scan
    NullPointerException ex1 =
        assertThrows(
            NullPointerException.class,
            () ->
                new DeltaKernelScan(
                    null /*kernelScan*/, DataTypes.createStructType(new StructField[] {})));
    assertTrue(ex1.getMessage().contains("kernel scan is null"));

    // Test null schema
    Scan mockScan = mock(Scan.class);
    NullPointerException ex2 =
        assertThrows(
            NullPointerException.class, () -> new DeltaKernelScan(mockScan, null /*readSchema*/));
    assertTrue(ex2.getMessage().contains("read schema is null"));
  }
}
