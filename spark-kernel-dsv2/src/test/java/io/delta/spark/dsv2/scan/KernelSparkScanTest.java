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

import io.delta.kernel.TableManager;
import io.delta.spark.dsv2.KernelSparkDsv2TestBase;
import java.io.File;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class KernelSparkScanTest extends KernelSparkDsv2TestBase {

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

    KernelSparkScan scan =
        new KernelSparkScan(
            TableManager.loadSnapshot(path).build(defaultEngine).getScanBuilder().build(),
            expectedSparkSchema);

    assertEquals(expectedSparkSchema, scan.readSchema());
  }
}
