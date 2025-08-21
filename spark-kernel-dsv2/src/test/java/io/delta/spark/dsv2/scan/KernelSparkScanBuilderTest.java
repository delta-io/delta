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
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.delta.kernel.Snapshot;
import io.delta.kernel.TableManager;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.spark.dsv2.KernelSparkDsv2TestBase;
import java.io.File;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class KernelSparkScanBuilderTest extends KernelSparkDsv2TestBase {

  @Test
  public void testBuild_returnsScanWithExpectedSchema(@TempDir File tempDir) {
    String path = tempDir.getAbsolutePath();
    String tableName = "scan_builder_test";
    spark.sql(
        String.format(
            "CREATE TABLE %s (id INT, data STRING) USING delta LOCATION '%s'", tableName, path));
    Snapshot snapshot = TableManager.loadSnapshot(path).build(defaultEngine);
    KernelSparkScanBuilder builder =
        new KernelSparkScanBuilder((SnapshotImpl) snapshot, spark.sessionState().newHadoopConf());

    org.apache.spark.sql.types.StructType expectedSparkSchema =
        DataTypes.createStructType(
            new StructField[] {
              DataTypes.createStructField("id", DataTypes.IntegerType, true /*nullable*/),
              DataTypes.createStructField("data", DataTypes.StringType, true /*nullable*/)
            });

    Scan scan = builder.build();

    assertTrue(scan instanceof KernelSparkScan);
    assertEquals(expectedSparkSchema, scan.readSchema());
  }
}
