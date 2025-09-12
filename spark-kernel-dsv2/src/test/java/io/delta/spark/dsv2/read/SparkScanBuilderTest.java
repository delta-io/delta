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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.delta.golden.GoldenTableUtils$;
import io.delta.kernel.Snapshot;
import io.delta.kernel.TableManager;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.spark.dsv2.SparkDsv2TestBase;
import io.delta.spark.dsv2.table.SparkTable;
import java.io.File;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class SparkScanBuilderTest extends SparkDsv2TestBase {

  @Test
  public void testBuild_returnsScanWithExpectedSchema(@TempDir File tempDir) {
    String path = tempDir.getAbsolutePath();
    String tableName = "scan_builder_test";
    spark.sql(
        String.format(
            "CREATE TABLE %s (id INT, name STRING, dep_id INT) USING delta PARTITIONED BY (dep_id) LOCATION '%s'",
            tableName, path));
    Snapshot snapshot = TableManager.loadSnapshot(path).build(defaultEngine);
    StructType dataSchema =
        DataTypes.createStructType(
            new StructField[] {
              DataTypes.createStructField("id", DataTypes.IntegerType, true),
              DataTypes.createStructField("name", DataTypes.StringType, true),
              DataTypes.createStructField("dep_id", DataTypes.IntegerType, true)
            });
    StructType partitionSchema =
        DataTypes.createStructType(
            new StructField[] {DataTypes.createStructField("dep_id", DataTypes.IntegerType, true)});
    SparkScanBuilder builder =
        new SparkScanBuilder(
            tableName,
            path,
            dataSchema,
            partitionSchema,
            (SnapshotImpl) snapshot,
            CaseInsensitiveStringMap.empty());

    StructType expectedSparkSchema =
        DataTypes.createStructType(
            new StructField[] {
              DataTypes.createStructField("id", DataTypes.IntegerType, true /*nullable*/),
              DataTypes.createStructField("dep_id", DataTypes.IntegerType, true)
            });

    builder.pruneColumns(expectedSparkSchema);
    Scan scan = builder.build();

    assertTrue(scan instanceof SparkScan);
    assertEquals(expectedSparkSchema, scan.readSchema());
  }

  @Test
  public void testBuild_partitionTable() {
    String tableName = "deltatbl-partition-prune";
    String tablePath = goldenTablePath("hive/" + tableName);
    SparkTable table =
        new SparkTable(
            Identifier.of(new String[] {"spark_catalog", "default"}, tableName), tablePath);
    StructType expectedDataSchema =
        DataTypes.createStructType(
            new StructField[] {
              DataTypes.createStructField("name", DataTypes.StringType, true),
              DataTypes.createStructField("cnt", DataTypes.IntegerType, true),
            });
    StructType expectedPartitionSchema =
        DataTypes.createStructType(
            new StructField[] {
              DataTypes.createStructField("date", DataTypes.StringType, true),
              DataTypes.createStructField("city", DataTypes.StringType, true),
            });
    StructType expectedSchema =
        DataTypes.createStructType(
            new StructField[] {
              DataTypes.createStructField("name", DataTypes.StringType, true),
              DataTypes.createStructField("cnt", DataTypes.IntegerType, true),
              DataTypes.createStructField("date", DataTypes.StringType, true),
              DataTypes.createStructField("city", DataTypes.StringType, true),
            });
    assertEquals(expectedSchema, table.schema());
    assertEquals(tableName, table.name());
    // Check table columns
    assertEquals(4, table.columns().length);
    assertEquals("name", table.columns()[0].name());
    assertEquals("cnt", table.columns()[1].name());
    assertEquals("date", table.columns()[2].name());
    assertEquals("city", table.columns()[3].name());

    // Check table partitioning
    assertEquals(2, table.partitioning().length);
    assertEquals("identity(date)", table.partitioning()[0].toString());
    assertEquals("identity(city)", table.partitioning()[1].toString());

    // Check table properties
    assertTrue(table.properties().isEmpty());

    CaseInsensitiveStringMap options =
        new CaseInsensitiveStringMap(
            java.util.Collections.singletonMap("some_option_key", "some_option_value"));
    ScanBuilder builder = table.newScanBuilder(options);
    assertTrue((builder instanceof SparkScanBuilder));
    SparkScanBuilder scanBuilder = (SparkScanBuilder) builder;
    assertEquals(expectedDataSchema, scanBuilder.getDataSchema());
    assertEquals(expectedPartitionSchema, scanBuilder.getPartitionSchema());
    assertEquals(options, scanBuilder.getOptions());

    Scan scan1 = scanBuilder.build();
    assertTrue(scan1 instanceof SparkScan);
    SparkScan sparkScan1 = (SparkScan) scan1;
    assertEquals(expectedDataSchema, sparkScan1.getDataSchema());
    assertEquals(expectedDataSchema, sparkScan1.getReadDataSchema());
    assertEquals(expectedPartitionSchema, sparkScan1.getPartitionSchema());
    assertEquals(options, sparkScan1.getOptions());

    StructType prunedSchema =
        DataTypes.createStructType(
            new StructField[] {
              DataTypes.createStructField("name", DataTypes.StringType, true),
              DataTypes.createStructField("date", DataTypes.StringType, true),
            });
    scanBuilder.pruneColumns(prunedSchema);
    Scan scan2 = scanBuilder.build();
    assertTrue(scan2 instanceof SparkScan);
    SparkScan sparkScan2 = (SparkScan) scan2;
    assertEquals(expectedDataSchema, sparkScan2.getDataSchema());
    StructType expectedReadDataSchemaAfterPrune =
        DataTypes.createStructType(
            new StructField[] {DataTypes.createStructField("name", DataTypes.StringType, true)});
    assertEquals(expectedReadDataSchemaAfterPrune, sparkScan2.getReadDataSchema());
    assertEquals(options, sparkScan2.getOptions());
  }

  private String goldenTablePath(String name) {
    return GoldenTableUtils$.MODULE$.goldenTablePath(name);
  }
}
