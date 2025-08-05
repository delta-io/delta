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
package io.delta.spark.dsv2.table;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.delta.kernel.Snapshot;
import io.delta.kernel.TableManager;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.SnapshotImpl;
import java.io.File;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Column;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.types.DataTypes;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class DeltaDsv2TableTest {

  private static SparkSession spark;
  private static Engine defaultEngine;

  @BeforeAll
  public static void setUp() {
    spark =
        SparkSession.builder()
            .master("local[*]")
            .appName("DeltaDsv2TableTest")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .getOrCreate();
    defaultEngine = DefaultEngine.create(spark.sessionState().newHadoopConf());
  }

  @AfterAll
  public static void tearDown() {
    if (spark != null) {
      spark.stop();
      spark = null;
    }
  }

  @Test
  public void testDeltaDsv2Table(@TempDir File tempDir) {
    // Create a Delta table using Spark SQL
    String path = tempDir.getAbsolutePath();
    String tableName = "tbl";
    spark.sql(
        String.format(
            "CREATE TABLE %s (id INT, data STRING, part INT) USING delta "
                + "PARTITIONED BY (part) "
                + "TBLPROPERTIES ('foo'='bar') "
                + "LOCATION '%s'",
            tableName, path));

    Snapshot snapshot = TableManager.loadSnapshot(path).build(defaultEngine);
    Identifier identifier = Identifier.of(new String[] {"test_namespace"}, "test_table");
    DeltaDsv2Table dsv2Table = new DeltaDsv2Table(identifier, (SnapshotImpl) snapshot);

    // Test name
    assertEquals("test_table", dsv2Table.name());

    // Test schema
    org.apache.spark.sql.types.StructType sparkSchema = dsv2Table.schema();
    assertEquals(3, sparkSchema.fields().length);
    assertEquals("id", sparkSchema.fields()[0].name());
    assertEquals(DataTypes.IntegerType, sparkSchema.fields()[0].dataType());
    assertEquals("data", sparkSchema.fields()[1].name());
    assertEquals(DataTypes.StringType, sparkSchema.fields()[1].dataType());
    assertEquals("part", sparkSchema.fields()[2].name());
    assertEquals(DataTypes.IntegerType, sparkSchema.fields()[2].dataType());

    // Test columns
    Column[] columns = dsv2Table.columns();
    assertEquals(3, columns.length);
    assertEquals("id", columns[0].name());
    assertEquals("data", columns[1].name());
    assertEquals("part", columns[2].name());

    // Test partitioning
    assertEquals(1, dsv2Table.partitioning().length);
    assertEquals("part", dsv2Table.partitioning()[0].references()[0].describe());

    // Test properties
    assertTrue(dsv2Table.properties().containsKey("foo"));
    assertEquals(dsv2Table.properties().get("foo"), "bar");

    // Test capabilities
    assertTrue(dsv2Table.capabilities().isEmpty());
  }
}
