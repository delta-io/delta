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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Column;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

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

  @ParameterizedTest(name = "{0}")
  @MethodSource("tableTestCases")
  public void testDeltaDsv2Table(TableTestCase testCase, @TempDir File tempDir) {
    String path = tempDir.getAbsolutePath();
    String tableName = "test_" + testCase.name.toLowerCase().replace(" ", "_");
    spark.sql(String.format(testCase.createTableSql, tableName, path));
    Snapshot snapshot = TableManager.loadSnapshot(path).build(defaultEngine);
    Identifier identifier = Identifier.of(new String[] {"test_namespace"}, tableName);

    DeltaDsv2Table dsv2Table = new DeltaDsv2Table(identifier, (SnapshotImpl) snapshot);

    // Test name
    assertEquals(tableName, dsv2Table.name());
    // Test schema
    StructType sparkSchema = dsv2Table.schema();
    Column[] actualColumns = dsv2Table.columns();
    assertEquals(testCase.expectedColumns.size(), sparkSchema.fields().length);

    for (int i = 0; i < testCase.expectedColumns.size(); i++) {
      Column expectedCol = testCase.expectedColumns.get(i);
      assertEquals(
          expectedCol.name(),
          sparkSchema.fields()[i].name(),
          "Column name mismatch at position " + i);
      assertEquals(
          expectedCol.dataType(),
          sparkSchema.fields()[i].dataType(),
          "Data type mismatch for column: " + expectedCol.name());
      // Check column object from table.column()
      assertEquals(expectedCol, actualColumns[i], "Column mismatch at position " + i);
    }

    // Test partitioning
    Transform[] partitioning = dsv2Table.partitioning();
    assertEquals(testCase.expectedPartitionColumns.length, partitioning.length);
    for (int i = 0; i < testCase.expectedPartitionColumns.length; i++) {
      assertEquals(
          testCase.expectedPartitionColumns[i],
          partitioning[i].references()[0].describe(),
          "Partition column mismatch at position " + i);
    }

    // Test properties
    Map<String, String> properties = dsv2Table.properties();
    testCase.expectedProperties.forEach(
        (key, value) -> {
          assertTrue(properties.containsKey(key), "Property not found: " + key);
          assertEquals(value, properties.get(key), "Property value mismatch for: " + key);
        });

    // Test capabilities
    assertTrue(dsv2Table.capabilities().isEmpty());
  }

  /** Represents a test case configuration for Delta tables */
  private static class TableTestCase {
    final String name;
    final String createTableSql;
    final List<Column> expectedColumns;
    final String[] expectedPartitionColumns;
    final Map<String, String> expectedProperties;

    public TableTestCase(
        String name,
        String createTableSql,
        List<Column> expectedColumns,
        String[] expectedPartitionColumns,
        Map<String, String> expectedProperties) {

      this.name = name;
      this.createTableSql = createTableSql;
      this.expectedColumns = expectedColumns;
      this.expectedPartitionColumns = expectedPartitionColumns;
      this.expectedProperties = expectedProperties;
    }

    @Override
    public String toString() {
      return name;
    }
  }

  /** Provides different test cases for Delta tables */
  static Stream<TableTestCase> tableTestCases() {
    // Basic table with partitioning and properties
    Map<String, String> basicProps = new HashMap<>();
    basicProps.put("foo", "bar");

    List<Column> partitionedTableColumns = new ArrayList<>();
    partitionedTableColumns.add(Column.create("id", DataTypes.IntegerType));
    partitionedTableColumns.add(Column.create("data", DataTypes.StringType));
    partitionedTableColumns.add(Column.create("part", DataTypes.IntegerType));

    List<Column> unPartitionedTableColumns = new ArrayList<>();
    unPartitionedTableColumns.add(Column.create("id", DataTypes.IntegerType));
    unPartitionedTableColumns.add(Column.create("data", DataTypes.StringType));

    Map<String, String> multiProps = new HashMap<>();
    multiProps.put("prop1", "value1");
    multiProps.put("prop2", "value2");
    multiProps.put("delta.enableChangeDataFeed", "true");

    List<Column> singleColumn = new ArrayList<>();
    singleColumn.add(Column.create("id", DataTypes.IntegerType));

    return Stream.of(
        new TableTestCase(
            "Partitioned Table",
            "CREATE TABLE %s (id INT, data STRING, part INT) USING delta "
                + "PARTITIONED BY (part) TBLPROPERTIES ('foo'='bar') LOCATION '%s'",
            partitionedTableColumns,
            new String[] {"part"},
            basicProps),
        new TableTestCase(
            "UnPartitioned Table",
            "CREATE TABLE %s (id INT, data STRING) USING delta LOCATION '%s'",
            unPartitionedTableColumns,
            new String[] {},
            new HashMap<>()),
        new TableTestCase(
            "Multiple Properties",
            "CREATE TABLE %s (id INT) USING delta "
                + "TBLPROPERTIES ('prop1'='value1', 'prop2'='value2', 'delta.enableChangeDataFeed'='true') "
                + "LOCATION '%s'",
            singleColumn,
            new String[] {},
            multiProps));
  }
}
