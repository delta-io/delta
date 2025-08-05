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
import java.util.HashMap;
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
    // Create the table
    String path = tempDir.getAbsolutePath();
    String tableName = "test_" + testCase.name.toLowerCase().replace(" ", "_");

    spark.sql(String.format(testCase.createTableSql, tableName, path));

    Snapshot snapshot = TableManager.loadSnapshot(path).build(defaultEngine);
    Identifier identifier = Identifier.of(new String[] {"test_namespace"}, "test_table");
    DeltaDsv2Table dsv2Table = new DeltaDsv2Table(identifier, (SnapshotImpl) snapshot);

    // Test name
    assertEquals("test_table", dsv2Table.name());

    // Test schema
    StructType sparkSchema = dsv2Table.schema();
    assertEquals(testCase.expectedColumnCount, sparkSchema.fields().length);

    // Test column names
    for (int i = 0; i < testCase.expectedColumnNames.length; i++) {
      assertEquals(
          testCase.expectedColumnNames[i],
          sparkSchema.fields()[i].name(),
          "Column name mismatch at position " + i);
    }

    // Test data types for specified columns
    testCase.expectedDataTypes.forEach(
        (colName, expectedType) -> {
          org.apache.spark.sql.types.StructField field = sparkSchema.apply(colName);
          assertEquals(expectedType, field.dataType(), "Data type mismatch for column: " + colName);
        });

    // Test columns
    Column[] columns = dsv2Table.columns();
    assertEquals(testCase.expectedColumnCount, columns.length);
    for (int i = 0; i < testCase.expectedColumnNames.length; i++) {
      assertEquals(
          testCase.expectedColumnNames[i],
          columns[i].name(),
          "Column object name mismatch at position " + i);
    }

    // Test partitioning
    Transform[] partitioning = dsv2Table.partitioning();
    assertEquals(testCase.expectedPartitionCount, partitioning.length);
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
    final int expectedColumnCount;
    final String[] expectedColumnNames;
    final int expectedPartitionCount;
    final String[] expectedPartitionColumns;
    final Map<String, String> expectedProperties;
    final Map<String, org.apache.spark.sql.types.DataType> expectedDataTypes;

    public TableTestCase(
        String name,
        String createTableSql,
        int expectedColumnCount,
        String[] expectedColumnNames,
        int expectedPartitionCount,
        String[] expectedPartitionColumns,
        Map<String, String> expectedProperties,
        Map<String, org.apache.spark.sql.types.DataType> expectedDataTypes) {

      this.name = name;
      this.createTableSql = createTableSql;
      this.expectedColumnCount = expectedColumnCount;
      this.expectedColumnNames = expectedColumnNames;
      this.expectedPartitionCount = expectedPartitionCount;
      this.expectedPartitionColumns = expectedPartitionColumns;
      this.expectedProperties = expectedProperties;
      this.expectedDataTypes = expectedDataTypes;
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

    Map<String, org.apache.spark.sql.types.DataType> basicTypes = new HashMap<>();
    basicTypes.put("id", DataTypes.IntegerType);
    basicTypes.put("data", DataTypes.StringType);
    basicTypes.put("part", DataTypes.IntegerType);

    // Table without partitioning
    Map<String, org.apache.spark.sql.types.DataType> simpleTypes = new HashMap<>();
    simpleTypes.put("id", DataTypes.IntegerType);
    simpleTypes.put("data", DataTypes.StringType);

    // Table with multiple properties
    Map<String, String> multiProps = new HashMap<>();
    multiProps.put("prop1", "value1");
    multiProps.put("prop2", "value2");
    multiProps.put("delta.enableChangeDataFeed", "true");

    Map<String, org.apache.spark.sql.types.DataType> singleType = new HashMap<>();
    singleType.put("id", DataTypes.IntegerType);

    return Stream.of(
        new TableTestCase(
            "Partitioned Table",
            "CREATE TABLE %s (id INT, data STRING, part INT) USING delta "
                + "PARTITIONED BY (part) TBLPROPERTIES ('foo'='bar') LOCATION '%s'",
            3,
            new String[] {"id", "data", "part"},
            1,
            new String[] {"part"},
            basicProps,
            basicTypes),
        new TableTestCase(
            "UnPartitioned Table",
            "CREATE TABLE %s (id INT, data STRING) USING delta LOCATION '%s'",
            2,
            new String[] {"id", "data"},
            0,
            new String[] {},
            new HashMap<>(),
            simpleTypes),
        new TableTestCase(
            "Multiple Properties",
            "CREATE TABLE %s (id INT) USING delta "
                + "TBLPROPERTIES ('prop1'='value1', 'prop2'='value2', 'delta.enableChangeDataFeed'='true') "
                + "LOCATION '%s'",
            1,
            new String[] {"id"},
            0,
            new String[] {},
            multiProps,
            singleType));
  }
}
