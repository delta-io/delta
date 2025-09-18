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
package io.delta.kernel.spark.catalog;

import static org.apache.spark.sql.connector.catalog.TableCapability.BATCH_READ;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.delta.kernel.spark.SparkDsv2TestBase;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.stream.Stream;
import org.apache.spark.sql.connector.catalog.Column;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class SparkTableTest extends SparkDsv2TestBase {

  @ParameterizedTest(name = "{0}")
  @MethodSource("tableTestCases")
  public void testDeltaKernelTable(TableTestCase testCase, @TempDir File tempDir) {
    String path = tempDir.getAbsolutePath();
    String tableName = "test_" + testCase.name.toLowerCase().replace(" ", "_");
    testCase.createTableSql.apply(tableName, path);
    Identifier identifier = Identifier.of(new String[] {"test_namespace"}, tableName);

    SparkTable kernelTable = new SparkTable(identifier, path);

    // ===== Test table name =====
    assertEquals(tableName, kernelTable.name());

    // ===== Test schema =====
    StructType sparkSchema = kernelTable.schema();
    Column[] actualColumns = kernelTable.columns();
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
      // Check column object from table.columns()
      assertEquals(expectedCol, actualColumns[i], "Column mismatch at position " + i);
    }

    // ===== Test partitioning =====
    Transform[] partitioning = kernelTable.partitioning();
    assertEquals(testCase.expectedPartitionColumns.length, partitioning.length);
    for (int i = 0; i < testCase.expectedPartitionColumns.length; i++) {
      assertEquals(
          testCase.expectedPartitionColumns[i],
          partitioning[i].references()[0].describe(),
          "Partition column mismatch at position " + i);
    }

    // ===== Test properties =====
    Map<String, String> properties = kernelTable.properties();
    testCase.expectedProperties.forEach(
        (key, value) -> {
          assertTrue(properties.containsKey(key), "Property not found: " + key);
          assertEquals(value, properties.get(key), "Property value mismatch for: " + key);
        });

    // ===== Test capabilities =====
    assertTrue(kernelTable.capabilities().contains(BATCH_READ));
  }

  /** Represents a test case configuration for Delta tables */
  private static class TableTestCase {
    final String name;
    final BiFunction<String, String, Void> createTableSql;
    final List<Column> expectedColumns;
    final String[] expectedPartitionColumns;
    final Map<String, String> expectedProperties;

    public TableTestCase(
        String name,
        BiFunction<String, String, Void> createTableSql,
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

    // ===== Partitioned Table =====
    List<Column> partitionedTableColumns = new ArrayList<>();
    partitionedTableColumns.add(Column.create("id", DataTypes.IntegerType));
    partitionedTableColumns.add(Column.create("data", DataTypes.StringType));
    partitionedTableColumns.add(Column.create("part", DataTypes.IntegerType));

    // ===== Unpartitioned Table =====
    List<Column> unPartitionedTableColumns = new ArrayList<>();
    unPartitionedTableColumns.add(Column.create("id", DataTypes.IntegerType));
    unPartitionedTableColumns.add(Column.create("data", DataTypes.StringType));

    // ===== Setup Single Properties =====
    Map<String, String> basicProps = new HashMap<>();
    basicProps.put("foo", "bar");

    // ===== Setup Multiple Properties =====
    Map<String, String> multiProps = new HashMap<>();
    multiProps.put("prop1", "value1");
    multiProps.put("prop2", "value2");
    multiProps.put("delta.enableChangeDataFeed", "true");

    List<Column> singleColumn = new ArrayList<>();
    singleColumn.add(Column.create("id", DataTypes.IntegerType));

    return Stream.of(
        new TableTestCase(
            "Partitioned Table",
            (tableName, path) -> {
              spark.sql(
                  String.format(
                      "CREATE TABLE %s (id INT, data STRING, part INT) USING delta "
                          + "PARTITIONED BY (part) TBLPROPERTIES ('foo'='bar') LOCATION '%s'",
                      tableName, path));
              return null;
            },
            partitionedTableColumns,
            new String[] {"part"},
            basicProps),
        new TableTestCase(
            "UnPartitioned Table",
            (tableName, path) -> {
              spark.sql(
                  String.format(
                      "CREATE TABLE %s (id INT, data STRING) USING delta LOCATION '%s'",
                      tableName, path));
              return null;
            },
            unPartitionedTableColumns,
            new String[] {},
            new HashMap<>()),
        new TableTestCase(
            "Multiple Properties",
            (tableName, path) -> {
              spark.sql(
                  String.format(
                      "CREATE TABLE %s (id INT) USING delta "
                          + "TBLPROPERTIES ('prop1'='value1', 'prop2'='value2', 'delta.enableChangeDataFeed'='true') "
                          + "LOCATION '%s'",
                      tableName, path));
              return null;
            },
            singleColumn,
            new String[] {},
            multiProps));
  }
}
