/*
 * Copyright (2026) The Delta Lake Project Authors.
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

package io.sparkuctest;

import static org.apache.spark.sql.functions.lit;

import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * DataFrame test suite for Delta Table operations through Unity Catalog.
 *
 * <p>Covers DataFrame Writer V1 (insertInto, saveAsTable, save) and Writer V2 (writeTo) operations
 * with various conditions and scenarios. Tests are parameterized to support different table types
 * (EXTERNAL and MANAGED).
 */
public class UCDeltaTableDataFrameTest extends UCDeltaTableIntegrationBaseTest {

  // ==================================================================================
  // DataFrame Writer V1 Tests
  // ==================================================================================

  @ParameterizedTest
  @MethodSource("allTableTypes")
  public void testInsertIntoAppend(TableType tableType) throws Exception {
    withNewTable(
        "insert_into_append_test",
        "id INT",
        tableType,
        tableName -> {
          // Setup initial data
          sql("INSERT INTO %s VALUES (1), (2), (3)", tableName);

          // Create DataFrame and use insertInto with append mode
          Dataset<Row> df =
              spark()
                  .createDataFrame(
                      List.of(RowFactory.create(4), RowFactory.create(5)),
                      new StructType().add("id", DataTypes.IntegerType));

          df.write().mode("append").insertInto(tableName);

          // Verify data was appended
          check(
              tableName,
              List.of(List.of("1"), List.of("2"), List.of("3"), List.of("4"), List.of("5")));
        });
  }

  @ParameterizedTest
  @MethodSource("allTableTypes")
  public void testInsertIntoOverwrite(TableType tableType) throws Exception {
    withNewTable(
        "insert_into_overwrite_test",
        "id INT",
        tableType,
        tableName -> {
          // Setup initial data
          sql("INSERT INTO %s VALUES (1), (2), (3)", tableName);

          // Create DataFrame and use insertInto with overwrite mode
          Dataset<Row> df =
              spark()
                  .createDataFrame(
                      List.of(RowFactory.create(5)),
                      new StructType().add("id", DataTypes.IntegerType));

          df.write().mode("overwrite").insertInto(tableName);

          // Verify data was overwritten
          check(tableName, List.of(List.of("5")));
        });
  }

  @ParameterizedTest
  @MethodSource("allTableTypes")
  public void testInsertIntoOverwriteReplaceWhere(TableType tableType) throws Exception {
    withNewTable(
        "insert_into_replace_where_test",
        "id INT",
        tableType,
        tableName -> {
          // Setup initial data
          sql("INSERT INTO %s VALUES (1), (2), (3)", tableName);

          // Create DataFrame and use insertInto with overwrite mode and replaceWhere option
          Dataset<Row> df =
              spark()
                  .createDataFrame(
                      List.of(RowFactory.create(5)),
                      new StructType().add("id", DataTypes.IntegerType));

          df.write().mode("overwrite").option("replaceWhere", "id > 1").insertInto(tableName);

          // Verify only rows matching the condition were replaced
          check(tableName, List.of(List.of("1"), List.of("5")));
        });
  }

  @ParameterizedTest
  @MethodSource("allTableTypes")
  public void testInsertIntoOverwritePartitionOverwrite(TableType tableType) throws Exception {
    withNewTable(
        "insert_into_partition_overwrite_test",
        "id INT",
        tableType,
        tableName -> {
          // Setup initial data
          sql("INSERT INTO %s VALUES (1), (2), (3)", tableName);

          // Create DataFrame and use insertInto with dynamic partition overwrite
          Dataset<Row> df =
              spark()
                  .createDataFrame(
                      List.of(RowFactory.create(5)),
                      new StructType().add("id", DataTypes.IntegerType));

          df.write()
              .mode("overwrite")
              .option("partitionOverwriteMode", "dynamic")
              .insertInto(tableName);

          // Verify result - should have replaced with dynamic partition overwrite logic
          check(tableName, List.of(List.of("5")));
        });
  }

  @ParameterizedTest
  @MethodSource("allTableTypes")
  public void testSaveAsTableAppendExistingTable(TableType tableType) throws Exception {
    withNewTable(
        "save_as_table_append_test",
        "id INT",
        tableType,
        tableName -> {
          // Setup initial data
          sql("INSERT INTO %s VALUES (1), (2), (3)", tableName);

          // Create DataFrame and use saveAsTable with append mode
          Dataset<Row> df =
              spark()
                  .createDataFrame(
                      List.of(RowFactory.create(4), RowFactory.create(5)),
                      new StructType().add("id", DataTypes.IntegerType));

          df.write().format("delta").mode("append").saveAsTable(tableName);

          // Verify data was appended
          check(
              tableName,
              List.of(List.of("1"), List.of("2"), List.of("3"), List.of("4"), List.of("5")));
        });
  }

  @ParameterizedTest
  @MethodSource("allTableTypes")
  public void testSaveAppendUsingPath(TableType tableType) throws Exception {
    // This test verifies that catalog-owned tables do not allow path-based access
    // Only test MANAGED tables as EXTERNAL tables allow path-based access
    if (tableType == TableType.EXTERNAL) {
      return; // Skip for external tables
    }

    withNewTable(
        "save_path_test",
        "id INT",
        tableType,
        tableName -> {
          // Setup initial data
          sql("INSERT INTO %s VALUES (1), (2), (3)", tableName);

          // Get the table path from DESCRIBE FORMATTED
          List<List<String>> descResult = sql("DESCRIBE FORMATTED %s", tableName);
          String tablePath = null;
          for (List<String> row : descResult) {
            if (row.size() >= 2 && "Location".equalsIgnoreCase(row.get(0).trim())) {
              tablePath = row.get(1).trim();
              break;
            }
          }

          // Attempt to write using path-based access
          Dataset<Row> df =
              spark()
                  .createDataFrame(
                      List.of(RowFactory.create(4), RowFactory.create(5)),
                      new StructType().add("id", DataTypes.IntegerType));

          try {
            df.write().format("delta").mode("append").save(tablePath);
            throw new AssertionError("Expected path-based access error for catalog-owned table");
          } catch (Exception e) {
            // Verify the exception is related to path-based access denial for catalog-managed
            // tables
            if (!e.getMessage().contains("AccessDeniedException")
                && !e.getMessage().contains("Access Denied")
                && !e.getMessage().contains("access denied")
                && !e.getMessage()
                    .contains("DELTA_PATH_BASED_ACCESS_TO_CATALOG_MANAGED_TABLE_BLOCKED")
                && !e.getMessage().contains("Path-based access is not allowed")) {
              throw new AssertionError(
                  "Expected path-based access error but got: " + e.getMessage(), e);
            }
          }

          // Verify original data is unchanged
          check(tableName, List.of(List.of("1"), List.of("2"), List.of("3")));
        });
  }

  // ==================================================================================
  // DataFrame Writer V2 Tests
  // ==================================================================================

  @ParameterizedTest
  @MethodSource("allTableTypes")
  public void testWriteToAppend(TableType tableType) throws Exception {
    withNewTable(
        "write_to_append_test",
        "id INT",
        tableType,
        tableName -> {
          // Setup initial data
          sql("INSERT INTO %s VALUES (1), (2), (3)", tableName);

          // Create DataFrame and use writeTo().append()
          Dataset<Row> df =
              spark()
                  .createDataFrame(
                      List.of(RowFactory.create(4), RowFactory.create(5)),
                      new StructType().add("id", DataTypes.IntegerType));

          df.writeTo(tableName).append();

          // Verify data was appended
          check(
              tableName,
              List.of(List.of("1"), List.of("2"), List.of("3"), List.of("4"), List.of("5")));
        });
  }

  @ParameterizedTest
  @MethodSource("allTableTypes")
  public void testWriteToOverwrite(TableType tableType) throws Exception {
    withNewTable(
        "write_to_overwrite_test",
        "id INT",
        tableType,
        tableName -> {
          // Setup initial data
          sql("INSERT INTO %s VALUES (1), (2), (3)", tableName);

          // Create DataFrame and use writeTo().overwrite()
          Dataset<Row> df =
              spark()
                  .createDataFrame(
                      List.of(RowFactory.create(5)),
                      new StructType().add("id", DataTypes.IntegerType));

          df.writeTo(tableName).overwrite(lit(true));

          // Verify data was overwritten
          check(tableName, List.of(List.of("5")));
        });
  }

  @ParameterizedTest
  @MethodSource("allTableTypes")
  public void testWriteToOverwritePartitions(TableType tableType) throws Exception {
    withNewTable(
        "write_to_overwrite_partitions_test",
        "id INT",
        tableType,
        tableName -> {
          // Setup initial data
          sql("INSERT INTO %s VALUES (1), (2), (3)", tableName);

          // Create DataFrame and use writeTo().overwritePartitions()
          Dataset<Row> df =
              spark()
                  .createDataFrame(
                      List.of(RowFactory.create(5)),
                      new StructType().add("id", DataTypes.IntegerType));

          df.writeTo(tableName).overwritePartitions();

          // Verify result - should have replaced with partition overwrite logic
          check(tableName, List.of(List.of("5")));
        });
  }
}
