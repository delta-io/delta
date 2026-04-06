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

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

/**
 * DataFrame write test suite for Delta Table operations through Unity Catalog.
 *
 * <p>Covers DataFrame Writer V1 (insertInto, save) and Writer V2 (writeTo) operations. Tests run
 * against both EXTERNAL and MANAGED table types.
 */
public class UCDeltaTableDataFrameWriteTest extends UCDeltaTableIntegrationBaseTest {

  // Writer V1: insertInto

  @TestAllTableTypes
  public void testInsertIntoAppend(TableType tableType) throws Exception {
    withNewTable(
        "insert_into_append_test",
        "id INT",
        tableType,
        tableName -> {
          sql("INSERT INTO %s VALUES (1), (2), (3)", tableName);
          intDf(4, 5).write().mode("append").insertInto(tableName);
          check(tableName, List.of(row("1"), row("2"), row("3"), row("4"), row("5")));
        });
  }

  @TestAllTableTypes
  public void testInsertIntoOverwrite(TableType tableType) throws Exception {
    withNewTable(
        "insert_into_overwrite_test",
        "id INT",
        tableType,
        tableName -> {
          sql("INSERT INTO %s VALUES (1), (2), (3)", tableName);
          intDf(9).write().mode("overwrite").insertInto(tableName);
          check(tableName, List.of(row("9")));
        });
  }

  @TestAllTableTypes
  public void testInsertIntoReplaceWhere(TableType tableType) throws Exception {
    withNewTable(
        "insert_into_replace_where_test",
        "id INT",
        tableType,
        tableName -> {
          sql("INSERT INTO %s VALUES (1), (2), (3)", tableName);
          intDf(9).write().mode("overwrite").option("replaceWhere", "id > 1").insertInto(tableName);
          check(tableName, List.of(row("1"), row("9")));
        });
  }

  @TestAllTableTypes
  public void testSaveAsTableAppend(TableType tableType) throws Exception {
    withNewTable(
        "save_as_table_append_test",
        "id INT",
        tableType,
        tableName -> {
          sql("INSERT INTO %s VALUES (1), (2), (3)", tableName);
          intDf(4, 5).write().format("delta").mode("append").saveAsTable(tableName);
          check(tableName, List.of(row("1"), row("2"), row("3"), row("4"), row("5")));
        });
  }

  // TODO: Add saveAsTable overwrite/replaceWhere coverage once UCSingleCatalog supports REPLACE
  // TABLE AS SELECT (RTAS). Currently, saveAsTable with mode("overwrite") routes through Spark's
  // V2 catalog path as RTAS, which throws UnsupportedOperationException in UCSingleCatalog.

  @Test
  public void testSaveByPathBlockedForManagedTable() throws Exception {
    withNewTable(
        "save_path_blocked_test",
        "id INT",
        TableType.MANAGED,
        tableName -> {
          sql("INSERT INTO %s VALUES (1), (2), (3)", tableName);
          String tablePath =
              sql("DESCRIBE FORMATTED %s", tableName).stream()
                  .filter(r -> r.size() >= 2 && "Location".equalsIgnoreCase(r.get(0).trim()))
                  .map(r -> r.get(1).trim())
                  .findFirst()
                  .orElseThrow();
          assertThatThrownBy(() -> intDf(4).write().format("delta").mode("append").save(tablePath))
              .satisfies(
                  e ->
                      assertThat(e.getMessage())
                          .containsAnyOf(
                              "Unable to load credentials",
                              "DELTA_PATH_BASED_ACCESS_TO_CATALOG_MANAGED_TABLE_BLOCKED",
                              "Path-based access is not allowed"));
          check(tableName, List.of(row("1"), row("2"), row("3")));
        });
  }

  // Writer V2: writeTo

  @TestAllTableTypes
  public void testWriteToAppend(TableType tableType) throws Exception {
    withNewTable(
        "write_to_append_test",
        "id INT",
        tableType,
        tableName -> {
          sql("INSERT INTO %s VALUES (1), (2), (3)", tableName);
          intDf(4, 5).writeTo(tableName).append();
          check(tableName, List.of(row("1"), row("2"), row("3"), row("4"), row("5")));
        });
  }

  @TestAllTableTypes
  public void testWriteToOverwrite(TableType tableType) throws Exception {
    withNewTable(
        "write_to_overwrite_test",
        "id INT",
        tableType,
        tableName -> {
          sql("INSERT INTO %s VALUES (1), (2), (3)", tableName);
          intDf(9).writeTo(tableName).overwrite(lit(true));
          check(tableName, List.of(row("9")));
        });
  }

  @TestAllTableTypes
  public void testWriteToOverwriteWithCondition(TableType tableType) throws Exception {
    withNewTable(
        "write_to_overwrite_condition_test",
        "id INT, category STRING",
        tableType,
        tableName -> {
          sql("INSERT INTO %s VALUES (1, 'A'), (2, 'B'), (3, 'A')", tableName);
          spark()
              .createDataFrame(
                  List.of(RowFactory.create(9, "A")),
                  new StructType()
                      .add("id", DataTypes.IntegerType)
                      .add("category", DataTypes.StringType))
              .writeTo(tableName)
              .overwrite(col("category").equalTo("A"));
          check(tableName, List.of(row("2", "B"), row("9", "A")));
        });
  }

  @TestAllTableTypes
  public void testWriteToOverwritePartitions(TableType tableType) throws Exception {
    withNewTable(
        "write_to_overwrite_partitions_test",
        "id INT, category STRING",
        "category",
        tableType,
        tableName -> {
          sql("INSERT INTO %s VALUES (1, 'A'), (2, 'A'), (3, 'B')", tableName);
          spark()
              .createDataFrame(
                  List.of(RowFactory.create(9, "A")),
                  new StructType()
                      .add("id", DataTypes.IntegerType)
                      .add("category", DataTypes.StringType))
              .writeTo(tableName)
              .overwritePartitions();
          // Only partition 'A' is replaced; partition 'B' remains untouched.
          check(tableName, List.of(row("3", "B"), row("9", "A")));
        });
  }

  @Test
  public void testWriteToCreateNewManagedTable() throws Exception {
    String tableName = fullTableName("write_to_create_test");
    try {
      intDf(1, 2)
          .writeTo(tableName)
          .using("delta")
          .tableProperty("delta.feature.catalogManaged", "supported")
          .create();
      check(tableName, List.of(row("1"), row("2")));
    } finally {
      sql("DROP TABLE IF EXISTS %s", tableName);
    }
  }

  @TestAllTableTypes
  public void testMergeSchema(TableType tableType) throws Exception {
    Assumptions.assumeFalse(
        isUCRemoteConfigured(), "mergeSchema not yet supported for UC managed tables remotely");
    if (tableType == TableType.MANAGED) {
      // mergeSchema triggers updateMetadata() with a new schema, which the kill switch blocks
      // on CatalogOwned tables. Assert the failure rather than skipping.
      withNewTable(
          "merge_schema_blocked_test",
          "id INT",
          tableType,
          tableName -> {
            sql("INSERT INTO %s VALUES (1), (2)", tableName);
            assertThrowsWithCauseContaining(
                "Metadata changes on Unity Catalog",
                () ->
                    spark()
                        .createDataFrame(
                            List.of(RowFactory.create(3, "extra")),
                            new StructType()
                                .add("id", DataTypes.IntegerType)
                                .add("name", DataTypes.StringType))
                        .write()
                        .format("delta")
                        .mode("append")
                        .option("mergeSchema", "true")
                        .saveAsTable(tableName));
          });
      return;
    }
    withNewTable(
        "merge_schema_test",
        "id INT",
        tableType,
        tableName -> {
          sql("INSERT INTO %s VALUES (1), (2)", tableName);
          spark()
              .createDataFrame(
                  List.of(RowFactory.create(3, "extra")),
                  new StructType()
                      .add("id", DataTypes.IntegerType)
                      .add("name", DataTypes.StringType))
              .write()
              .format("delta")
              .mode("append")
              .option("mergeSchema", "true")
              .saveAsTable(tableName);
          check(tableName, List.of(row("1", "null"), row("2", "null"), row("3", "extra")));
        });
  }

  @TestAllTableTypes
  public void testWriteToPartitionedTable(TableType tableType) throws Exception {
    withNewTable(
        "df_partitioned_write_test",
        "id INT, category STRING",
        "category",
        tableType,
        tableName -> {
          sql("INSERT INTO %s VALUES (1, 'A'), (2, 'B')", tableName);
          spark()
              .createDataFrame(
                  List.of(RowFactory.create(3, "A"), RowFactory.create(4, "B")),
                  new StructType()
                      .add("id", DataTypes.IntegerType)
                      .add("category", DataTypes.StringType))
              .write()
              .mode("append")
              .insertInto(tableName);
          check(tableName, List.of(row("1", "A"), row("2", "B"), row("3", "A"), row("4", "B")));
        });
  }

  private Dataset<Row> intDf(Integer... ids) {
    return spark()
        .createDataFrame(
            Arrays.stream(ids).map(RowFactory::create).collect(Collectors.toList()),
            new StructType().add("id", DataTypes.IntegerType));
  }
}
