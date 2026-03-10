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
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.delta.tables.DeltaTable;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

/**
 * Programmatic DeltaTable API test suite for Delta Table operations through Unity Catalog.
 *
 * <p>Covers DeltaTable.forName(), update(), delete(), merge(), history(), detail(), optimize(),
 * vacuum(), and restore() via the io.delta.tables.DeltaTable API. These go through different code
 * paths than SQL equivalents tested in UCDeltaTableDMLTest and UCDeltaUtilityTest. Tests run
 * against both EXTERNAL and MANAGED table types.
 */
public class UCDeltaTableDeltaAPITest extends UCDeltaTableIntegrationBaseTest {

  @TestAllTableTypes
  public void testForNameAndToDF(TableType tableType) throws Exception {
    withNewTable(
        "dt_api_read",
        "id INT",
        tableType,
        tableName -> {
          sql("INSERT INTO %s VALUES (1), (2), (3)", tableName);
          assertThat(forName(tableName).toDF().orderBy("id").collectAsList())
              .extracting(r -> r.getInt(0))
              .containsExactly(1, 2, 3);
        });
  }

  @TestAllTableTypes
  public void testUpdate(TableType tableType) throws Exception {
    withNewTable(
        "dt_api_update",
        "id INT, status STRING",
        tableType,
        tableName -> {
          sql("INSERT INTO %s VALUES (1, 'pending'), (2, 'pending'), (3, 'done')", tableName);
          forName(tableName).updateExpr("id = 1", Map.of("status", "'processed'"));
          check(tableName, List.of(row("1", "processed"), row("2", "pending"), row("3", "done")));
        });
  }

  @TestAllTableTypes
  public void testDelete(TableType tableType) throws Exception {
    withNewTable(
        "dt_api_delete",
        "id INT, active BOOLEAN",
        tableType,
        tableName -> {
          sql("INSERT INTO %s VALUES (1, true), (2, false), (3, true)", tableName);
          forName(tableName).delete(col("active").equalTo(false));
          check(tableName, List.of(row("1", "true"), row("3", "true")));
        });
  }

  @TestAllTableTypes
  public void testMerge(TableType tableType) throws Exception {
    withNewTable(
        "dt_api_merge",
        "id INT, value STRING",
        tableType,
        tableName -> {
          sql("INSERT INTO %s VALUES (1, 'old1'), (2, 'old2')", tableName);
          Dataset<Row> source =
              spark()
                  .createDataFrame(
                      List.of(RowFactory.create(2, "updated2"), RowFactory.create(3, "new3")),
                      new StructType()
                          .add("id", DataTypes.IntegerType)
                          .add("value", DataTypes.StringType));
          forName(tableName)
              .as("target")
              .merge(source.as("source"), "target.id = source.id")
              .whenMatched()
              .updateExpr(Map.of("value", "source.value"))
              .whenNotMatched()
              .insertExpr(Map.of("id", "source.id", "value", "source.value"))
              .execute();
          check(tableName, List.of(row("1", "old1"), row("2", "updated2"), row("3", "new3")));
        });
  }

  @TestAllTableTypes
  public void testHistory(TableType tableType) throws Exception {
    withNewTable(
        "dt_api_history",
        "id INT",
        tableType,
        tableName -> {
          sql("INSERT INTO %s VALUES (1)", tableName);
          sql("INSERT INTO %s VALUES (2)", tableName);
          // CREATE TABLE (v0) + 2 INSERTs (v1, v2)
          assertThat(forName(tableName).history().collectAsList()).hasSize(3);
        });
  }

  @TestAllTableTypes
  public void testDetail(TableType tableType) throws Exception {
    withNewTable(
        "dt_api_detail",
        "id INT",
        tableType,
        tableName -> assertThat(forName(tableName).detail().collectAsList()).hasSize(1));
  }

  @TestAllTableTypes
  public void testOptimize(TableType tableType) throws Exception {
    withNewTable(
        "dt_api_optimize",
        "id INT, category STRING",
        tableType,
        tableName -> {
          sql("INSERT INTO %s VALUES (1, 'A'), (2, 'B'), (3, 'A')", tableName);
          if (tableType == TableType.MANAGED) {
            assertThatThrownBy(() -> forName(tableName).optimize().executeCompaction())
                .hasMessageContaining("DELTA_UNSUPPORTED_CATALOG_MANAGED_TABLE_OPERATION");
          } else {
            assertThat(forName(tableName).optimize().executeCompaction().collectAsList())
                .isNotEmpty();
            assertThat(forName(tableName).optimize().executeZOrderBy("category").collectAsList())
                .isNotEmpty();
          }
        });
  }

  @TestAllTableTypes
  public void testVacuum(TableType tableType) throws Exception {
    withNewTable(
        "dt_api_vacuum",
        "id INT",
        tableType,
        tableName -> {
          sql("INSERT INTO %s VALUES (1)", tableName);
          if (tableType == TableType.MANAGED) {
            assertThatThrownBy(() -> forName(tableName).vacuum(168))
                .hasMessageContaining("DELTA_UNSUPPORTED_CATALOG_MANAGED_TABLE_OPERATION");
          } else {
            // vacuum(168) uses default 7-day retention; just verify it completes without error.
            forName(tableName).vacuum(168).collect();
          }
        });
  }

  @TestAllTableTypes
  public void testRestoreToVersion(TableType tableType) throws Exception {
    withNewTable(
        "dt_api_restore_version",
        "id INT",
        tableType,
        tableName -> {
          sql("INSERT INTO %s VALUES (1), (2), (3)", tableName);
          long v1 = currentVersion(tableName);
          sql("INSERT INTO %s VALUES (4), (5)", tableName);
          forName(tableName).restoreToVersion(v1);
          check(tableName, List.of(row("1"), row("2"), row("3")));
        });
  }

  @TestAllTableTypes
  public void testRestoreToTimestamp(TableType tableType) throws Exception {
    withNewTable(
        "dt_api_restore_ts",
        "id INT",
        tableType,
        tableName -> {
          sql("INSERT INTO %s VALUES (1), (2), (3)", tableName);
          String ts = currentTimestamp(tableName);
          sql("INSERT INTO %s VALUES (4), (5)", tableName);
          forName(tableName).restoreToTimestamp(ts);
          check(tableName, List.of(row("1"), row("2"), row("3")));
        });
  }

  private DeltaTable forName(String tableName) {
    return DeltaTable.forName(spark(), tableName);
  }

  private static List<String> row(String... values) {
    return List.of(values);
  }
}
