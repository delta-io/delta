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

import java.util.List;
import java.util.stream.Collectors;
import org.apache.spark.sql.Row;

/**
 * DataFrame read test suite for Delta Table operations through Unity Catalog.
 *
 * <p>Covers spark.table(), DataFrameReader, time travel, column pruning, and filter pushdown. Tests
 * run against both EXTERNAL and MANAGED table types.
 */
public class UCDeltaTableDataFrameReadTest extends UCDeltaTableIntegrationBaseTest {

  @TestAllTableTypes
  public void testReadViaSparkTable(TableType tableType) throws Exception {
    withNewTable(
        "df_read_spark_table",
        "id INT",
        tableType,
        tableName -> {
          sql("INSERT INTO %s VALUES (1), (2), (3)", tableName);
          assertThat(ids(spark().table(tableName).orderBy("id"))).containsExactly(1, 2, 3);
        });
  }

  @TestAllTableTypes
  public void testReadViaDataFrameReader(TableType tableType) throws Exception {
    withNewTable(
        "df_read_reader",
        "id INT",
        tableType,
        tableName -> {
          sql("INSERT INTO %s VALUES (1), (2), (3)", tableName);
          assertThat(ids(spark().read().format("delta").table(tableName).orderBy("id")))
              .containsExactly(1, 2, 3);
        });
  }

  @TestAllTableTypes
  public void testTimeTravelByVersion(TableType tableType) throws Exception {
    withNewTable(
        "df_time_travel_version",
        "id INT",
        tableType,
        tableName -> {
          sql("INSERT INTO %s VALUES (1), (2), (3)", tableName);
          long v1 = currentVersion(tableName);
          sql("INSERT INTO %s VALUES (4), (5)", tableName);
          assertThat(
                  ids(
                      spark()
                          .read()
                          .format("delta")
                          .option("versionAsOf", v1)
                          .table(tableName)
                          .orderBy("id")))
              .containsExactly(1, 2, 3);
        });
  }

  @TestAllTableTypes
  public void testTimeTravelByTimestamp(TableType tableType) throws Exception {
    withNewTable(
        "df_time_travel_ts",
        "id INT",
        tableType,
        tableName -> {
          sql("INSERT INTO %s VALUES (1), (2), (3)", tableName);
          String ts = currentTimestamp(tableName);
          sql("INSERT INTO %s VALUES (4), (5)", tableName);
          assertThat(
                  ids(
                      spark()
                          .read()
                          .format("delta")
                          .option("timestampAsOf", ts)
                          .table(tableName)
                          .orderBy("id")))
              .containsExactly(1, 2, 3);
        });
  }

  @TestAllTableTypes
  public void testColumnPruning(TableType tableType) throws Exception {
    withNewTable(
        "df_column_pruning",
        "id INT, name STRING, value INT",
        tableType,
        tableName -> {
          sql("INSERT INTO %s VALUES (1, 'Alice', 100), (2, 'Bob', 200)", tableName);
          List<Row> rows =
              spark().table(tableName).select("id", "name").orderBy("id").collectAsList();
          assertThat(rows.get(0).schema().fieldNames()).containsExactly("id", "name");
          assertThat(rows.stream().map(r -> r.getInt(0)).collect(Collectors.toList()))
              .containsExactly(1, 2);
          assertThat(rows.stream().map(r -> r.getString(1)).collect(Collectors.toList()))
              .containsExactly("Alice", "Bob");
        });
  }

  @TestAllTableTypes
  public void testFilterPushdown(TableType tableType) throws Exception {
    withNewTable(
        "df_filter_pushdown",
        "id INT, category STRING",
        tableType,
        tableName -> {
          sql("INSERT INTO %s VALUES (1, 'A'), (2, 'B'), (3, 'A'), (4, 'B')", tableName);
          assertThat(
                  ids(spark().table(tableName).filter(col("category").equalTo("A")).orderBy("id")))
              .containsExactly(1, 3);
        });
  }

  private List<Integer> ids(org.apache.spark.sql.Dataset<Row> df) {
    return df.collectAsList().stream().map(r -> r.getInt(0)).collect(Collectors.toList());
  }

  private long currentVersion(String tableName) {
    return Long.parseLong(sql("DESCRIBE HISTORY %s LIMIT 1", tableName).get(0).get(0));
  }

  private String currentTimestamp(String tableName) {
    return sql("DESCRIBE HISTORY %s LIMIT 1", tableName).get(0).get(1);
  }
}
