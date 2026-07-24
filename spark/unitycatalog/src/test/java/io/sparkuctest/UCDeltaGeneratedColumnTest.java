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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.stream.Stream;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.delta.catalog.DeltaTableV2;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class UCDeltaGeneratedColumnTest extends UCDeltaTableIntegrationBaseTest {

  private static final String ORDER_SCHEMA =
      "order_id BIGINT, "
          + "order_ts TIMESTAMP, "
          + "order_date DATE GENERATED ALWAYS AS (CAST(order_ts AS DATE)), "
          + "quantity INT, "
          + "unit_price DECIMAL(12, 2), "
          + "line_total DECIMAL(18, 2) GENERATED ALWAYS AS ("
          + "CAST(quantity * unit_price AS DECIMAL(18, 2)))";

  @TestAllTableTypes
  public void testCreateInsertAndReadGeneratedColumns(TableType tableType) throws Exception {
    withNewTable(
        "generated_columns_create_insert_read",
        ORDER_SCHEMA,
        "order_date",
        tableType,
        tableName -> {
          sql(
              "INSERT INTO %s (order_id, order_ts, quantity, unit_price) VALUES "
                  + "(1001, TIMESTAMP '2026-07-10 09:30:00', 3, 19.99), "
                  + "(1002, TIMESTAMP '2026-07-11 14:15:00', 2, 125.00), "
                  + "(1003, NULL, NULL, NULL)",
              tableName);

          check(
              sql("SELECT order_id, order_date, line_total FROM %s ORDER BY order_id", tableName),
              List.of(
                  row("1001", "2026-07-10", "59.97"),
                  row("1002", "2026-07-11", "250.00"),
                  row("1003", "null", "null")));
        });
  }

  @Test
  public void testGeneratedColumnDmlLifecycleAndMetadata() throws Exception {
    withNewTable(
        "generated_columns_dml_lifecycle",
        "id INT, base_value INT, " + "generated_value INT GENERATED ALWAYS AS (base_value * 10)",
        TableType.MANAGED,
        tableName -> {
          String metadataKey = "delta.generationExpression";
          assertThat(
                  spark()
                      .table(tableName)
                      .schema()
                      .apply("generated_value")
                      .metadata()
                      .contains(metadataKey))
              .isFalse();
          assertThat(
                  loadDeltaTableV2(tableName)
                      .update()
                      .metadata()
                      .schema()
                      .apply("generated_value")
                      .metadata()
                      .getString(metadataKey))
              .isEqualTo("base_value * 10");

          sql("INSERT INTO %s (id, base_value) VALUES (1, 2)", tableName);
          check(tableName, List.of(row("1", "2", "20")));

          sql("UPDATE %s SET base_value = 3 WHERE id = 1", tableName);
          check(tableName, List.of(row("1", "3", "30")));

          assertThrowsWithCauseContaining(
              "Generated Column",
              () -> sql("UPDATE %s SET generated_value = 99 WHERE id = 1", tableName));
          check(tableName, List.of(row("1", "3", "30")));

          sql("INSERT OVERWRITE %s (id, base_value) VALUES (2, 4)", tableName);
          check(tableName, List.of(row("2", "4", "40")));

          sql(
              "MERGE INTO %s AS target "
                  + "USING (SELECT 3 AS id, 5 AS base_value) AS source "
                  + "ON target.id = source.id "
                  + "WHEN NOT MATCHED THEN INSERT (id, base_value) "
                  + "VALUES (source.id, source.base_value)",
              tableName);
          check(tableName, List.of(row("2", "4", "40"), row("3", "5", "50")));
        });
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("stagedTableOperations")
  public void testStagedOperationsPreserveGeneratedColumns(
      String caseName, String operation, boolean tableExists) {
    String tableName = fullTableName("generated_columns_staged_" + caseName);
    sql("DROP TABLE IF EXISTS %s", tableName);
    try {
      if (tableExists) {
        createGeneratedTable(tableName, "base_value + 1");
        sql("INSERT INTO %s (base_value) VALUES (1)", tableName);
        check(sql("SELECT * FROM %s", tableName), List.of(row("1", "2")));
      }

      sql(
          "%s %s (base_value INT, generated_value INT GENERATED ALWAYS AS (base_value * 10)) "
              + "USING DELTA TBLPROPERTIES ('delta.feature.catalogManaged'='supported')",
          operation, tableName);
      sql("INSERT INTO %s (base_value) VALUES (2)", tableName);

      // The replacement expression must be persisted, and replacement must remove existing data.
      check(sql("SELECT * FROM %s", tableName), List.of(row("2", "20")));
    } finally {
      sql("DROP TABLE IF EXISTS %s", tableName);
    }
  }

  @Test
  public void testRejectsIncorrectExplicitGeneratedColumnValues() throws Exception {
    withNewTable(
        "generated_columns_invalid_explicit_value",
        ORDER_SCHEMA,
        TableType.MANAGED,
        tableName -> {
          assertThrowsWithCauseContaining(
              "Generated Column",
              () ->
                  sql(
                      "INSERT INTO %s VALUES (1001, TIMESTAMP '2026-07-10 09:30:00', "
                          + "DATE '2026-07-09', 3, 19.99, 1.00)",
                      tableName));
          check(tableName, List.of());
        });
  }

  @Test
  public void testGeneratedColumnCoexistsWithOtherColumnMetadata() throws Exception {
    withNewTable(
        "generated_columns_metadata_coexistence",
        "id INT, "
            + "`base.value` INT DEFAULT 7 COMMENT 'source value', "
            + "`generated.value` INT GENERATED ALWAYS AS (`base.value` * 3) "
            + "COMMENT 'derived value'",
        null,
        TableType.MANAGED,
        "'delta.feature.allowColumnDefaults'='supported'",
        tableName -> {
          sql("INSERT INTO %s (id) VALUES (1)", tableName);

          check(
              sql("SELECT id, `base.value`, `generated.value` FROM %s", tableName),
              List.of(row("1", "7", "21")));
          assertThat(spark().table(tableName).schema().apply("base.value").getComment().get())
              .isEqualTo("source value");
          assertThat(spark().table(tableName).schema().apply("generated.value").getComment().get())
              .isEqualTo("derived value");
        });
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("invalidGeneratedColumnDefinitions")
  public void testRejectsInvalidGeneratedColumnDefinitions(
      String caseName, String schema, String expectedMessage) {
    String tableName = fullTableName("generated_columns_invalid_" + caseName);
    sql("DROP TABLE IF EXISTS %s", tableName);
    try {
      assertThrowsWithCauseContaining(
          expectedMessage,
          () ->
              sql(
                  "CREATE TABLE %s (%s) USING DELTA "
                      + "TBLPROPERTIES ('delta.feature.catalogManaged'='supported')",
                  tableName, schema));
      assertThat(spark().catalog().tableExists(tableName)).isFalse();
    } finally {
      sql("DROP TABLE IF EXISTS %s", tableName);
    }
  }

  private static Stream<Arguments> invalidGeneratedColumnDefinitions() {
    return Stream.of(
        Arguments.of(
            "missing_reference",
            "base_value INT, generated_value INT GENERATED ALWAYS AS (missing_value + 1)",
            "cannot be resolved"),
        Arguments.of(
            "generated_reference",
            "base_value INT, first_generated INT GENERATED ALWAYS AS (base_value + 1), "
                + "second_generated INT GENERATED ALWAYS AS (first_generated + 1)",
            "generation expression cannot reference another generated column"),
        Arguments.of(
            "nondeterministic_expression",
            "base_value INT, generated_value DOUBLE GENERATED ALWAYS AS (rand())",
            "generation expression is not deterministic"));
  }

  private static Stream<Arguments> stagedTableOperations() {
    return Stream.of(
        Arguments.of("replace_existing", "REPLACE TABLE", true),
        Arguments.of("create_or_replace_existing", "CREATE OR REPLACE TABLE", true),
        Arguments.of("create_or_replace_missing", "CREATE OR REPLACE TABLE", false));
  }

  private void createGeneratedTable(String tableName, String expression) {
    sql(
        "CREATE TABLE %s "
            + "(base_value INT, generated_value INT GENERATED ALWAYS AS (%s)) USING DELTA "
            + "TBLPROPERTIES ('delta.feature.catalogManaged'='supported')",
        tableName, expression);
  }

  private DeltaTableV2 loadDeltaTableV2(String fullTableName) throws Exception {
    String[] parts = fullTableName.split("\\.");
    TableCatalog catalog = (TableCatalog) spark().sessionState().catalogManager().catalog(parts[0]);
    return (DeltaTableV2) catalog.loadTable(Identifier.of(new String[] {parts[1]}, parts[2]));
  }
}
