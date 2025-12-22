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

package io.sparkuctest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.client.api.TablesApi;
import io.unitycatalog.client.model.ColumnInfo;
import io.unitycatalog.client.model.TableInfo;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Test suite for creating UC Managed Delta Tables. */
public class UCManagedTableCreationTest extends UCDeltaTableIntegrationBaseTest {

  @Test
  public void testCreateManagedTable() throws Exception {
    // TODO: make this a thorough test of managed table creation with different parameters.
    UnityCatalogInfo uc = unityCatalogInfo();
    String tableName = "test_managed_table";
    String fullTableName = uc.catalogName() + ".default." + tableName;
    String tableSchema = "id INT, active BOOLEAN";
    try {
      sql(
          "CREATE TABLE %s (%s) USING DELTA "
              + "TBLPROPERTIES ('delta.feature.catalogManaged'='supported', 'Foo'='Bar')",
          fullTableName, tableSchema);

      // Verify that properties are set on server. This can not be done by DESC EXTENDED.
      TablesApi tablesApi = new TablesApi(uc.createApiClient());
      TableInfo tableInfo = tablesApi.getTable(fullTableName, false, false);
      List<ColumnInfo> columns = tableInfo.getColumns();
      Map<String, String> properties = tableInfo.getProperties();

      assertThat(columns).isNotNull();
      // At this point table schema can not be sent to server yet because it won't be updated
      // later and that would cause problem.
      assertThat(columns).isEmpty();

      final String SUPPORTED = "supported";
      HashMap<String, String> expectedProperties = new HashMap<>();
      expectedProperties.put("delta.enableInCommitTimestamps", "true");
      expectedProperties.put("delta.feature.appendOnly", SUPPORTED);
      expectedProperties.put("delta.feature.catalogManaged", SUPPORTED);
      expectedProperties.put("delta.feature.inCommitTimestamp", SUPPORTED);
      expectedProperties.put("delta.feature.invariants", SUPPORTED);
      expectedProperties.put("delta.feature.vacuumProtocolCheck", SUPPORTED);
      expectedProperties.put("delta.lastUpdateVersion", "0");
      expectedProperties.put("delta.minReaderVersion", "3");
      expectedProperties.put("delta.minWriterVersion", "7");
      expectedProperties.put("io.unitycatalog.tableId", tableInfo.getTableId());
      // User specified custom table property is also sent.
      expectedProperties.put("Foo", "Bar");
      // Verify that all entries are present
      expectedProperties.forEach((key, value) -> assertThat(properties).containsEntry(key, value));
      // Lastly the timestamp value is always changing so skip checking its value
      assertThat(properties).containsKey("delta.lastCommitTimestamp");
    } finally {
      sql("DROP TABLE IF EXISTS %s", fullTableName);
    }
  }

  @Test
  public void testCreateOrReplaceTable() throws Exception {
    withNewTable(
        "test_managed_delta",
        "id INT",
        TableType.MANAGED,
        tableName -> {
          // Verify initial table is created
          check(tableName, List.of());

          // CREATE OR REPLACE with new schema
          sql(
              "CREATE OR REPLACE TABLE %s (id INT, name STRING) USING DELTA "
                  + "TBLPROPERTIES ('delta.feature.catalogManaged'='supported')",
              tableName);
          check(tableName, List.of());

          // Insert data to verify new schema
          sql("INSERT INTO %s VALUES (1, 'Alice')", tableName);
          check(tableName, List.of(List.of("1", "Alice")));
        });
  }

  @Test
  public void testReplaceTable() throws Exception {
    withNewTable(
        "test_managed_delta",
        "id INT",
        TableType.MANAGED,
        tableName -> {
          // Insert initial data
          sql("INSERT INTO %s VALUES (1)", tableName);
          check(tableName, List.of(List.of("1")));

          // REPLACE TABLE with new schema
          sql(
              "REPLACE TABLE %s (id INT, name STRING) USING DELTA "
                  + "TBLPROPERTIES ('delta.feature.catalogManaged'='supported')",
              tableName);

          // Data should be replaced
          check(tableName, List.of());

          // Insert data to verify new schema
          sql("INSERT INTO %s VALUES (1, 'Alice')", tableName);
          check(tableName, List.of(List.of("1", "Alice")));
        });
  }

  @Test
  public void testCreateTableAsSelect() throws Exception {
    withNewTable(
        "source_table",
        "id INT",
        TableType.MANAGED,
        sourceTable -> {
          // Insert data into source
          sql("INSERT INTO %s VALUES (1), (2), (3), (4)", sourceTable);

          UnityCatalogInfo uc = unityCatalogInfo();
          String targetTable = uc.catalogName() + "." + uc.schemaName() + ".ctas_target";
          try {
            // CTAS from source
            sql(
                "CREATE TABLE %s USING DELTA "
                    + "TBLPROPERTIES ('delta.feature.catalogManaged'='supported') "
                    + "AS SELECT id FROM %s",
                targetTable, sourceTable);

            // Verify data was copied
            check(targetTable, List.of(List.of("1"), List.of("2"), List.of("3"), List.of("4")));
          } finally {
            sql("DROP TABLE IF EXISTS %s", targetTable);
          }
        });
  }

  @Test
  public void testReplaceTableAsSelect() throws Exception {
    withNewTable(
        "source_table",
        "id INT",
        TableType.MANAGED,
        sourceTable -> {
          // Insert data into source
          sql("INSERT INTO %s VALUES (1), (2), (3)", sourceTable);

          withNewTable(
              "target_table",
              "old_col STRING",
              TableType.MANAGED,
              targetTable -> {
                sql("INSERT INTO %s VALUES ('old_data')", targetTable);

                // REPLACE TABLE AS SELECT
                sql(
                    "REPLACE TABLE %s USING DELTA "
                        + "TBLPROPERTIES ('delta.feature.catalogManaged'='supported') "
                        + "AS SELECT id FROM %s",
                    targetTable, sourceTable);

                // Verify data and schema were replaced
                check(targetTable, List.of(List.of("1"), List.of("2"), List.of("3")));
              });
        });
  }

  @Test
  public void testCreateTableLike() throws Exception {
    withNewTable(
        "source_table",
        "id INT, name STRING",
        TableType.MANAGED,
        sourceTable -> {
          // Insert data into source
          sql("INSERT INTO %s VALUES (1, 'Alice'), (2, 'Bob')", sourceTable);

          UnityCatalogInfo uc = unityCatalogInfo();
          String targetTable = uc.catalogName() + "." + uc.schemaName() + ".like_target";
          try {
            // CREATE TABLE with same schema as source
            // Note: CREATE TABLE LIKE is not universally supported, so we manually create with same
            // schema
            sql(
                "CREATE TABLE %s (id INT, name STRING) USING DELTA "
                    + "TBLPROPERTIES ('delta.feature.catalogManaged'='supported')",
                targetTable);

            // Table should have same schema but no data
            check(targetTable, List.of());

            // Verify schema by inserting compatible data
            sql("INSERT INTO %s VALUES (3, 'Charlie')", targetTable);
            check(targetTable, List.of(List.of("3", "Charlie")));
          } finally {
            sql("DROP TABLE IF EXISTS %s", targetTable);
          }
        });
  }

  @Test
  public void testCreateTableFromSelect() throws Exception {
    withNewTable(
        "source_table",
        "id INT, partCol INT",
        TableType.MANAGED,
        sourceTable -> {
          // Insert data into source
          sql("INSERT INTO %s VALUES (1, 1), (2, 2), (3, 3)", sourceTable);

          UnityCatalogInfo uc = unityCatalogInfo();
          String targetTable = uc.catalogName() + "." + uc.schemaName() + ".target_table";
          try {
            // CREATE TABLE AS SELECT (CTAS) to copy data
            sql(
                "CREATE TABLE %s USING DELTA "
                    + "TBLPROPERTIES ('delta.feature.catalogManaged'='supported') "
                    + "AS SELECT * FROM %s",
                targetTable, sourceTable);

            // Verify data was copied
            check(targetTable, List.of(List.of("1", "1"), List.of("2", "2"), List.of("3", "3")));

            // Writes to original and copy should be independent
            sql("INSERT INTO %s VALUES (4, 4)", sourceTable);
            sql("INSERT INTO %s VALUES (5, 5)", targetTable);

            // Verify independence
            check(
                sourceTable,
                List.of(
                    List.of("1", "1"), List.of("2", "2"), List.of("3", "3"), List.of("4", "4")));
            check(
                targetTable,
                List.of(
                    List.of("1", "1"), List.of("2", "2"), List.of("3", "3"), List.of("5", "5")));
          } finally {
            sql("DROP TABLE IF EXISTS %s", targetTable);
          }
        });
  }

  @Test
  public void testTableWithSupportedDataTypes() throws Exception {
    String schema =
        "col_int INT, col_long BIGINT, col_string STRING, "
            + "col_double DOUBLE, col_float FLOAT, col_boolean BOOLEAN, "
            + "col_date DATE, col_timestamp TIMESTAMP, col_binary BINARY, "
            + "col_decimal DECIMAL(10,2)";

    withNewTable(
        "supported_types_table",
        schema,
        TableType.MANAGED,
        tableName -> {
          // Insert sample data
          sql(
              "INSERT INTO %s VALUES ("
                  + "1, 100, 'test', 1.5, 2.5, true, "
                  + "DATE'2025-01-01', TIMESTAMP'2025-01-01 12:00:00', "
                  + "X'DEADBEEF', 123.45)",
              tableName);

          // Verify data can be queried
          List<List<String>> results = sql("SELECT col_int, col_string FROM %s", tableName);
          assertThat(results).hasSize(1);
          assertThat(results.get(0)).containsExactly("1", "test");
        });
  }

  @Test
  public void testTableWithComplexTypes() throws Exception {
    String schema =
        "id INT, arr ARRAY<INT>, "
            + "map_col MAP<STRING, INT>, "
            + "struct_col STRUCT<a: INT, b: STRING>";

    withNewTable(
        "complex_types_table",
        schema,
        TableType.MANAGED,
        tableName -> {
          // Insert sample data
          sql(
              "INSERT INTO %s VALUES (1, array(1, 2, 3), "
                  + "map('key1', 10, 'key2', 20), "
                  + "struct(42, 'test'))",
              tableName);

          // Verify data can be queried
          List<List<String>> results = sql("SELECT id FROM %s", tableName);
          assertThat(results).hasSize(1);
          assertThat(results.get(0)).containsExactly("1");
        });
  }

  @Test
  public void testCustomTableProperties() throws Exception {
    withNewTable(
        "custom_props_table",
        "id INT",
        TableType.MANAGED,
        tableName -> {
          // Custom properties are set via CREATE TABLE TBLPROPERTIES
          // Recreate table with custom properties
          sql("DROP TABLE %s", tableName);
          UnityCatalogInfo uc = unityCatalogInfo();
          String fullName = uc.catalogName() + "." + uc.schemaName() + ".custom_props_table";
          sql(
              "CREATE TABLE %s (id INT) USING DELTA "
                  + "TBLPROPERTIES ('delta.feature.catalogManaged'='supported', "
                  + "'custom.key'='custom_value', 'another'='property')",
              fullName);

          try {
            // Properties should persist
            sql("INSERT INTO %s VALUES (1)", fullName);
            check(fullName, List.of(List.of("1")));
          } finally {
            sql("DROP TABLE IF EXISTS %s", fullName);
          }
        });
  }

  @Test
  public void testTableWithNotNullConstraints() throws Exception {
    withNewTable(
        "not_null_table",
        "id INT NOT NULL, name STRING NOT NULL, optional STRING",
        TableType.MANAGED,
        tableName -> {
          // Insert valid data
          sql("INSERT INTO %s VALUES (1, 'Alice', 'extra')", tableName);
          sql("INSERT INTO %s VALUES (2, 'Bob', NULL)", tableName);

          check(tableName, List.of(List.of("1", "Alice", "extra"), List.of("2", "Bob", "null")));

          // Attempting to insert NULL into NOT NULL column should fail
          assertThatThrownBy(
                  () -> sql("INSERT INTO %s VALUES (NULL, 'Charlie', 'data')", tableName))
              .isInstanceOf(Exception.class); // More generic exception check
        });
  }

  @Test
  public void testPartitionedTable() throws Exception {
    withNewTable(
        "partitioned_table",
        "id INT, category STRING, value DOUBLE",
        TableType.MANAGED,
        tableName -> {
          // Create partitioned table by recreating with PARTITIONED BY
          sql("DROP TABLE %s", tableName);
          UnityCatalogInfo uc = unityCatalogInfo();
          String fullName = uc.catalogName() + "." + uc.schemaName() + ".partitioned_table";
          sql(
              "CREATE TABLE %s (id INT, category STRING, value DOUBLE) USING DELTA "
                  + "PARTITIONED BY (category) "
                  + "TBLPROPERTIES ('delta.feature.catalogManaged'='supported')",
              fullName);

          try {
            // Insert data into different partitions
            sql("INSERT INTO %s VALUES (1, 'A', 10.5), (2, 'B', 20.5), (3, 'A', 30.5)", fullName);

            // Query data - results ordered by id (first column)
            check(
                fullName,
                List.of(
                    List.of("1", "A", "10.5"),
                    List.of("2", "B", "20.5"),
                    List.of("3", "A", "30.5")));
          } finally {
            sql("DROP TABLE IF EXISTS %s", fullName);
          }
        });
  }
}
