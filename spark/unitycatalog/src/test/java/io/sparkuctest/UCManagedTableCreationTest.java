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
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

import io.unitycatalog.client.api.TablesApi;
import io.unitycatalog.client.model.ColumnInfo;
import io.unitycatalog.client.model.DataSourceFormat;
import io.unitycatalog.client.model.TableInfo;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** Test suite for creating UC Managed Delta Tables. */
public class UCManagedTableCreationTest extends UCDeltaTableIntegrationBaseTest {

  // Property constants to avoid duplication
  private static final String UC_TABLE_ID_KEY = "io.unitycatalog.tableId";
  private static final String UC_TABLE_ID_KEY_OLD = "ucTableId";
  private static final String DELTA_CATALOG_MANAGED_KEY_OLD = "delta.feature.catalogOwned-preview";
  private static final String DELTA_CATALOG_MANAGED_KEY = "delta.feature.catalogManaged";
  private static final String DELTA_CATALOG_MANAGED_VALUE = "supported";
  private static final String TBLPROPERTIES_CLAUSE =
      String.format(
          "TBLPROPERTIES ('%s'='%s', 'Foo'='Bar')",
          DELTA_CATALOG_MANAGED_KEY, DELTA_CATALOG_MANAGED_VALUE);
  private static final String TBLPROPERTIES_CLAUSE_OTHER =
      String.format(
          "TBLPROPERTIES ('%s'='%s', 'Foo2'='Bar2')",
          DELTA_CATALOG_MANAGED_KEY, DELTA_CATALOG_MANAGED_VALUE);
  private static final String SCHEMA_NAME = "default";

  private Set<String> tablesToCleanUp = new HashSet<>();

  @AfterEach
  public void cleanUpTables() {
    for (String fullTableName : tablesToCleanUp) {
      try {
        sql("DROP TABLE IF EXISTS %s", fullTableName);
      } catch (Exception e) {
        // Ignore during clean up.
      }
    }
    tablesToCleanUp.clear();
  }

  /**
   * Helper class for controlling table creation options during tests. Simplified version that only
   * supports DELTA format and local filesystem.
   */
  @Accessors(chain = true)
  @Getter
  @Setter
  private static class TableSetupOptions {
    private String catalogName;
    private String schemaName = SCHEMA_NAME;
    private String tableName;
    private Optional<String> partitionColumn = Optional.empty();
    private Optional<Pair<Integer, String>> asSelect = Optional.empty();
    private Optional<String> comment = Optional.empty();
    private boolean replaceTable = false;

    public TableSetupOptions() {}

    public TableSetupOptions setPartitionColumn(String column) {
      assert List.of("i", "s").contains(column);
      partitionColumn = Optional.of(column);
      return this;
    }

    public TableSetupOptions setAsSelect(int i, String s) {
      asSelect = Optional.of(Pair.of(i, s));
      return this;
    }

    public TableSetupOptions setComment(String c) {
      comment = Optional.of(c);
      return this;
    }

    public String partitionClause() {
      return partitionColumn.map(c -> String.format("PARTITIONED BY (%s)", c)).orElse("");
    }

    public String columnsClause() {
      if (asSelect.isEmpty()) {
        return "(i INT, s STRING)";
      } else {
        // CTAS can't specify columns
        return "";
      }
    }

    public String asSelectClause() {
      return asSelect
          .map(x -> String.format("AS SELECT %d AS i, '%s' AS s", x.getLeft(), x.getRight()))
          .orElse("");
    }

    public String commentClause() {
      return comment.map(c -> String.format("COMMENT '%s'", c)).orElse("");
    }

    public String ddlCommand() {
      return replaceTable ? "REPLACE" : "CREATE";
    }

    public String createManagedTableSql() {
      return String.format(
          "%s TABLE %s.%s.%s %s USING DELTA %s %s %s %s",
          ddlCommand(),
          catalogName,
          schemaName,
          tableName,
          columnsClause(),
          partitionClause(),
          TBLPROPERTIES_CLAUSE,
          commentClause(),
          asSelectClause());
    }

    public String fullTableName() {
      return String.join(".", catalogName, schemaName, tableName);
    }
  }

  @Test
  public void testCreateManagedTable() throws Exception {
    UnityCatalogInfo uc = unityCatalogInfo();
    int counter = 0;
    final String comment = "This is comment.";
    // Test with unity catalog only (spark_catalog is not configured as UC catalog)
    final String catalogName = uc.catalogName();

    for (boolean withPartition : List.of(true, false)) {
      for (boolean ctas : List.of(true, false)) {
        for (boolean replaceTable : List.of(true, false)) {
          String tableName = "test_delta" + counter;
          counter++;

          TableSetupOptions options =
              new TableSetupOptions()
                  .setCatalogName(catalogName)
                  .setTableName(tableName)
                  .setReplaceTable(replaceTable)
                  .setComment(comment);
          if (withPartition) {
            options.setPartitionColumn("i");
          }
          if (ctas) {
            options.setAsSelect(1, "a");
          }

          String fullTableName = options.fullTableName();
          if (replaceTable) {
            // First, create a different table to replace.
            sql(
                "CREATE TABLE %s USING DELTA %s AS SELECT 0.1 AS col1",
                fullTableName, TBLPROPERTIES_CLAUSE_OTHER);
            tablesToCleanUp.add(fullTableName);
          }
          // Create table
          sql(options.createManagedTableSql());
          tablesToCleanUp.add(fullTableName);
          // Basic read/write test
          sql("INSERT INTO %s SELECT 2, 'b'", fullTableName);
          if (ctas) {
            check(fullTableName, List.of(List.of("1", "a"), List.of("2", "b")));
          } else {
            check(fullTableName, List.of(List.of("2", "b")));
          }

          // Verify table using DESC EXTENDED
          List<List<String>> rows = sql("DESC EXTENDED " + fullTableName);
          Map<String, String> describeResult = new HashMap<>();
          for (List<String> row : rows) {
            String key = row.get(0);
            // Skip duplicate column names that appear in partition info
            if (!List.of("i", "s").contains(key)) {
              describeResult.put(key, row.get(1));
            }
          }

          // Verify basic table properties
          assertThat(describeResult.get("Name")).isEqualTo(fullTableName);
          assertThat(describeResult.get("Type")).isEqualTo("MANAGED");
          assertThat(describeResult.get("Provider")).isEqualToIgnoringCase("delta");
          assertThat(describeResult.get("Is_managed_location")).isEqualTo("true");
          assertThat(describeResult).containsKey("Table Properties");
          String tableProperties = describeResult.get("Table Properties");
          // Check for UC table ID
          assertThat(tableProperties).contains(UC_TABLE_ID_KEY);
          // Check for catalogManaged feature
          assertThat(tableProperties)
              .contains(
                  String.format("%s=%s", DELTA_CATALOG_MANAGED_KEY, DELTA_CATALOG_MANAGED_VALUE));

          // Verify that properties are set on server. This can not be done by DESC EXTENDED.
          TablesApi tablesApi = new TablesApi(uc.createApiClient());
          TableInfo tableInfo = tablesApi.getTable(fullTableName, false, false);
          assertThat(tableInfo.getCatalogName()).isEqualTo(catalogName);
          assertThat(tableInfo.getName()).isEqualTo(tableName);
          assertThat(tableInfo.getSchemaName()).isEqualTo(SCHEMA_NAME);
          assertThat(tableInfo.getTableType().name()).isEqualTo(TableType.MANAGED.name());
          assertThat(tableInfo.getDataSourceFormat().name())
              .isEqualTo(DataSourceFormat.DELTA.name());
          assertThat(tableInfo.getComment()).isEqualTo(comment);

          // At this point table schema can not be sent to server yet because it won't be updated
          // later and that would cause problem.
          List<ColumnInfo> columns = tableInfo.getColumns();
          assertThat(columns).isNotNull();
          assertThat(columns).isEmpty();

          // Delta sent table properties to server
          Map<String, String> tablePropertiesFromServer = tableInfo.getProperties();
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
          expectedProperties.forEach(
              (key, value) -> assertThat(tablePropertiesFromServer).containsEntry(key, value));
          // Lastly the timestamp value is always changing so skip checking its value
          assertThat(tablePropertiesFromServer).containsKey("delta.lastCommitTimestamp");
        }
      }
    }
  }

  @Test
  public void testCreateManagedTableErrors() {
    String tableName = "test_delta_errors";
    String fullTableName = getCatalogName() + "." + SCHEMA_NAME + "." + tableName;

    // Test 1: Non-Delta managed tables are not supported
    assertThatThrownBy(
            () ->
                sql(
                    "CREATE TABLE %s(name STRING) USING parquet %s",
                    fullTableName, TBLPROPERTIES_CLAUSE))
        .hasMessageContaining("not support non-Delta managed table");

    // Test 2: Invalid property value 'disabled' for catalogManaged feature
    for (String featureProperty :
        List.of(DELTA_CATALOG_MANAGED_KEY_OLD, DELTA_CATALOG_MANAGED_KEY)) {
      assertThatThrownBy(
              () ->
                  sql(
                      "CREATE TABLE %s(name STRING) USING delta TBLPROPERTIES ('%s' = 'disabled')",
                      fullTableName, featureProperty))
          .hasMessageContaining(
              String.format("Invalid property value 'disabled' for '%s'", featureProperty));
    }

    // Test 3: Cannot set UC table ID manually
    for (String ucTableIdProperty : List.of(UC_TABLE_ID_KEY, UC_TABLE_ID_KEY_OLD)) {
      assertThatThrownBy(
              () ->
                  sql(
                      "CREATE TABLE %s(name STRING) USING delta TBLPROPERTIES ('%s' = 'some_id')",
                      fullTableName, ucTableIdProperty))
          .hasMessageContaining(ucTableIdProperty);
    }

    // Test 4: Cannot set is_managed_location to false for managed tables
    assertThatThrownBy(
            () ->
                sql(
                    "CREATE TABLE %s(name STRING) USING delta TBLPROPERTIES ('%s' = 'false')",
                    fullTableName, TableCatalog.PROP_IS_MANAGED_LOCATION))
        .hasMessageContaining("is_managed_location");

    // Test 5: Managed table creation requires catalogManaged property
    assertThatThrownBy(() -> sql("CREATE TABLE %s(name STRING) USING delta", fullTableName))
        .hasMessageContaining(
            String.format(
                "Managed table creation requires table property '%s'='%s' to be set",
                DELTA_CATALOG_MANAGED_KEY, DELTA_CATALOG_MANAGED_VALUE));
  }
}
