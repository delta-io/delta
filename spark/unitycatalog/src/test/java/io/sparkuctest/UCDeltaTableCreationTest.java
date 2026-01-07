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
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import io.unitycatalog.client.api.TablesApi;
import io.unitycatalog.client.model.ColumnInfo;
import io.unitycatalog.client.model.DataSourceFormat;
import io.unitycatalog.client.model.TableInfo;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Logger;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/** Test suite for creating UC Delta Tables. */
public class UCDeltaTableCreationTest extends UCDeltaTableIntegrationBaseTest {

  private static final Logger LOG = Logger.getLogger(UCDeltaTableCreationTest.class);

  // Property constants related to managed table creation
  private static final String UC_TABLE_ID_KEY = "io.unitycatalog.tableId";
  private static final String UC_TABLE_ID_KEY_OLD = "ucTableId";
  private static final String DELTA_CATALOG_MANAGED_KEY = "delta.feature.catalogManaged";
  private static final String SUPPORTED = "supported";
  private static final String MANAGED_TBLPROPERTIES_CLAUSE =
      String.format("TBLPROPERTIES ('%s'='%s', 'Foo'='Bar')", DELTA_CATALOG_MANAGED_KEY, SUPPORTED);
  // In the table REPLACE test, a slightly different table property clause will be used to create
  // the first table. Then the REPLACE command would use TBLPROPERTIES_CLAUSE. This is to make sure
  // that the table properties are properly updated in the REPLACE command.
  private static final String MANAGED_TBLPROPERTIES_CLAUSE_OTHER =
      String.format(
          "TBLPROPERTIES ('%s'='%s', 'Foo2'='Bar2')", DELTA_CATALOG_MANAGED_KEY, SUPPORTED);

  // Expected table features to be enabled for managed tables
  private static final List<String> EXPECTED_MANAGED_TABLE_FEATURES =
      List.of(
          "delta.feature.appendOnly",
          DELTA_CATALOG_MANAGED_KEY,
          "delta.feature.inCommitTimestamp",
          "delta.feature.invariants",
          "delta.feature.vacuumProtocolCheck");
  private static final Map<String, String> EXPECTED_MANAGED_TABLE_FEATURES_PROPERTIES =
      EXPECTED_MANAGED_TABLE_FEATURES.stream()
          .collect(Collectors.toMap(Function.identity(), k -> SUPPORTED));

  private static final String EXTERNAL_TBLPROPERTIES_CLAUSE = "TBLPROPERTIES ('Foo'='Bar')";

  String tempDir;
  private Set<String> tablesToCleanUp = new HashSet<>();

  @BeforeEach
  public void setUp() {
    tempDir = unityCatalogInfo().baseTableLocation() + "/temp-" + UUID.randomUUID();
  }

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

  /** Helper class for controlling table creation options during tests. */
  @Accessors(chain = true)
  @Getter
  @Setter
  @ToString
  private class TableSetupOptions {
    private TableType tableType;
    private String catalogName;
    private String schemaName;
    private String tableName;
    private Optional<String> partitionColumn = Optional.empty();
    private Optional<String> clusterColumn = Optional.empty();
    private Optional<Pair<Integer, String>> asSelect = Optional.empty();
    private Optional<String> comment = Optional.empty();
    private boolean replaceTable = false;

    public TableSetupOptions() {}

    public TableSetupOptions setPartitionColumn(String column) {
      Preconditions.checkArgument(List.of("i", "s").contains(column));
      Preconditions.checkState(
          clusterColumn.isEmpty(), "Can not have both PARTITIONED BY and CLUSTER BY.");
      partitionColumn = Optional.of(column);
      return this;
    }

    public TableSetupOptions setClusterColumn(String column) {
      Preconditions.checkArgument(List.of("i", "s").contains(column));
      Preconditions.checkState(
          partitionColumn.isEmpty(), "Can not have both PARTITIONED BY and CLUSTER BY.");
      clusterColumn = Optional.of(column);
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

    public String clusterClause() {
      return clusterColumn.map(c -> String.format("CLUSTER BY (%s)", c)).orElse("");
    }

    public String columnsClause() {
      if (asSelect.isEmpty()) {
        return "(i INT, s STRING)";
      } else {
        // "AS SELECT" can't specify columns
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

    private String createManagedTableSql() {
      return String.format(
          "%s TABLE %s.%s.%s %s USING DELTA %s %s %s %s %s",
          ddlCommand(),
          catalogName,
          schemaName,
          tableName,
          columnsClause(),
          partitionClause(),
          clusterClause(),
          MANAGED_TBLPROPERTIES_CLAUSE,
          commentClause(),
          asSelectClause());
    }

    public String getExternalTableLocation() {
      return tempDir + "/" + tableName;
    }

    private String createExternalTableSql() {
      return String.format(
          "%s TABLE %s.%s.%s %s USING DELTA %s %s %s %s LOCATION '%s' %s",
          ddlCommand(),
          catalogName,
          schemaName,
          tableName,
          columnsClause(),
          partitionClause(),
          clusterClause(),
          EXTERNAL_TBLPROPERTIES_CLAUSE,
          commentClause(),
          getExternalTableLocation(),
          asSelectClause());
    }

    public String createTableSql() {
      if (tableType == TableType.MANAGED) {
        return createManagedTableSql();
      } else {
        return createExternalTableSql();
      }
    }

    public String fullTableName() {
      return String.join(".", catalogName, schemaName, tableName);
    }
  }

  private static Stream<Arguments> tableCreationParameters() {
    List<Arguments> args = new ArrayList<>();
    for (TableType tableType : TableType.values()) {
      for (boolean withPartition : List.of(true, false)) {
        for (boolean withCluster : List.of(true, false)) {
          if (withCluster && withPartition) {
            // Can not have CLUSTER BY and PARTITIONED BY on the same table
            continue;
          }
          for (boolean withAsSelect : List.of(true, false)) {
            for (boolean replaceTable : List.of(true, false)) {
              args.add(
                  Arguments.of(tableType, withPartition, withCluster, withAsSelect, replaceTable));
            }
          }
        }
      }
    }
    return args.stream();
  }

  @ParameterizedTest(
      name =
          "[{index}] {0}, withPartition={1}, withCluster={2}, withAsSelect={3}, replaceTable={4}")
  @MethodSource("tableCreationParameters")
  public void testCreateTable(
      TableType tableType,
      boolean withPartition,
      boolean withCluster,
      boolean withAsSelect,
      boolean replaceTable)
      throws Exception {
    UnityCatalogInfo uc = unityCatalogInfo();
    final String comment = "This is comment.";
    // Test with unity catalog only (spark_catalog is not configured as UC catalog)
    final String catalogName = uc.catalogName();
    final String schemaName = uc.schemaName();
    String tableName = "test_delta_table";

    TableSetupOptions options =
        new TableSetupOptions()
            .setCatalogName(catalogName)
            .setSchemaName(schemaName)
            .setTableName(tableName)
            .setTableType(tableType)
            .setReplaceTable(replaceTable)
            .setComment(comment);
    if (withPartition) {
      options.setPartitionColumn("i");
    }
    if (withCluster) {
      options.setClusterColumn("s");
    }
    if (withAsSelect) {
      options.setAsSelect(1, "a");
    }
    LOG.info("Running table creation test: " + options);

    String fullTableName = options.fullTableName();
    if (replaceTable) {
      // First, create a different table to replace.
      sql(
          "CREATE TABLE %s USING DELTA %s AS SELECT 0.1 AS col1",
          fullTableName, MANAGED_TBLPROPERTIES_CLAUSE_OTHER);
      tablesToCleanUp.add(fullTableName);
    }
    // Create table
    sql(options.createTableSql());
    tablesToCleanUp.add(fullTableName);
    // Basic read/write test
    sql("INSERT INTO %s SELECT 2, 'b'", fullTableName);
    if (withAsSelect) {
      check(fullTableName, List.of(List.of("1", "a"), List.of("2", "b")));
    } else {
      check(fullTableName, List.of(List.of("2", "b")));
    }

    // Verify that properties are set on server. This can not be done by DESC EXTENDED.
    TablesApi tablesApi = new TablesApi(uc.createApiClient());
    TableInfo tableInfo = tablesApi.getTable(fullTableName, false, false);
    assertThat(tableInfo.getCatalogName()).isEqualTo(catalogName);
    assertThat(tableInfo.getName()).isEqualTo(tableName);
    assertThat(tableInfo.getSchemaName()).isEqualTo(schemaName);
    assertThat(tableInfo.getTableType().name()).isEqualTo(tableType.name());
    assertThat(tableInfo.getDataSourceFormat().name()).isEqualTo(DataSourceFormat.DELTA.name());
    assertThat(tableInfo.getComment()).isEqualTo(comment);
    if (tableType == TableType.EXTERNAL) {
      assertThat(tableInfo.getStorageLocation()).isEqualTo(options.getExternalTableLocation());
    }

    // At this point table schema can not be sent to server yet because it won't be
    // updated
    // later and that would cause problem.
    List<ColumnInfo> columns = tableInfo.getColumns();
    assertThat(columns).isNotNull();
    assertThat(columns).isEmpty();

    if (tableType == TableType.MANAGED) {
      // Delta sent properties of managed tables to server
      Map<String, String> tablePropertiesFromServer = tableInfo.getProperties();

      Map<String, String> expectedOtherProperties =
          ImmutableMap.of(
              "delta.enableInCommitTimestamps",
              "true",
              "delta.lastUpdateVersion",
              "0",
              "delta.minReaderVersion",
              "3",
              "delta.minWriterVersion",
              "7",
              UC_TABLE_ID_KEY,
              tableInfo.getTableId(),
              // User specified custom table property is also sent.
              "Foo",
              "Bar");
      // This is combination of expectedOtherProperties and
      //  EXPECTED_MANAGED_TABLE_FEATURES_PROPERTIES.
      Map<String, String> expectedProperties =
          Stream.concat(
                  EXPECTED_MANAGED_TABLE_FEATURES_PROPERTIES.entrySet().stream(),
                  expectedOtherProperties.entrySet().stream())
              .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

      expectedProperties.forEach(
          (key, value) -> assertThat(tablePropertiesFromServer).containsEntry(key, value));
      // Lastly the timestamp value is always changing so skip checking its value
      assertThat(tablePropertiesFromServer).containsKey("delta.lastCommitTimestamp");
    }

    // Also verify table using DESC EXTENDED
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
    assertThat(describeResult.get("Type")).isEqualTo(tableType.name());
    assertThat(describeResult.get("Provider")).isEqualToIgnoringCase("delta");
    assertThat(describeResult.get("Is_managed_location"))
        .isEqualTo(tableType == TableType.MANAGED ? "true" : null);
    assertThat(describeResult).containsKey("Table Properties");
    String tableProperties = describeResult.get("Table Properties");
    if (tableType == TableType.MANAGED) {
      // Check for UC table ID
      assertThat(tableProperties).contains(UC_TABLE_ID_KEY);
      // Check for catalogManaged feature
      assertThat(tableProperties)
          .contains(String.format("%s=%s", DELTA_CATALOG_MANAGED_KEY, SUPPORTED));
    } else {
      // Check for UC table ID
      assertThat(tableProperties).doesNotContain(UC_TABLE_ID_KEY);
      // Check for catalogManaged feature
      assertThat(tableProperties).doesNotContain(DELTA_CATALOG_MANAGED_KEY);
    }
  }

  @Test
  public void testCreateManagedTableErrors() {
    String tableName = "test_delta_errors";
    UnityCatalogInfo uc = unityCatalogInfo();
    String fullTableName = uc.catalogName() + "." + uc.schemaName() + "." + tableName;

    // Test 1: Non-Delta managed tables are not supported
    assertThatThrownBy(
            () ->
                sql(
                    "CREATE TABLE %s(name STRING) USING parquet %s",
                    fullTableName, MANAGED_TBLPROPERTIES_CLAUSE))
        .hasMessageContaining("not support non-Delta managed table");

    // Test 2: Invalid property value 'disabled' for catalogManaged feature
    assertThatThrownBy(
            () ->
                sql(
                    "CREATE TABLE %s(name STRING) USING delta TBLPROPERTIES ('%s' = 'disabled')",
                    fullTableName, DELTA_CATALOG_MANAGED_KEY))
        .hasMessageContaining(
            String.format("Invalid property value 'disabled' for '%s'", DELTA_CATALOG_MANAGED_KEY));

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
                DELTA_CATALOG_MANAGED_KEY, SUPPORTED));
  }
}
