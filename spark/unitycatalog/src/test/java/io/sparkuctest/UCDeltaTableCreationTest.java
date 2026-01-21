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
import io.unitycatalog.client.ApiException;
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
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;

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

  @TestFactory
  public Stream<DynamicTest> testCreateTable() {
    int counter = 0;
    List<DynamicTest> tests = new ArrayList<>();
    for (TableType tableType : TableType.values()) {
      for (boolean withPartition : List.of(true, false)) {
        for (boolean withCluster : List.of(true, false)) {
          if (withCluster && withPartition) {
            // Can not have CLUSTER BY and PARTITIONED BY on the same table
            continue;
          }
          for (boolean withAsSelect : List.of(true, false)) {
            for (boolean replaceTable : List.of(true, false)) {
              String displayName =
                  String.format(
                      "tableType=%s, withPartition=%s, withCluster=%s, withAsSelect=%s, replaceTable=%s",
                      tableType, withPartition, withCluster, withAsSelect, replaceTable);
              counter++;
              int finalCounter = counter;
              tests.add(
                  DynamicTest.dynamicTest(
                      displayName,
                      () ->
                          runTableCreationTest(
                              finalCounter,
                              tableType,
                              withPartition,
                              withCluster,
                              withAsSelect,
                              replaceTable)));
            }
          }
        }
      }
    }
    return tests.stream();
  }

  private void runTableCreationTest(
      int count,
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
    String tableName = "test_delta_table_" + count;

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

    // Verify that table information maintained at the uc server side are expected.
    assertUCTableInfo(
        tableType,
        fullTableName,
        List.of("i", "s"),
        Map.of("Foo", "Bar"),
        comment,
        options.getExternalTableLocation());
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

  @TestAllTableTypes
  public void testCreateOrReplaceTable(TableType tableType) throws Exception {
    UnityCatalogInfo uc = unityCatalogInfo();
    String tableName = String.format("%s.%s.create_or_replace", uc.catalogName(), uc.schemaName());
    withTempDir(
        (Path dir) -> {
          try {
            // CREATE OR REPLACE with new schema
            if (tableType == TableType.MANAGED) {
              sql(
                  "CREATE OR REPLACE TABLE %s (id INT, name STRING) USING DELTA %s ",
                  tableName, MANAGED_TBLPROPERTIES_CLAUSE);
            } else {
              sql(
                  "CREATE OR REPLACE TABLE %s (id INT, name STRING) USING DELTA LOCATION '%s'",
                  tableName, dir.toString());
            }

            // Assert the unity catalog table information.
            assertUCTableInfo(tableType, tableName, List.of("id", "name"), Map.of(), null, null);

            // Insert data to verify new schema
            sql("INSERT INTO %s VALUES (1, 'Alice')", tableName);
            check(tableName, List.of(List.of("1", "Alice")));
          } finally {
            sql("DROP TABLE IF EXISTS %s", tableName);
          }
        });
  }

  @TestAllTableTypes
  public void testTableWithSupportedDataTypes(TableType tableType) throws Exception {
    String schema =
        // Numeric types
        "col_tinyint TINYINT, col_smallint SMALLINT, col_int INT, col_bigint BIGINT, "
            + "col_float FLOAT, col_double DOUBLE, col_decimal DECIMAL(10,2), "
            // String and binary types
            + "col_string STRING, col_char CHAR(10), col_varchar VARCHAR(20), col_binary BINARY, "
            // Boolean type
            + "col_boolean BOOLEAN, "
            // Date and time types
            + "col_date DATE, col_timestamp TIMESTAMP, col_timestamp_ntz TIMESTAMP_NTZ";

    withNewTable(
        "supported_types_table",
        schema,
        tableType,
        tableName -> {
          // Insert sample data
          sql(
              "INSERT INTO %s VALUES ("
                  // Numeric values
                  + "CAST(1 AS TINYINT), CAST(100 AS SMALLINT), 1000, 100000, "
                  + "2.5, 1.5, 123.45, "
                  // String and binary values
                  + "'test', 'char_test', 'varchar_test', X'CAFEBABE', "
                  // Boolean value
                  + "true, "
                  // Date and time values
                  + "DATE'2025-01-01', TIMESTAMP'2025-01-01 12:00:00', "
                  + "TIMESTAMP_NTZ'2025-01-01 12:00:00')",
              tableName);

          // Assert the unity catalog table information.
          assertUCTableInfo(
              tableType,
              tableName,
              List.of(
                  "col_tinyint",
                  "col_smallint",
                  "col_int",
                  "col_bigint",
                  "col_float",
                  "col_double",
                  "col_decimal",
                  "col_string",
                  "col_char",
                  "col_varchar",
                  "col_binary",
                  "col_boolean",
                  "col_date",
                  "col_timestamp",
                  "col_timestamp_ntz"),
              Map.of(),
              null,
              null);

          // Verify data can be queried - checking that each column type is correctly
          // stored/retrieved
          List<List<String>> results = sql("SELECT * FROM %s", tableName);
          assertThat(results).hasSize(1);
          List<String> row = results.get(0);

          // Verify each column value
          assertThat(row.get(0)).isEqualTo("1"); // TINYINT
          assertThat(row.get(1)).isEqualTo("100"); // SMALLINT
          assertThat(row.get(2)).isEqualTo("1000"); // INT
          assertThat(row.get(3)).isEqualTo("100000"); // BIGINT
          assertThat(row.get(4)).isEqualTo("2.5"); // FLOAT
          assertThat(row.get(5)).isEqualTo("1.5"); // DOUBLE
          assertThat(row.get(6)).isEqualTo("123.45"); // DECIMAL
          assertThat(row.get(7)).isEqualTo("test"); // STRING
          assertThat(row.get(8)).isEqualTo("char_test "); // CHAR (padded with space)
          assertThat(row.get(9)).isEqualTo("varchar_test"); // VARCHAR
          assertThat(row.get(10)).startsWith("[B@"); // BINARY (Java byte array object reference)
          assertThat(row.get(11)).isEqualTo("true"); // BOOLEAN
          assertThat(row.get(12)).isEqualTo("2025-01-01"); // DATE
          assertThat(row.get(13)).isEqualTo("2025-01-01 12:00:00.0"); // TIMESTAMP
          assertThat(row.get(14)).isEqualTo("2025-01-01T12:00"); // TIMESTAMP_NTZ
        });
  }

  @TestAllTableTypes
  public void testTableWithComplexTypes(TableType tableType) throws Exception {
    String schema =
        "id INT, arr ARRAY<INT>, "
            + "map_col MAP<STRING, INT>, "
            + "struct_col STRUCT<a: INT, b: STRING>";

    withNewTable(
        "complex_types_table",
        schema,
        tableType,
        tableName -> {
          // Insert sample data
          sql(
              "INSERT INTO %s VALUES (1, array(1, 2, 3), "
                  + "map('key1', 10, 'key2', 20), "
                  + "struct(42, 'test'))",
              tableName);

          // Assert the unity catalog table information.
          assertUCTableInfo(
              tableType,
              tableName,
              List.of("id", "arr", "map_col", "struct_col"),
              Map.of(),
              null,
              null);

          // Verify data can be queried
          check(
              tableName,
              List.of(
                  List.of("1", "ArraySeq(1, 2, 3)", "Map(key1 -> 10, key2 -> 20)", "[42,test]")));
        });
  }

  @TestAllTableTypes
  public void testTableWithNotNullConstraints(TableType tableType) throws Exception {
    withNewTable(
        "not_null_table",
        "id INT NOT NULL, name STRING NOT NULL, optional STRING",
        tableType,
        tableName -> {
          // Insert valid data
          sql("INSERT INTO %s VALUES (1, 'Alice', 'extra')", tableName);
          sql("INSERT INTO %s VALUES (2, 'Bob', NULL)", tableName);

          check(tableName, List.of(List.of("1", "Alice", "extra"), List.of("2", "Bob", "null")));

          // Assert the unity catalog table information.
          assertUCTableInfo(
              tableType, tableName, List.of("id", "name", "optional"), Map.of(), null, null);

          // Attempting to insert NULL into NOT NULL column should fail
          Assertions.assertThatThrownBy(
                  () -> sql("INSERT INTO %s VALUES (NULL, 'Charlie', 'data')", tableName))
              .isInstanceOf(Exception.class);
        });
  }

  private void assertUCTableInfo(
      TableType tableType,
      String fullTableName,
      List<String> expectedColumns,
      Map<String, String> customizedProps,
      String comment,
      String externalTableLocation)
      throws ApiException {
    UnityCatalogInfo uc = unityCatalogInfo();
    String catalogName = uc.catalogName();
    String schemaName = uc.schemaName();

    // Verify that properties are set on server. This can not be done by DESC EXTENDED.
    TablesApi tablesApi = new TablesApi(uc.createApiClient());
    TableInfo tableInfo = tablesApi.getTable(fullTableName, false, false);
    assertThat(tableInfo.getCatalogName()).isEqualTo(catalogName);
    assertThat(tableInfo.getName()).isEqualTo(parseTableName(fullTableName));
    assertThat(tableInfo.getSchemaName()).isEqualTo(schemaName);
    assertThat(tableInfo.getTableType().name()).isEqualTo(tableType.name());
    assertThat(tableInfo.getDataSourceFormat().name()).isEqualTo(DataSourceFormat.DELTA.name());
    assertThat(tableInfo.getComment()).isEqualTo(comment);
    if (tableType == TableType.EXTERNAL && externalTableLocation != null) {
      assertThat(tableInfo.getStorageLocation()).isEqualTo(externalTableLocation);
    }

    // At this point table schema can not be sent to server yet because it won't be
    // updated later and that would cause problem.
    List<ColumnInfo> columns = tableInfo.getColumns();
    assertThat(columns).isNotNull();
    assertThat(columns).isEmpty();

    if (tableType == TableType.MANAGED) {
      // Delta sent properties of managed tables to server
      Map<String, String> tablePropertiesFromServer = tableInfo.getProperties();

      // The server-side table properties should contain feature properties.
      assertThat(tablePropertiesFromServer)
          .containsAllEntriesOf(EXPECTED_MANAGED_TABLE_FEATURES_PROPERTIES);

      // The server-side table properties should include customized properties.
      assertThat(tablePropertiesFromServer).containsAllEntriesOf(customizedProps);

      // The server-side table properties should include built-in table properties.
      assertThat(tablePropertiesFromServer)
          .containsAllEntriesOf(
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
                  tableInfo.getTableId()));

      // Lastly the timestamp value is always changing so skip checking its value
      assertThat(tablePropertiesFromServer).containsKey("delta.lastCommitTimestamp");
    }

    // Also verify table using DESC EXTENDED
    List<List<String>> rows = sql("DESC EXTENDED %s", fullTableName);
    Map<String, String> describeResult = new HashMap<>();
    for (List<String> row : rows) {
      String key = row.get(0);
      // Skip duplicate column names that appear in partition info
      if (!expectedColumns.contains(key)) {
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

  private static String parseTableName(String fullTableName) {
    String[] splits = fullTableName.split("\\.");
    assertThat(splits.length).isEqualTo(3);
    return splits[splits.length - 1];
  }
}
