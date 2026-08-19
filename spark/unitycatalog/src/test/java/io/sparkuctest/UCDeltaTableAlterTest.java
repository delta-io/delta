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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.unitycatalog.client.delta.api.DeltaTablesApi;
import io.unitycatalog.client.delta.model.DeltaLoadTableResponse;
import io.unitycatalog.client.delta.model.DeltaStructField;
import io.unitycatalog.client.delta.model.DeltaStructType;
import io.unitycatalog.client.model.TableInfo;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Test;

/** ALTER TABLE test suite for Delta Table operations through Unity Catalog. */
public class UCDeltaTableAlterTest extends UCDeltaTableIntegrationBaseTest {

  private static final String COLUMN_MAPPING_PROPERTIES = "'delta.columnMapping.mode' = 'name'";
  private static final String CHAR_VARCHAR_TYPE_METADATA_KEY = "__CHAR_VARCHAR_TYPE_STRING";

  @Test
  public void testAlterTableCustomPropertiesUpdateUcDeltaMetadata() throws Exception {
    withNewTable(
        "alter_custom_props_test",
        "id INT, name STRING",
        TableType.MANAGED,
        tableName -> {
          sql("ALTER TABLE %s SET TBLPROPERTIES ('custom.key' = 'custom.value')", tableName);

          DeltaLoadTableResponse response = loadTableViaDeltaRest(tableName);
          assertEquals("custom.value", response.getMetadata().getProperties().get("custom.key"));

          sql("ALTER TABLE %s UNSET TBLPROPERTIES ('custom.key')", tableName);

          response = loadTableViaDeltaRest(tableName);
          assertFalse(response.getMetadata().getProperties().containsKey("custom.key"));
        });
  }

  @Test
  public void testAlterTableDeltaPropertiesUpdateUcDeltaMetadata() throws Exception {
    withNewTable(
        "alter_delta_props_test",
        "id INT, name STRING",
        TableType.MANAGED,
        tableName -> {
          sql(
              "ALTER TABLE %s SET TBLPROPERTIES ("
                  + "'delta.autoOptimize.optimizeWrite' = 'true', "
                  + "'delta.autoOptimize.autoCompact' = 'true')",
              tableName);

          assertEquals("true", tableProperty(tableName, "delta.autoOptimize.optimizeWrite"));
          assertEquals("true", tableProperty(tableName, "delta.autoOptimize.autoCompact"));

          DeltaLoadTableResponse response = loadTableViaDeltaRest(tableName);
          Map<String, String> properties = response.getMetadata().getProperties();
          assertEquals("true", properties.get("delta.autoOptimize.optimizeWrite"));
          assertEquals("true", properties.get("delta.autoOptimize.autoCompact"));

          sql(
              "ALTER TABLE %s UNSET TBLPROPERTIES ("
                  + "'delta.autoOptimize.optimizeWrite', "
                  + "'delta.autoOptimize.autoCompact')",
              tableName);

          assertNull(tableProperty(tableName, "delta.autoOptimize.optimizeWrite"));
          assertNull(tableProperty(tableName, "delta.autoOptimize.autoCompact"));

          response = loadTableViaDeltaRest(tableName);
          properties = response.getMetadata().getProperties();
          assertFalse(properties.containsKey("delta.autoOptimize.optimizeWrite"));
          assertFalse(properties.containsKey("delta.autoOptimize.autoCompact"));
        });
  }

  // Lock a table down via ALTER SET so further row-removing mutations (DELETE / UPDATE) are
  // rejected, while INSERT (append) keeps working. The typical flow is "the table is finalized,
  // lock it"; nobody sets appendOnly at CREATE-time. Verifies that the prop sticks (in UC
  // metadata + SHOW TBLPROPERTIES) and that Delta enforces it on both a DELETE and an UPDATE,
  // since both rewrite rows the contract forbids.
  @Test
  public void testAlterTableEnableAppendOnlyLocksOutMutations() throws Exception {
    withNewTable(
        "alter_enable_append_only_test",
        "id INT, name STRING",
        TableType.MANAGED,
        tableName -> {
          sql("INSERT INTO %s VALUES (1, 'a')", tableName);

          sql("ALTER TABLE %s SET TBLPROPERTIES ('delta.appendOnly' = 'true')", tableName);

          assertEquals("true", tableProperty(tableName, "delta.appendOnly"));
          assertEquals(
              "true",
              loadTableViaDeltaRest(tableName)
                  .getMetadata()
                  .getProperties()
                  .get("delta.appendOnly"));

          // INSERT still allowed -- appendOnly only blocks operations that remove or rewrite
          // existing rows.
          sql("INSERT INTO %s VALUES (2, 'b')", tableName);
          check(
              sql("SELECT id, name FROM %s ORDER BY id", tableName),
              List.of(row("1", "a"), row("2", "b")));

          List<String> appendOnlyErrors =
              List.of("appendOnly", "append-only", "DELTA_CANNOT_MODIFY_APPEND_ONLY");
          assertThrowsWithCauseContainingAny(
              appendOnlyErrors, () -> sql("DELETE FROM %s WHERE id = 1", tableName));
          assertThrowsWithCauseContainingAny(
              appendOnlyErrors, () -> sql("UPDATE %s SET name = 'x' WHERE id = 1", tableName));
        });
  }

  // VACUUM retention tuning: operators adjust these two durations to balance storage costs
  // against the time-travel / history-debugging window. Both are string-encoded intervals that
  // should round-trip through UC's metadata unchanged. The SET path is the entire contract
  // here -- Delta's enforcement at VACUUM time is covered by Delta's own suite.
  @Test
  public void testAlterTableSetVacuumRetentionDurationsUpdatesUcDeltaMetadata() throws Exception {
    withNewTable(
        "alter_set_retention_durations_test",
        "id INT, name STRING",
        TableType.MANAGED,
        tableName -> {
          sql(
              "ALTER TABLE %s SET TBLPROPERTIES ("
                  + "'delta.logRetentionDuration' = 'interval 14 days', "
                  + "'delta.deletedFileRetentionDuration' = 'interval 2 days')",
              tableName);

          assertEquals("interval 14 days", tableProperty(tableName, "delta.logRetentionDuration"));
          assertEquals(
              "interval 2 days", tableProperty(tableName, "delta.deletedFileRetentionDuration"));

          Map<String, String> properties =
              loadTableViaDeltaRest(tableName).getMetadata().getProperties();
          assertEquals("interval 14 days", properties.get("delta.logRetentionDuration"));
          assertEquals("interval 2 days", properties.get("delta.deletedFileRetentionDuration"));

          sql(
              "ALTER TABLE %s UNSET TBLPROPERTIES ("
                  + "'delta.logRetentionDuration', 'delta.deletedFileRetentionDuration')",
              tableName);

          assertNull(tableProperty(tableName, "delta.logRetentionDuration"));
          assertNull(tableProperty(tableName, "delta.deletedFileRetentionDuration"));

          properties = loadTableViaDeltaRest(tableName).getMetadata().getProperties();
          assertFalse(properties.containsKey("delta.logRetentionDuration"));
          assertFalse(properties.containsKey("delta.deletedFileRetentionDuration"));
        });
  }

  // Pin current behaviour of ALTER TABLE SET ('delta.enableTypeWidening' = 'true') on a managed
  // CCv2 Delta table: the SET succeeds, the prop appears in UC metadata, the writer feature
  // lands in the protocol, and a subsequent ALTER COLUMN ... TYPE widens for real with the new
  // value round-tripping. If a future change either no-ops this SET or rejects it, every
  // assertion below flips and forces a review.
  @Test
  public void testAlterTableEnableTypeWideningOnManagedCcv2PinsCurrentBehavior() throws Exception {
    withNewTable(
        "alter_enable_type_widening_test",
        "id INT, name STRING",
        TableType.MANAGED,
        tableName -> {
          sql("ALTER TABLE %s SET TBLPROPERTIES ('delta.enableTypeWidening' = 'true')", tableName);

          assertEquals("true", tableProperty(tableName, "delta.enableTypeWidening"));
          Map<String, String> properties =
              loadTableViaDeltaRest(tableName).getMetadata().getProperties();
          assertEquals("true", properties.get("delta.enableTypeWidening"));
          assertEquals("supported", properties.get("delta.feature.typeWidening"));

          // Confirm the enablement is not a no-op: a subsequent widening ALTER actually works
          // and a value that overflows INT round-trips. 9223372036854775807 is Long.MAX_VALUE;
          // it can only be stored once the column has been widened to BIGINT, so a successful
          // INSERT + SELECT here is direct evidence that the widening took effect.
          sql("INSERT INTO %s VALUES (1, 'a')", tableName);
          sql("ALTER TABLE %s ALTER COLUMN id TYPE BIGINT", tableName);
          sql("INSERT INTO %s VALUES (9223372036854775807, 'b')", tableName);
          check(
              sql("SELECT id, name FROM %s ORDER BY id", tableName),
              List.of(row("1", "a"), row("9223372036854775807", "b")));
        });
  }

  @Test
  public void testAlterTableProtocolDerivedPropertiesNoOpButCommit() throws Exception {
    withNewTable(
        "alter_protocol_props_test",
        "id INT, name STRING",
        TableType.MANAGED,
        tableName -> {
          assertEquals("supported", tableProperty(tableName, "delta.feature.catalogManaged"));
          long versionBeforeUnset = currentVersion(tableName);

          sql("ALTER TABLE %s UNSET TBLPROPERTIES ('delta.feature.catalogManaged')", tableName);

          assertEquals(versionBeforeUnset + 1, currentVersion(tableName));
          assertLatestHistoryOperation(tableName, versionBeforeUnset + 1, "UNSET TBLPROPERTIES");
          assertEquals("supported", tableProperty(tableName, "delta.feature.catalogManaged"));

          assertEquals("3", tableProperty(tableName, "delta.minReaderVersion"));
          long versionBeforeSet = currentVersion(tableName);

          sql("ALTER TABLE %s SET TBLPROPERTIES ('delta.minReaderVersion' = '2')", tableName);

          assertEquals(versionBeforeSet + 1, currentVersion(tableName));
          assertLatestHistoryOperation(tableName, versionBeforeSet + 1, "SET TBLPROPERTIES");
          assertEquals("3", tableProperty(tableName, "delta.minReaderVersion"));
        });
  }

  @Test
  public void testAlterTableAddColumnsWithDifferentTypesUpdatesUcDeltaMetadata() throws Exception {
    withNewTable(
        "alter_add_columns_types_test",
        "id INT, name STRING",
        TableType.MANAGED,
        tableName -> {
          sql(
              "ALTER TABLE %s ADD COLUMNS ("
                  + "price DECIMAL(10, 2), "
                  + "active BOOLEAN, "
                  + "created_at TIMESTAMP)",
              tableName);

          DeltaLoadTableResponse response = loadTableViaDeltaRest(tableName);
          assertEquals(
              List.of("id", "name", "price", "active", "created_at"),
              fieldNames(response.getMetadata().getColumns()));
        });
  }

  @Test
  public void testAlterTableColumnCommentUpdatesUcDeltaMetadata() throws Exception {
    withNewTable(
        "alter_column_comment_test",
        "id INT, name STRING",
        TableType.MANAGED,
        tableName -> {
          sql("ALTER TABLE %s CHANGE COLUMN name COMMENT 'display name'", tableName);

          DeltaStructField name =
              field(loadTableViaDeltaRest(tableName).getMetadata().getColumns(), "name");
          assertEquals("display name", name.getMetadata().get("comment"));

          sql("ALTER TABLE %s CHANGE COLUMN name COMMENT ''", tableName);

          name = field(loadTableViaDeltaRest(tableName).getMetadata().getColumns(), "name");
          assertEquals("", name.getMetadata().get("comment"));
        });
  }

  @Test
  public void testCommentOnTableUpdatesUcMetadata() throws Exception {
    withNewTable(
        "alter_table_comment_test",
        "id INT, name STRING",
        TableType.MANAGED,
        tableName -> {
          sql("COMMENT ON TABLE %s IS 'table comment'", tableName);

          TableInfo tableInfo = loadTableInfoViaUc(tableName);
          assertEquals("table comment", tableInfo.getComment());

          sql("COMMENT ON TABLE %s IS NULL", tableName);

          tableInfo = loadTableInfoViaUc(tableName);
          assertEquals("", tableInfo.getComment());
        });
  }

  @Test
  public void testAlterTableFeatureBackedPropertiesUpdateUcDeltaMetadata() throws Exception {
    withNewTable(
        "alter_feature_backed_props_test",
        "id INT, name STRING",
        TableType.MANAGED,
        tableName -> {
          sql(
              "ALTER TABLE %s SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')",
              tableName);

          DeltaLoadTableResponse response = loadTableViaDeltaRest(tableName);
          assertEquals(
              "true", response.getMetadata().getProperties().get("delta.enableChangeDataFeed"));
        });
  }

  @Test
  public void testAlterTableClusterKeysUpdateUcDeltaMetadata() throws Exception {
    withNewTable(
        "alter_cluster_keys_test",
        "id INT, name STRING",
        TableType.MANAGED,
        tableName -> {
          sql("ALTER TABLE %s CLUSTER BY (id)", tableName);

          DeltaLoadTableResponse response = loadTableViaDeltaRest(tableName);
          Map<String, String> properties = response.getMetadata().getProperties();
          assertEquals("supported", properties.get("delta.feature.clustering"));
          assertEquals("[[\"id\"]]", tableProperty(tableName, "clusteringColumns"));
        });
  }

  @Test
  public void testAlterTableModifyColumnTypeUpdatesUcDeltaMetadata() throws Exception {
    withNewTable(
        "alter_modify_column_type_test",
        "id INT, code VARCHAR(5)",
        TableType.MANAGED,
        tableName -> {
          DeltaStructField codeBefore =
              field(loadTableViaDeltaRest(tableName).getMetadata().getColumns(), "code");
          assertEquals("varchar(5)", codeBefore.getMetadata().get(CHAR_VARCHAR_TYPE_METADATA_KEY));

          sql("ALTER TABLE %s CHANGE COLUMN code TYPE STRING", tableName);

          DeltaStructField codeAfter =
              field(loadTableViaDeltaRest(tableName).getMetadata().getColumns(), "code");
          assertEquals("string", codeAfter.getType().getType());
          assertFalse(codeAfter.getMetadata().containsKey(CHAR_VARCHAR_TYPE_METADATA_KEY));
        });
  }

  @Test
  public void testAlterTableRenameColumnIsRejectedForUcManagedTable() throws Exception {
    // UCSingleCatalog rejects ALTER TABLE RENAME COLUMN ahead of any Delta routing. Pin
    // this contract so we notice if UC ever re-enables it.
    withNewTable(
        "alter_rename_column_test",
        "id INT, old_name STRING",
        null,
        TableType.MANAGED,
        COLUMN_MAPPING_PROPERTIES,
        tableName -> {
          sql("INSERT INTO %s VALUES (1, 'before_rename')", tableName);
          UnsupportedOperationException ex =
              assertThrows(
                  UnsupportedOperationException.class,
                  () -> sql("ALTER TABLE %s RENAME COLUMN old_name TO new_name", tableName));
          assertTrue(
              ex.getMessage().contains("RENAME COLUMN is not supported for Unity Catalog"),
              "Unexpected error message: " + ex.getMessage());
        });
  }

  @Test
  public void testAlterTableDropColumnUpdatesUcDeltaMetadata() throws Exception {
    withNewTable(
        "alter_drop_column_test",
        "id INT, name STRING, extra STRING",
        null,
        TableType.MANAGED,
        COLUMN_MAPPING_PROPERTIES,
        tableName -> {
          sql("INSERT INTO %s VALUES (1, 'kept', 'dropped')", tableName);
          sql("ALTER TABLE %s DROP COLUMN (extra)", tableName);

          DeltaLoadTableResponse response = loadTableViaDeltaRest(tableName);
          assertEquals(List.of("id", "name"), fieldNames(response.getMetadata().getColumns()));
          check(sql("SELECT id, name FROM %s ORDER BY id", tableName), List.of(row("1", "kept")));
        });
  }

  @Test
  public void testAlterTableNestedColumnUpdatesUcDeltaMetadata() throws Exception {
    // RENAME COLUMN is rejected upstream by UCSingleCatalog (see
    // testAlterTableRenameColumnIsRejectedForUcManagedTable); this test covers nested-column
    // ADD COLUMNS, the other nested-schema mutation that still propagates to UC.
    withNewTable(
        "alter_nested_column_test",
        "id INT, info STRUCT<first: STRING, last: STRING>",
        null,
        TableType.MANAGED,
        COLUMN_MAPPING_PROPERTIES,
        tableName -> {
          sql("ALTER TABLE %s ADD COLUMNS (info.age INT AFTER first)", tableName);

          DeltaLoadTableResponse response = loadTableViaDeltaRest(tableName);
          DeltaStructType info = structField(response.getMetadata().getColumns(), "info");
          assertEquals(List.of("first", "age", "last"), fieldNames(info));
        });
  }

  @Test
  public void testAlterTableChangeColumnCommentAndPositionUpdatesUcDeltaMetadata()
      throws Exception {
    withNewTable(
        "alter_change_column_test",
        "id INT, name STRING, note STRING",
        TableType.MANAGED,
        tableName -> {
          sql(
              "ALTER TABLE %s CHANGE COLUMN name name STRING COMMENT 'display name' AFTER note",
              tableName);

          DeltaLoadTableResponse response = loadTableViaDeltaRest(tableName);
          DeltaStructType columns = response.getMetadata().getColumns();
          assertEquals(List.of("id", "note", "name"), fieldNames(columns));
          assertEquals("display name", field(columns, "name").getMetadata().get("comment"));
        });
  }

  @Test
  public void testAlterTableConstraintUpdatesUcDeltaMetadataAndEnforcement() throws Exception {
    withNewTable(
        "alter_constraint_test",
        "id INT, name STRING",
        TableType.MANAGED,
        tableName -> {
          sql("INSERT INTO %s VALUES (1, 'valid')", tableName);
          sql("ALTER TABLE %s ADD CONSTRAINT positive_id CHECK (id > 0)", tableName);

          DeltaLoadTableResponse withConstraint = loadTableViaDeltaRest(tableName);
          assertEquals(
              "id > 0",
              withConstraint.getMetadata().getProperties().get("delta.constraints.positive_id"));
          assertThrowsWithCauseContaining(
              "CHECK constraint", () -> sql("INSERT INTO %s VALUES (-1, 'invalid')", tableName));

          sql("ALTER TABLE %s DROP CONSTRAINT positive_id", tableName);

          DeltaLoadTableResponse withoutConstraint = loadTableViaDeltaRest(tableName);
          assertFalse(
              withoutConstraint
                  .getMetadata()
                  .getProperties()
                  .containsKey("delta.constraints.positive_id"));
          sql("INSERT INTO %s VALUES (-1, 'allowed_after_drop')", tableName);
          check(
              sql("SELECT id, name FROM %s ORDER BY id", tableName),
              List.of(row("-1", "allowed_after_drop"), row("1", "valid")));
        });
  }

  @Test
  public void testAlterTableSetLocationRemainsUnsupportedForUcCatalog() throws Exception {
    withTempDir(
        dir -> {
          String tableName = fullTableName("alter_set_location_test");
          String seedTableName = fullTableName("alter_set_location_seed_test");
          Path oldLocation = new Path(dir, "old_location");
          Path newLocation = new Path(dir, "new_location");
          try {
            sql("DROP TABLE IF EXISTS %s", tableName);
            sql("DROP TABLE IF EXISTS %s", seedTableName);
            sql(
                "CREATE TABLE %s (id BIGINT) USING DELTA LOCATION '%s'",
                tableName, oldLocation.toString());
            sql("INSERT INTO %s VALUES (1)", tableName);
            sql(
                "CREATE TABLE %s (id BIGINT) USING DELTA LOCATION '%s'",
                seedTableName, newLocation.toString());
            sql("INSERT INTO %s VALUES (10)", seedTableName);
            sql("DROP TABLE %s", seedTableName);

            assertThrowsWithCauseContainingAny(
                List.of("TABLE_OR_VIEW_NOT_FOUND", "cannot be found", "not supported"),
                () -> sql("ALTER TABLE %s SET LOCATION '%s'", tableName, newLocation.toString()));
            check(sql("SELECT id FROM %s ORDER BY id", tableName), List.of(row("1")));
          } finally {
            sql("DROP TABLE IF EXISTS %s", tableName);
            sql("DROP TABLE IF EXISTS %s", seedTableName);
          }
        });
  }

  @Test
  public void testAlterTablePartitionManagementRemainsUnsupported() throws Exception {
    withNewTable(
        "alter_partition_management_test",
        "id INT, part STRING",
        "part",
        TableType.MANAGED,
        tableName -> {
          assertThrowsWithCauseContaining(
              "does not support partition management",
              () -> sql("ALTER TABLE %s ADD PARTITION (part='a')", tableName));
          assertThrowsWithCauseContaining(
              "does not support partition management",
              () -> sql("ALTER TABLE %s DROP PARTITION (part='a')", tableName));
          assertThrowsWithCauseContainingAny(
              List.of("partition management", "not supported", "Syntax error"),
              () ->
                  sql(
                      "ALTER TABLE %s PARTITION (part='a') RENAME TO PARTITION (part='b')",
                      tableName));
          assertThrowsWithCauseContainingAny(
              List.of("recover partitions is not supported", "not supported for v2 tables"),
              () -> sql("ALTER TABLE %s RECOVER PARTITIONS", tableName));
        });
  }

  @Test
  public void testAlterTableCatalogManagedUpgradeAndDowngradeRemainUnsupported() throws Exception {
    withNewTable(
        "alter_upgrade_catalog_managed_test",
        "id INT, name STRING",
        TableType.EXTERNAL,
        tableName ->
            assertThrowsWithCauseContaining(
                "Upgrading to CatalogOwned table is not yet supported",
                () ->
                    sql(
                        "ALTER TABLE %s SET TBLPROPERTIES "
                            + "('delta.feature.catalogManaged' = 'supported')",
                        tableName)));

    withNewTable(
        "alter_downgrade_catalog_managed_test",
        "id INT, name STRING",
        TableType.MANAGED,
        tableName ->
            assertThrowsWithCauseContainingAny(
                List.of("Cannot drop catalogManaged", "Dropping Delta table feature"),
                () -> sql("ALTER TABLE %s DROP FEATURE 'catalogManaged'", tableName)));
  }

  @Test
  public void testAlterTableSetSerdeRemainsUnsupported() throws Exception {
    withNewTable(
        "alter_set_serde_test",
        "id INT, name STRING",
        TableType.MANAGED,
        tableName ->
            assertThrowsWithCauseContainingAny(
                List.of("set [serde|serdeproperties] is not supported", "not supported"),
                () ->
                    sql(
                        "ALTER TABLE %s SET SERDEPROPERTIES ('serde.key' = 'serde.value')",
                        tableName)));
  }

  @Test
  public void testAlterTableRenameTableRemainsUnsupported() throws Exception {
    String oldTableName = fullTableName("alter_rename_table_old_test");
    String newTableName = fullTableName("alter_rename_table_new_test");
    try {
      sql("DROP TABLE IF EXISTS %s", newTableName);
      sql(
          "CREATE TABLE %s (id INT, name STRING) USING DELTA "
              + "TBLPROPERTIES ('delta.feature.catalogManaged'='supported')",
          oldTableName);

      assertThrowsWithCauseContainingAny(
          List.of("Renaming a table is not supported yet", "not supported"),
          () -> sql("ALTER TABLE %s RENAME TO %s", oldTableName, newTableName));
    } finally {
      sql("DROP TABLE IF EXISTS %s", oldTableName);
      sql("DROP TABLE IF EXISTS %s", newTableName);
    }
  }

  @Test
  public void testAlterTableSetOwnerRemainsUnsupported() throws Exception {
    withNewTable(
        "alter_set_owner_test",
        "id INT, name STRING",
        TableType.MANAGED,
        tableName ->
            assertThrowsWithCauseContainingAny(
                List.of("SET OWNER", "owner", "not supported"),
                () -> sql("ALTER TABLE %s SET OWNER TO test_owner", tableName)));
  }

  @Test
  public void testAlterTableTagsRemainUnsupported() throws Exception {
    withNewTable(
        "alter_tags_test",
        "id INT, name STRING",
        TableType.MANAGED,
        tableName -> {
          assertThrowsWithCauseContainingAny(
              List.of("Syntax error", "PARSE_SYNTAX_ERROR", "TAG", "not supported"),
              () -> sql("ALTER TABLE %s SET TAGS ('purpose' = 'coverage')", tableName));
          assertThrowsWithCauseContainingAny(
              List.of("Syntax error", "PARSE_SYNTAX_ERROR", "TAG", "not supported"),
              () -> sql("ALTER TABLE %s UNSET TAGS ('purpose')", tableName));
        });
  }

  private DeltaLoadTableResponse loadTableViaDeltaRest(String tableName) throws Exception {
    String[] parts = tableName.split("\\.", 3);
    return new DeltaTablesApi(unityCatalogInfo().createApiClient())
        .loadTable(parts[0], parts[1], parts[2]);
  }

  private TableInfo loadTableInfoViaUc(String tableName) throws Exception {
    return new io.unitycatalog.client.api.TablesApi(unityCatalogInfo().createApiClient())
        .getTable(tableName, false, false);
  }

  private String tableProperty(String tableName, String key) {
    return sql("SHOW TBLPROPERTIES %s", tableName).stream()
        .filter(row -> row.size() >= 2 && key.equals(row.get(0)))
        .map(row -> row.get(1))
        .findFirst()
        .orElse(null);
  }

  private void assertLatestHistoryOperation(
      String tableName, long expectedVersion, String expectedOperation) {
    List<List<String>> history = sql("DESCRIBE HISTORY %s LIMIT 1", tableName);
    assertEquals(String.valueOf(expectedVersion), history.get(0).get(0));
    assertEquals(expectedOperation, history.get(0).get(4));
  }

  private static List<String> fieldNames(DeltaStructType structType) {
    return structType.getFields().stream()
        .map(DeltaStructField::getName)
        .collect(Collectors.toList());
  }

  private static DeltaStructField field(DeltaStructType structType, String name) {
    return structType.getFields().stream()
        .filter(f -> name.equals(f.getName()))
        .findFirst()
        .orElseThrow(() -> new AssertionError("Missing field: " + name));
  }

  private static DeltaStructType structField(DeltaStructType structType, String name) {
    return (DeltaStructType) field(structType, name).getType();
  }
}
