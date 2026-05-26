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

import io.unitycatalog.client.delta.api.TablesApi;
import io.unitycatalog.client.delta.model.LoadTableResponse;
import io.unitycatalog.client.delta.model.StructField;
import io.unitycatalog.client.delta.model.StructType;
import io.unitycatalog.client.model.TableInfo;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Test;

/** ALTER TABLE test suite for Delta Table operations through Unity Catalog. */
public class UCDeltaTableAlterTest extends UCDeltaTableIntegrationBaseTest {

  private static final String COLUMN_MAPPING_PROPERTIES = "'delta.columnMapping.mode' = 'name'";
  private static final String CHAR_VARCHAR_TYPE_METADATA_KEY = "__CHAR_VARCHAR_TYPE_STRING";

  @Test
  public void testAlterTableSetPropertiesUpdatesUcDeltaMetadata() throws Exception {
    withNewTable(
        "alter_set_properties_test",
        "id INT, name STRING",
        TableType.MANAGED,
        tableName -> {
          sql("ALTER TABLE %s SET TBLPROPERTIES ('custom.key' = 'custom.value')", tableName);

          LoadTableResponse response = loadTableViaDeltaRest(tableName);
          assertEquals("custom.value", response.getMetadata().getProperties().get("custom.key"));
        });
  }

  @Test
  public void testAlterTableUnsetPropertiesUpdatesUcDeltaMetadata() throws Exception {
    withNewTable(
        "alter_unset_properties_test",
        "id INT, name STRING",
        TableType.MANAGED,
        tableName -> {
          sql("ALTER TABLE %s SET TBLPROPERTIES ('custom.key' = 'custom.value')", tableName);
          sql("ALTER TABLE %s UNSET TBLPROPERTIES ('custom.key')", tableName);

          LoadTableResponse response = loadTableViaDeltaRest(tableName);
          assertFalse(response.getMetadata().getProperties().containsKey("custom.key"));
        });
  }

  @Test
  public void testAlterTableAddColumnsUpdatesUcDeltaMetadata() throws Exception {
    withNewTable(
        "alter_add_columns_test",
        "id INT, name STRING",
        TableType.MANAGED,
        tableName -> {
          sql("ALTER TABLE %s ADD COLUMNS (extra STRING)", tableName);

          LoadTableResponse response = loadTableViaDeltaRest(tableName);
          assertEquals(3, response.getMetadata().getColumns().getFields().size());
          assertEquals("extra", response.getMetadata().getColumns().getFields().get(2).getName());
        });
  }

  @Test
  public void testAlterTableModifyColumnTypeUpdatesUcDeltaMetadata() throws Exception {
    withNewTable(
        "alter_modify_column_type_test",
        "id INT, code VARCHAR(5)",
        TableType.MANAGED,
        tableName -> {
          StructField codeBefore =
              field(loadTableViaDeltaRest(tableName).getMetadata().getColumns(), "code");
          assertEquals("varchar(5)", codeBefore.getMetadata().get(CHAR_VARCHAR_TYPE_METADATA_KEY));

          sql("ALTER TABLE %s CHANGE COLUMN code TYPE STRING", tableName);

          StructField codeAfter =
              field(loadTableViaDeltaRest(tableName).getMetadata().getColumns(), "code");
          assertEquals("string", codeAfter.getType().getType());
          assertFalse(codeAfter.getMetadata().containsKey(CHAR_VARCHAR_TYPE_METADATA_KEY));
        });
  }

  @Test
  public void testAlterTableRenameColumnUpdatesUcDeltaMetadata() throws Exception {
    withNewTable(
        "alter_rename_column_test",
        "id INT, old_name STRING",
        null,
        TableType.MANAGED,
        COLUMN_MAPPING_PROPERTIES,
        tableName -> {
          sql("INSERT INTO %s VALUES (1, 'before_rename')", tableName);
          sql("ALTER TABLE %s RENAME COLUMN old_name TO new_name", tableName);

          LoadTableResponse response = loadTableViaDeltaRest(tableName);
          assertEquals(List.of("id", "new_name"), fieldNames(response.getMetadata().getColumns()));
          check(
              sql("SELECT id, new_name FROM %s ORDER BY id", tableName),
              List.of(row("1", "before_rename")));
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

          LoadTableResponse response = loadTableViaDeltaRest(tableName);
          assertEquals(List.of("id", "name"), fieldNames(response.getMetadata().getColumns()));
          check(sql("SELECT id, name FROM %s ORDER BY id", tableName), List.of(row("1", "kept")));
        });
  }

  @Test
  public void testAlterTableNestedColumnUpdatesUcDeltaMetadata() throws Exception {
    withNewTable(
        "alter_nested_column_test",
        "id INT, info STRUCT<first: STRING, last: STRING>",
        null,
        TableType.MANAGED,
        COLUMN_MAPPING_PROPERTIES,
        tableName -> {
          sql("ALTER TABLE %s RENAME COLUMN info.first TO given", tableName);
          sql("ALTER TABLE %s ADD COLUMNS (info.age INT AFTER given)", tableName);

          LoadTableResponse response = loadTableViaDeltaRest(tableName);
          StructType info = structField(response.getMetadata().getColumns(), "info");
          assertEquals(List.of("given", "age", "last"), fieldNames(info));
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

          LoadTableResponse response = loadTableViaDeltaRest(tableName);
          StructType columns = response.getMetadata().getColumns();
          assertEquals(List.of("id", "note", "name"), fieldNames(columns));
          assertEquals("display name", field(columns, "name").getMetadata().get("comment"));
        });
  }

  @Test
  public void testAlterTableColumnCommentCanBeSetAndClearedInUcDeltaMetadata() throws Exception {
    withNewTable(
        "alter_column_comment_test",
        "id INT, name STRING",
        TableType.MANAGED,
        tableName -> {
          sql("ALTER TABLE %s CHANGE COLUMN name COMMENT 'display name'", tableName);

          StructField name =
              field(loadTableViaDeltaRest(tableName).getMetadata().getColumns(), "name");
          assertEquals("display name", name.getMetadata().get("comment"));

          sql("ALTER TABLE %s CHANGE COLUMN name COMMENT ''", tableName);

          name = field(loadTableViaDeltaRest(tableName).getMetadata().getColumns(), "name");
          assertEquals("", name.getMetadata().get("comment"));
        });
  }

  @Test
  public void testAlterTableFeaturePropertyUpdatesUcDeltaMetadata() throws Exception {
    withNewTable(
        "alter_feature_property_test",
        "id INT, name STRING",
        TableType.MANAGED,
        tableName -> {
          sql(
              "ALTER TABLE %s SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')",
              tableName);

          LoadTableResponse response = loadTableViaDeltaRest(tableName);
          assertEquals(
              "true", response.getMetadata().getProperties().get("delta.enableChangeDataFeed"));
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

          LoadTableResponse withConstraint = loadTableViaDeltaRest(tableName);
          assertEquals(
              "id > 0",
              withConstraint.getMetadata().getProperties().get("delta.constraints.positive_id"));
          assertThrowsWithCauseContaining(
              "CHECK constraint", () -> sql("INSERT INTO %s VALUES (-1, 'invalid')", tableName));

          sql("ALTER TABLE %s DROP CONSTRAINT positive_id", tableName);

          LoadTableResponse withoutConstraint = loadTableViaDeltaRest(tableName);
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
  public void testCommentOnTableUpdatesUcTableComment() throws Exception {
    withNewTable(
        "alter_table_comment_test",
        "id INT, name STRING",
        TableType.MANAGED,
        tableName -> {
          sql("COMMENT ON TABLE %s IS 'table comment'", tableName);

          TableInfo tableInfo = loadTableInfoViaUc(tableName);
          assertEquals("table comment", tableInfo.getComment());
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
  public void testAlterTableClusterByUpdatesUcDeltaMetadata() throws Exception {
    withNewTable(
        "alter_cluster_by_test",
        "id INT, name STRING",
        TableType.MANAGED,
        tableName -> {
          sql("ALTER TABLE %s CLUSTER BY (id)", tableName);

          LoadTableResponse response = loadTableViaDeltaRest(tableName);
          assertEquals(
              "supported", response.getMetadata().getProperties().get("delta.feature.clustering"));
          assertEquals(
              "[[\"id\"]]", response.getMetadata().getProperties().get("clusteringColumns"));
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

  private LoadTableResponse loadTableViaDeltaRest(String tableName) throws Exception {
    String[] parts = tableName.split("\\.", 3);
    return new TablesApi(unityCatalogInfo().createApiClient())
        .loadTable(parts[0], parts[1], parts[2]);
  }

  private TableInfo loadTableInfoViaUc(String tableName) throws Exception {
    return new io.unitycatalog.client.api.TablesApi(unityCatalogInfo().createApiClient())
        .getTable(tableName, false, false);
  }

  private static List<String> fieldNames(StructType structType) {
    return structType.getFields().stream().map(StructField::getName).collect(Collectors.toList());
  }

  private static StructField field(StructType structType, String name) {
    return structType.getFields().stream()
        .filter(f -> name.equals(f.getName()))
        .findFirst()
        .orElseThrow(() -> new AssertionError("Missing field: " + name));
  }

  private static StructType structField(StructType structType, String name) {
    return (StructType) field(structType, name).getType();
  }
}
