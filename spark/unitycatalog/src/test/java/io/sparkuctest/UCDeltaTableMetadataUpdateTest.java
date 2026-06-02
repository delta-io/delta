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
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.unitycatalog.client.delta.api.TablesApi;
import io.unitycatalog.client.delta.model.LoadTableResponse;
import io.unitycatalog.client.model.TableInfo;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Tests metadata-changing operations on Unity Catalog managed (CatalogOwned) tables. */
public class UCDeltaTableMetadataUpdateTest extends UCDeltaTableIntegrationBaseTest {

  private static final String MANAGED_PROPS =
      "TBLPROPERTIES ('delta.feature.catalogManaged'='supported')";
  private static final String CLUSTERING_KILL_SWITCH_ERROR =
      "Clustering column changes on Unity Catalog managed tables";
  private static final String CLUSTERING_COLUMNS = "clusteringColumns";
  private static final String ROW_ID_HIGH_WATERMARK = "delta.rowTracking.rowIdHighWaterMark";

  // ---------------------------------------------------------------------------
  // Metadata updates supported through UC updateTable
  // ---------------------------------------------------------------------------

  @Test
  public void testMetadataChangesViaWritesSucceed() throws Exception {
    withNewTable(
        "schema_evolution_insert_target",
        "id INT, name STRING",
        TableType.MANAGED,
        targetTable -> {
          withNewTable(
              "schema_evolution_insert_source",
              "id INT, name STRING, extra STRING",
              TableType.EXTERNAL,
              sourceTable -> {
                sql("INSERT INTO %s VALUES (2, 'new', 'extra_value')", sourceTable);
                sql("SET spark.databricks.delta.schema.autoMerge.enabled = true");
                try {
                  sql("INSERT INTO %s SELECT * FROM %s", targetTable, sourceTable);
                  check(
                      sql("SELECT id, name, extra FROM %s ORDER BY id", targetTable),
                      List.of(row("2", "new", "extra_value")));
                } finally {
                  sql("SET spark.databricks.delta.schema.autoMerge.enabled = false");
                }
              });
        });

    withNewTable(
        "schema_evolution_merge_target",
        "id INT, name STRING",
        TableType.MANAGED,
        targetTable -> {
          sql("INSERT INTO %s VALUES (1, 'initial')", targetTable);
          withNewTable(
              "schema_evolution_merge_source",
              "id INT, name STRING, extra STRING",
              TableType.EXTERNAL,
              sourceTable -> {
                sql("INSERT INTO %s VALUES (2, 'new', 'extra_value')", sourceTable);
                sql("SET spark.databricks.delta.schema.autoMerge.enabled = true");
                try {
                  sql(
                      "MERGE INTO %s AS target "
                          + "USING %s AS source "
                          + "ON target.id = source.id "
                          + "WHEN NOT MATCHED THEN INSERT *",
                      targetTable, sourceTable);
                  check(
                      sql("SELECT id, name, extra FROM %s ORDER BY id", targetTable),
                      List.of(row("1", "initial", "null"), row("2", "new", "extra_value")));
                } finally {
                  sql("SET spark.databricks.delta.schema.autoMerge.enabled = false");
                }
              });
        });
  }

  @Test
  public void testAlterTableOperationsSucceed() throws Exception {
    withNewTable(
        "alter_table_metadata_test",
        "id INT, name STRING",
        TableType.MANAGED,
        tableName -> {
          sql("INSERT INTO %s VALUES (1, 'initial')", tableName);
          sql("ALTER TABLE %s SET TBLPROPERTIES ('custom.key' = 'value')", tableName);
          sql("ALTER TABLE %s ADD COLUMNS (extra STRING)", tableName);
          sql("ALTER TABLE %s CLUSTER BY (id)", tableName);
          sql("INSERT INTO %s (id, name, extra) VALUES (2, 'new', 'extra_value')", tableName);
          check(
              sql("SELECT id, name, extra FROM %s ORDER BY id", tableName),
              List.of(row("1", "initial", "null"), row("2", "new", "extra_value")));
        });
  }

  @Test
  public void testReplaceTableWithoutClusterByRemovesClusteringDomainMetadata() throws Exception {
    String tableName = fullTableName("replace_remove_clustering_test");
    try {
      sql(
          "CREATE TABLE %s (id INT, name STRING) USING DELTA CLUSTER BY (id) %s",
          tableName, MANAGED_PROPS);
      assertClusteringColumnsEverywhere(tableName, "[[\"id\"]]");

      sql("REPLACE TABLE %s (id INT, name STRING) USING DELTA %s", tableName, MANAGED_PROPS);

      assertNoClusteringColumnsEverywhere(tableName);
    } finally {
      sql("DROP TABLE IF EXISTS %s", tableName);
    }
  }

  @Test
  public void testReplaceTableWithClusterByAddsClusteringDomainMetadata() throws Exception {
    String tableName = fullTableName("replace_add_clustering_test");
    try {
      sql("CREATE TABLE %s (id INT, name STRING) USING DELTA %s", tableName, MANAGED_PROPS);
      assertNoClusteringColumnsEverywhere(tableName);

      sql(
          "REPLACE TABLE %s (id INT, name STRING) USING DELTA CLUSTER BY (id) %s",
          tableName, MANAGED_PROPS);

      assertClusteringColumnsEverywhere(tableName, "[[\"id\"]]");
    } finally {
      sql("DROP TABLE IF EXISTS %s", tableName);
    }
  }

  /**
   * RESTORE TABLE to a version with unchanged clustering must succeed. The clustering kill switch
   * only fires when clustering actually changes.
   */
  @Test
  public void testRestoreTableWithUnchangedClusteringSucceeds() throws Exception {
    String tableName = fullTableName("restore_unchanged_clustering_test");
    try {
      sql(
          "CREATE TABLE %s (id INT, name STRING) USING DELTA CLUSTER BY (id)"
              + " TBLPROPERTIES ('delta.feature.catalogManaged'='supported')",
          tableName);
      sql("INSERT INTO %s VALUES (1, 'a'), (2, 'b')", tableName);
      long versionAfterInsert = currentVersion(tableName);
      // Restore to version 0 (before the insert): clustering is unchanged, must succeed.
      sql("RESTORE TABLE %s TO VERSION AS OF %d", tableName, versionAfterInsert - 1);
      check(tableName, List.of());
    } finally {
      sql("DROP TABLE IF EXISTS %s", tableName);
    }
  }

  /**
   * RESTORE TABLE to a version whose clustering differs from the current version is still blocked.
   * A failed restore must not partially update UC clustering domain metadata.
   */
  @Test
  public void testRestoreTableWithClusteringChangeIsBlockedWithoutUcDrift() throws Exception {
    String tableName = fullTableName("restore_clustering_change_test");
    try {
      sql(
          "CREATE TABLE %s (id INT, name STRING) USING DELTA CLUSTER BY (id)"
              + " TBLPROPERTIES ('delta.feature.catalogManaged'='supported')",
          tableName);
      sql("INSERT INTO %s VALUES (1, 'a'), (2, 'b')", tableName);
      long versionBeforeClusteringChange = currentVersion(tableName);
      sql("ALTER TABLE %s CLUSTER BY (name)", tableName);
      assertClusteringColumnsEverywhere(tableName, "[[\"name\"]]");

      assertThrowsWithCauseContaining(
          CLUSTERING_KILL_SWITCH_ERROR,
          () ->
              sql(
                  "RESTORE TABLE %s TO VERSION AS OF %d",
                  tableName, versionBeforeClusteringChange));

      assertClusteringColumnsEverywhere(tableName, "[[\"name\"]]");
      check(tableName, List.of(row("1", "a"), row("2", "b")));
    } finally {
      sql("DROP TABLE IF EXISTS %s", tableName);
    }
  }

  /**
   * RESTORE from an unclustered current table to a clustered previous version is also a clustering
   * change, so it must be blocked and leave UC clustering domain metadata unchanged.
   */
  @Test
  public void testRestoreTableFromUnclusteredCurrentToClusteredVersionIsBlockedWithoutUcDrift()
      throws Exception {
    String tableName = fullTableName("restore_to_clustered_version_test");
    try {
      sql(
          "CREATE TABLE %s (id INT, name STRING) USING DELTA CLUSTER BY (id)"
              + " TBLPROPERTIES ('delta.feature.catalogManaged'='supported')",
          tableName);
      sql("INSERT INTO %s VALUES (1, 'a'), (2, 'b')", tableName);
      long clusteredVersion = currentVersion(tableName);
      assertClusteringColumnsEverywhere(tableName, "[[\"id\"]]");

      sql("ALTER TABLE %s CLUSTER BY NONE", tableName);
      assertNoClusteringColumnsEverywhere(tableName);

      assertThrowsWithCauseContaining(
          CLUSTERING_KILL_SWITCH_ERROR,
          () -> sql("RESTORE TABLE %s TO VERSION AS OF %d", tableName, clusteredVersion));

      assertNoClusteringColumnsEverywhere(tableName);
      check(tableName, List.of(row("1", "a"), row("2", "b")));
    } finally {
      sql("DROP TABLE IF EXISTS %s", tableName);
    }
  }

  @Test
  public void testRowTrackingHighWatermarkDomainMetadataAdvancesInUc() throws Exception {
    withNewTable(
        "row_tracking_domain_metadata_test",
        "id INT, name STRING",
        TableType.MANAGED,
        tableName -> {
          sql("INSERT INTO %s VALUES (1, 'a'), (2, 'b')", tableName);
          long firstHighWatermark = rowTrackingHighWatermark(tableName);

          sql("INSERT INTO %s VALUES (3, 'c')", tableName);
          long secondHighWatermark = rowTrackingHighWatermark(tableName);

          assertTrue(secondHighWatermark > firstHighWatermark);
        });
  }

  @Test
  public void testRowTrackingBackfillHighWatermarkDomainMetadataSyncsToUc() throws Exception {
    withNewTable(
        "row_tracking_backfill_domain_metadata_test",
        "id INT, name STRING",
        TableType.MANAGED,
        tableName -> {
          sql("INSERT INTO %s VALUES (1, 'a'), (2, 'b')", tableName);

          sql("ALTER TABLE %s SET TBLPROPERTIES ('delta.enableRowTracking' = 'true')", tableName);

          long highWatermarkAfterBackfill = rowTrackingHighWatermark(tableName);
          assertTrue(highWatermarkAfterBackfill >= 1);
        });
  }

  @Test
  public void testInsertOverwriteWithOverwriteSchemaSucceeds() throws Exception {
    withNewTable(
        "overwrite_schema_target",
        "id INT, name STRING",
        TableType.MANAGED,
        targetTable -> {
          sql("INSERT INTO %s VALUES (1, 'initial')", targetTable);
          withNewTable(
              "overwrite_schema_source",
              "id INT, name STRING, extra STRING",
              TableType.EXTERNAL,
              sourceTable -> {
                sql("INSERT INTO %s VALUES (2, 'new', 'extra_val')", sourceTable);
                spark()
                    .read()
                    .table(sourceTable)
                    .write()
                    .format("delta")
                    .mode("overwrite")
                    .option("overwriteSchema", "true")
                    .saveAsTable(targetTable);
                check(
                    sql("SELECT id, name, extra FROM %s ORDER BY id", targetTable),
                    List.of(row("2", "new", "extra_val")));
              });
        });
  }

  /**
   * {@code CREATE OR REPLACE TABLE} with a different schema on an existing CatalogOwned table must
   * succeed.
   */
  @Test
  public void testReplaceTableWithNewSchemaSucceeds() throws Exception {
    withNewTable(
        "replace_schema_test",
        "id INT, name STRING",
        TableType.MANAGED,
        tableName -> {
          sql("INSERT INTO %s VALUES (1, 'initial')", tableName);
          sql(
              "CREATE OR REPLACE TABLE %s (id INT, name STRING, extra STRING) "
                  + "USING DELTA "
                  + "TBLPROPERTIES ('delta.feature.catalogManaged'='supported')",
              tableName);
          sql("INSERT INTO %s VALUES (2, 'new', 'extra_value')", tableName);
          check(
              sql("SELECT id, name, extra FROM %s ORDER BY id", tableName),
              List.of(row("2", "new", "extra_value")));
        });
  }

  // ---------------------------------------------------------------------------
  // Positive tests: operations that must still succeed
  // ---------------------------------------------------------------------------

  /** Normal INSERT with no metadata change must still succeed on CatalogOwned tables. */
  @Test
  public void testNormalInsertSucceedsForManagedTable() throws Exception {
    withNewTable(
        "normal_insert_managed_test",
        "id INT, name STRING",
        TableType.MANAGED,
        tableName -> {
          sql("INSERT INTO %s VALUES (1, 'foo'), (2, 'bar')", tableName);
          check(tableName, List.of(List.of("1", "foo"), List.of("2", "bar")));
        });
  }

  /**
   * INSERT with {@code autoMerge=true} but no new columns must succeed -- {@code autoMerge} only
   * triggers a schema update when the incoming data actually introduces extra columns.
   */
  @Test
  public void testInsertWithAutoMergeAndNoSchemaChangeSucceeds() throws Exception {
    withNewTable(
        "auto_merge_no_change_managed_test",
        "id INT, name STRING",
        TableType.MANAGED,
        tableName -> {
          sql("INSERT INTO %s VALUES (1, 'initial')", tableName);
          sql("SET spark.databricks.delta.schema.autoMerge.enabled = true");
          try {
            sql("INSERT INTO %s VALUES (2, 'second')", tableName);
            check(tableName, List.of(List.of("1", "initial"), List.of("2", "second")));
          } finally {
            sql("SET spark.databricks.delta.schema.autoMerge.enabled = false");
          }
        });
  }

  /**
   * Schema evolution via INSERT with {@code autoMerge=true} must still work on EXTERNAL (non-
   * CatalogOwned) tables. The kill switch must not affect tables that are not CatalogOwned.
   */
  @Test
  public void testInsertWithMergeSchemaStillWorksForExternalTable() throws Exception {
    withNewTable(
        "merge_schema_external_target",
        "id INT, name STRING",
        TableType.EXTERNAL,
        targetTable -> {
          sql("INSERT INTO %s VALUES (1, 'initial')", targetTable);
          withNewTable(
              "merge_schema_external_source",
              "id INT, name STRING, extra STRING",
              TableType.EXTERNAL,
              sourceTable -> {
                sql("INSERT INTO %s VALUES (2, 'new', 'extra_value')", sourceTable);
                sql("SET spark.databricks.delta.schema.autoMerge.enabled = true");
                try {
                  // Should succeed: EXTERNAL tables are not CatalogOwned.
                  sql("INSERT INTO %s SELECT * FROM %s", targetTable, sourceTable);
                  // The target now has 3 columns; row 1 has null for 'extra'.
                  check(
                      targetTable,
                      List.of(List.of("1", "initial", "null"), List.of("2", "new", "extra_value")));
                } finally {
                  sql("SET spark.databricks.delta.schema.autoMerge.enabled = false");
                }
              });
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

  private void assertClusteringColumnsEverywhere(String tableName, String expected)
      throws Exception {
    assertEquals(
        expected,
        loadTableViaDeltaRest(tableName).getMetadata().getProperties().get(CLUSTERING_COLUMNS));
    assertEquals(expected, loadTableInfoViaUc(tableName).getProperties().get(CLUSTERING_COLUMNS));
  }

  private void assertNoClusteringColumnsEverywhere(String tableName) throws Exception {
    assertNoClusteringColumns(loadTableViaDeltaRest(tableName).getMetadata().getProperties());
    assertNoClusteringColumns(loadTableInfoViaUc(tableName).getProperties());
  }

  private static void assertNoClusteringColumns(Map<String, String> properties) {
    assertTrue(
        !properties.containsKey(CLUSTERING_COLUMNS)
            || "[]".equals(properties.get(CLUSTERING_COLUMNS)));
  }

  private long rowTrackingHighWatermark(String tableName) throws Exception {
    Map<String, String> properties = loadTableViaDeltaRest(tableName).getMetadata().getProperties();
    String highWatermark = properties.get(ROW_ID_HIGH_WATERMARK);
    assertTrue(highWatermark != null, "UC Delta loadTable is missing row-tracking high watermark");
    return Long.parseLong(highWatermark);
  }
}
