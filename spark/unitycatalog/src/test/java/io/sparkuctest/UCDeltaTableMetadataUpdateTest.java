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

import io.unitycatalog.client.delta.api.DeltaTablesApi;
import io.unitycatalog.client.delta.model.DeltaLoadTableResponse;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Tests metadata-changing operations on Unity Catalog managed (CatalogOwned) tables. */
public class UCDeltaTableMetadataUpdateTest extends UCDeltaTableIntegrationBaseTest {

  // UC's `clusteringColumns` value for a single column-mapped clustering column. Column mapping
  // rewrites the logical column to a physical `col-<UUID>` reference, and the UC API exposes only
  // the logical schema, so the exact physical name can't be resolved -- match its shape instead.
  private static final String SINGLE_CLUSTERING_COLUMN = "\\[\\[\"col-[0-9a-f-]+\"\\]\\]";

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
  public void testAlterTableClusterByUpdatesUcDomainMetadata() throws Exception {
    withNewTable(
        "alter_cluster_by_domain_metadata_test",
        "id INT, name STRING",
        TableType.MANAGED,
        tableName -> {
          assertThat(loadTable(tableName).getMetadata().getProperties())
              .doesNotContainKey("clusteringColumns");

          sql("ALTER TABLE %s CLUSTER BY (id)", tableName);

          DeltaLoadTableResponse response = loadTable(tableName);
          assertThat(response.getMetadata().getProperties())
              .containsEntry("delta.feature.clustering", "supported");
          assertThat(response.getMetadata().getProperties().get("clusteringColumns"))
              .matches(SINGLE_CLUSTERING_COLUMN);

          sql("ALTER TABLE %s CLUSTER BY NONE", tableName);

          response = loadTable(tableName);
          assertThat(response.getMetadata().getProperties())
              .containsEntry("delta.feature.clustering", "supported")
              .containsEntry("clusteringColumns", "[]");
        });
  }

  /**
   * RESTORE TABLE to a version with unchanged clustering succeeds (basic RESTORE coverage for a
   * UC-managed clustered table, alongside testRestoreTableWithClusteringChangePropagatesToUc).
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
   * RESTORE TABLE to a version whose clustering differs from the current version succeeds and
   * propagates the restored clustering columns back to Unity Catalog. RESTORE commits through the
   * {@code commitLarge} path, so this exercises clustering DomainMetadata forwarding on that path.
   */
  @Test
  public void testRestoreTableWithClusteringChangePropagatesToUc() throws Exception {
    String tableName = fullTableName("restore_clustering_change_test");
    try {
      sql(
          "CREATE TABLE %s (id INT, name STRING) USING DELTA CLUSTER BY (id)"
              + " TBLPROPERTIES ('delta.feature.catalogManaged'='supported')",
          tableName);
      sql("INSERT INTO %s VALUES (1, 'a'), (2, 'b')", tableName);
      // UC's view of the clustering columns while clustered by `id`.
      String clusteringById = clusteringColumns(tableName);
      assertThat(clusteringById).matches(SINGLE_CLUSTERING_COLUMN);
      long versionClusteredById = currentVersion(tableName);

      // Change clustering to `name` and add a row, so the RESTORE below has both clustering and
      // data to revert. UC must reflect a different single clustering column after the ALTER.
      sql("ALTER TABLE %s CLUSTER BY (name)", tableName);
      sql("INSERT INTO %s VALUES (3, 'c')", tableName);
      assertThat(clusteringColumns(tableName)).matches(SINGLE_CLUSTERING_COLUMN);
      assertThat(clusteringColumns(tableName)).isNotEqualTo(clusteringById);

      // RESTORE back to the `id`-clustered version (committed via the commitLarge path): both the
      // data and UC's clustering columns must revert -- the latter via the clustering
      // DomainMetadata forwarded through CatalogTrackedInfo.
      sql("RESTORE TABLE %s TO VERSION AS OF %d", tableName, versionClusteredById);
      check(
          sql("SELECT id, name FROM %s ORDER BY id", tableName),
          List.of(row("1", "a"), row("2", "b")));
      assertThat(clusteringColumns(tableName)).isEqualTo(clusteringById);
    } finally {
      sql("DROP TABLE IF EXISTS %s", tableName);
    }
  }

  /**
   * REPLACE TABLE (CREATE OR REPLACE) that changes the clustering columns propagates the new
   * clustering to Unity Catalog. The clustering goes from one column to two, which UC reflects
   * unambiguously -- unlike a same-count column swap, whose physical `col-<UUID>` names are
   * reassigned by REPLACE anyway and so can't distinguish the new clustering from the old.
   */
  @Test
  public void testReplaceTableWithClusteringChangePropagatesToUc() throws Exception {
    String tableName = fullTableName("replace_clustering_change_test");
    try {
      // CREATE clustered by `id`: UC reflects a single clustering column.
      sql(
          "CREATE TABLE %s (id INT, name STRING) USING DELTA CLUSTER BY (id)"
              + " TBLPROPERTIES ('delta.feature.catalogManaged'='supported')",
          tableName);
      sql("INSERT INTO %s VALUES (1, 'a')", tableName);
      assertThat(clusteringColumns(tableName)).matches(SINGLE_CLUSTERING_COLUMN);

      // CREATE OR REPLACE clustered by (id, name): UC must now reflect two clustering columns.
      sql(
          "CREATE OR REPLACE TABLE %s (id INT, name STRING) USING DELTA CLUSTER BY (id, name)"
              + " TBLPROPERTIES ('delta.feature.catalogManaged'='supported')",
          tableName);
      assertThat(clusteringColumns(tableName))
          .matches("\\[\\[\"col-[0-9a-f-]+\"\\],\\[\"col-[0-9a-f-]+\"\\]\\]");
    } finally {
      sql("DROP TABLE IF EXISTS %s", tableName);
    }
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

  private DeltaLoadTableResponse loadTable(String tableName) throws Exception {
    String[] parts = tableName.split("\\.", 3);
    return new DeltaTablesApi(unityCatalogInfo().createApiClient())
        .loadTable(parts[0], parts[1], parts[2]);
  }

  /** UC's view of the table's clustering columns (the {@code clusteringColumns} table property). */
  private String clusteringColumns(String tableName) throws Exception {
    return loadTable(tableName).getMetadata().getProperties().get("clusteringColumns");
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
}
