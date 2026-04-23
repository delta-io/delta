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

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

/**
 * Tests that schema-changing and property-changing operations are blocked on Unity Catalog managed
 * (CatalogOwned) tables — regardless of which layer does the blocking.
 *
 * <p>There are two distinct layers of protection:
 *
 * <ol>
 *   <li><strong>Kill switch</strong> in {@code OptimisticTransaction.updateMetadata()}: blocks any
 *       commit that would change schema, partitions, description, or configuration on an existing
 *       CatalogOwned table. A second guard in {@code prepareCommit()} and {@code commitLarge()}
 *       blocks {@code delta.clustering} {@code DomainMetadata} changes (e.g. via RESTORE TABLE to a
 *       version written by an older client that had different clustering columns).
 *   <li><strong>UC catalog layer</strong> in {@code UCSingleCatalog}: {@code alterTable()} throws
 *       {@code UnsupportedOperationException} for all ALTER TABLE variants. INSERT OVERWRITE with
 *       {@code overwriteSchema=true} and {@code CREATE OR REPLACE TABLE} both route through REPLACE
 *       TABLE AS SELECT (RTAS) because {@code UCSingleCatalog} does not implement {@code
 *       StagingTableCatalog}; RTAS is not supported in OSS Delta.
 * </ol>
 *
 * <p>EXTERNAL tables are not CatalogOwned and are NOT affected by the kill switch; they continue to
 * allow schema evolution as before.
 */
public class UCDeltaTableBlockMetadataUpdateTest extends UCDeltaTableIntegrationBaseTest {

  // Error produced by the kill switch in OptimisticTransaction.updateMetadata().
  private static final String KILL_SWITCH_ERROR =
      "Metadata changes on Unity Catalog managed tables";

  // Error produced by the clustering kill switch in commitLarge().
  private static final String CLUSTERING_KILL_SWITCH_ERROR =
      "Clustering column changes on Unity Catalog managed tables";

  // Error produced by UCSingleCatalog.alterTable() for all ALTER TABLE variants.
  private static final String ALTER_TABLE_ERROR = "Altering a table is not supported yet";

  // Error produced by OSS Delta when REPLACE TABLE AS SELECT (RTAS) is attempted.
  // Triggered by CREATE OR REPLACE TABLE and DataFrame saveAsTable(overwrite+overwriteSchema)
  // when the target catalog does not implement StagingTableCatalog.
  private static final String RTAS_ERROR = "REPLACE TABLE AS SELECT (RTAS) is not supported";

  private static boolean supportsManagedReplaceViaUC() {
    return isUnityCatalogSparkAtLeast(0, 4, 1);
  }

  private static String expectedManagedReplaceFailure() {
    return supportsManagedReplaceViaUC() ? KILL_SWITCH_ERROR : RTAS_ERROR;
  }

  private static List<String> expectedAlterMetadataFailure() {
    return supportsManagedReplaceViaUC()
        ? List.of(ALTER_TABLE_ERROR, KILL_SWITCH_ERROR, CLUSTERING_KILL_SWITCH_ERROR)
        : List.of(ALTER_TABLE_ERROR);
  }

  // ---------------------------------------------------------------------------
  // Kill-switch tests: operations blocked by OptimisticTransaction.updateMetadata()
  // ---------------------------------------------------------------------------

  /**
   * INSERT with {@code autoMerge=true} and MERGE INTO with schema evolution must be blocked by the
   * kill switch in {@code updateMetadata()} on CatalogOwned tables. The kill switch covers all
   * metadata fields (schema, partitions, description, properties); schema evolution via autoMerge
   * is the primary user-facing path that reaches it without going through ALTER TABLE.
   */
  @Test
  public void testMetadataChangesViaWritesAreBlocked() throws Exception {
    withNewTable(
        "block_schema_evolution_target",
        "id INT, name STRING",
        TableType.MANAGED,
        targetTable -> {
          sql("INSERT INTO %s VALUES (1, 'initial')", targetTable);
          withNewTable(
              "block_schema_evolution_source",
              "id INT, name STRING, extra STRING",
              TableType.EXTERNAL,
              sourceTable -> {
                sql("INSERT INTO %s VALUES (2, 'new', 'extra_value')", sourceTable);
                sql("SET spark.databricks.delta.schema.autoMerge.enabled = true");
                try {
                  // INSERT with autoMerge introduces a new column.
                  assertThrowsWithCauseContaining(
                      KILL_SWITCH_ERROR,
                      () -> sql("INSERT INTO %s SELECT * FROM %s", targetTable, sourceTable));

                  // MERGE INTO with autoMerge introduces a new column from the source.
                  assertThrowsWithCauseContaining(
                      KILL_SWITCH_ERROR,
                      () ->
                          sql(
                              "MERGE INTO %s AS target "
                                  + "USING %s AS source "
                                  + "ON target.id = source.id "
                                  + "WHEN NOT MATCHED THEN INSERT *",
                              targetTable, sourceTable));
                } finally {
                  sql("SET spark.databricks.delta.schema.autoMerge.enabled = false");
                }
              });
        });
  }

  // ---------------------------------------------------------------------------
  // UC catalog layer tests: operations blocked by UCSingleCatalog before reaching Delta
  // ---------------------------------------------------------------------------

  /**
   * All ALTER TABLE variants on a CatalogOwned table must be blocked by {@code
   * UCSingleCatalog.alterTable()}, which throws {@code UnsupportedOperationException} for every
   * table change regardless of the specific operation.
   *
   * <p>SET TBLPROPERTIES and ADD COLUMNS remain blocked across UC versions, but the blocking layer
   * changes as more ALTER paths are delegated to Delta. Starting in UC Spark 0.4.1, ALTER TABLE
   * CLUSTER BY is supported for managed tables.
   */
  @Test
  public void testAlterTableOperationsAreBlocked() throws Exception {
    withNewTable(
        "block_alter_table_test",
        "id INT, name STRING",
        TableType.MANAGED,
        tableName -> {
          sql("INSERT INTO %s VALUES (1, 'initial')", tableName);

          // ALTER TABLE SET TBLPROPERTIES would change configuration.
          assertThrowsWithCauseContainingAny(
              expectedAlterMetadataFailure(),
              () -> sql("ALTER TABLE %s SET TBLPROPERTIES ('custom.key' = 'value')", tableName));

          // ALTER TABLE ADD COLUMNS would change the schema.
          assertThrowsWithCauseContainingAny(
              expectedAlterMetadataFailure(),
              () -> sql("ALTER TABLE %s ADD COLUMNS (extra STRING)", tableName));

          // ALTER TABLE CLUSTER BY would change clustering columns.
          assertThrowsWithCauseContainingAny(
              expectedAlterMetadataFailure(),
              () -> sql("ALTER TABLE %s CLUSTER BY (id)", tableName));
        });
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
   * RESTORE TABLE to a version whose clustering differs from the current version must be blocked by
   * the kill switch in {@code commitLarge()}.
   *
   * <p>A table can have different clustering at different versions if an older client or another
   * connector wrote a version before this guard was in place. This test simulates that by writing a
   * Delta log entry with different clustering directly to the table's underlying storage (bypassing
   * the kill switch), then attempting a RESTORE that would change clustering back.
   *
   * <p>This test is local-only: it needs direct filesystem access to write the fake commit, which
   * is only available when backed by {@code S3CredentialFileSystem} (local UC).
   */
  @Test
  public void testRestoreTableWithClusteringChangeIsBlocked() throws Exception {
    Assumptions.assumeFalse(
        isUCRemoteConfigured(), "Requires local filesystem access (local UC only)");

    String tableName = fullTableName("restore_clustering_change_test");
    try {
      sql(
          "CREATE TABLE %s (id INT, name STRING) USING DELTA CLUSTER BY (id)"
              + " TBLPROPERTIES ('delta.feature.catalogManaged'='supported')",
          tableName);
      sql("INSERT INTO %s VALUES (1, 'a'), (2, 'b')", tableName);
      long insertVersion = currentVersion(tableName);

      // Simulate an older client writing a version with different clustering directly to the
      // table's storage, bypassing the kill switch. This is how a real-world scenario could arise.
      String s3Location =
          sql("DESCRIBE FORMATTED %s", tableName).stream()
              .filter(r -> r.size() >= 2 && "Location".equalsIgnoreCase(r.get(0).trim()))
              .map(r -> r.get(1).trim())
              .findFirst()
              .orElseThrow();
      // Convert URI to a local filesystem path:
      // - S3CredentialFileSystem maps s3://fakeS3Bucket/abs/path → /abs/path
      // - Local UC may return a file: URI directly
      String localTablePath;
      if (s3Location.startsWith("s3://")) {
        localTablePath = s3Location.replaceAll("s3://[^/]+", "");
      } else if (s3Location.startsWith("file:")) {
        localTablePath = s3Location.replaceAll("^file:/+", "/");
      } else {
        localTablePath = s3Location;
      }
      long hackedVersion = insertVersion + 1;
      Path hackedCommitFile =
          Paths.get(localTablePath, "_delta_log", String.format("%020d.json", hackedVersion));
      // Write a minimal Delta commit with delta.clustering on 'name' instead of 'id'.
      String clusteringOnName = "{\\\"clusteringColumns\\\":[[\\\"name\\\"]]}";
      Files.writeString(
          hackedCommitFile,
          "{\"commitInfo\":{\"timestamp\":1000000000000,\"inCommitTimestamp\":1000000000000,"
              + "\"operation\":\"MANUAL UPDATE\",\"operationParameters\":{},\"isBlindAppend\":false}}\n"
              + "{\"domainMetadata\":{\"domain\":\"delta.clustering\","
              + "\"configuration\":\""
              + clusteringOnName
              + "\",\"removed\":false}}\n");

      // RESTORE to a version before the hacked commit: the current snapshot now shows 'name'
      // clustering, so restoring to 'id' clustering fires the kill switch.
      assertThrowsWithCauseContaining(
          CLUSTERING_KILL_SWITCH_ERROR,
          () -> sql("RESTORE TABLE %s TO VERSION AS OF %d", tableName, insertVersion - 1));
    } finally {
      sql("DROP TABLE IF EXISTS %s", tableName);
    }
  }

  /**
   * INSERT OVERWRITE with {@code overwriteSchema=true} that would replace the schema of an existing
   * CatalogOwned table must be blocked.
   *
   * <p>Before UC Spark 0.4.1 this routes through RTAS and fails in OSS Delta. Starting with UC
   * Spark 0.4.1, the replace path is supported and the Delta metadata kill switch blocks the schema
   * change instead.
   */
  @Test
  public void testInsertOverwriteWithOverwriteSchemaIsBlocked() throws Exception {
    withNewTable(
        "block_overwrite_schema_target",
        "id INT, name STRING",
        TableType.MANAGED,
        targetTable -> {
          sql("INSERT INTO %s VALUES (1, 'initial')", targetTable);
          withNewTable(
              "block_overwrite_schema_source",
              "id INT, name STRING, extra STRING",
              TableType.EXTERNAL,
              sourceTable -> {
                sql("INSERT INTO %s VALUES (2, 'new', 'extra_val')", sourceTable);
                assertThrowsWithCauseContaining(
                    expectedManagedReplaceFailure(),
                    () ->
                        spark()
                            .read()
                            .table(sourceTable)
                            .write()
                            .format("delta")
                            .mode("overwrite")
                            .option("overwriteSchema", "true")
                            .saveAsTable(targetTable));
              });
        });
  }

  /**
   * {@code CREATE OR REPLACE TABLE} with a different schema on an existing CatalogOwned table must
   * be blocked.
   *
   * <p>Before UC Spark 0.4.1 this routes through RTAS and fails in OSS Delta. Starting with UC
   * Spark 0.4.1, the replace path is supported and the Delta metadata kill switch blocks the schema
   * change instead.
   */
  @Test
  public void testReplaceTableWithNewSchemaIsBlocked() throws Exception {
    withNewTable(
        "block_replace_schema_test",
        "id INT, name STRING",
        TableType.MANAGED,
        tableName -> {
          sql("INSERT INTO %s VALUES (1, 'initial')", tableName);
          assertThrowsWithCauseContaining(
              expectedManagedReplaceFailure(),
              () ->
                  sql(
                      "CREATE OR REPLACE TABLE %s (id INT, name STRING, extra STRING) "
                          + "USING DELTA "
                          + "TBLPROPERTIES ('delta.feature.catalogManaged'='supported')",
                      tableName));
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
}
