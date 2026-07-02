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

import io.unitycatalog.client.api.TablesApi;
import io.unitycatalog.client.model.TableInfo;
import java.nio.file.AccessDeniedException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.catalyst.analysis.CannotReplaceMissingTableException;
import org.apache.spark.sql.delta.sources.DeltaSQLConf;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class UCDeltaManagedReplaceSemanticsTest extends UCDeltaTableIntegrationBaseTest {

  private static final String DEFAULT_FEATURES_RESTATEMENT =
      "TBLPROPERTIES ("
          + "'delta.feature.catalogManaged'='supported', "
          + "'delta.feature.vacuumProtocolCheck'='supported', "
          + "'delta.feature.inCommitTimestamp'='supported')";

  private static final String FULL_NON_DEFAULT_RESTATEMENT =
      "TBLPROPERTIES ("
          + "'delta.feature.catalogManaged'='supported', "
          + "'delta.enableChangeDataFeed'='true', "
          + "'delta.enableTypeWidening'='true')";

  private static final String PARTIAL_NON_DEFAULT_RESTATEMENT =
      "TBLPROPERTIES ("
          + "'delta.feature.catalogManaged'='supported', "
          + "'delta.enableChangeDataFeed'='true')";

  private enum ReplaceOperation {
    REPLACE("REPLACE TABLE", false),
    REPLACE_AS_SELECT("REPLACE TABLE", true),
    CREATE_OR_REPLACE("CREATE OR REPLACE TABLE", false),
    CREATE_OR_REPLACE_AS_SELECT("CREATE OR REPLACE TABLE", true);

    private final String sqlPrefix;
    private final boolean asSelect;

    ReplaceOperation(String sqlPrefix, boolean asSelect) {
      this.sqlPrefix = sqlPrefix;
      this.asSelect = asSelect;
    }

    private boolean isAsSelect() {
      return asSelect;
    }
  }

  // TODO: Once external delta table RTAS is supported, use @TestAllTableTypes for these tests.

  // Default features (catalogManaged, vacuumProtocolCheck, ICT) are always implicitly present on
  // managed tables, so restating them in a REPLACE is always safe and should succeed.
  @Test
  public void testDefaultFeatureRestatementIsAllowedForManagedReplaceOperations() throws Exception {
    for (ReplaceOperation operation : ReplaceOperation.values()) {
      String tableName = uniqueTableName("default_features", operation);
      withNewTable(
          tableName,
          "i INT, s STRING",
          TableType.MANAGED,
          fullTableName -> {
            sql("INSERT INTO %s VALUES (1, 'old')", fullTableName);

            assertSuccessfulReplace(
                operation,
                fullTableName,
                buildStatement(
                    operation,
                    fullTableName,
                    "i INT, s STRING",
                    DEFAULT_FEATURES_RESTATEMENT,
                    null,
                    "2 AS i, 'new' AS s"));
          });
    }
  }

  // Non-default features (CDF, type widening) were explicitly enabled at create time, so REPLACE
  // semantics requires them to be restated exactly — otherwise the replace could silently drop
  // features the user enabled intentionally.
  @Test
  public void testExactNonDefaultFeatureMatchIsAllowedForManagedReplaceOperations()
      throws Exception {
    for (ReplaceOperation operation : ReplaceOperation.values()) {
      String tableName = uniqueTableName("full_feature_match", operation);
      withNewTable(
          tableName,
          "i INT, s STRING",
          null,
          TableType.MANAGED,
          "'delta.enableChangeDataFeed'='true', 'delta.enableTypeWidening'='true'",
          fullTableName -> {
            sql("INSERT INTO %s VALUES (1, 'old')", fullTableName);

            assertSuccessfulReplace(
                operation,
                fullTableName,
                buildStatement(
                    operation,
                    fullTableName,
                    "i INT, s STRING",
                    FULL_NON_DEFAULT_RESTATEMENT,
                    null,
                    "2 AS i, 'new' AS s"));
          });
    }
  }

  // The table was created with CDF + type widening enabled. A REPLACE statement that omits all
  // TBLPROPERTIES still succeeds through UC updateTable.
  @Test
  public void testMissingNonDefaultFeatureRestatementIsAllowedForManagedReplaceOperations()
      throws Exception {
    for (ReplaceOperation operation : ReplaceOperation.values()) {
      String tableName = uniqueTableName("missing_feature_match", operation);
      withNewTable(
          tableName,
          "i INT, s STRING",
          null,
          TableType.MANAGED,
          "'delta.enableChangeDataFeed'='true', 'delta.enableTypeWidening'='true'",
          fullTableName -> {
            sql("INSERT INTO %s VALUES (1, 'old')", fullTableName);

            assertSuccessfulReplace(
                operation,
                fullTableName,
                buildStatement(
                    operation, fullTableName, "i INT, s STRING", "", null, "2 AS i, 'new' AS s"));
          });
    }
  }

  // Same as above, but only CDF is restated and type widening is omitted.
  @Test
  public void testPartialNonDefaultFeatureRestatementIsAllowedForManagedReplaceOperations()
      throws Exception {
    for (ReplaceOperation operation : ReplaceOperation.values()) {
      String tableName = uniqueTableName("partial_feature_match", operation);
      withNewTable(
          tableName,
          "i INT, s STRING",
          null,
          TableType.MANAGED,
          "'delta.enableChangeDataFeed'='true', 'delta.enableTypeWidening'='true'",
          fullTableName -> {
            sql("INSERT INTO %s VALUES (1, 'old')", fullTableName);

            assertSuccessfulReplace(
                operation,
                fullTableName,
                buildStatement(
                    operation,
                    fullTableName,
                    "i INT, s STRING",
                    PARTIAL_NON_DEFAULT_RESTATEMENT,
                    null,
                    "2 AS i, 'new' AS s"));
          });
    }
  }

  @Test
  public void testCommentChangeIsAllowedForManagedReplaceOperations() throws Exception {
    for (ReplaceOperation operation : ReplaceOperation.values()) {
      String tableName = uniqueTableName("comment_change", operation);
      withNewTable(
          tableName,
          "i INT, s STRING",
          TableType.MANAGED,
          fullTableName -> {
            sql("INSERT INTO %s VALUES (1, 'old')", fullTableName);

            assertSuccessfulReplace(
                operation,
                fullTableName,
                buildStatement(
                    operation,
                    fullTableName,
                    "i INT, s STRING",
                    DEFAULT_FEATURES_RESTATEMENT,
                    "new description",
                    "2 AS i, 'new' AS s"));
            assertThat(describeExtendedValue(fullTableName, "Comment"))
                .isEqualTo("new description");
          });
    }
  }

  @Test
  public void testUserPropertyChangeIsAllowedForManagedReplaceOperations() throws Exception {
    for (ReplaceOperation operation : ReplaceOperation.values()) {
      String tableName = uniqueTableName("property_change", operation);
      withNewTable(
          tableName,
          "i INT, s STRING",
          TableType.MANAGED,
          fullTableName -> {
            sql("INSERT INTO %s VALUES (1, 'old')", fullTableName);

            assertSuccessfulReplace(
                operation,
                fullTableName,
                buildStatement(
                    operation,
                    fullTableName,
                    "i INT, s STRING",
                    "TBLPROPERTIES ("
                        + "'delta.feature.catalogManaged'='supported', "
                        + "'delta.feature.vacuumProtocolCheck'='supported', "
                        + "'delta.feature.inCommitTimestamp'='supported', "
                        + "'myapp.version'='2')",
                    null,
                    "2 AS i, 'new' AS s"));
            assertThat(tableProperty(fullTableName, "myapp.version")).isEqualTo("2");
          });
    }
  }

  // An existing managed table can carry `delta.*` properties this Delta build does not recognize
  // (e.g. `delta.random`). Such keys can only be set with `allowArbitraryProperties` on, since
  // normal validation rejects unknown `delta.*` keys.
  // A later REPLACE that does NOT restate them -- with validation back on -- must carry them
  // forward into the replaced table rather than dropping them or failing the REPLACE.
  @Test
  public void testCarryForwardOfArbitraryDeltaPropertyOnManagedReplace() throws Exception {
    String tableName = "carry_fwd_arbitrary_" + UUID.randomUUID().toString().replace("-", "");
    String fullTableName = fullTableName(tableName);
    String arbitraryPropsConf = DeltaSQLConf.ALLOW_ARBITRARY_TABLE_PROPERTIES().key();
    try {
      // Seed the table with unknown delta.* keys -- only possible with arbitrary props allowed.
      spark().conf().set(arbitraryPropsConf, "true");
      sql(
          "CREATE TABLE %s (i INT, s STRING) USING DELTA TBLPROPERTIES ("
              + "'delta.feature.catalogManaged'='supported', "
              + "'delta.dummy_fake_key'='dummyValue', "
              + "'delta.random'='carried')",
          fullTableName);
      sql("INSERT INTO %s VALUES (1, 'old')", fullTableName);
      long versionBefore = currentVersion(fullTableName);

      // Validation back on: a bare REPLACE that doesn't restate the unknown keys must still carry
      // them forward (they bypass validateConfigurations via the carry-forward tagging path).
      spark().conf().set(arbitraryPropsConf, "false");
      sql("REPLACE TABLE %s (i INT, s STRING) USING DELTA", fullTableName);

      // The REPLACE actually committed and replaced the data (bare REPLACE leaves the table empty),
      // so the property carry-forward below is asserted on the post-REPLACE table.
      assertThat(currentVersion(fullTableName)).isEqualTo(versionBefore + 1);
      assertThat(sql("SELECT COUNT(*) FROM %s", fullTableName)).containsExactly(row("0"));
      // SHOW TBLPROPERTIES resolves from the Delta snapshot metadata, so this confirms the keys
      // are persisted in the committed `Metadata.configuration`, not merely in the catalog.
      assertThat(tableProperty(fullTableName, "delta.dummy_fake_key")).isEqualTo("dummyValue");
      assertThat(tableProperty(fullTableName, "delta.random")).isEqualTo("carried");
    } finally {
      spark().conf().set(arbitraryPropsConf, "false");
      sql("DROP TABLE IF EXISTS %s", fullTableName);
    }
  }

  // The flip side of carry-forward: a caller that explicitly puts an unknown `delta.*` key in the
  // REPLACE TBLPROPERTIES (with validation on) must be rejected -- only the existing table's
  // already-committed value is grandfathered, never fresh caller input. The existing table must
  // survive the rejected REPLACE intact.
  @Test
  public void testCallerSpecifiedUnknownDeltaPropertyIsRejectedOnManagedReplace() throws Exception {
    withNewTable(
        "reject_caller_unknown_delta",
        "i INT, s STRING",
        TableType.MANAGED,
        fullTableName -> {
          sql("INSERT INTO %s VALUES (1, 'old')", fullTableName);
          long versionBefore = currentVersion(fullTableName);

          assertThrowsWithCauseContaining(
              "DELTA_UNKNOWN_CONFIGURATION",
              () ->
                  sql(
                      "REPLACE TABLE %s (i INT, s STRING) USING DELTA TBLPROPERTIES ("
                          + "'delta.feature.catalogManaged'='supported', 'delta.random'='x')",
                      fullTableName));

          // The rejected REPLACE must not have committed a new version.
          assertThat(currentVersion(fullTableName)).isEqualTo(versionBefore);
          assertThat(sql("SELECT * FROM %s", fullTableName)).containsExactly(row("1", "old"));
        });
  }

  // Schema change (adding a column) during REPLACE succeeds through UC updateTable.
  @Test
  public void testSchemaChangeIsAllowedForManagedReplaceOperations() throws Exception {
    for (ReplaceOperation operation : ReplaceOperation.values()) {
      String tableName = uniqueTableName("schema_change", operation);
      withNewTable(
          tableName,
          "i INT, s STRING",
          TableType.MANAGED,
          fullTableName -> {
            sql("INSERT INTO %s VALUES (1, 'old')", fullTableName);

            assertSuccessfulReplace(
                operation,
                fullTableName,
                buildStatement(
                    operation,
                    fullTableName,
                    "i INT, s STRING, extra INT",
                    DEFAULT_FEATURES_RESTATEMENT,
                    null,
                    "2 AS i, 'new' AS s, 3 AS extra"));
            if (operation.isAsSelect()) {
              assertThat(sql("SELECT extra FROM %s", fullTableName)).containsExactly(row("3"));
            } else {
              sql("INSERT INTO %s VALUES (2, 'new', 3)", fullTableName);
              assertThat(sql("SELECT extra FROM %s", fullTableName)).containsExactly(row("3"));
            }
          });
    }
  }

  @Test
  public void testReplaceTableWithColumnDefaultIsAllowedAndDefaultIsApplied() throws Exception {
    String tableName = "replace_column_default_" + UUID.randomUUID().toString().replace("-", "");
    withNewTable(
        tableName,
        "i INT, s STRING",
        TableType.MANAGED,
        fullTableName -> {
          sql("INSERT INTO %s VALUES (1, 'old')", fullTableName);

          assertSuccessfulReplace(
              ReplaceOperation.REPLACE,
              fullTableName,
              "REPLACE TABLE "
                  + fullTableName
                  + " (i INT, s STRING DEFAULT 'new-default') USING DELTA "
                  + "TBLPROPERTIES ("
                  + "'delta.feature.catalogManaged'='supported', "
                  + "'delta.feature.allowColumnDefaults'='supported')");

          sql("INSERT INTO %s (i) VALUES (2)", fullTableName);
          assertThat(sql("SELECT i, s FROM %s", fullTableName))
              .containsExactly(row("2", "new-default"));
        });
  }

  // Most common user case: REPLACE without specifying any TBLPROPERTIES clause. Delta auto-restates
  // default features for managed tables, so the replace should succeed.
  @Test
  public void testBareReplaceWithNoPropertiesIsAllowedForManagedReplaceOperations()
      throws Exception {
    for (ReplaceOperation operation : ReplaceOperation.values()) {
      String tableName = uniqueTableName("bare_replace", operation);
      withNewTable(
          tableName,
          "i INT, s STRING",
          TableType.MANAGED,
          fullTableName -> {
            sql("INSERT INTO %s VALUES (1, 'old')", fullTableName);

            assertSuccessfulReplace(
                operation,
                fullTableName,
                buildStatement(
                    operation, fullTableName, "i INT, s STRING", "", null, "2 AS i, 'new' AS s"));
          });
    }
  }

  // REPLACE with `USING <non-Delta>` on a managed Delta table is rejected. The statement's
  // non-Delta provider makes `shouldDelegateReplaceToDeltaApi` return false in
  // UCSingleCatalog, so the legacy validation throws before the Delta-side
  // `loadTableAndBuildReplaceProps` is reached. The existing table must survive intact.
  @Test
  public void testProviderChangeIsRejectedAndPreservesExistingTable() throws Exception {
    String tableName = "provider_change_" + UUID.randomUUID().toString().replace("-", "");
    withNewTable(
        tableName,
        "i INT, s STRING",
        TableType.MANAGED,
        fullTableName -> {
          sql("INSERT INTO %s VALUES (1, 'old')", fullTableName);
          String ucTableIdBefore = currentUcTableId(fullTableName);
          long versionBefore = currentVersion(fullTableName);

          assertThrowsWithCauseContaining(
              "Cannot change table format from DELTA to PARQUET",
              () ->
                  sql(
                      "REPLACE TABLE %s USING PARQUET AS SELECT 2 AS i, 'new' AS s",
                      fullTableName));

          assertThat(currentUcTableId(fullTableName)).isEqualTo(ucTableIdBefore);
          assertThat(currentVersion(fullTableName)).isEqualTo(versionBefore);
          assertThat(sql("SELECT * FROM %s", fullTableName)).containsExactly(row("1", "old"));
        });
  }

  // REPLACE that specifies a `LOCATION` on an existing UC-managed Delta table is
  // rejected by `loadTableAndBuildReplaceProps` before any Delta commit lands. The
  // REPLACE routes through the Delta-side path even when `PROP_LOCATION` is set so the
  // validation can fire early (a previous version skipped routing on `PROP_LOCATION`,
  // which let the REPLACE write a Delta commit before the post-commit catalog-update
  // hook discovered the inconsistency).
  @Test
  public void testReplaceManagedTableWithLocationIsRejected() throws Exception {
    withNewTable(
        "managed_replace_with_location",
        "i INT, s STRING",
        TableType.MANAGED,
        fullTableName ->
            withTempDir(
                (Path externalLocation) ->
                    assertThatThrownBy(
                            () ->
                                sql(
                                    "REPLACE TABLE %s (i INT, s STRING) USING DELTA LOCATION '%s'",
                                    fullTableName, externalLocation.toString()))
                        .hasMessageContaining(
                            "only catalog-managed Delta tables can be replaced on this path")));
  }

  // Same as above for the EXTERNAL->EXTERNAL same-location case: the legacy UC path
  // would have rejected this with a clear "REPLACE TABLE is only supported for
  // catalog-managed UC Delta tables" message. With the delegation gate in place, the
  // Delta-side validation now fires and produces the location-rejection error.
  @Test
  public void testReplaceExternalTableWithSameLocationIsRejected() throws Exception {
    withTempDir(
        (Path location) -> {
          String tableName =
              "external_same_loc_replace_" + UUID.randomUUID().toString().replace("-", "");
          String fullTableName = fullTableName(tableName);
          try {
            sql("DROP TABLE IF EXISTS %s", fullTableName);
            sql(
                "CREATE TABLE %s (i INT, s STRING) USING DELTA LOCATION '%s'",
                fullTableName, location.toString());
            assertThatThrownBy(
                    () ->
                        sql(
                            "REPLACE TABLE %s (i INT, s STRING) USING DELTA LOCATION '%s'",
                            fullTableName, location.toString()))
                .hasMessageContaining(
                    "only catalog-managed Delta tables can be replaced on this path");
          } finally {
            sql("DROP TABLE IF EXISTS %s", fullTableName);
          }
        });
  }

  // Replacing a clustered managed Delta table with a non-clustered, non-partitioned one
  // succeeds at the data plane and forwards the clustering domain metadata intent to UC.
  // UC clears the clustering columns while keeping the clustering table feature supported.
  @Test
  public void testReplaceClusteredManagedTableWithNoneClearsUcClusteringColumns() throws Exception {
    String tableName = "cluster_to_none_" + UUID.randomUUID().toString().replace("-", "");
    String fullTableName = fullTableName(tableName);
    try {
      sql(
          "CREATE TABLE %s (col1 STRING, col2 STRING) USING DELTA "
              + "TBLPROPERTIES ('delta.feature.catalogManaged'='supported') CLUSTER BY (col1)",
          fullTableName);

      sql(
          "REPLACE TABLE %s (i INT, s STRING) USING DELTA "
              + "TBLPROPERTIES ('delta.feature.catalogManaged'='supported')",
          fullTableName);

      sql("INSERT INTO %s VALUES (1, 'a')", fullTableName);
      assertThat(sql("SELECT * FROM %s", fullTableName)).containsExactly(row("1", "a"));

      TablesApi tablesApi = new TablesApi(unityCatalogInfo().createApiClient());
      TableInfo info = tablesApi.getTable(fullTableName, false, false);
      assertThat(info.getProperties())
          .containsEntry("clusteringColumns", "[]")
          .containsEntry("delta.feature.clustering", "supported");
    } finally {
      sql("DROP TABLE IF EXISTS %s", fullTableName);
    }
  }

  // Delta unconditionally rejects replacing a clustered table with a partitioned one
  // (`DELTA_CLUSTERING_REPLACE_TABLE_WITH_PARTITIONED_TABLE`, raised by
  // `CreateDeltaTableCommand.validatePrerequisitesForClusteredTable`).
  @Test
  public void testReplaceClusteredManagedTableWithPartitionedIsRejected() throws Exception {
    String tableName = "cluster_to_partition_" + UUID.randomUUID().toString().replace("-", "");
    String fullTableName = fullTableName(tableName);
    try {
      sql(
          "CREATE TABLE %s (col1 STRING, col2 STRING) USING DELTA "
              + "TBLPROPERTIES ('delta.feature.catalogManaged'='supported') CLUSTER BY (col1)",
          fullTableName);

      assertThatThrownBy(
              () ->
                  sql(
                      "REPLACE TABLE %s (i INT, s STRING) USING DELTA "
                          + "TBLPROPERTIES ('delta.feature.catalogManaged'='supported') PARTITIONED BY (i)",
                      fullTableName))
          .hasMessageContaining("DELTA_CLUSTERING_REPLACE_TABLE_WITH_PARTITIONED_TABLE");
    } finally {
      sql("DROP TABLE IF EXISTS %s", fullTableName);
    }
  }

  // Path-based identifiers (e.g. `delta.`/tmp/foo``) are not valid UC table references --
  // UC has no entry for them. Spark V2's `AtomicReplaceTableExec` calls
  // `catalog.tableExists(ident)` before `stageReplace`, and `AbstractDeltaCatalog.tableExists`
  // probes the filesystem (`fs.exists(path)`) for path-based identifiers. The end-user-visible
  // exception therefore depends on whether the underlying filesystem is reachable:
  //   - filesystem reachable, path missing -> `tableExists` returns false ->
  //     Spark V2 throws `CannotReplaceMissingTableException` (errorClass TABLE_OR_VIEW_NOT_FOUND).
  //   - filesystem unreachable (e.g. S3 403 when the UC server's federated credentials are
  //     not loaded into the Spark session) -> `fs.exists` throws `AccessDeniedException`,
  //     which bubbles up out of `AtomicReplaceTableExec.run`.
  // Either outcome is an acceptable rejection of path-based REPLACE on a UC catalog; this
  // test pins both as the only outcomes (no silent success, no surprise new exception class).
  @Test
  public void testReplaceOnPathBasedIdentifierIsRejected() throws Exception {
    withTempDir(
        (Path location) -> {
          assertThatThrownBy(
                  () ->
                      sql(
                          "REPLACE TABLE delta.`%s` (i INT, s STRING) USING DELTA "
                              + "TBLPROPERTIES ('delta.feature.catalogManaged'='supported')",
                          location.toString()))
              .satisfiesAnyOf(
                  t ->
                      assertThat(t)
                          .isInstanceOf(CannotReplaceMissingTableException.class)
                          .hasMessageContaining("TABLE_OR_VIEW_NOT_FOUND")
                          .hasMessageContaining(location.toString()),
                  t -> assertThat(t).isInstanceOf(AccessDeniedException.class));
        });
  }

  // CREATE OR REPLACE on a non-existent identifier must take the fresh-CREATE fallback in
  // `AbstractDeltaCatalog.maybeStageDeltaCreateOrReplace` (catching `NoSuchTableException`
  // from `loadTableAndBuildReplaceProps` and calling `createStagingTable` instead).
  @Test
  public void testCreateOrReplaceCreatesNewTableWhenMissing() throws Exception {
    String tableName = "cor_missing_" + UUID.randomUUID().toString().replace("-", "");
    String fullTableName = fullTableName(tableName);
    try {
      sql("DROP TABLE IF EXISTS %s", fullTableName);

      sql(
          "CREATE OR REPLACE TABLE %s (i INT, s STRING) USING DELTA "
              + "TBLPROPERTIES ('delta.feature.catalogManaged'='supported')",
          fullTableName);

      sql("INSERT INTO %s VALUES (1, 'a')", fullTableName);
      assertThat(sql("SELECT * FROM %s", fullTableName)).containsExactly(row("1", "a"));
    } finally {
      sql("DROP TABLE IF EXISTS %s", fullTableName);
    }
  }

  private void assertSuccessfulReplace(
      ReplaceOperation operation, String tableName, String statement) throws Exception {
    String ucTableIdBeforeReplace = currentUcTableId(tableName);
    long versionBeforeReplace = currentVersion(tableName);

    sql(statement);

    assertThat(currentUcTableId(tableName))
        .as("ucTableId after %s on %s", operation, tableName)
        .isEqualTo(ucTableIdBeforeReplace);
    assertThat(currentVersion(tableName))
        .as("version after %s on %s", operation, tableName)
        .isEqualTo(versionBeforeReplace + 1);
    assertThat(sql("SELECT COUNT(*) FROM %s", tableName))
        .as("row count after %s on %s", operation, tableName)
        .containsExactly(row(operation.isAsSelect() ? "1" : "0"));
  }

  private String uniqueTableName(String prefix, ReplaceOperation operation) {
    return prefix
        + "_"
        + operation.name().toLowerCase()
        + "_"
        + UUID.randomUUID().toString().replace("-", "");
  }

  private String buildStatement(
      ReplaceOperation operation,
      String tableName,
      String schema,
      String tablePropertiesClause,
      String comment,
      String query) {
    List<String> parts = new ArrayList<>();
    parts.add(operation.sqlPrefix);
    parts.add(tableName);
    if (!operation.isAsSelect()) {
      parts.add("(" + schema + ")");
    }
    parts.add("USING DELTA");
    if (!tablePropertiesClause.isEmpty()) {
      parts.add(tablePropertiesClause);
    }
    if (comment != null) {
      parts.add(String.format("COMMENT '%s'", comment));
    }
    if (operation.isAsSelect()) {
      parts.add("AS SELECT " + query);
    }
    return String.join(" ", parts);
  }

  private String tableProperty(String tableName, String key) {
    return sql("SHOW TBLPROPERTIES %s", tableName).stream()
        .filter(r -> r.size() >= 2 && key.equals(r.get(0)))
        .map(r -> r.get(1))
        .findFirst()
        .orElse(null);
  }

  private String describeExtendedValue(String tableName, String key) {
    return sql("DESCRIBE EXTENDED %s", tableName).stream()
        .filter(r -> r.size() >= 2 && key.equalsIgnoreCase(r.get(0).trim()))
        .map(r -> r.get(1).trim())
        .findFirst()
        .orElse(null);
  }

  /**
   * TODO(uc-identity-replace): Re-enable once the unitycatalog-spark connector overrides {@code
   * stageReplace(Column[])} and {@code stageCreateOrReplace(Column[])}.
   *
   * <p>Root cause: {@code UCSingleCatalog} only overrides the deprecated {@code StructType}
   * variants of {@code stageReplace}/{@code stageCreateOrReplace}. Spark's {@code ReplaceTableExec}
   * calls the {@code Column[]} variants, which fall through to the default impl in {@code
   * StagingTableCatalog}. The default runs {@code CatalogV2Util.v2ColumnsToStructType(columns)} —
   * which drops {@code Column#identityColumnSpec()} — and then forwards the stripped {@code
   * StructType} to UC's {@code StructType} override, which delegates to Delta. Delta's {@code
   * stageReplace(Column[])} override in {@code AbstractDeltaCatalog} is therefore never reached on
   * the UC path, and the post-REPLACE schema commits with no {@code delta.identity.*} metadata.
   *
   * <p>Symptom this test catches: REPLACE on a UC-managed table that declares {@code GENERATED
   * ALWAYS AS IDENTITY} succeeds, but the next INSERT generates {@code NULL} for the identity
   * column instead of the new {@code START WITH} value.
   *
   * <p>Verified on the spark_catalog path (V1 DDL) where the existing {@code IdentityColumnSuite
   * "create or replace on a table resets high watermark"} test passes. CREATE on UC-managed works
   * (see {@code UCDeltaTableDDLTest#testCreateWithIdentityColumn}) because {@code
   * UCSingleCatalog.createTable(Column[])} IS overridden and preserves the spec.
   */
  @Test
  @Disabled(
      "Blocked on unitycatalog-spark: stageReplace(Column[]) / stageCreateOrReplace(Column[])"
          + " not overridden, so identityColumnSpec() is stripped by the default"
          + " StagingTableCatalog impl before Delta sees the call. Re-enable after the UC"
          + " connector adds Column[] overrides for the stage* methods.")
  public void testReplaceManagedTableResetsIdentityHighWaterMark() throws Exception {
    for (ReplaceOperation operation : ReplaceOperation.values()) {
      String tableName = uniqueTableName("identity_hwm_reset", operation);
      String fullTableName = fullTableName(tableName);
      try {
        sql(
            "CREATE TABLE %s ("
                + "id BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 100 INCREMENT BY 1),"
                + "val STRING) USING delta"
                + " TBLPROPERTIES ('delta.feature.catalogManaged'='supported')",
            fullTableName);
        sql("INSERT INTO %s (val) VALUES ('a'), ('b'), ('c')", fullTableName);
        assertThat(sql("SELECT id FROM %s ORDER BY id", fullTableName))
            .containsExactly(row("100"), row("101"), row("102"));

        String ucTableIdBefore = currentUcTableId(fullTableName);
        if (operation.isAsSelect()) {
          sql(
              "%s TABLE %s USING delta"
                  + " TBLPROPERTIES ('delta.feature.catalogManaged'='supported')"
                  + " AS SELECT CAST(NULL AS BIGINT) AS id, CAST('seed' AS STRING) AS val",
              operation.name().contains("CREATE_OR_REPLACE") ? "CREATE OR REPLACE" : "REPLACE",
              fullTableName);
        } else {
          sql(
              "%s TABLE %s ("
                  + "id BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 50000 INCREMENT BY 1),"
                  + "val STRING) USING delta"
                  + " TBLPROPERTIES ('delta.feature.catalogManaged'='supported')",
              operation.name().contains("CREATE_OR_REPLACE") ? "CREATE OR REPLACE" : "REPLACE",
              fullTableName);
        }
        assertThat(currentUcTableId(fullTableName))
            .as("UC table id preserved across %s", operation)
            .isEqualTo(ucTableIdBefore);

        sql("INSERT INTO %s (val) VALUES ('after_replace')", fullTableName);
        assertThat(sql("SELECT id FROM %s ORDER BY id", fullTableName))
            .as(
                "first identity value after %s must equal the new START WITH (HWM was reset)",
                operation)
            .containsExactly(row("50000"));
      } finally {
        sql("DROP TABLE IF EXISTS %s", fullTableName);
      }
    }
  }
}
