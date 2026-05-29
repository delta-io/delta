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
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.hadoop.fs.Path;
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
  // succeeds at the data plane (the table is queryable with the new schema, INSERTs land
  // correctly), but UC's metastore retains the seed's `clusteringColumns` and
  // `delta.feature.clustering` properties. The REPLACE does not propagate the cluster
  // removal to UC's domain metadata. Pinned so we notice if the behavior changes; when
  // fixed, update the assertion to reflect the cleared state.
  @Test
  public void testReplaceClusteredManagedTableWithNoneRetainsStaleUcClustering() throws Exception {
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
          .containsEntry("clusteringColumns", "[[\"col1\"]]")
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
}
