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

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
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

  private static final String METADATA_CHANGE_ERROR =
      "Metadata changes on Unity Catalog managed tables";

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

  // The table was created with CDF + type widening enabled. The REPLACE statement omits all
  // TBLPROPERTIES, so those non-default features are not restated. Replace semantics rejects this
  // as a potential unintended feature downgrade.
  @Test
  public void testMissingNonDefaultFeatureRestatementIsRejectedForManagedReplaceOperations()
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

            assertRejectedReplacePreservesTable(
                operation,
                fullTableName,
                buildStatement(
                    operation, fullTableName, "i INT, s STRING", "", null, "2 AS i, 'new' AS s"),
                METADATA_CHANGE_ERROR);
          });
    }
  }

  // Same as above, but only CDF is restated — type widening is omitted. Partial restatement is
  // still rejected because the replaced table would lose the type widening feature.
  @Test
  public void testPartialNonDefaultFeatureRestatementIsRejectedForManagedReplaceOperations()
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

            assertRejectedReplacePreservesTable(
                operation,
                fullTableName,
                buildStatement(
                    operation,
                    fullTableName,
                    "i INT, s STRING",
                    PARTIAL_NON_DEFAULT_RESTATEMENT,
                    null,
                    "2 AS i, 'new' AS s"),
                METADATA_CHANGE_ERROR);
          });
    }
  }

  @Test
  public void testCommentChangeIsRejectedForManagedReplaceOperations() throws Exception {
    for (ReplaceOperation operation : ReplaceOperation.values()) {
      String tableName = uniqueTableName("comment_change", operation);
      withNewTable(
          tableName,
          "i INT, s STRING",
          TableType.MANAGED,
          fullTableName -> {
            sql("INSERT INTO %s VALUES (1, 'old')", fullTableName);

            assertRejectedReplacePreservesTable(
                operation,
                fullTableName,
                buildStatement(
                    operation,
                    fullTableName,
                    "i INT, s STRING",
                    DEFAULT_FEATURES_RESTATEMENT,
                    "new description",
                    "2 AS i, 'new' AS s"),
                METADATA_CHANGE_ERROR);
          });
    }
  }

  @Test
  public void testUserPropertyChangeIsRejectedForManagedReplaceOperations() throws Exception {
    for (ReplaceOperation operation : ReplaceOperation.values()) {
      String tableName = uniqueTableName("property_change", operation);
      withNewTable(
          tableName,
          "i INT, s STRING",
          TableType.MANAGED,
          fullTableName -> {
            sql("INSERT INTO %s VALUES (1, 'old')", fullTableName);

            assertRejectedReplacePreservesTable(
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
                    "2 AS i, 'new' AS s"),
                METADATA_CHANGE_ERROR);
          });
    }
  }

  // Schema change (adding a column) during REPLACE is rejected because the replaced table would
  // have a different schema than the original, which is a metadata change.
  @Test
  public void testSchemaChangeIsRejectedForManagedReplaceOperations() throws Exception {
    for (ReplaceOperation operation : ReplaceOperation.values()) {
      String tableName = uniqueTableName("schema_change", operation);
      withNewTable(
          tableName,
          "i INT, s STRING",
          TableType.MANAGED,
          fullTableName -> {
            sql("INSERT INTO %s VALUES (1, 'old')", fullTableName);

            assertRejectedReplacePreservesTable(
                operation,
                fullTableName,
                buildStatement(
                    operation,
                    fullTableName,
                    "i INT, s STRING, extra INT",
                    DEFAULT_FEATURES_RESTATEMENT,
                    null,
                    "2 AS i, 'new' AS s, 3 AS extra"),
                METADATA_CHANGE_ERROR);
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

  private void assertRejectedReplacePreservesTable(
      ReplaceOperation operation, String tableName, String statement, String expectedError)
      throws Exception {
    String ucTableIdBeforeReplace = currentUcTableId(tableName);
    long versionBeforeReplace = currentVersion(tableName);

    assertThrowsWithCauseContaining(expectedError, () -> sql(statement));

    assertThat(currentUcTableId(tableName))
        .as("ucTableId after rejected %s on %s", operation, tableName)
        .isEqualTo(ucTableIdBeforeReplace);
    assertThat(currentVersion(tableName))
        .as("version after rejected %s on %s", operation, tableName)
        .isEqualTo(versionBeforeReplace);
    assertThat(sql("SELECT COUNT(*) FROM %s", tableName))
        .as("row count after rejected %s on %s", operation, tableName)
        .containsExactly(row("1"));
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
}
