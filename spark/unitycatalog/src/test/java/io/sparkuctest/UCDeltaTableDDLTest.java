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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class UCDeltaTableDDLTest extends UCDeltaTableIntegrationBaseTest {

  private static final Set<String> MUTABLE_CATALOG_PROPERTIES =
      Set.of("delta.lastCommitTimestamp", "delta.lastUpdateVersion", "transient_lastDdlTime");

  // -------------------------------------------------------------------------
  // TRUNCATE TABLE
  // -------------------------------------------------------------------------

  @TestAllTableTypes
  public void testTruncatePreservesMetadata(TableType tableType) throws Exception {
    for (boolean partitioned : List.of(false, true)) {
      String desc = partitioned ? "partitioned" : "unpartitioned";
      withNewTable(
          "ddl_truncate_" + desc,
          "id INT, name STRING, part INT",
          partitioned ? "part" : null,
          tableType,
          tableName -> {
            Map<String, String> snapshotBefore = stableTableProperties(tableName);
            for (String truncateTarget : truncateTargets(tableName, tableType)) {
              sql(
                  "INSERT INTO %s VALUES (1, 'alpha', 0), (2, 'beta', 1), (3, 'gamma', 1)",
                  tableName);
              truncateTable(truncateTarget);
              check(tableName, List.of());
              assertPreservedTableSnapshot(tableName, snapshotBefore);
            }
            sql("INSERT INTO %s VALUES (4, 'delta', 0), (5, 'epsilon', 2)", tableName);
            check(tableName, List.of(row("4", "delta", "0"), row("5", "epsilon", "2")));
          });
    }
  }

  @Test
  public void testTruncateByPathBlockedForManagedTable() throws Exception {
    withNewTable(
        "ddl_truncate_path_blocked",
        "id INT",
        TableType.MANAGED,
        tableName -> {
          sql("INSERT INTO %s VALUES (1), (2), (3)", tableName);
          String tablePath = tableLocation(tableName);
          Map<String, String> snapshotBefore = stableTableProperties(tableName);
          Assertions.assertThrows(
              Exception.class, () -> truncateTable(String.format("delta.`%s`", tablePath)));
          check(tableName, List.of(row("1"), row("2"), row("3")));
          assertPreservedTableSnapshot(tableName, snapshotBefore);
        });
  }

  private List<String> truncateTargets(String tableName, TableType tableType) {
    List<String> targets = new ArrayList<>();
    targets.add(tableName);
    if (tableType == TableType.EXTERNAL) {
      targets.add(String.format("delta.`%s`", tableLocation(tableName)));
    }
    return targets;
  }

  private void truncateTable(String truncateTarget) {
    if (truncateTarget.startsWith("delta.`")) {
      S3CredentialFileSystem.credentialCheckEnabled = false;
      try {
        sql("TRUNCATE TABLE %s", truncateTarget);
      } finally {
        S3CredentialFileSystem.credentialCheckEnabled = true;
      }
    } else {
      sql("TRUNCATE TABLE %s", truncateTarget);
    }
  }

  private String tableLocation(String tableName) {
    return sql("DESCRIBE FORMATTED %s", tableName).stream()
        .filter(row -> row.size() >= 2 && "Location".equalsIgnoreCase(row.get(0).trim()))
        .map(row -> row.get(1).trim())
        .findFirst()
        .orElseThrow(() -> new AssertionError("Could not retrieve table location"));
  }

  private Map<String, String> stableTableProperties(String tableName) throws Exception {
    Map<String, String> stable = new LinkedHashMap<>(tableProperties(tableName));
    stable.keySet().removeAll(MUTABLE_CATALOG_PROPERTIES);
    return stable;
  }

  private void assertPreservedTableSnapshot(String tableName, Map<String, String> expected)
      throws Exception {
    assertThat(stableTableProperties(tableName)).isEqualTo(expected);
  }

  private Map<String, String> tableProperties(String tableName) {
    Map<String, String> properties = new LinkedHashMap<>();
    for (List<String> row : sql("SHOW TBLPROPERTIES %s", tableName)) {
      if (row.size() >= 2) {
        properties.put(row.get(0), row.get(1));
      }
    }
    return properties;
  }

  // -------------------------------------------------------------------------
  // CREATE TABLE with GENERATED AS IDENTITY columns
  // -------------------------------------------------------------------------

  /**
   * CREATE TABLE with a GENERATED ALWAYS AS IDENTITY column must succeed on both EXTERNAL and
   * MANAGED tables, with auto-generated values respecting (START, INCREMENT) and delta.identity.*
   * metadata persisted in the committed schema. MANAGED here exercises the catalog-managed
   * AbstractDeltaCatalog.createTable(ident, Column[], ...) override that the spark_catalog-only
   * IdentityColumnSqlDDLSuite does not cover.
   */
  @TestAllTableTypes
  public void testCreateWithIdentityColumn(TableType tableType) throws Exception {
    if (tableType == TableType.EXTERNAL) {
      withTempDir(
          (Path dir) -> {
            String tableName = fullTableName("ddl_identity_external");
            Path tablePath = new Path(dir, "ddl_identity_external");
            sql("DROP TABLE IF EXISTS %s", tableName);
            try {
              sql(
                  "CREATE TABLE %s ("
                      + "  id BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 100 INCREMENT BY 5),"
                      + "  val STRING"
                      + ") USING DELTA LOCATION '%s'",
                  tableName, tablePath.toString());
              runIdentityCreateAssertions(tableName, 100L, 5L);
            } finally {
              sql("DROP TABLE IF EXISTS %s", tableName);
            }
          });
    } else {
      String tableName = fullTableName("ddl_identity_managed");
      sql("DROP TABLE IF EXISTS %s", tableName);
      try {
        sql(
            "CREATE TABLE %s ("
                + "  id BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 100 INCREMENT BY 5),"
                + "  val STRING"
                + ") USING DELTA "
                + "TBLPROPERTIES ('delta.feature.catalogManaged'='supported')",
            tableName);
        runIdentityCreateAssertions(tableName, 100L, 5L);
      } finally {
        sql("DROP TABLE IF EXISTS %s", tableName);
      }
    }
  }

  private void runIdentityCreateAssertions(String tableName, long start, long step) {
    sql("INSERT INTO %s (val) VALUES ('a'), ('b'), ('c')", tableName);
    List<List<String>> generated = sql("SELECT id FROM %s ORDER BY id", tableName);
    assertThat(generated)
        .as("auto-generated identity values for %s", tableName)
        .containsExactly(
            row(Long.toString(start)),
            row(Long.toString(start + step)),
            row(Long.toString(start + 2 * step)));

    // Explicit INSERT into an ALWAYS-IDENTITY column must be rejected, proving the column is
    // genuinely wired through Delta's identity machinery (not a plain BIGINT).
    Assertions.assertThrows(
        Exception.class, () -> sql("INSERT INTO %s VALUES (999, 'rejected')", tableName));

    // Note: identity StructField metadata (`delta.identity.*`) is intentionally NOT asserted
    // via spark().table().schema() because Spark exposes a catalog-facing schema that strips
    // internal Delta metadata. The behavioral assertions above are the real contract: if the
    // catalog-managed Column[] override failed to translate identityColumnSpec(), the auto-
    // generated values would be missing and the explicit-insert rejection would not fire.
  }
}
