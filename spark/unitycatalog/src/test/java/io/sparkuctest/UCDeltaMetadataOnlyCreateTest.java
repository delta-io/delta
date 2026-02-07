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

import java.io.File;
import java.net.URI;
import java.util.List;
import java.util.UUID;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * POC test: metadata-only CREATE TABLE through DSv2 Kernel-backed flow with STRICT mode.
 *
 * <p>Enables {@code spark.databricks.delta.v2.enableMode=STRICT} so that CREATE TABLE DDL is routed
 * through the Kernel-based V2 path in {@code DeltaCatalog.createTableV2()}.
 *
 * <p>Verifies that SQL {@code CREATE TABLE (...) USING DELTA} produces a valid Delta log commit
 * ({@code _delta_log/00000000000000000000.json}) without writing any data files, and that the
 * resulting table is queryable via the V2 SparkTable connector.
 */
public class UCDeltaMetadataOnlyCreateTest extends UCDeltaTableIntegrationBaseTest {

  private static final Logger LOG = Logger.getLogger(UCDeltaMetadataOnlyCreateTest.class);

  /** Enable STRICT mode so DDL routes through the Kernel-based V2 path. */
  @BeforeAll
  public void enableStrictMode() {
    spark().conf().set("spark.databricks.delta.v2.enableMode", "STRICT");
  }

  /**
   * Simplest possible POC: external table CREATE TABLE with explicit LOCATION, then verify the
   * commit 0 JSON file exists on the filesystem.
   */
  @Test
  public void testExternalCreateTableProducesCommitZero() throws Exception {
    UnityCatalogInfo uc = unityCatalogInfo();
    String tableName = "metadata_only_ext_" + UUID.randomUUID().toString().replace("-", "");
    String fullTableName = uc.catalogName() + "." + uc.schemaName() + "." + tableName;

    withTempDir(
        (Path dir) -> {
          Path tablePath = new Path(dir, tableName);
          try {
            // Metadata-only CREATE TABLE — no data, just schema
            sql(
                "CREATE TABLE %s (id BIGINT, name STRING) USING DELTA LOCATION '%s'",
                fullTableName, tablePath.toString());

            // Assert: _delta_log/00000000000000000000.json must exist
            File tableRoot = new File(new URI(tablePath.toString()));
            File deltaLog = new File(tableRoot, "_delta_log/00000000000000000000.json");
            LOG.info("Checking for commit 0 at: " + deltaLog.getAbsolutePath());
            assertThat(deltaLog)
                .as("Commit 0 JSON must exist after metadata-only CREATE TABLE")
                .exists()
                .isFile();
            assertThat(deltaLog.length()).as("Commit 0 JSON must not be empty").isGreaterThan(0);

            // Assert: no data files written (only _delta_log directory should exist)
            File[] topLevelEntries = tableRoot.listFiles();
            assertThat(topLevelEntries).isNotNull();
            List<String> entryNames =
                java.util.Arrays.stream(topLevelEntries)
                    .map(File::getName)
                    .collect(java.util.stream.Collectors.toList());
            LOG.info("Table root contents: " + entryNames);
            assertThat(entryNames)
                .as("Only _delta_log should exist — no data files for metadata-only create")
                .containsExactly("_delta_log");

            // Assert: table is queryable and returns zero rows (metadata-only, no data)
            List<List<String>> rows = sql("SELECT * FROM %s", fullTableName);
            assertThat(rows).as("Metadata-only table should have zero rows").isEmpty();

            // Assert: schema is visible via DESCRIBE
            List<List<String>> desc = sql("DESCRIBE %s", fullTableName);
            List<String> columnNames =
                desc.stream()
                    .map(row -> row.get(0))
                    .filter(name -> !name.isEmpty() && !name.startsWith("#"))
                    .collect(java.util.stream.Collectors.toList());
            assertThat(columnNames).contains("id", "name");

            LOG.info("POC PASSED: metadata-only CREATE TABLE produced valid commit 0");
          } finally {
            sql("DROP TABLE IF EXISTS %s", fullTableName);
          }
        });
  }

  /**
   * Managed table variant: UC allocates the location. Verify commit 0 exists at the
   * catalog-reported storage location.
   */
  @Test
  public void testManagedCreateTableProducesCommitZero() throws Exception {
    UnityCatalogInfo uc = unityCatalogInfo();
    String tableName = "metadata_only_mgd_" + UUID.randomUUID().toString().replace("-", "");
    String fullTableName = uc.catalogName() + "." + uc.schemaName() + "." + tableName;

    try {
      // Managed table requires catalogManaged property
      sql(
          "CREATE TABLE %s (id BIGINT, name STRING) USING DELTA "
              + "TBLPROPERTIES ('delta.feature.catalogManaged'='supported')",
          fullTableName);

      // Get the storage location from DESC EXTENDED
      List<List<String>> descRows = sql("DESC EXTENDED %s", fullTableName);
      String location = null;
      for (List<String> row : descRows) {
        if ("Location".equals(row.get(0).trim())) {
          location = row.get(1).trim();
          break;
        }
      }
      assertThat(location)
          .as("DESC EXTENDED must report a Location for the managed table")
          .isNotNull()
          .isNotEmpty();
      LOG.info("Managed table location: " + location);

      // Assert: commit 0 exists at the reported location
      File tableDir = new File(new URI(location));
      File deltaLog = new File(tableDir, "_delta_log/00000000000000000000.json");
      LOG.info("Checking for commit 0 at: " + deltaLog.getAbsolutePath());
      assertThat(deltaLog)
          .as("Commit 0 JSON must exist after managed metadata-only CREATE TABLE")
          .exists()
          .isFile();
      assertThat(deltaLog.length()).as("Commit 0 JSON must not be empty").isGreaterThan(0);

      // Assert: table is queryable and returns zero rows
      List<List<String>> rows = sql("SELECT * FROM %s", fullTableName);
      assertThat(rows).as("Metadata-only table should have zero rows").isEmpty();

      LOG.info("POC PASSED: managed metadata-only CREATE TABLE produced valid commit 0");
    } finally {
      sql("DROP TABLE IF EXISTS %s", fullTableName);
    }
  }
}
