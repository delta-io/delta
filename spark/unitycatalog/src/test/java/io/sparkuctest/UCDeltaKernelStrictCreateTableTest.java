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

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.api.TablesApi;
import io.unitycatalog.client.model.DataSourceFormat;
import io.unitycatalog.client.model.TableInfo;
import java.util.List;
import java.util.UUID;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests regular CREATE TABLE in DSv2 STRICT mode against Unity Catalog.
 *
 * <p>When {@code spark.databricks.delta.v2.enableMode=STRICT}, the DeltaCatalog kernel-based path
 * is activated for {@code spark_catalog} tables. This test verifies that STRICT mode does not break
 * UC table creation and that both managed and external tables are correctly registered in Unity
 * Catalog.
 *
 * <p>Runs against a local embedded UC server by default, or a remote server when {@code
 * UC_REMOTE=true} is set.
 */
public class UCDeltaKernelStrictCreateTableTest extends UCDeltaTableIntegrationBaseTest {

  private static final Logger LOG = Logger.getLogger(UCDeltaKernelStrictCreateTableTest.class);

  private static final String DELTA_CATALOG_MANAGED_KEY = "delta.feature.catalogManaged";
  private static final String SUPPORTED = "supported";

  private String tempDir;
  private String managedTable;
  private String externalTable;

  /**
   * Override the SparkSession setup to enable STRICT mode. This sets {@code
   * spark.databricks.delta.v2.enableMode=STRICT} so that DeltaCatalog returns SparkTable (V2
   * connector) instead of DeltaTableV2 (V1 connector).
   */
  @BeforeAll
  @Override
  public void setUpSpark() {
    SparkConf conf =
        new SparkConf()
            .setAppName("UC Kernel STRICT CREATE TABLE Tests")
            .setMaster("local[2]")
            .set("spark.ui.enabled", "false")
            // Delta Lake required configurations
            .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .set(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            // Enable STRICT mode: kernel-based V2 connector for all DeltaCatalog operations
            .set("spark.databricks.delta.v2.enableMode", "STRICT");

    // Configure with Unity Catalog
    UnityCatalogInfo uc = unityCatalogInfo();
    String catalogName = uc.catalogName();
    conf.set("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
    conf.set("spark.sql.catalog." + catalogName, "io.unitycatalog.spark.UCSingleCatalog");
    conf.set("spark.sql.catalog." + catalogName + ".uri", uc.serverUri());
    conf.set("spark.sql.catalog." + catalogName + ".token", uc.serverToken());

    SparkSession session = SparkSession.builder().config(conf).getOrCreate();
    // Store the SparkSession in the parent field via reflection-free approach:
    // The parent class provides spark() accessor, but we need to set the field.
    // Since the parent setUpSpark() creates the session, we override entirely.
    setSparkSession(session);
  }

  @BeforeEach
  public void setUp() {
    UnityCatalogInfo uc = unityCatalogInfo();
    // Spark-safe random suffix: no hyphens in identifiers
    String suffix = UUID.randomUUID().toString().replace("-", "").substring(0, 12);
    tempDir = uc.baseTableLocation() + "/strict_temp_" + suffix;
    managedTable = null;
    externalTable = null;
  }

  @AfterEach
  public void cleanUp() {
    if (managedTable != null) {
      try {
        sql("DROP TABLE IF EXISTS %s", managedTable);
      } catch (Exception e) {
        LOG.warn("Failed to drop managed table: " + managedTable, e);
      }
    }
    if (externalTable != null) {
      try {
        sql("DROP TABLE IF EXISTS %s", externalTable);
      } catch (Exception e) {
        LOG.warn("Failed to drop external table: " + externalTable, e);
      }
    }
  }

  /**
   * Validates that CREATE TABLE in STRICT mode succeeds for both managed and external UC tables.
   *
   * <p>For each table type, this test:
   *
   * <ol>
   *   <li>Creates the table via the UC catalog.
   *   <li>Verifies basic read/write round-trip.
   *   <li>Verifies UC table metadata (table type, data source format).
   *   <li>Verifies commit history is present via DESCRIBE HISTORY.
   * </ol>
   */
  @Test
  public void testStrictCreateTable() throws Exception {
    UnityCatalogInfo uc = unityCatalogInfo();
    String catalogName = uc.catalogName();
    String schemaName = uc.schemaName();
    String suffix = UUID.randomUUID().toString().replace("-", "").substring(0, 12);

    // --- Managed table ---
    String managedName = "strict_managed_" + suffix;
    managedTable = String.format("%s.%s.%s", catalogName, schemaName, managedName);
    LOG.info("Creating managed table in STRICT mode: " + managedTable);

    sql(
        "CREATE TABLE %s (id INT, name STRING) USING DELTA TBLPROPERTIES ('%s'='%s')",
        managedTable, DELTA_CATALOG_MANAGED_KEY, SUPPORTED);

    // Verify read/write round-trip
    sql("INSERT INTO %s VALUES (1, 'alice')", managedTable);
    check(managedTable, List.of(List.of("1", "alice")));

    // Verify UC metadata
    assertUCTableMetadata(managedTable, "MANAGED");

    // Verify commit history
    assertCommitHistoryExists(managedTable);

    // --- External table ---
    String externalName = "strict_external_" + suffix;
    externalTable = String.format("%s.%s.%s", catalogName, schemaName, externalName);
    String externalLocation = tempDir + "/" + externalName;
    LOG.info("Creating external table in STRICT mode: " + externalTable);

    sql(
        "CREATE TABLE %s (id INT, name STRING) USING DELTA LOCATION '%s'",
        externalTable, externalLocation);

    // Verify read/write round-trip
    sql("INSERT INTO %s VALUES (2, 'bob')", externalTable);
    check(externalTable, List.of(List.of("2", "bob")));

    // Verify UC metadata
    assertUCTableMetadata(externalTable, "EXTERNAL");

    // Verify commit history
    assertCommitHistoryExists(externalTable);
  }

  // ---------------------------------------------------------------------------
  // Assertion helpers
  // ---------------------------------------------------------------------------

  /**
   * Verifies Unity Catalog metadata for the given table. Checks table type, data source format, and
   * that the table is queryable via the UC API.
   */
  private void assertUCTableMetadata(String fullTableName, String expectedTableType)
      throws ApiException {
    UnityCatalogInfo uc = unityCatalogInfo();
    TablesApi tablesApi = new TablesApi(uc.createApiClient());
    TableInfo tableInfo = tablesApi.getTable(fullTableName, false, false);

    assertThat(tableInfo.getTableType().name())
        .as("UC table type for " + fullTableName)
        .isEqualTo(expectedTableType);
    assertThat(tableInfo.getDataSourceFormat().name())
        .as("UC data source format for " + fullTableName)
        .isEqualTo(DataSourceFormat.DELTA.name());

    LOG.info(
        String.format(
            "UC metadata OK for %s: type=%s, format=%s",
            fullTableName, tableInfo.getTableType(), tableInfo.getDataSourceFormat()));
  }

  /**
   * Verifies that DESCRIBE HISTORY returns at least the CREATE TABLE commit (version 0) and that
   * the engineInfo field is present.
   */
  private void assertCommitHistoryExists(String fullTableName) {
    List<List<String>> history = sql("DESCRIBE HISTORY %s", fullTableName);
    assertThat(history).as("DESCRIBE HISTORY for " + fullTableName).isNotEmpty();

    // First row is the latest commit. For a freshly created table with one insert,
    // there should be at least 2 versions: 0 (CREATE TABLE) and 1 (WRITE).
    assertThat(history.size())
        .as("Expected at least 2 history entries (CREATE + INSERT)")
        .isGreaterThanOrEqualTo(2);

    // Version 0 is the last row (history is ordered newest-first).
    List<String> createCommit = history.get(history.size() - 1);
    assertThat(createCommit.get(0)).as("Version of first commit").isEqualTo("0");
    assertThat(createCommit.get(4)).as("Operation of first commit").isEqualTo("CREATE TABLE");

    // engineInfo is at index 14 in DESCRIBE HISTORY output.
    // Log the value for diagnostic purposes; the exact value depends on which
    // catalog path handled the creation.
    if (createCommit.size() > 14) {
      String engineInfo = createCommit.get(14);
      LOG.info("engineInfo for " + fullTableName + " version 0: " + engineInfo);
      assertThat(engineInfo)
          .as("engineInfo should be non-null for " + fullTableName)
          .isNotNull()
          .isNotEqualTo("null");
    }
  }

  /**
   * Sets the SparkSession on the parent class. The parent class stores the session in a private
   * field that we need to populate since we override setUpSpark() entirely.
   */
  private void setSparkSession(SparkSession session) {
    // Use reflection to set the private sparkSession field in the parent class.
    try {
      java.lang.reflect.Field field =
          UCDeltaTableIntegrationBaseTest.class.getDeclaredField("sparkSession");
      field.setAccessible(true);
      field.set(this, session);
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException("Failed to set SparkSession on parent class", e);
    }
  }
}
