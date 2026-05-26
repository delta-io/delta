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

import io.unitycatalog.client.delta.api.TablesApi;
import io.unitycatalog.client.delta.model.LoadTableResponse;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.delta.catalog.DeltaTableV2;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import scala.collection.JavaConverters;

/** Integration test that verifies UniForm Iceberg behaviors */
public class UCDeltaTableUniformIcebergTest extends UCDeltaTableIntegrationBaseTest {

  @Override
  protected boolean useDeltaRestApiForTests() {
    // UniForm-Iceberg isn't supported by creating a table yet.
    return false;
  }

  private static final String UNIFORM_TABLE_PROPS =
      "'delta.universalFormat.enabledFormats'='iceberg', "
          + "'delta.columnMapping.mode'='name', "
          + "'delta.enableIcebergCompatV2'='true', "
          + "'delta.enableDeletionVectors'='false'";

  /**
   * Asserts that none of the in-memory-only {@code deltaUniformIceberg.*} keys are present in the
   * table's server-side properties. These keys are populated transiently in {@code
   * catalogTable.storage.properties} from the {@code LoadTableResponse.uniform} field and must
   * never be written back to the UC catalog.
   */
  private void assertNoUniformPropsOnServer(String fullTableName) throws Exception {
    String[] parts = fullTableName.split("\\.");
    TablesApi deltaApi = new TablesApi(unityCatalogInfo().createApiClient());
    LoadTableResponse resp = deltaApi.loadTable(parts[0], parts[1], parts[2]);
    for (String key : resp.getMetadata().getProperties().keySet()) {
      Assertions.assertFalse(
          key.startsWith("deltaUniformIceberg."),
          "Server-side table properties must not contain in-memory-only key: " + key);
    }
  }

  /**
   * Calls {@code loadTable} on the Delta REST API, validates the uniform fields in the response,
   * and returns the Iceberg metadata JSON file path for the given table.
   *
   * @param expectedConvertedDeltaVersion the Delta version the server should report as converted
   */
  private String verifyUCMetadataAndFetchIcebergPath(
      String fullTableName, long expectedConvertedDeltaVersion) throws Exception {
    String[] parts = fullTableName.split("\\.");
    TablesApi deltaApi = new TablesApi(unityCatalogInfo().createApiClient());
    LoadTableResponse resp = deltaApi.loadTable(parts[0], parts[1], parts[2]);
    if (resp.getUniform() == null || resp.getUniform().getIceberg() == null) {
      throw new IllegalStateException("No Iceberg metadata found for table: " + fullTableName);
    }
    var iceberg = resp.getUniform().getIceberg();
    Assertions.assertEquals(
        expectedConvertedDeltaVersion,
        iceberg.getConvertedDeltaVersion().longValue(),
        "loadTable response convertedDeltaVersion mismatch");
    Assertions.assertNotNull(
        iceberg.getConvertedDeltaTimestamp(), "loadTable response convertedDeltaTimestamp is null");
    // Validate timestamp is a valid epoch-millis value by parsing it as an Instant.
    Instant.ofEpochMilli(iceberg.getConvertedDeltaTimestamp());
    return iceberg.getMetadataLocation();
  }

  /**
   * Builds a Hadoop {@link Configuration} seeded from the Spark session (which registers {@link
   * S3CredentialFileSystem} as {@code fs.s3.impl}) and enriched with the UC-vended storage
   * credentials from the table's catalog storage properties.
   */
  private Configuration buildTestHadoopConf(String fullTableName) throws Exception {
    String[] parts = fullTableName.split("\\.");
    TableCatalog catalog = (TableCatalog) spark().sessionState().catalogManager().catalog(parts[0]);
    DeltaTableV2 table =
        (DeltaTableV2) catalog.loadTable(Identifier.of(new String[] {parts[1]}, parts[2]));
    Configuration conf = spark().sessionState().newHadoopConf();
    if (table.catalogTable().isDefined()) {
      JavaConverters.mapAsJavaMap(table.catalogTable().get().storage().properties())
          .forEach(conf::set);
    }
    return conf;
  }

  /**
   * Verifies UniForm Iceberg incremental conversion works correctly on the UC REST path (real
   * embedded UC server).
   *
   * <p>The test creates a UniForm-enabled Delta table, writes twice, and verifies:
   *
   * <ul>
   *   <li>Write 1 → full Iceberg conversion: no {@code base-delta-version} table property.
   *   <li>Write 2 → incremental conversion: {@code base-delta-version} equals the prior converted
   *       Delta version, and the Iceberg snapshot chain is preserved.
   * </ul>
   */
  @Test
  public void uniformIcebergIncrementalConversion() throws Exception {
    Assumptions.assumeTrue(
        Boolean.getBoolean("supportIceberg"),
        "Skipping: Iceberg support not available for this Spark version");
    withNewTable(
        "uniform_iceberg",
        "id INT, data STRING",
        null,
        TableType.MANAGED,
        UNIFORM_TABLE_PROPS,
        fullTableName -> {
          // Write 1 — full Iceberg conversion (no prior Iceberg state)
          sql("INSERT INTO %s VALUES (1, 'a')", fullTableName);
          check(fullTableName, List.of(row("1", "a")));
          assertNoUniformPropsOnServer(fullTableName);
          IcebergMeta meta1 = verifyNonIncrementalUniForm(fullTableName, 1L);

          // Write 2 — must produce an incremental conversion on the Delta REST API path
          sql("INSERT INTO %s VALUES (2, 'b')", fullTableName);
          check(fullTableName, List.of(row("1", "a"), row("2", "b")));
          assertNoUniformPropsOnServer(fullTableName);
          verifyIncrementalUniForm(fullTableName, 2L, 1L, meta1.currentSnapshotId);
        });
  }

  /**
   * Verifies that {@code RESTORE TABLE} (which internally uses {@code commitLarge}) produces an
   * incremental Iceberg conversion on the UC REST path.
   *
   * <p>The test writes two versions, then restores to version 1. At restore time the current
   * snapshot already carries Iceberg metadata (from the version-2 conversion), so {@code
   * commitLarge} performs an incremental conversion and the Iceberg snapshot chain is preserved.
   *
   * <p>Note: {@code CREATE OR REPLACE TABLE ... SHALLOW CLONE} on a UC-managed table is currently
   * blocked by the UC-managed metadata-change guard (Delta tracks {@code REPLACE TABLE} as a
   * metadata change). CLONE-based {@code commitLarge} tests will be added once that restriction is
   * lifted.
   */
  @Test
  public void uniformIcebergCommitLargeConversion() throws Exception {
    Assumptions.assumeTrue(
        Boolean.getBoolean("supportIceberg"),
        "Skipping: Iceberg support not available for this Spark version");
    withNewTable(
        "uniform_iceberg_restore",
        "id INT, data STRING",
        null,
        TableType.MANAGED,
        UNIFORM_TABLE_PROPS,
        fullTableName -> {
          // Write 1 — non-incremental Iceberg conversion (no prior state)
          sql("INSERT INTO %s VALUES (1, 'a')", fullTableName);
          assertNoUniformPropsOnServer(fullTableName);
          IcebergMeta meta1 = verifyNonIncrementalUniForm(fullTableName, 1L);

          // Write 2 — regular incremental conversion
          sql("INSERT INTO %s VALUES (2, 'b')", fullTableName);
          assertNoUniformPropsOnServer(fullTableName);
          IcebergMeta meta2 =
              verifyIncrementalUniForm(fullTableName, 2L, 1L, meta1.currentSnapshotId);

          // RESTORE to version 1 → commitLarge, incremental.
          // INSERT did not change schema or configuration, so RESTORE passes the UC-managed
          // metadata guard. readSnapshot (v2) carries Iceberg metadata → incremental.
          sql("RESTORE TABLE %s TO VERSION AS OF 1", fullTableName);
          assertNoUniformPropsOnServer(fullTableName);
          long restoreVersion = currentVersion(fullTableName);
          verifyIncrementalUniForm(fullTableName, restoreVersion, 2L, meta2.currentSnapshotId);
        });
  }

  /**
   * Verifies that {@code CREATE OR REPLACE TABLE ... SHALLOW CLONE} on a UC-managed UniForm table
   * is currently rejected with {@code DELTA_OPERATION_NOT_ALLOWED}.
   *
   * <p>{@code REPLACE TABLE} via CLONE always copies the source's column-mapping physical-name
   * UUIDs, which differ from the target's, triggering the UC-managed metadata-change guard in
   * {@code throwIfUCManagedMetadataChanged}. This test documents the current behavior; it should be
   * updated to assert success once the restriction is lifted.
   */
  @Test
  public void uniformIcebergCloneReplaceBlocked() throws Exception {
    Assumptions.assumeTrue(
        Boolean.getBoolean("supportIceberg"),
        "Skipping: Iceberg support not available for this Spark version");
    String sourceFullName = fullTableName("cl_blocked_source");
    withNewTable(
        "cl_blocked_target",
        "col1 INT",
        null,
        TableType.MANAGED,
        UNIFORM_TABLE_PROPS,
        targetFullName -> {
          sql(
              "CREATE TABLE %s (col1 INT) USING DELTA TBLPROPERTIES ("
                  + "'delta.feature.catalogManaged'='supported', "
                  + "'delta.columnMapping.mode'='name')",
              sourceFullName);
          try {
            sql("INSERT INTO %s VALUES (1), (2), (3)", sourceFullName);
            assertThrowsWithCauseContaining(
                "Metadata changes on Unity Catalog managed tables",
                () ->
                    sql(
                        "CREATE OR REPLACE TABLE %s SHALLOW CLONE %s TBLPROPERTIES ("
                            + "'delta.enableIcebergCompatV2'='true', "
                            + "'delta.universalFormat.enabledFormats'='iceberg', "
                            + "'delta.enableDeletionVectors'='false')",
                        targetFullName, sourceFullName));
          } finally {
            sql("DROP TABLE IF EXISTS %s", sourceFullName);
          }
        });
  }

  // ---------- UniForm assertion helpers ----------

  /**
   * Verifies a non-incremental UniForm Iceberg conversion: {@code delta-version} matches the
   * expected version, {@code base-delta-version} is absent, and a snapshot exists.
   */
  private IcebergMeta verifyNonIncrementalUniForm(String fullTableName, long expectedDeltaVersion)
      throws Exception {
    IcebergMeta meta = verifyUCMetadataAndReadIceberg(fullTableName, expectedDeltaVersion);
    Assertions.assertEquals(
        String.valueOf(expectedDeltaVersion),
        meta.properties.get("delta-version"),
        "delta-version mismatch after non-incremental conversion");
    Assertions.assertNull(
        meta.properties.get("base-delta-version"),
        "Non-incremental conversion must not set base-delta-version");
    Assertions.assertTrue(
        meta.currentSnapshotId != -1L, "Iceberg snapshot should exist after conversion");
    return meta;
  }

  /**
   * Verifies an incremental UniForm Iceberg conversion: {@code delta-version} matches, {@code
   * base-delta-version} equals {@code expectedBaseDeltaVersion}, and the Iceberg snapshot chain is
   * preserved ({@code currentSnapshotParentId} equals {@code expectedParentSnapshotId}).
   */
  private IcebergMeta verifyIncrementalUniForm(
      String fullTableName,
      long expectedDeltaVersion,
      long expectedBaseDeltaVersion,
      long expectedParentSnapshotId)
      throws Exception {
    IcebergMeta meta = verifyUCMetadataAndReadIceberg(fullTableName, expectedDeltaVersion);
    Assertions.assertEquals(
        String.valueOf(expectedDeltaVersion),
        meta.properties.get("delta-version"),
        "delta-version mismatch after incremental conversion");
    Assertions.assertEquals(
        String.valueOf(expectedBaseDeltaVersion),
        meta.properties.get("base-delta-version"),
        "Incremental conversion: base-delta-version should equal prior converted version");
    Assertions.assertEquals(
        expectedParentSnapshotId,
        meta.currentSnapshotParentId,
        "Iceberg snapshot chain must be preserved: parent snapshot ID mismatch");
    return meta;
  }

  // ---------- Iceberg metadata reading ----------

  private static final class IcebergMeta {
    final Map<String, String> properties;
    final long currentSnapshotId;
    final long currentSnapshotParentId;

    IcebergMeta(
        Map<String, String> properties, long currentSnapshotId, long currentSnapshotParentId) {
      this.properties = properties;
      this.currentSnapshotId = currentSnapshotId;
      this.currentSnapshotParentId = currentSnapshotParentId;
    }
  }

  private IcebergMeta verifyUCMetadataAndReadIceberg(
      String fullTableName, long expectedConvertedDeltaVersion) throws Exception {
    String icebergMetadataPath =
        verifyUCMetadataAndFetchIcebergPath(fullTableName, expectedConvertedDeltaVersion);
    Configuration conf = buildTestHadoopConf(fullTableName);
    return readIcebergMetaViaHadoopTables(icebergMetadataPath, conf);
  }

  private IcebergMeta readIcebergMetaViaHadoopTables(String icebergMetadataPath, Configuration conf)
      throws Exception {
    HadoopTables tables = new HadoopTables(conf);
    Table table = tables.load(icebergMetadataPath);

    Map<String, String> properties = new HashMap<>(table.properties());

    Snapshot current = table.currentSnapshot();
    long currentSnapshotId = current != null ? current.snapshotId() : -1L;
    long parentSnapshotId =
        (current != null && current.parentId() != null) ? current.parentId() : -1L;

    return new IcebergMeta(properties, currentSnapshotId, parentSnapshotId);
  }
}
