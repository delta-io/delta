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

import io.unitycatalog.client.delta.api.DeltaTablesApi;
import io.unitycatalog.client.delta.model.DeltaLoadTableResponse;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
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
    return true;
  }

  @Override
  protected Properties serverProperties() {
    Properties props = super.serverProperties();
    props.setProperty("server.managed-table.uniform-iceberg-v2.allow-missing-dv", "true");
    return props;
  }

  private static final String UNIFORM_TABLE_PROPS =
      "'delta.universalFormat.enabledFormats'='iceberg', "
          + "'delta.columnMapping.mode'='name', "
          + "'delta.enableIcebergCompatV2'='true', "
          + "'delta.enableDeletionVectors'='false'";

  /**
   * Asserts that none of the in-memory-only {@code deltaUniformIceberg.*} keys are present in the
   * table's server-side properties. These keys are populated transiently in {@code
   * catalogTable.storage.properties} from the {@code DeltaLoadTableResponse.uniform} field and must
   * never be written back to the UC catalog.
   */
  private void assertNoUniformPropsOnServer(String fullTableName) throws Exception {
    String[] parts = fullTableName.split("\\.");
    DeltaTablesApi deltaApi = new DeltaTablesApi(unityCatalogInfo().createApiClient());
    DeltaLoadTableResponse resp = deltaApi.loadTable(parts[0], parts[1], parts[2]);
    for (String key : resp.getMetadata().getProperties().keySet()) {
      Assertions.assertFalse(
          key.startsWith("deltaUniformIceberg."),
          "Server-side table properties must not contain in-memory-only key: " + key);
    }
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
   * Verifies a non-incremental UniForm Iceberg conversion: converted {@code delta-version} matches
   * the expected version and {@code base-delta-version} is absent.
   *
   * @param expectSnapshot whether to assert that an Iceberg current snapshot exists. Pass {@code
   *     true} for CTAS or any write that produces data files (snapshot must exist); pass {@code
   *     false} for an empty {@code CREATE TABLE} where the initial Iceberg state has no snapshot.
   */
  private IcebergMeta verifyNonIncrementalUniForm(
      String fullTableName, long expectedDeltaVersion, boolean expectSnapshot) throws Exception {
    IcebergMeta meta = verifyUCMetadataAndReadIceberg(fullTableName, expectedDeltaVersion);
    Assertions.assertEquals(
        String.valueOf(expectedDeltaVersion),
        meta.properties.get("delta-version"),
        "delta-version mismatch after non-incremental conversion");
    Assertions.assertNull(
        meta.properties.get("base-delta-version"),
        "Non-incremental conversion must not set base-delta-version");
    if (expectSnapshot) {
      Assertions.assertTrue(
          meta.currentSnapshotId != -1L, "Iceberg snapshot should exist after conversion");
    }
    return meta;
  }

  /**
   * Verifies an incremental UniForm Iceberg conversion: converted {@code delta-version} matches,
   * {@code base-delta-version} equals {@code expectedBaseDeltaVersion}, and the Iceberg snapshot
   * chain is preserved ({@code currentSnapshotParentId} equals {@code expectedParentSnapshotId}).
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

  /**
   * Verifies UniForm Iceberg incremental conversion works correctly on the UC REST path (real
   * embedded UC server).
   *
   * <p>The test creates a UniForm-enabled Delta table and verifies:
   *
   * <ul>
   *   <li>CREATE TABLE → full Iceberg conversion at delta v0: no {@code base-delta-version}.
   *   <li>Write 1 → incremental conversion: {@code base-delta-version=0} (the CREATE TABLE
   *       version), Iceberg snapshot parent equals the CREATE TABLE snapshot.
   *   <li>Write 2 → incremental conversion: {@code base-delta-version=1}, Iceberg snapshot chain
   *       preserved.
   * </ul>
   */
  @Test
  public void uniformIcebergIncrementalConversionTest() throws Exception {
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
          // CREATE TABLE atomically generates a full Iceberg conversion at delta version 0.
          // The empty table has no data files, so no Iceberg snapshot exists yet.
          IcebergMeta meta0 =
              verifyNonIncrementalUniForm(fullTableName, 0L, /* expectSnapshot= */ false);

          // Write 1 — incremental from delta v0 (CREATE TABLE)
          sql("INSERT INTO %s VALUES (1, 'a')", fullTableName);
          check(fullTableName, List.of(row("1", "a")));
          assertNoUniformPropsOnServer(fullTableName);
          IcebergMeta meta1 =
              verifyIncrementalUniForm(
                  fullTableName, 1L, 0L, /* expectedParentSnapshotId= */ meta0.currentSnapshotId);

          // Write 2 — incremental from delta v1
          sql("INSERT INTO %s VALUES (2, 'b')", fullTableName);
          check(fullTableName, List.of(row("1", "a"), row("2", "b")));
          assertNoUniformPropsOnServer(fullTableName);
          verifyIncrementalUniForm(
              fullTableName, 2L, 1L, /* expectedParentSnapshotId= */ meta1.currentSnapshotId);
        });
  }

  /**
   * Verifies UniForm Iceberg incremental conversion for a CTAS (CREATE TABLE AS SELECT) table.
   *
   * <p>CTAS writes data into the table atomically at delta version 0, so the initial Iceberg
   * conversion at v0 is non-incremental and already has a snapshot. Subsequent writes must be
   * incremental and preserve the snapshot chain.
   */
  @Test
  public void uniformIcebergCTASTest() throws Exception {
    Assumptions.assumeTrue(
        Boolean.getBoolean("supportIceberg"),
        "Skipping: Iceberg support not available for this Spark version");
    String fullTableName = fullTableName("uniform_iceberg_ctas");
    sql("DROP TABLE IF EXISTS %s", fullTableName);
    try {
      // CTAS: schema is inferred from the SELECT; data lands atomically at delta version 0.
      sql(
          "CREATE TABLE %s USING DELTA"
              + " TBLPROPERTIES ('delta.feature.catalogManaged'='supported', %s)"
              + " AS SELECT 1 AS id, 'a' AS data",
          fullTableName, UNIFORM_TABLE_PROPS);

      // CTAS writes data at delta v0, so the initial Iceberg conversion has a snapshot.
      check(fullTableName, List.of(row("1", "a")));
      assertNoUniformPropsOnServer(fullTableName);
      IcebergMeta meta0 =
          verifyNonIncrementalUniForm(fullTableName, 0L, /* expectSnapshot= */ true);

      // Write 1 — incremental from delta v0 (CTAS)
      sql("INSERT INTO %s VALUES (2, 'b')", fullTableName);
      check(fullTableName, List.of(row("1", "a"), row("2", "b")));
      assertNoUniformPropsOnServer(fullTableName);
      verifyIncrementalUniForm(
          fullTableName, 1L, 0L, /* expectedParentSnapshotId= */ meta0.currentSnapshotId);
    } finally {
      sql("DROP TABLE IF EXISTS %s", fullTableName);
    }
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

  /**
   * Calls {@code loadTable} on the Delta REST API, validates the uniform fields in the response,
   * and returns the Iceberg metadata JSON file path for the given table.
   *
   * @param expectedConvertedDeltaVersion the Delta version the server should report as converted
   */
  private String verifyUCMetadataAndFetchIcebergPath(
      String fullTableName, long expectedConvertedDeltaVersion) throws Exception {
    String[] parts = fullTableName.split("\\.");
    DeltaTablesApi deltaApi = new DeltaTablesApi(unityCatalogInfo().createApiClient());
    DeltaLoadTableResponse resp = deltaApi.loadTable(parts[0], parts[1], parts[2]);
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
