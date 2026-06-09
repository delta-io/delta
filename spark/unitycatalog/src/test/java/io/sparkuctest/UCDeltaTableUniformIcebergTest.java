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
          IcebergMeta meta1 = verifyUCMetadataAndReadIceberg(fullTableName, 1L);
          Assertions.assertEquals(
              "1",
              meta1.properties.get("delta-version"),
              "After first write, delta-version in Iceberg metadata should be 1");
          Assertions.assertNull(
              meta1.properties.get("base-delta-version"),
              "First (full) conversion must NOT set base-delta-version");
          Assertions.assertTrue(
              meta1.currentSnapshotId != -1L, "Iceberg snapshot should exist after first write");

          // Write 2 — must produce an incremental conversion on the Delta REST API path
          sql("INSERT INTO %s VALUES (2, 'b')", fullTableName);
          check(fullTableName, List.of(row("1", "a"), row("2", "b")));
          assertNoUniformPropsOnServer(fullTableName);
          IcebergMeta meta2 = verifyUCMetadataAndReadIceberg(fullTableName, 2L);
          Assertions.assertEquals(
              "2",
              meta2.properties.get("delta-version"),
              "After second write, delta-version in Iceberg metadata should be 2");
          if (useDeltaRestApiForTests()) {
            Assertions.assertEquals(
                "1",
                meta2.properties.get("base-delta-version"),
                "Second conversion must be incremental: base-delta-version should equal 1");
            Assertions.assertEquals(
                meta1.currentSnapshotId,
                meta2.currentSnapshotParentId,
                "Iceberg snapshot chain must be preserved: parent of snapshot-2 must be snapshot-1");
          }
        });
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
