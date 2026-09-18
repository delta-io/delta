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
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.delta.DeltaLog;
import org.apache.spark.sql.delta.Snapshot;
import org.apache.spark.sql.delta.actions.AddFile;
import org.apache.spark.sql.delta.catalog.DeltaTableV2;
import org.junit.jupiter.api.Assertions;
import scala.collection.JavaConverters;

/** Shared support for Unity Catalog UniForm Iceberg integration tests. */
public abstract class UCDeltaTableUniformIcebergTestBase extends UCDeltaTableIntegrationBaseTest {

  @Override
  protected boolean useDeltaRestApiForTests() {
    return true;
  }

  /** The Iceberg compatibility version enabled by the concrete test class. */
  protected abstract int icebergCompatVersion();

  /** Additional table properties required by a concrete compatibility version. */
  protected String additionalUniformTableProperties() {
    return "";
  }

  /** Builds the common UniForm table properties for the concrete compatibility version. */
  protected final String uniformTableProperties() {
    String properties =
        "'delta.universalFormat.enabledFormats'='iceberg', "
            + "'delta.columnMapping.mode'='name', "
            + "'delta.enableIcebergCompatV"
            + icebergCompatVersion()
            + "'='true'";
    String additionalProperties = additionalUniformTableProperties();
    if (additionalProperties == null || additionalProperties.trim().isEmpty()) {
      return properties;
    }
    return properties + ", " + additionalProperties;
  }

  /**
   * Asserts that none of the in-memory-only {@code deltaUniformIceberg.*} keys are present in the
   * table's server-side properties.
   */
  protected final void assertNoUniformPropsOnServer(String fullTableName) throws Exception {
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
   * Builds a Hadoop {@link Configuration} seeded from Spark and enriched with UC-vended storage
   * credentials from the table's catalog storage properties.
   */
  protected final Configuration buildTestHadoopConf(String fullTableName) throws Exception {
    DeltaTableV2 table = loadDeltaTableV2(fullTableName);
    Configuration conf = spark().sessionState().newHadoopConf();
    if (table.catalogTable().isDefined()) {
      JavaConverters.mapAsJavaMap(table.catalogTable().get().storage().properties())
          .forEach(conf::set);
    }
    return conf;
  }

  /** Loads the {@link DeltaTableV2} for a fully-qualified UC table via the V2 catalog. */
  protected final DeltaTableV2 loadDeltaTableV2(String fullTableName) throws Exception {
    String[] parts = fullTableName.split("\\.");
    TableCatalog catalog = (TableCatalog) spark().sessionState().catalogManager().catalog(parts[0]);
    return (DeltaTableV2) catalog.loadTable(Identifier.of(new String[] {parts[1]}, parts[2]));
  }

  /** Returns the latest Delta snapshot for the given UC table. */
  protected final Snapshot loadDeltaSnapshot(String fullTableName) throws Exception {
    return loadDeltaTableV2(fullTableName).update();
  }

  /** Returns the Delta table properties from the latest snapshot as a Java map. */
  protected final Map<String, String> deltaTableProperties(String fullTableName) throws Exception {
    return JavaConverters.mapAsJavaMap(loadDeltaSnapshot(fullTableName).metadata().configuration());
  }

  /**
   * Verifies a non-incremental UniForm conversion: {@code delta-version} matches and {@code
   * base-delta-version} is absent.
   */
  protected final IcebergMeta verifyNonIncrementalUniForm(
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

  /** Verifies an incremental UniForm conversion, including its base version and snapshot parent. */
  protected final IcebergMeta verifyIncrementalUniForm(
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

  /** Verifies the active data-file count reported by the current Iceberg snapshot. */
  protected final void assertIcebergDataFileCount(Table icebergTable, long expectedCount) {
    org.apache.iceberg.Snapshot icebergSnapshot = icebergTable.currentSnapshot();
    Assertions.assertNotNull(icebergSnapshot, "Expected an Iceberg snapshot with active data");
    String actualCount = icebergSnapshot.summary().get(SnapshotSummary.TOTAL_DATA_FILES_PROP);
    Assertions.assertNotNull(actualCount, "Iceberg snapshot summary is missing total-data-files");
    Assertions.assertEquals(
        expectedCount,
        Long.parseLong(actualCount),
        "Unexpected number of active Iceberg data files");
  }

  /**
   * Matches every active Delta and Iceberg data file by absolute path and verifies Iceberg V3 row
   * lineage metadata.
   */
  protected final void assertIcebergRowTrackingMatchesDelta(
      String fullTableName, Table icebergTable) throws Exception {
    DeltaTableV2 deltaTable = loadDeltaTableV2(fullTableName);
    DeltaLog deltaLog = deltaTable.deltaLog();
    Snapshot deltaSnapshot = deltaTable.update();

    Map<String, AddFile> deltaFilesByPath = new HashMap<>();
    for (AddFile file : deltaSnapshot.allFiles().collectAsList()) {
      String path = file.absolutePath(deltaLog).toString();
      Assertions.assertNull(
          deltaFilesByPath.put(path, file), "Duplicate active Delta data file path: " + path);
    }

    Map<String, DataFile> icebergFilesByPath = new HashMap<>();
    org.apache.iceberg.Snapshot icebergSnapshot = icebergTable.currentSnapshot();
    Assertions.assertNotNull(icebergSnapshot, "Expected an Iceberg snapshot with active data");
    for (ManifestFile manifest : icebergSnapshot.dataManifests(icebergTable.io())) {
      try (ManifestReader<DataFile> reader =
          ManifestFiles.read(manifest, icebergTable.io(), icebergTable.specs())) {
        for (DataFile file : reader) {
          String path = file.path().toString();
          Assertions.assertNull(
              icebergFilesByPath.put(path, file),
              "Duplicate active Iceberg data file path: " + path);
        }
      }
    }

    Assertions.assertEquals(
        deltaFilesByPath.keySet(),
        icebergFilesByPath.keySet(),
        "Active file sets differ between Delta and Iceberg");
    for (Map.Entry<String, AddFile> entry : deltaFilesByPath.entrySet()) {
      String path = entry.getKey();
      AddFile deltaFile = entry.getValue();
      DataFile icebergFile = icebergFilesByPath.get(path);

      Assertions.assertTrue(
          deltaFile.baseRowId().isDefined(), "Delta baseRowId is missing for " + path);
      Assertions.assertTrue(
          deltaFile.defaultRowCommitVersion().isDefined(),
          "Delta defaultRowCommitVersion is missing for " + path);
      Assertions.assertNotNull(
          icebergFile.firstRowId(), "Iceberg firstRowId is missing for " + path);
      Assertions.assertNotNull(
          icebergFile.dataSequenceNumber(), "Iceberg dataSequenceNumber is missing for " + path);

      long deltaBaseRowId = ((Number) deltaFile.baseRowId().get()).longValue();
      long deltaRowCommitVersion = ((Number) deltaFile.defaultRowCommitVersion().get()).longValue();
      Assertions.assertEquals(
          deltaBaseRowId,
          icebergFile.firstRowId().longValue(),
          "Delta baseRowId does not match Iceberg firstRowId for " + path);
      Assertions.assertEquals(
          deltaRowCommitVersion,
          icebergFile.dataSequenceNumber().longValue(),
          "Delta defaultRowCommitVersion does not match Iceberg dataSequenceNumber for " + path);
    }
  }

  private IcebergMeta verifyUCMetadataAndReadIceberg(
      String fullTableName, long expectedConvertedDeltaVersion) throws Exception {
    String icebergMetadataPath =
        verifyUCMetadataAndFetchIcebergPath(fullTableName, expectedConvertedDeltaVersion);
    Configuration conf = buildTestHadoopConf(fullTableName);
    return readIcebergMetaViaHadoopTables(icebergMetadataPath, conf);
  }

  /** Validates the UC REST UniForm response and returns the Iceberg metadata location. */
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
    Instant.ofEpochMilli(iceberg.getConvertedDeltaTimestamp());
    return iceberg.getMetadataLocation();
  }

  private IcebergMeta readIcebergMetaViaHadoopTables(
      String icebergMetadataPath, Configuration conf) {
    HadoopTables tables = new HadoopTables(conf);
    Table table = tables.load(icebergMetadataPath);

    Assertions.assertTrue(
        table instanceof BaseTable,
        "Expected HadoopTables to load an Iceberg BaseTable, but got " + table.getClass());
    int formatVersion = ((BaseTable) table).operations().current().formatVersion();
    Assertions.assertEquals(
        icebergCompatVersion(), formatVersion, "Unexpected Iceberg table format version");

    Map<String, String> properties = new HashMap<>(table.properties());
    org.apache.iceberg.Snapshot current = table.currentSnapshot();
    long currentSnapshotId = current != null ? current.snapshotId() : -1L;
    long parentSnapshotId =
        (current != null && current.parentId() != null) ? current.parentId() : -1L;

    return new IcebergMeta(table, properties, currentSnapshotId, parentSnapshotId);
  }

  /** Iceberg state loaded from the exact metadata location returned by UC. */
  protected static final class IcebergMeta {
    final Table table;
    final Map<String, String> properties;
    final long currentSnapshotId;
    final long currentSnapshotParentId;

    IcebergMeta(
        Table table,
        Map<String, String> properties,
        long currentSnapshotId,
        long currentSnapshotParentId) {
      this.table = table;
      this.properties = properties;
      this.currentSnapshotId = currentSnapshotId;
      this.currentSnapshotParentId = currentSnapshotParentId;
    }
  }
}
