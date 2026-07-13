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

import com.databricks.spark.util.Log4jUsageLogger;
import com.databricks.spark.util.UsageRecord;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.unitycatalog.client.delta.api.DeltaTablesApi;
import io.unitycatalog.client.delta.model.DeltaLoadTableResponse;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.delta.DeltaLog;
import org.apache.spark.sql.delta.DeltaOperations;
import org.apache.spark.sql.delta.DeltaTestUtils;
import org.apache.spark.sql.delta.OptimisticTransaction;
import org.apache.spark.sql.delta.Snapshot;
import org.apache.spark.sql.delta.actions.Action;
import org.apache.spark.sql.delta.catalog.DeltaTableV2;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import scala.Option;
import scala.collection.JavaConverters;
import scala.runtime.AbstractFunction0;
import scala.runtime.BoxedUnit;

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
    DeltaTableV2 table = loadDeltaTableV2(fullTableName);
    Configuration conf = spark().sessionState().newHadoopConf();
    if (table.catalogTable().isDefined()) {
      JavaConverters.mapAsJavaMap(table.catalogTable().get().storage().properties())
          .forEach(conf::set);
    }
    return conf;
  }

  /** Loads the {@link DeltaTableV2} for a fully-qualified UC table via the V2 catalog. */
  private DeltaTableV2 loadDeltaTableV2(String fullTableName) throws Exception {
    String[] parts = fullTableName.split("\\.");
    TableCatalog catalog = (TableCatalog) spark().sessionState().catalogManager().catalog(parts[0]);
    return (DeltaTableV2) catalog.loadTable(Identifier.of(new String[] {parts[1]}, parts[2]));
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

  /**
   * Verifies that {@code RESTORE TABLE} (which internally uses {@code commitLarge}) produces an
   * incremental Iceberg conversion on the UC REST path.
   *
   * <p>The test writes two versions, then restores to version 1. At restore time the current
   * snapshot already carries Iceberg metadata (from the version-2 conversion), so {@code
   * commitLarge} performs an incremental conversion and the Iceberg snapshot chain is preserved.
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
          // Write 1 — incremental from delta v0 (CREATE TABLE); this INSERT writes data files, so
          // an Iceberg snapshot exists.
          sql("INSERT INTO %s VALUES (1, 'a')", fullTableName);
          assertNoUniformPropsOnServer(fullTableName);
          IcebergMeta meta1 = verifyIncrementalUniForm(fullTableName, 1L, 0L, -1L);

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
   * Verifies that conflict resolution on the UC REST path refreshes the transaction's {@code
   * catalogTable} with fresh UniForm metadata, so the retry performs a minimal incremental Iceberg
   * conversion instead of re-converting from a stale base version.
   *
   * <p>Scenario:
   *
   * <ul>
   *   <li>v1: {@code INSERT} — atomic Iceberg conversion at v1.
   *   <li>Start a transaction pinned to the v1 snapshot and v1 catalog table.
   *   <li>v2: a concurrent {@code INSERT} lands — the "winning" commit. The pinned transaction's
   *       snapshot and Iceberg catalog are now stale.
   *   <li>Commit the pinned transaction: it conflicts with v2, and during conflict resolution
   *       {@code getCommits} returns v2's UniformMetadata, refreshing the catalog table to {@code
   *       convertedDeltaVersion=2}. The retry then converts only v3 ({@code fromVersion=3,
   *       toVersion=3}).
   * </ul>
   *
   * <p>Two {@code delta.iceberg.conversion.deltaCommitRange} events are emitted: the failed first
   * attempt (stale v1 base) and the successful retry (fresh v2 base). Without the refresh, the
   * retry would still use the stale v1 base and report {@code fromVersion=2}.
   */
  @Test
  public void uniformIcebergConflictResolutionRefreshesCatalog() throws Exception {
    Assumptions.assumeTrue(
        Boolean.getBoolean("supportIceberg"),
        "Skipping: Iceberg support not available for this Spark version");
    withNewTable(
        "uniform_iceberg_conflict",
        "id INT",
        null,
        TableType.MANAGED,
        UNIFORM_TABLE_PROPS,
        fullTableName -> {
          // v1: insert row 1 — triggers atomic Iceberg conversion at v1.
          sql("INSERT INTO %s VALUES (1)", fullTableName);

          // Pin a transaction to the v1 snapshot and v1 catalog table.
          DeltaTableV2 table = loadDeltaTableV2(fullTableName);
          DeltaLog deltaLog = table.deltaLog();
          Snapshot v1Snapshot = table.update();
          Option<CatalogTable> v1CatalogTable = table.catalogTable();
          OptimisticTransaction txn =
              deltaLog.startTransaction(v1CatalogTable, Option.apply(v1Snapshot));

          // v2: concurrent insert — the "winning" commit that makes the pinned txn stale.
          sql("INSERT INTO %s VALUES (2)", fullTableName);

          // Commit the pinned txn: conflicts with v2, then retries as v3. Conflict resolution
          // refreshes the catalog with v2's UniformMetadata (convertedDeltaVersion=2), so the retry
          // converts only v3 incrementally.
          scala.collection.immutable.Seq<Action> noActions =
              JavaConverters.asScalaBuffer(java.util.Collections.<Action>emptyList()).toList();
          List<UsageRecord> events =
              JavaConverters.seqAsJavaList(
                  Log4jUsageLogger.track(
                      new AbstractFunction0<BoxedUnit>() {
                        @Override
                        public BoxedUnit apply() {
                          txn.commit(noActions, DeltaOperations.ManualUpdate$.MODULE$);
                          return BoxedUnit.UNIT;
                        }
                      }));

          long latestVersion = table.update().version();
          List<UsageRecord> rangeEvents =
              scala.collection.JavaConverters.seqAsJavaList(
                  DeltaTestUtils.filterUsageRecords(
                      JavaConverters.asScalaBuffer(events).toSeq(),
                      "delta.iceberg.conversion.deltaCommitRange"));
          Assertions.assertEquals(
              2, rangeEvents.size(), "Expected 2 deltaCommitRange events (failed attempt + retry)");

          // The retry (last event) must use the refreshed v2 base, converting only v3.
          ObjectMapper mapper = new ObjectMapper();
          JsonNode retry = mapper.readTree(rangeEvents.get(rangeEvents.size() - 1).blob());
          Assertions.assertEquals(
              latestVersion,
              retry.get("fromVersion").asLong(),
              "Retry must convert from the refreshed v2 base (fromVersion="
                  + latestVersion
                  + "); a stale v1 base would give "
                  + (latestVersion - 1));
          Assertions.assertEquals(
              latestVersion,
              retry.get("toVersion").asLong(),
              "Retry must convert up to the latest version");
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

    org.apache.iceberg.Snapshot current = table.currentSnapshot();
    long currentSnapshotId = current != null ? current.snapshotId() : -1L;
    long parentSnapshotId =
        (current != null && current.parentId() != null) ? current.parentId() : -1L;

    return new IcebergMeta(properties, currentSnapshotId, parentSnapshotId);
  }
}
