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

import io.delta.kernel.DataWriteContext;
import io.delta.kernel.Operation;
import io.delta.kernel.Scan;
import io.delta.kernel.Snapshot;
import io.delta.kernel.Transaction;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.defaults.internal.data.DefaultColumnarBatch;
import io.delta.kernel.defaults.internal.data.vector.DefaultGenericVector;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.expressions.Column;
import io.delta.kernel.internal.InternalScanFileUtils;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.internal.data.ScanStateRow;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.transaction.CreateTableTransactionBuilder;
import io.delta.kernel.transaction.DataLayoutSpec;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;
import io.delta.kernel.unitycatalog.UCCatalogManagedClient;
import io.delta.kernel.unitycatalog.UCTableIdentifier;
import io.delta.kernel.utils.CloseableIterable;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.DataFileStatus;
import io.delta.kernel.utils.FileStatus;
import io.delta.storage.commit.uccommitcoordinator.UCDeltaTokenBasedRestClient;
import io.unitycatalog.client.api.TablesApi;
import io.unitycatalog.client.auth.TokenProvider;
import io.unitycatalog.client.delta.api.DeltaTablesApi;
import io.unitycatalog.client.delta.model.DeltaCreateStagingTableRequest;
import io.unitycatalog.client.delta.model.DeltaStagingTableResponse;
import io.unitycatalog.client.model.TableInfo;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * End-to-end test that drives the kernel {@link UCCatalogManagedClient} / {@code
 * UCCatalogManagedCommitter} against a <b>real</b> Unity Catalog server over the Delta-Tables API,
 * covering create, ALTER, insert + read-back, and clustered create.
 *
 * <p>Runs against the in-process UC server started by {@link UnityCatalogSupport} by default (no
 * env flag needed). To run against a remote server instead, set {@code UC_REMOTE=true UC_URI=...
 * UC_TOKEN=... UC_CATALOG_NAME=... UC_SCHEMA_NAME=...} (see {@link UnityCatalogSupport}).
 *
 * <p>This exercises Kernel APIs but lives under {@code spark/unitycatalog} because booting the
 * in-process UC server (and its {@link UnityCatalogSupport} harness) needs the coherent Jackson +
 * server dependency stack the Spark UC module already provides; standing that stack up under {@code
 * kernel/unitycatalog} pulls conflicting transitive Jackson versions that crash server startup.
 */
public class UCKernelDeltaTablesLiveE2ETest extends UnityCatalogSupport {

  @Override
  protected AuthMode authMode() {
    return AuthMode.STATIC;
  }

  // The server vends s3://<fake-bucket>/... locations backed by local files, which the test's
  // S3CredentialFileSystem maps to local paths.
  private boolean priorCredentialCheckEnabled;

  @BeforeEach
  public void disableS3CredentialCheck() {
    priorCredentialCheckEnabled = S3CredentialFileSystem.credentialCheckEnabled;
    S3CredentialFileSystem.credentialCheckEnabled = false;
  }

  @AfterEach
  public void restoreS3CredentialCheck() {
    S3CredentialFileSystem.credentialCheckEnabled = priorCredentialCheckEnabled;
  }

  private UnityCatalogInfo uc;
  private Engine engine;
  private UCDeltaTokenBasedRestClient deltaClient;
  private UCCatalogManagedClient catalogClient;

  @BeforeEach
  public void setUpCatalogClients() {
    uc = unityCatalogInfo();
    engine = newEngine();
    deltaClient = newDeltaClient(uc);
    catalogClient = new UCCatalogManagedClient(deltaClient);
  }

  @AfterEach
  public void closeCatalogClients() throws Exception {
    // UCCatalogManagedClient does not own the underlying UCClient, so only the REST client is
    // released here.
    if (deltaClient != null) {
      deltaClient.close();
    }
  }

  private static final String ENGINE_INFO = "kernel-uc-live-e2e";

  private static final StructType TEST_SCHEMA =
      new StructType().add("id", IntegerType.INTEGER).add("name", StringType.STRING);

  private UCDeltaTokenBasedRestClient newDeltaClient(UnityCatalogInfo uc) {
    Map<String, String> authConfig = Map.of("type", "static", "token", uc.serverToken());
    TokenProvider tokenProvider = TokenProvider.create(authConfig);
    return new UCDeltaTokenBasedRestClient(uc.serverUri(), tokenProvider, Collections.emptyMap());
  }

  private Engine newEngine() {
    // Route the s3 scheme through the test's S3CredentialFileSystem.
    Configuration conf = new Configuration();
    conf.set("fs.s3.impl", S3CredentialFileSystem.class.getName());
    return DefaultEngine.create(conf);
  }

  /**
   * Table properties a connector must set to satisfy the server's create contract, beyond the
   * catalog-managed defaults that {@code buildCreateTableTransaction} already sets. Each feature
   * the staging response's required protocol advertises becomes {@code
   * delta.feature.<name>=supported}, and the server's advertised required properties are applied
   * verbatim.
   */
  private Map<String, String> requiredFeatureProperties(DeltaStagingTableResponse staging) {
    Map<String, String> props = new HashMap<>();
    if (staging.getRequiredProtocol() != null) {
      List<String> reader =
          staging.getRequiredProtocol().getReaderFeatures() == null
              ? Collections.emptyList()
              : staging.getRequiredProtocol().getReaderFeatures();
      List<String> writer =
          staging.getRequiredProtocol().getWriterFeatures() == null
              ? Collections.emptyList()
              : staging.getRequiredProtocol().getWriterFeatures();
      for (String feature : reader) {
        props.put("delta.feature." + feature, "supported");
      }
      for (String feature : writer) {
        props.put("delta.feature." + feature, "supported");
      }
      if (writer.contains("inCommitTimestamp")) {
        props.put("delta.enableInCommitTimestamps", "true");
      }
    }
    if (staging.getRequiredProperties() != null) {
      props.putAll(staging.getRequiredProperties());
    }
    return props;
  }

  /**
   * Full create round-trip: reserve a staging table, run the kernel create-table transaction
   * (writes 000.json + finalizeCreate through the Delta-Tables API), then load the table back and
   * assert the server returned the protocol we sent.
   */
  @Test
  public void createTable_sendsStructuredProtocol_andRoundTrips() throws Exception {
    CreatedTable table = createManagedTable(uc, engine, catalogClient);

    Snapshot snapshot = loadAtVersion(catalogClient, engine, table, 0L);
    assertThat(snapshot.getVersion()).isEqualTo(0L);
    assertThat(snapshot.getSchema().fieldNames())
        .containsExactlyElementsOf(TEST_SCHEMA.fieldNames());

    Protocol protocol = ((SnapshotImpl) snapshot).getProtocol();
    assertThat(protocol.getMinReaderVersion()).isGreaterThanOrEqualTo(3);
    assertThat(protocol.getMinWriterVersion()).isGreaterThanOrEqualTo(7);
    assertThat(protocol.getWriterFeatures()).contains("catalogManaged");

    assertThat(snapshot.getTableProperties()).containsAllEntriesOf(table.requiredProperties);
  }

  /**
   * Creates a clustered table through the kernel committer path and asserts clustering round-trips
   * on both surfaces: (a) the catalog's raw property bag returned by the server, which carries
   * {@code clusteringColumns} plus {@code delta.feature.clustering}; and (b) the kernel snapshot,
   * whose clustering columns come from the clustering domain metadata in the log.
   */
  @Test
  public void createClusteredTable_roundTripsClusteringColumns() throws Exception {
    CreatedTable table =
        createManagedTable(
            uc, engine, catalogClient, DataLayoutSpec.clustered(List.of(new Column("id"))));

    // (a) Raw catalog properties as persisted by the server.
    TablesApi tablesApi = new TablesApi(uc.createApiClient());
    String fullName =
        String.format(
            "%s.%s.%s",
            table.identifier.getCatalogName(),
            table.identifier.getSchemaName(),
            table.identifier.getTableName());
    TableInfo tableInfo = tablesApi.getTable(fullName, false, false);
    Map<String, String> serverProps = tableInfo.getProperties();
    assertThat(serverProps).containsEntry("delta.feature.clustering", "supported");
    // The clusteringColumns value is a column-mapped physical reference, so assert presence only.
    assertThat(serverProps).containsKey("clusteringColumns");

    // (b) Kernel snapshot: clustering columns resolved from the clustering domain metadata.
    Snapshot snapshot = loadAtVersion(catalogClient, engine, table, 0L);
    Optional<List<Column>> clusteringColumns =
        ((SnapshotImpl) snapshot).getPhysicalClusteringColumns();
    assertThat(clusteringColumns).isPresent();
    assertThat(clusteringColumns.get()).hasSize(1);
  }

  @Test
  public void alterTable_setTableProperty_roundTripsThroughUpdateTable() throws Exception {
    CreatedTable table = createManagedTable(uc, engine, catalogClient);

    // ALTER: set a user table property on the v0 table via the update-table (V1+) path.
    Snapshot v0 = loadAtVersion(catalogClient, engine, table, 0L);
    v0.buildUpdateTableTransaction(ENGINE_INFO, Operation.MANUAL_UPDATE)
        .withTablePropertiesAdded(Map.of("user.key", "user-value"))
        .build(engine)
        .commit(engine, CloseableIterable.emptyIterable());

    // Reload at the new version and assert the property is present.
    Snapshot v1 = loadAtVersion(catalogClient, engine, table, 1L);
    assertThat(v1.getVersion()).isEqualTo(1L);
    assertThat(v1.getTableProperties()).containsEntry("user.key", "user-value");
  }

  /** Holds the identifiers of a table created via {@link #createManagedTable}. */
  private static final class CreatedTable {
    final String ucTableId;
    final String tablePath;
    final UCTableIdentifier identifier;
    final Map<String, String> requiredProperties;

    CreatedTable(
        String ucTableId,
        String tablePath,
        UCTableIdentifier identifier,
        Map<String, String> requiredProperties) {
      this.ucTableId = ucTableId;
      this.tablePath = tablePath;
      this.identifier = identifier;
      this.requiredProperties = requiredProperties;
    }
  }

  /**
   * Reserves a staging table and runs the kernel create-table transaction (writes 000.json + the
   * Delta-Tables createTable finalize), honoring the server's required protocol/properties.
   */
  private CreatedTable createManagedTable(
      UnityCatalogInfo uc, Engine engine, UCCatalogManagedClient catalogClient) throws Exception {
    return createManagedTable(uc, engine, catalogClient, null);
  }

  /**
   * Reserves a staging table and runs the kernel create-table transaction. When {@code dataLayout}
   * is non-null it is applied to the create transaction (e.g. clustering), so the write and the
   * Delta-Tables finalize carry the layout.
   */
  private CreatedTable createManagedTable(
      UnityCatalogInfo uc,
      Engine engine,
      UCCatalogManagedClient catalogClient,
      DataLayoutSpec dataLayout)
      throws Exception {
    String tableName = "kernel_e2e_" + UUID.randomUUID().toString().replace("-", "");
    UCTableIdentifier identifier =
        new UCTableIdentifier(uc.catalogName(), uc.schemaName(), tableName);

    // Reserve a staging table via the Delta-Tables API to obtain the UC table id + storage
    // location. (The legacy TablesApi staging endpoint is disabled in Delta-API-only mode.)
    DeltaTablesApi deltaTablesApi = new DeltaTablesApi(uc.createApiClient());
    DeltaStagingTableResponse staging =
        deltaTablesApi.createStagingTable(
            uc.catalogName(),
            uc.schemaName(),
            new DeltaCreateStagingTableRequest().name(tableName));
    String ucTableId = staging.getTableId().toString();
    String tablePath = staging.getLocation();

    // Honor the server's required protocol/properties from the staging response (the OSS server
    // requires catalog-managed tables to carry features/properties beyond the defaults that
    // buildCreateTableTransaction sets).
    CreateTableTransactionBuilder createBuilder =
        catalogClient
            .buildCreateTableTransaction(ucTableId, tablePath, TEST_SCHEMA, ENGINE_INFO, identifier)
            .withTableProperties(requiredFeatureProperties(staging));
    if (dataLayout != null) {
      createBuilder = createBuilder.withDataLayoutSpec(dataLayout);
    }
    createBuilder.build(engine).commit(engine, CloseableIterable.emptyIterable());

    // The server's required *properties* (as opposed to required features, which land in the
    // protocol) persist verbatim into table metadata, so tests can assert they round-tripped.
    Map<String, String> requiredProperties =
        staging.getRequiredProperties() == null ? Map.of() : staging.getRequiredProperties();
    return new CreatedTable(ucTableId, tablePath, identifier, requiredProperties);
  }

  /** Loads {@code table} at a specific version through the catalog client. */
  private Snapshot loadAtVersion(
      UCCatalogManagedClient catalogClient, Engine engine, CreatedTable table, long version) {
    return catalogClient.loadSnapshot(
        engine,
        table.ucTableId,
        table.tablePath,
        table.identifier,
        Optional.of(version),
        Optional.empty());
  }

  @Test
  public void insertData_thenReadBack_roundTripsThroughUpdateTable() throws Exception {
    CreatedTable table = createManagedTable(uc, engine, catalogClient);

    // INSERT: append 3 rows to the v0 table through the update-table (V1+) commit path.
    Snapshot v0 = loadAtVersion(catalogClient, engine, table, 0L);
    insertRows(engine, v0, 3);

    // READ: reload at the new version and scan the data back, asserting the exact rows written.
    Snapshot v1 = loadAtVersion(catalogClient, engine, table, 1L);
    assertThat(v1.getVersion()).isEqualTo(1L);
    assertThat(readRows(engine, v1)).containsExactlyInAnyOrder("0=row-0", "1=row-1", "2=row-2");
  }

  /** Appends {@code rowCount} rows of test data to {@code snapshot} via a WRITE transaction. */
  private void insertRows(Engine engine, Snapshot snapshot, int rowCount) throws Exception {
    Transaction txn =
        snapshot.buildUpdateTableTransaction(ENGINE_INFO, Operation.WRITE).build(engine);
    Row txnState = txn.getTransactionState(engine);

    Object[] ids = new Object[rowCount];
    Object[] names = new Object[rowCount];
    for (int i = 0; i < rowCount; i++) {
      ids[i] = i;
      names[i] = "row-" + i;
    }
    ColumnVector[] vectors =
        new ColumnVector[] {
          DefaultGenericVector.fromArray(IntegerType.INTEGER, ids),
          DefaultGenericVector.fromArray(StringType.STRING, names)
        };
    FilteredColumnarBatch batch =
        new FilteredColumnarBatch(
            new DefaultColumnarBatch(rowCount, TEST_SCHEMA, vectors), Optional.empty());

    DataWriteContext writeContext =
        Transaction.getWriteContext(engine, txnState, Collections.emptyMap());
    try (CloseableIterator<FilteredColumnarBatch> logicalData =
            Utils.toCloseableIterator(Collections.singletonList(batch).iterator());
        CloseableIterator<FilteredColumnarBatch> physicalData =
            Transaction.transformLogicalData(
                engine, txnState, logicalData, Collections.emptyMap());
        CloseableIterator<DataFileStatus> dataFiles =
            engine
                .getParquetHandler()
                .writeParquetFiles(
                    writeContext.getTargetDirectory(),
                    physicalData,
                    writeContext.getStatisticsColumns());
        CloseableIterator<Row> dataActions =
            Transaction.generateAppendActions(engine, txnState, dataFiles, writeContext)) {
      txn.commit(engine, CloseableIterable.inMemoryIterable(dataActions));
    }
  }

  /** Scans {@code snapshot} and returns each row rendered as {@code "<id>=<name>"}. */
  private List<String> readRows(Engine engine, Snapshot snapshot) throws Exception {
    Scan scan = snapshot.getScanBuilder().build();
    Row scanState = scan.getScanState(engine);
    StructType physicalReadSchema = ScanStateRow.getPhysicalDataReadSchema(scanState);

    List<String> rows = new ArrayList<>();
    try (CloseableIterator<FilteredColumnarBatch> scanFiles = scan.getScanFiles(engine)) {
      while (scanFiles.hasNext()) {
        try (CloseableIterator<Row> scanFileRows = scanFiles.next().getRows()) {
          while (scanFileRows.hasNext()) {
            Row scanFileRow = scanFileRows.next();
            FileStatus fileStatus = InternalScanFileUtils.getAddFileStatus(scanFileRow);
            try (CloseableIterator<ColumnarBatch> physicalData =
                    engine
                        .getParquetHandler()
                        .readParquetFiles(
                            Utils.singletonCloseableIterator(fileStatus),
                            physicalReadSchema,
                            Optional.empty())
                        .map(r -> r.getData());
                CloseableIterator<FilteredColumnarBatch> transformed =
                    Scan.transformPhysicalData(engine, scanState, scanFileRow, physicalData)) {
              while (transformed.hasNext()) {
                try (CloseableIterator<Row> dataRows = transformed.next().getRows()) {
                  while (dataRows.hasNext()) {
                    Row row = dataRows.next();
                    rows.add(row.getInt(0) + "=" + row.getString(1));
                  }
                }
              }
            }
          }
        }
      }
    }
    return rows;
  }
}
