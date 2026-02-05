/*
 * Copyright (2025) The Delta Lake Project Authors.
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
package org.apache.spark.sql.delta.catalog;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import io.delta.kernel.TableManager;
import io.delta.kernel.Snapshot;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.internal.tablefeatures.TableFeatures;
import io.delta.kernel.unitycatalog.UCCatalogManagedClient;
import io.delta.spark.internal.v2.utils.SchemaUtils;
import io.delta.storage.commit.Commit;
import io.delta.storage.commit.CommitFailedException;
import io.delta.storage.commit.GetCommitsResponse;
import io.delta.storage.commit.actions.AbstractMetadata;
import io.delta.storage.commit.actions.AbstractProtocol;
import io.delta.storage.commit.uccommitcoordinator.InvalidTargetTableException;
import io.delta.storage.commit.uccommitcoordinator.UCClient;
import io.delta.storage.commit.uccommitcoordinator.UCCommitCoordinatorClient;
import io.delta.storage.commit.uccommitcoordinator.UCCommitCoordinatorException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.expressions.Expressions;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.delta.DeltaSQLCommandJavaTest;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.Utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class KernelStagedDeltaTableSuite implements DeltaSQLCommandJavaTest {

  private transient SparkSession spark;

  @Before
  public void setUp() {
    spark = buildSparkSession();
  }

  @After
  public void tearDown() {
    if (spark != null) {
      spark.stop();
      spark = null;
    }
  }

  // Validates path-based CREATE writes a kernel log and preserves schema/partition/properties.
  @Test
  public void testCommitStagedChangesCreatesKernelTableForPathCreate() throws Exception {
    File tempDir = Utils.createTempDir(System.getProperty("java.io.tmpdir"), "spark");
    try {
      String path = tempDir.getAbsolutePath();
      Identifier ident = Identifier.of(new String[] {"delta"}, path);
      StructType schema = new StructType().add("id", DataTypes.IntegerType);
      Map<String, String> properties = new HashMap<>();
      properties.put("delta.appendOnly", "true");
      Transform[] partitions = new Transform[] {Expressions.identity("id")};
      ResolvedCreateRequest request = ResolvedCreateRequest.forPathCreate(
          ident,
          schema,
          partitions,
          properties);

      KernelStagedDeltaTable staged = new KernelStagedDeltaTable(spark, ident, request);
      staged.commitStagedChanges();

      assertTrue(new File(path, "_delta_log/00000000000000000000.json").exists());
      DefaultEngine engine = DefaultEngine.create(spark.sessionState().newHadoopConf());
      Snapshot snapshot = TableManager.loadSnapshot(path).build(engine);
      StructType sparkSchema = SchemaUtils.convertKernelSchemaToSparkSchema(snapshot.getSchema());
      assertEquals(schema, sparkSchema);
      assertEquals(Collections.singletonList("id"), snapshot.getPartitionColumnNames());
      assertEquals("true", snapshot.getTableProperties().get("delta.appendOnly"));
    } finally {
      Utils.deleteRecursively(tempDir);
    }
  }

  // Validates UC-managed CREATE registers via UC, loads with UC protocol features, and fixes UC ID.
  @Test
  public void testCommitStagedChangesUsesUcClientForUcManagedCreate() throws Exception {
    File tempDir = Utils.createTempDir(System.getProperty("java.io.tmpdir"), "spark");
    try {
      String localPath = tempDir.getAbsolutePath();
      DefaultEngine engine = DefaultEngine.create(spark.sessionState().newHadoopConf());
      String stagingPath = localPath;
      String resolvedPath = engine.getFileSystemClient().resolvePath(localPath);
      Identifier ident = Identifier.of(new String[] {"default"}, "uc_table");
      StructType schema = new StructType().add("id", DataTypes.IntegerType);
      CreateOnlyUCClient ucClient = new CreateOnlyUCClient("test-metastore");
      String tableId = UUID.randomUUID().toString();
      Map<String, String> properties = new HashMap<>();
      properties.put("delta.appendOnly", "true");
      properties.put(UCCommitCoordinatorClient.UC_TABLE_ID_KEY, "wrong-id");
      Transform[] partitions = new Transform[] {Expressions.identity("id")};
      ResolvedCreateRequest request = ResolvedCreateRequest.forUCManagedCreate(
          ident,
          schema,
          partitions,
          properties,
          tableId,
          stagingPath);

      KernelStagedDeltaTable staged = new KernelStagedDeltaTable(spark, ident, request, ucClient);
      staged.commitStagedChanges();

      assertTrue(new File(localPath, "_delta_log/00000000000000000000.json").exists());
      ucClient.registerTableAfterCreate(tableId);
      Snapshot snapshot = new UCCatalogManagedClient(ucClient)
          .loadSnapshot(
              engine,
              tableId,
              resolvedPath,
              Optional.empty(),
              Optional.empty());
      StructType sparkSchema = SchemaUtils.convertKernelSchemaToSparkSchema(snapshot.getSchema());
      assertEquals(schema, sparkSchema);
      assertEquals(Collections.singletonList("id"), snapshot.getPartitionColumnNames());
      assertEquals("true", snapshot.getTableProperties().get("delta.appendOnly"));
      assertEquals(
          tableId,
          snapshot.getTableProperties().get(UCCommitCoordinatorClient.UC_TABLE_ID_KEY));
      SnapshotImpl snapshotImpl = (SnapshotImpl) snapshot;
      assertTrue(snapshotImpl.getProtocol()
          .getReaderFeatures()
          .contains(TableFeatures.CATALOG_MANAGED_RW_FEATURE.featureName()));
      assertTrue(snapshotImpl.getProtocol()
          .getWriterFeatures()
          .contains(TableFeatures.CATALOG_MANAGED_RW_FEATURE.featureName()));
    } finally {
      Utils.deleteRecursively(tempDir);
    }
  }

  private static final class CreateOnlyUCClient implements UCClient {
    private final String metastoreId;
    private String registeredTableId;
    private long maxVersion = -1L;

    private CreateOnlyUCClient(String metastoreId) {
      this.metastoreId = metastoreId;
    }

    private void registerTableAfterCreate(String tableId) {
      if (registeredTableId != null) {
        throw new IllegalArgumentException("Table already registered: " + registeredTableId);
      }
      registeredTableId = tableId;
      maxVersion = 0L;
    }

    @Override
    public String getMetastoreId() throws IOException {
      return metastoreId;
    }

    @Override
    public void commit(
        String tableId,
        URI tableUri,
        Optional<Commit> commit,
        Optional<Long> lastKnownBackfilledVersion,
        boolean disown,
        Optional<AbstractMetadata> newMetadata,
        Optional<AbstractProtocol> newProtocol)
        throws IOException, CommitFailedException, UCCommitCoordinatorException {
      throw new UnsupportedOperationException(
          "UC commit should not be called for catalog-managed CREATE");
    }

    @Override
    public GetCommitsResponse getCommits(
        String tableId,
        URI tableUri,
        Optional<Long> startVersion,
        Optional<Long> endVersion)
        throws IOException, UCCommitCoordinatorException {
      if (!tableId.equals(registeredTableId)) {
        throw new InvalidTargetTableException("Table not found: " + tableId);
      }
      return new GetCommitsResponse(Collections.emptyList(), maxVersion);
    }

    @Override
    public void close() throws IOException {
    }
  }
}
