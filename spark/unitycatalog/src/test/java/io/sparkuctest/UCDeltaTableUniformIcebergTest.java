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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.unitycatalog.client.delta.api.TablesApi;
import io.unitycatalog.client.delta.model.LoadTableResponse;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopTables;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Integration test that verifies UniForm Iceberg incremental conversion works correctly on the UC
 * REST path (real embedded UC server).
 *
 * <p>The test creates a UniForm-enabled Delta table, writes twice, and verifies:
 *
 * <ul>
 *   <li>Write 1 → full Iceberg conversion: no {@code base-delta-version} table property.
 *   <li>Write 2 → incremental conversion: {@code base-delta-version} equals the prior converted
 *       Delta version, and the Iceberg snapshot chain is preserved.
 * </ul>
 *
 * <p>Iceberg metadata is read via {@link HadoopTables} (from {@code iceberg-core}, already on the
 * test classpath transitively from the UC server dependency). If the class is unavailable at
 * runtime, verification falls back to parsing the Iceberg metadata JSON file directly.
 */
public class UCDeltaTableUniformIcebergTest extends UCDeltaTableIntegrationBaseTest {

  private static final String UNIFORM_TABLE_PROPS =
      "'delta.universalFormat.enabledFormats'='iceberg', "
          + "'delta.columnMapping.mode'='name', "
          + "'delta.enableIcebergCompatV2'='true', "
          + "'delta.enableDeletionVectors'='false'";

  /**
   * Calls {@code loadTable} on the Delta REST API to get the Iceberg metadata JSON file path for
   * the given table. The path points to the exact {@code .metadata.json} file written by the last
   * UniForm conversion.
   */
  private String icebergMetadataPath(String fullTableName) throws Exception {
    String[] parts = fullTableName.split("\\.");
    TablesApi deltaApi = new TablesApi(unityCatalogInfo().createApiClient());
    LoadTableResponse resp = deltaApi.loadTable(parts[0], parts[1], parts[2]);
    if (resp.getUniform() == null || resp.getUniform().getIceberg() == null) {
      throw new IllegalStateException("No Iceberg metadata found for table: " + fullTableName);
    }
    return resp.getUniform().getIceberg().getMetadataLocation();
  }

  /**
   * Builds a Hadoop {@link Configuration} for reading Iceberg metadata from the embedded UC test's
   * fake S3 filesystem. {@link S3CredentialFileSystem} maps {@code s3://fakeS3Bucket/...} to local
   * paths and asserts that the static fake credentials are present in the conf.
   */
  private Configuration buildTestHadoopConf() {
    Configuration conf = new Configuration(false);
    conf.set("fs.s3.impl", S3CredentialFileSystem.class.getName());
    conf.set("fs.s3a.access.key", "fakeAccessKey");
    conf.set("fs.s3a.secret.key", "fakeSecretKey");
    conf.set("fs.s3a.session.token", "fakeSessionToken");
    return conf;
  }

  @Test
  public void uniformIcebergIncrementalConversion() throws Exception {
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
          IcebergMeta meta1 = readIcebergMeta(fullTableName);
          Assertions.assertNull(
              meta1.properties.get("base-delta-version"),
              "First (full) conversion must NOT set base-delta-version");
          Assertions.assertTrue(
              meta1.currentSnapshotId != -1L, "Iceberg snapshot should exist after first write");

          // Write 2 — must produce an incremental conversion
          sql("INSERT INTO %s VALUES (2, 'b')", fullTableName);
          check(fullTableName, List.of(row("1", "a"), row("2", "b")));
          IcebergMeta meta2 = readIcebergMeta(fullTableName);
          Assertions.assertEquals(
              "1",
              meta2.properties.get("base-delta-version"),
              "Second conversion must be incremental: base-delta-version should equal 1");
          Assertions.assertEquals(
              meta1.currentSnapshotId,
              meta2.currentSnapshotParentId,
              "Iceberg snapshot chain must be preserved: parent of snapshot-2 must be snapshot-1");
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

  private IcebergMeta readIcebergMeta(String fullTableName) throws Exception {
    String icebergMetadataPath = icebergMetadataPath(fullTableName);
    Configuration conf = buildTestHadoopConf();
    try {
      return readIcebergMetaViaHadoopTables(icebergMetadataPath, conf);
    } catch (NoClassDefFoundError | ClassNotFoundException | RuntimeException e) {
      return readIcebergMetaViaJson(icebergMetadataPath, conf);
    }
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

  private IcebergMeta readIcebergMetaViaJson(String icebergMetadataPath, Configuration conf)
      throws Exception {
    // icebergMetadataPath is the specific metadata JSON file — read it directly.
    Path metadataFilePath = new Path(icebergMetadataPath);
    FileSystem fs = metadataFilePath.getFileSystem(conf);

    String json;
    try (InputStream in = fs.open(metadataFilePath)) {
      json = new String(in.readAllBytes());
    }

    ObjectMapper mapper = new ObjectMapper();
    JsonNode root = mapper.readTree(json);

    // Extract table properties
    Map<String, String> properties = new HashMap<>();
    JsonNode propsNode = root.get("properties");
    if (propsNode != null && !propsNode.isNull()) {
      propsNode.fields().forEachRemaining(e -> properties.put(e.getKey(), e.getValue().asText()));
    }

    // Extract current-snapshot-id
    long currentSnapshotId =
        root.has("current-snapshot-id") ? root.get("current-snapshot-id").asLong(-1L) : -1L;

    // Find parent-snapshot-id for the current snapshot in the snapshots array
    long parentSnapshotId = -1L;
    JsonNode snapshots = root.get("snapshots");
    if (snapshots != null && currentSnapshotId != -1L) {
      for (JsonNode snap : snapshots) {
        if (snap.get("snapshot-id").asLong() == currentSnapshotId) {
          if (snap.has("parent-snapshot-id")) {
            parentSnapshotId = snap.get("parent-snapshot-id").asLong(-1L);
          }
          break;
        }
      }
    }

    return new IcebergMeta(properties, currentSnapshotId, parentSnapshotId);
  }
}
