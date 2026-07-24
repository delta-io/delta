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

package io.delta.flink.table;

import static io.delta.kernel.unitycatalog.UCCatalogManagedClient.UC_TABLE_ID_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.delta.flink.MockHttp;
import io.delta.flink.TestHelper;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.internal.tablefeatures.TableFeatures;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterable;
import java.net.URI;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/** JUnit test suite for CCv2Table. */
class CatalogManagedTableTest extends TestHelper {

  @Test
  void testCreateCatalogManagedTable() {
    withTempDir(
        dir -> {
          String uuid = UUID.randomUUID().toString();
          StructType schema = new StructType().add("id", IntegerType.INTEGER);
          MockHttp.withMock(
              MockHttp.forNewUCTable(uuid, dir.getAbsolutePath()),
              dummyHttp -> {
                try (CatalogManagedTable table =
                    new CatalogManagedTable(
                        new UnityCatalog("main", dummyHttp.uri(), ""),
                        "main.abc.def",
                        Collections.emptyMap(),
                        schema,
                        List.of(),
                        dummyHttp.uri(),
                        "")) {
                  table.open();

                  assertEquals(
                      AbstractKernelTable.normalize(URI.create(dir.getAbsolutePath())),
                      table.getTablePath());

                  assertEquals(uuid, table.conf.catalogConf().get(UC_TABLE_ID_KEY));
                  assertEquals("true", table.conf.catalogConf().get("delta.enableDeletionVectors"));

                  SnapshotImpl snapshot = (SnapshotImpl) table.snapshot().get();
                  assertEquals(uuid, snapshot.getTableProperties().get(UC_TABLE_ID_KEY));
                  assertEquals(
                      "true", snapshot.getTableProperties().get("delta.enableDeletionVectors"));

                  assertTrue(
                      snapshot
                          .getProtocol()
                          .supportsFeature(TableFeatures.DELETION_VECTORS_RW_FEATURE));
                  dummyHttp.verifyStagingCredentialRequests(1);
                }
              });
        });
  }

  @Test
  void testCreateCatalogManagedTableWithAmbientCredentials() {
    withTempDir(
        dir -> {
          String uuid = UUID.randomUUID().toString();
          StructType schema = new StructType().add("id", IntegerType.INTEGER);
          MockHttp.withMock(
              MockHttp.forNewUCTable(uuid, dir.getAbsolutePath()),
              mockHttp -> {
                try (CatalogManagedTable table =
                    new CatalogManagedTable(
                        new UnityCatalog(
                            "main", mockHttp.uri(), "", /* credentialVendingEnabled = */ false),
                        "main.abc.def",
                        Map.of(TableConf.CREDENTIALS_SOURCE.key(), "ambient"),
                        schema,
                        List.of(),
                        mockHttp.uri(),
                        "")) {
                  table.open();
                  mockHttp.verifyStagingCredentialRequests(0);
                }
              });
        });
  }

  @Test
  void testCommitCatalogManagedTable() {
    withTempDir(
        dir -> {
          String uuid = UUID.randomUUID().toString();
          StructType schema = new StructType().add("id", IntegerType.INTEGER);
          MockHttp.withMock(
              MockHttp.forNewUCTable(uuid, dir.getAbsolutePath()),
              mockHttp -> {
                try (CatalogManagedTable table =
                    new CatalogManagedTable(
                        new UnityCatalog("main", mockHttp.uri(), ""),
                        "main.abc.def",
                        Collections.emptyMap(),
                        schema,
                        List.of(),
                        mockHttp.uri(),
                        "")) {
                  table.open();

                  table.commit(
                      CloseableIterable.emptyIterable(), "test-app", 1L, Collections.emptyMap());

                  mockHttp.verifyPostRequest(
                      "/api/2.1/unity-catalog/delta/v1/catalogs/main/schemas/abc/tables/def");

                  // Let asynchronous post-commit work finish before the mock and temp table close.
                  table.generalThreadPool.shutdown();
                  assertTrue(table.generalThreadPool.awaitTermination(10, TimeUnit.SECONDS));
                }
              });
        });
  }

  @Test
  void testSerializability() throws Exception {
    StructType schema = new StructType().add("id", IntegerType.INTEGER);
    URI mockUri = URI.create("http://localhost");
    try (CatalogManagedTable table =
        new CatalogManagedTable(
            new UnityCatalog("main", mockUri, ""),
            "main.default.abc",
            Collections.emptyMap(),
            schema,
            List.of(),
            mockUri,
            "")) {
      checkSerializability(table);
    }
  }
}
