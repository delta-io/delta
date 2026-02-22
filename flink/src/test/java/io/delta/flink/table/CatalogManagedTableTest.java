/*
 *  Copyright (2021) The Delta Lake Project Authors.
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.delta.flink.MockHttp;
import io.delta.flink.TestHelper;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.internal.tablefeatures.TableFeatures;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.StructType;
import java.net.URI;
import java.util.*;
import org.junit.jupiter.api.Test;

/** JUnit test suite for CCv2Table. */
class CatalogManagedTableTest extends TestHelper {

  @Test
  void testCreateCCv2Table() {
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

                  CatalogManagedTable.CCV2_FEATURES_CONF.forEach(
                      (key, value) -> assertEquals(value, table.conf.catalogConf().get(key)));
                  assertEquals(uuid, table.conf.catalogConf().get("io.unitycatalog.tableId"));

                  SnapshotImpl snapshot = (SnapshotImpl) table.snapshot().get();
                  assertEquals(uuid, snapshot.getTableProperties().get("io.unitycatalog.tableId"));

                  assertTrue(
                      CatalogManagedTable.CCV2_FEATURES_CONF.keySet().stream()
                          .map(s -> s.replace("delta.feature.", ""))
                          .allMatch(
                              s ->
                                  snapshot
                                      .getProtocol()
                                      .supportsFeature(TableFeatures.getTableFeature(s))));
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
