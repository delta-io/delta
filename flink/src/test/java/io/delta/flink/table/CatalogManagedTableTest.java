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
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.internal.data.DefaultColumnarBatch;
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.internal.tablefeatures.TableFeatures;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterable;
import io.delta.kernel.utils.CloseableIterator;
import java.net.URI;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/** JUnit 6 test suite for CCv2Table. */
class CatalogManagedTableTest extends TestHelper {

  private static final URI CATALOG_ENDPOINT =
      URI.create("https://e2-dogfood.staging.cloud.databricks.com/");
  private static final String CATALOG_TOKEN = "";
  private static final String TABLE_ID = "main.hao.writetest";
  private static final String TABLE_CREATE_ID = "main.hao.testcreatewrite";

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
            TABLE_ID,
            Collections.emptyMap(),
            schema,
            List.of(),
            mockUri,
            "")) {
      checkSerializability(table);
    }
  }

  @Disabled("Requires Unity Catalog access")
  @Test
  void testLoadTableFromE2Dogfood() throws Exception {
    try (CatalogManagedTable table =
        new CatalogManagedTable(
            new UnityCatalog("main", CATALOG_ENDPOINT, CATALOG_TOKEN),
            TABLE_ID,
            Collections.emptyMap(),
            CATALOG_ENDPOINT,
            CATALOG_TOKEN)) {
      table.open();

      assertEquals("main.hao.writetest", table.getId());
      assertEquals(
          URI.create(
              "s3://us-west-2-extstaging-managed-catalog-test-bucket-1/"
                  + "19a85dee-54bc-43a2-87ab-023d0ec16013/tables/b7c3e881-4f7f-40f2-88c1-dff715835a81/"),
          table.getTablePath());
      assertTrue(table.getSchema().equivalent(new StructType().add("id", IntegerType.INTEGER)));
    }
  }

  @Disabled("Requires Unity Catalog access")
  @Test
  void testCommitDataToE2Dogfood() throws Exception {
    CatalogManagedTable table =
        new CatalogManagedTable(
            new UnityCatalog("main", CATALOG_ENDPOINT, CATALOG_TOKEN),
            TABLE_ID,
            Collections.emptyMap(),
            CATALOG_ENDPOINT,
            CATALOG_TOKEN);
    table.open();

    for (int i = 0; i < 100; i++) {
      List<List<?>> values =
          IntStream.range(0, 10).boxed().map(List::of).collect(Collectors.toList());
      ColumnVector colVector = new DataColumnVectorView(values, 0, IntegerType.INTEGER);

      DefaultColumnarBatch columnarBatchData =
          new DefaultColumnarBatch(
              values.size(), table.getSchema(), new ColumnVector[] {colVector});
      FilteredColumnarBatch filteredColumnarBatchData =
          new FilteredColumnarBatch(columnarBatchData, Optional.empty());
      Map<String, Literal> partitionValues = Collections.emptyMap();

      CloseableIterator<FilteredColumnarBatch> data =
          Utils.toCloseableIterator(List.of(filteredColumnarBatchData).iterator());
      try (CloseableIterator<Row> rows = table.writeParquet("abc", data, partitionValues)) {
        table.commit(CloseableIterable.inMemoryIterable(rows), "a", i, Map.of("a", "b"));
      }
    }
  }

  @Disabled("Requires Unity Catalog access")
  @Test
  void testWriteNewTablesToE2Dogfood() throws Exception {
    StructType schema = new StructType().add("id", IntegerType.INTEGER);
    CatalogManagedTable table =
        new CatalogManagedTable(
            new UnityCatalog("main", CATALOG_ENDPOINT, CATALOG_TOKEN),
            TABLE_CREATE_ID,
            Collections.emptyMap(),
            schema,
            List.of(),
            CATALOG_ENDPOINT,
            CATALOG_TOKEN);
    table.open();

    for (int i = 0; i < 10; i++) {
      List<List<?>> values =
          IntStream.range(0, 10).boxed().map(List::of).collect(Collectors.toList());
      ColumnVector colVector = new DataColumnVectorView(values, 0, IntegerType.INTEGER);

      DefaultColumnarBatch columnarBatchData =
          new DefaultColumnarBatch(
              values.size(), table.getSchema(), new ColumnVector[] {colVector});
      FilteredColumnarBatch filteredColumnarBatchData =
          new FilteredColumnarBatch(columnarBatchData, Optional.empty());
      Map<String, Literal> partitionValues = Collections.emptyMap();

      CloseableIterator<FilteredColumnarBatch> data =
          Utils.toCloseableIterator(List.of(filteredColumnarBatchData).iterator());
      try (CloseableIterator<Row> rows = table.writeParquet("abc", data, partitionValues)) {
        table.commit(CloseableIterable.inMemoryIterable(rows), "a", i, Map.of("a", "b"));
      }
    }
  }
}
