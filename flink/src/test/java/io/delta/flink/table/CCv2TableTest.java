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

import io.delta.flink.TestHelper;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.defaults.internal.data.DefaultColumnarBatch;
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterable;
import io.delta.kernel.utils.CloseableIterator;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/** JUnit 6 test suite for CCv2Table. */
class CCv2TableTest extends TestHelper {

  private static final URI CATALOG_ENDPOINT =
      URI.create("https://e2-dogfood.staging.cloud.databricks.com/");
  private static final String CATALOG_TOKEN = "<PAT>";
  private static final String TABLE_ID = "main.hao.writetest";

  @Disabled("Requires Unity Catalog access")
  @Test
  void testLoadTableFromE2Dogfood() {
    CCv2Table table =
        new CCv2Table(
            new UnityCatalog("main", CATALOG_ENDPOINT, CATALOG_TOKEN),
            TABLE_ID,
            Collections.emptyMap(),
            CATALOG_ENDPOINT,
            CATALOG_TOKEN);
    table.open();

    assertEquals("main.hao.writetest", table.getId());
    assertEquals(
        URI.create(
            "s3://us-west-2-extstaging-managed-catalog-test-bucket-1/"
                + "19a85dee-54bc-43a2-87ab-023d0ec16013/tables/b7c3e881-4f7f-40f2-88c1-dff715835a81/"),
        table.getTablePath());
    assertTrue(table.getSchema().equivalent(new StructType().add("id", IntegerType.INTEGER)));
  }

  @Disabled("Requires Unity Catalog access")
  @Test
  void testCommitDataToE2DogfoodViaCcv2() {
    CCv2Table table =
        new CCv2Table(
            new UnityCatalog("main", CATALOG_ENDPOINT, CATALOG_TOKEN),
            TABLE_ID,
            Collections.emptyMap(),
            CATALOG_ENDPOINT,
            CATALOG_TOKEN);
    table.open();

    for (int i = 0; i < 100; i++) {
      List<Integer> values = IntStream.range(0, 10).boxed().collect(Collectors.toList());
      ColumnVector colVector =
          new ColumnVector() {
            @Override
            public DataType getDataType() {
              return IntegerType.INTEGER;
            }

            @Override
            public int getSize() {
              return values.size();
            }

            @Override
            public void close() {}

            @Override
            public boolean isNullAt(int rowId) {
              return values.get(rowId) == null;
            }

            @Override
            public int getInt(int rowId) {
              return values.get(rowId);
            }
          };

      DefaultColumnarBatch columnarBatchData =
          new DefaultColumnarBatch(
              values.size(), table.getSchema(), new ColumnVector[] {colVector});
      FilteredColumnarBatch filteredColumnarBatchData =
          new FilteredColumnarBatch(columnarBatchData, Optional.empty());
      Map<String, Literal> partitionValues = Collections.emptyMap();

      CloseableIterator<FilteredColumnarBatch> data =
          Utils.toCloseableIterator(List.of(filteredColumnarBatchData).iterator());
      CloseableIterator rows = table.writeParquet("abc", data, partitionValues);

      table.commit(CloseableIterable.inMemoryIterable(rows), "a", i, Map.of("a", "b"));
    }
  }

  @Disabled("Requires Unity Catalog access")
  @Test
  void testSerializability() {
    CCv2Table table =
        new CCv2Table(
            new UnityCatalog("main", CATALOG_ENDPOINT, CATALOG_TOKEN),
            TABLE_ID,
            Collections.emptyMap(),
            CATALOG_ENDPOINT,
            CATALOG_TOKEN);

    checkSerializability(table);
  }
}
