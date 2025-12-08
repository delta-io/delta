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

package io.delta.flink.inttest;

import static org.junit.jupiter.api.Assertions.*;

import io.delta.flink.table.CatalogManagedTable;
import io.delta.flink.table.DataColumnVectorView;
import io.delta.flink.table.UnityCatalog;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.internal.data.DefaultColumnarBatch;
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.internal.util.Utils;
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
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

public class CatalogManagedTableIntTest extends IntTestBase {

  private static final String TEST_TABLE_NAME = "main.hao.flinkint_cc";
  private static final String TEST_NEW_TABLE_NAME = "main.hao.flinkint_ccnew";

  public CatalogManagedTableIntTest(SparkSession spark, URI catalogEndpoint, String catalogToken) {
    super(spark, catalogEndpoint, catalogToken);
  }

  @BeforeEach
  public void before() {
    spark.sql(
        String.format(
            "CREATE TABLE IF NOT EXISTS %s (id INT) USING delta TBLPROPERTIES ('delta.feature.catalogManaged'= 'supported')",
            TEST_TABLE_NAME));
    spark.sql(String.format("INSERT INTO %s (id) VALUES (1), (2)", TEST_TABLE_NAME));
  }

  @AfterEach
  public void after() {
    spark.sql(String.format("DROP TABLE IF EXISTS %s", TEST_TABLE_NAME));
    spark.sql(String.format("DROP TABLE IF EXISTS %s", TEST_NEW_TABLE_NAME));
  }

  @IntTest
  void testLoadTable() throws Exception {
    try (CatalogManagedTable table =
        new CatalogManagedTable(
            new UnityCatalog("main", catalogEndpoint, catalogToken),
            TEST_TABLE_NAME,
            Collections.emptyMap(),
            catalogEndpoint,
            catalogToken)) {
      table.open();

      assertEquals(TEST_TABLE_NAME, table.getId());
      assertNotNull(table.getTablePath());
      assertTrue(table.getSchema().equivalent(new StructType().add("id", IntegerType.INTEGER)));
    }
  }

  @IntTest
  void testWriteTable() throws Exception {
    CatalogManagedTable table =
        new CatalogManagedTable(
            new UnityCatalog("main", catalogEndpoint, catalogToken),
            TEST_TABLE_NAME,
            Collections.emptyMap(),
            catalogEndpoint,
            catalogToken);
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

    Dataset<org.apache.spark.sql.Row> count =
        spark.sql(String.format("SELECT COUNT(1) FROM %s", TEST_TABLE_NAME));
    assertCount(1002, count);
  }

  @IntTest
  void testWriteNewTables() throws Exception {
    StructType schema = new StructType().add("id", IntegerType.INTEGER);
    CatalogManagedTable table =
        new CatalogManagedTable(
            new UnityCatalog("main", catalogEndpoint, catalogToken),
            TEST_NEW_TABLE_NAME,
            Collections.emptyMap(),
            schema,
            List.of(),
            catalogEndpoint,
            catalogToken);
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

    Dataset<org.apache.spark.sql.Row> count =
        spark.sql(String.format("SELECT COUNT(1) FROM %s", TEST_NEW_TABLE_NAME));
    assertCount(100, count);
  }
}
