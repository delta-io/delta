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

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.delta.flink.TestHelper;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.defaults.internal.data.DefaultColumnarBatch;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterable;
import io.delta.kernel.utils.CloseableIterator;
import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/** JUnit 6 test suite for HadoopTable. */
class HadoopTableTest extends TestHelper {

  private static final URI CATALOG_ENDPOINT =
      URI.create("https://e2-dogfood.staging.cloud.databricks.com/");
  private static final String CATALOG_TOKEN = "<PAT>";
  private static final String TABLE_ID = "main.hao.writetest";

  @Test
  void testCommitWithSameTxnId() {
    withTempDir(
        dir -> {
          String tablePath = dir.getAbsolutePath();
          StructType schema =
              new StructType().add("id", IntegerType.INTEGER).add("part", StringType.STRING);

          HadoopTable table =
              new HadoopTable(
                  URI.create(tablePath), Collections.emptyMap(), schema, List.of("part"));
          table.open();

          for (int i = 0; i < 10; i++) {
            List<Row> actions =
                IntStream.range(0, 5)
                    .mapToObj(
                        j ->
                            dummyAddFileRow(
                                schema, 10 + j, Map.of("part", Literal.ofString("p" + j))))
                    .collect(Collectors.toList());

            CloseableIterable<Row> dataActions =
                new CloseableIterable<Row>() {
                  @Override
                  public CloseableIterator<Row> iterator() {
                    return Utils.toCloseableIterator(actions.iterator());
                  }

                  @Override
                  public void close() {
                    // Nothing to close
                  }
                };
            table.commit(dataActions, "a", 100, Collections.emptyMap());
          }
          // There should be only one version
          assertEquals(1, table.loadLatestSnapshot().getVersion());
        });
  }

  @Disabled("Requires S3 access")
  @Test
  void testCommitToS3Path() {
    String tablePath = "s3://hao-extstaging/flinksink/dest2";
    StructType schema =
        new StructType().add("id", IntegerType.INTEGER).add("part", StringType.STRING);

    HadoopTable table =
        new HadoopTable(URI.create(tablePath), Collections.emptyMap(), schema, List.of("part"));
    table.open();

    for (int i = 0; i < 10; i++) {
      List<Row> actions =
          IntStream.range(0, 5)
              .mapToObj(
                  j -> dummyAddFileRow(schema, 10 + j, Map.of("part", Literal.ofString("p" + j))))
              .collect(Collectors.toList());

      CloseableIterable<Row> dataActions =
          new CloseableIterable<Row>() {
            @Override
            public CloseableIterator<Row> iterator() {
              return Utils.toCloseableIterator(actions.iterator());
            }

            @Override
            public void close() {
              // Nothing to close
            }
          };
      table.commit(dataActions, "a", 100, Collections.emptyMap());
    }
    // There should be only one version
    assertEquals(1, table.loadLatestSnapshot().getVersion());
  }

  @Disabled("Requires Unity Catalog access")
  @Test
  void testCommitDataToE2DogfoodViaPath() {
    HadoopTable table =
        new HadoopTable(
            new UnityCatalog("main", CATALOG_ENDPOINT, CATALOG_TOKEN),
            TABLE_ID,
            Collections.emptyMap());
    table.open();

    List<Integer> values = IntStream.range(0, 100).boxed().collect(Collectors.toList());
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
        new DefaultColumnarBatch(values.size(), table.getSchema(), new ColumnVector[] {colVector});
    FilteredColumnarBatch filteredColumnarBatchData =
        new FilteredColumnarBatch(columnarBatchData, Optional.empty());
    Map<String, Literal> partitionValues = Collections.emptyMap();

    CloseableIterator<FilteredColumnarBatch> data =
        Utils.toCloseableIterator(List.of(filteredColumnarBatchData).iterator());
    CloseableIterator<Row> rows = table.writeParquet("abc", data, partitionValues);

    table.commit(CloseableIterable.inMemoryIterable(rows), "a", 1000L, Map.of("a", "b"));
  }

  @Disabled("Performance test - manual execution only")
  @Test
  void testLoadTableLatency() {
    Engine engine = DefaultEngine.create(new Configuration());
    long counter = 0L;
    long phase1 = 0L;
    long phase2 = 0L;
    long phase3 = 0L;
    int times = 20;

    for (int i = 0; i < times; i++) {
      long start = System.currentTimeMillis();
      HadoopTable hadoopTable =
          new HadoopTable(
              URI.create("file:///Users/hajiang/test/big_table"), Collections.emptyMap());
      hadoopTable.open();
      long p1 = System.currentTimeMillis();
      phase1 += p1 - start;
      hadoopTable.refresh();
      long p2 = System.currentTimeMillis();
      phase2 += p2 - p1;
      hadoopTable
          .loadLatestSnapshot()
          .getScanBuilder()
          .build()
          .getScanFiles(engine)
          .toInMemoryList();
      phase3 += System.currentTimeMillis() - p2;
      counter += System.currentTimeMillis() - start;
    }

    // This is a performance measurement test
    // The assertion just ensures the test runs without errors
    long avgTotal = counter / times;
    long avgPhase1 = phase1 / times;
    long avgPhase2 = phase2 / times;
    long avgPhase3 = phase3 / times;

    // Log results (in a real test, you might want to use a proper logging framework)
    System.out.println(
        String.format(
            "Average times - Total: %dms, Phase1: %dms, Phase2: %dms, Phase3: %dms",
            avgTotal, avgPhase1, avgPhase2, avgPhase3));
  }

  @Disabled("Requires S3 credentials")
  @Test
  void testConnectToS3() {
    Map<String, String> conf = new HashMap<>();
    conf.put("fs.s3a.access.key", "aaa");
    conf.put("fs.s3a.secret.key", "bbb");
    conf.put("fs.s3a.session.token", "ccc");
    HadoopTable table = new HadoopTable(URI.create("s3://east1-cred/"), conf);
    // Test passes if no exception is thrown during table creation
  }
}
