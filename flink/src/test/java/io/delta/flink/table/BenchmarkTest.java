/*
 *  Copyright (2021) The Delta Lake Project Authors.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package io.delta.flink.table;

import io.delta.flink.TestHelper;
import io.delta.flink.table.MetricListener.StatsListener;
import io.delta.kernel.data.Row;
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterable;
import io.delta.kernel.utils.CloseableIterator;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** JUnit 6 benchmark test suite for performance measurements. */
class BenchmarkTest extends TestHelper {

  private static final Logger LOGGER = LoggerFactory.getLogger(BenchmarkTest.class);
  private static final URI CATALOG_ENDPOINT =
      URI.create("https://e2-dogfood.staging.cloud.databricks.com/");
  private static final String CATALOG_TOKEN = "<PAT>";

  @Disabled("Benchmark test - manual execution only")
  @Test
  void testBenchmarkTheLocalFsWrite() {
    withTempDir(
        dir -> {
          String tablePath = dir.getAbsolutePath();
          StructType schema =
              new StructType().add("id", IntegerType.INTEGER).add("part", StringType.STRING);

          HadoopTable table =
              new HadoopTable(
                  URI.create(tablePath), Collections.emptyMap(), schema, List.of("part"));
          table.open();

          StatsListener statsListener = new StatsListener();
          table.addMetricListener(statsListener);

          for (int i = 0; i < 100; i++) {
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
            table.commit(dataActions, "a", i, Collections.emptyMap());
          }

          statsListener
              .report()
              .forEach(
                  (key, values) -> {
                    LOGGER.error(
                        "{}, [{}, {}, {}, {}]", key, values[0], values[1], values[2], values[3]);
                  });
        });
  }

  @Disabled("Benchmark test - requires Unity Catalog access")
  @Test
  void testBenchmarkTheS3FsWrite() {
    StructType schema =
        new StructType().add("id", IntegerType.INTEGER).add("part", StringType.STRING);

    String tableId = "main.hao.flink_test";

    HadoopTable table =
        new HadoopTable(
            new UnityCatalog("main", CATALOG_ENDPOINT, CATALOG_TOKEN),
            tableId,
            Collections.emptyMap());
    table.open();

    StatsListener statsListener = new StatsListener();
    table.addMetricListener(statsListener);

    for (int i = 0; i < 100; i++) {
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
      table.commit(dataActions, "a", i, Collections.emptyMap());
    }

    statsListener
        .report()
        .forEach(
            (key, values) -> {
              LOGGER.error("{}, [{}, {}, {}, {}]", key, values[0], values[1], values[2], values[3]);
            });
  }

  @Disabled("Benchmark test - requires Unity Catalog access")
  @Test
  void testBenchmarkTheCcv2Write() throws Exception {
    StructType schema =
        new StructType().add("id", IntegerType.INTEGER).add("name", StringType.STRING);

    String tableId = "main.hao.flink_test";

    CatalogManagedTable table =
        new CatalogManagedTable(
            new UnityCatalog("main", CATALOG_ENDPOINT, CATALOG_TOKEN),
            tableId,
            Collections.emptyMap(),
            CATALOG_ENDPOINT,
            CATALOG_TOKEN);
    table.open();

    for (int i = 0; i < 100; i++) {
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
      table.commit(dataActions, "a", i, Collections.emptyMap());
    }

    table.close();
  }
}
