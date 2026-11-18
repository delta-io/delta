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
import org.junit.jupiter.api.Test;

/** JUnit test suite for HadoopTable. */
class HadoopTableTest extends TestHelper {

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
}
