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

package io.delta.flink.sink.sql;

import static org.junit.jupiter.api.Assertions.assertTrue;

import io.delta.flink.TestHelper;
import java.util.List;
import java.util.Map;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.junit.jupiter.api.Test;

/** JUnit 6 test suite for {@link DeltaDynamicTableSink}. */
class DeltaDynamicTableSinkTest extends TestHelper {

  @Test
  void testLoadTable() {
    withTempDir(
        dir -> {
          Map<String, String> options = Map.of("connector", "delta", "table_path", dir.getPath());

          CatalogTable table =
              CatalogTable.newBuilder()
                  .schema(
                      Schema.newBuilder()
                          .column("id", DataTypes.BIGINT())
                          .column("dt", DataTypes.STRING())
                          .build())
                  .comment("test table")
                  .partitionKeys(List.of())
                  .options(options)
                  .build();

          ResolvedCatalogTable resolvedTable =
              new ResolvedCatalogTable(
                  table,
                  ResolvedSchema.physical(
                      new String[] {"id", "dt"},
                      new org.apache.flink.table.types.DataType[] {
                        DataTypes.BIGINT(), DataTypes.STRING()
                      }));

          TestDynamicTableSinkContext context = new TestDynamicTableSinkContext(resolvedTable);

          DeltaDynamicTableSinkFactory factory = new DeltaDynamicTableSinkFactory();
          var sink = factory.createDynamicTableSink(context);

          assertTrue(sink instanceof DeltaDynamicTableSink);
        });
  }
}
