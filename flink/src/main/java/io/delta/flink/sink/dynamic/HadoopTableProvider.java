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

package io.delta.flink.sink.dynamic;

import io.delta.flink.sink.TableBuilder;
import io.delta.flink.table.DeltaTable;
import io.delta.flink.table.HadoopTable;
import io.delta.kernel.types.StructType;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * {@link DeltaTableProvider} backed by {@link io.delta.flink.table.HadoopCatalog} and {@link
 * HadoopTable}, built via {@link TableBuilder}.
 *
 * <p>Each logical {@code tableName} from the stream must be a Hadoop-compatible table root URI (for
 * example {@code file:///…}, {@code s3a://bucket/path}, {@code abfss://…}), matching {@link
 * TableBuilder#withTablePath(String)}.
 */
public final class HadoopTableProvider implements DeltaTableProvider {

  private final Map<String, String> tableConfigurations;

  /**
   * @param tableConfigurations storage / credential and other options (same keys as {@link
   *     TableBuilder#withConfigurations(Map)}); must not include a per-stream {@code
   *     hadoop.table_path}, which is supplied as {@code tableName} on each {@link #getOrCreate}.
   */
  public HadoopTableProvider(Map<String, String> tableConfigurations) {
    this.tableConfigurations =
        new HashMap<>(Objects.requireNonNull(tableConfigurations, "tableConfigurations"));
  }

  @Override
  public DeltaTable getOrCreate(
      String tableName, StructType schema, List<String> partitionColumns) {
    Objects.requireNonNull(tableName, "tableName");
    List<String> parts = partitionColumns == null ? List.of() : partitionColumns;
    DeltaTable table =
        new TableBuilder()
            .withConfigurations(new HashMap<>(tableConfigurations))
            .withTablePath(tableName)
            .withSchema(schema)
            .withPartitionColNames(parts)
            .build();
    table.open();
    return table;
  }
}
