/*
 * Copyright (2021) The Delta Lake Project Authors.
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

import io.delta.flink.sink.DeltaSink;
import java.util.Arrays;
import java.util.Map;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.connector.sink.abilities.SupportsPartitioning;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

public class DeltaDynamicTableSink implements DynamicTableSink, SupportsPartitioning {

  private final DataType consumedDataType;
  private final Integer configuredParallelism;
  private final Map<String, String> options; // whatever delta-flink.properties you need

  public DeltaDynamicTableSink(
      DataType consumedDataType, Integer configuredParallelism, Map<String, String> options) {
    this.consumedDataType = consumedDataType;
    this.configuredParallelism = configuredParallelism;
    this.options = options;
  }

  @Override
  public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
    return ChangelogMode.insertOnly();
  }

  @Override
  public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
    RowType rowType = (RowType) consumedDataType.getLogicalType();

    DeltaSink deltaSink =
        new DeltaSink.Builder()
            .withFlinkSchema(rowType)
            .withTablePath(options.get("table_path"))
            .withPartitionColNames(Arrays.asList(options.getOrDefault("partitions", "").split(",")))
            .build();

    if (configuredParallelism != null) {
      return SinkV2Provider.of(deltaSink, configuredParallelism);
    } else {
      return SinkV2Provider.of(deltaSink);
    }
  }

  @Override
  public DynamicTableSink copy() {
    return new DeltaDynamicTableSink(
        this.consumedDataType, this.configuredParallelism, this.options);
  }

  @Override
  public String asSummaryString() {
    return "DeltaDynamicTableSink";
  }

  @Override
  public void applyStaticPartition(Map<String, String> partition) {
    if (!partition.isEmpty()) {
      throw new ValidationException("DeltaSink does not support static partitioning");
    }
  }

  @Override
  public boolean requiresPartitionGrouping(boolean supportsGrouping) {
    return SupportsPartitioning.super.requiresPartitionGrouping(supportsGrouping);
  }
}
