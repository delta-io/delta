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

import static io.delta.flink.sink.dynamic.DeltaDynamicTableSinkFactory.*;

import io.delta.flink.sink.DeltaSink;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.ProviderContext;
import org.apache.flink.table.connector.sink.DataStreamSinkProvider;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.abilities.SupportsPartitioning;
import org.apache.flink.table.data.RowData;
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

    // TODO Support Catalog-based sink creation
    DeltaSink deltaSink =
        new DeltaSink.Builder()
            .withFlinkSchema(rowType)
            .withTablePath(options.get(TABLE_PATH.key()))
            .withPartitionColNames(
                Arrays.asList(options.getOrDefault(PARTITIONS.key(), "").split(",")))
            .build();

    String uid = options.getOrDefault(UID.key(), UUID.randomUUID().toString());
    // There is a 80 char limit to operator name.
    String name = options.getOrDefault(NAME.key(), deltaSink.getTable().getId().substring(0, 60));

    return new DataStreamSinkProvider() {
      @Override
      public DataStreamSink<?> consumeDataStream(
          ProviderContext providerContext, DataStream<RowData> dataStream) {
        return dataStream.sinkTo(deltaSink).uid(uid).name(name);
      }

      @Override
      public Optional<Integer> getParallelism() {
        return Optional.ofNullable(configuredParallelism);
      }
    };
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
