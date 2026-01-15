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

import static io.delta.flink.sink.sql.DeltaDynamicTableSinkFactory.*;

import io.delta.flink.sink.DeltaSink;
import io.delta.kernel.internal.util.Preconditions;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.ProviderContext;
import org.apache.flink.table.connector.sink.DataStreamSinkProvider;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.abilities.SupportsPartitioning;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

public class DeltaDynamicTableSink implements DynamicTableSink, SupportsPartitioning {

  private final ObjectIdentifier tableId;
  private final DataType consumedDataType;
  private final Integer configuredParallelism;
  private final Map<String, String> options;

  public DeltaDynamicTableSink(
      ObjectIdentifier tableId,
      DataType consumedDataType,
      Integer configuredParallelism,
      Map<String, String> options) {
    this.tableId = tableId;
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

    Preconditions.checkArgument(
        options.get(FactoryUtil.CONNECTOR.key()).equals(IDENTIFIER), "Target table must be delta");

    DeltaSink deltaSink;
    if (options.containsKey(TABLE_PATH.key())) {
      // Hadoop-based Sink
      deltaSink =
          DeltaSink.builder()
              .withFlinkSchema(rowType)
              .withTablePath(options.get(TABLE_PATH.key()))
              .withPartitionColNames(
                  Arrays.asList(options.getOrDefault(PARTITIONS.key(), "").split(",")))
              .build();
    } else {
      // Fetch catalog and configs
      Preconditions.checkArgument(options.containsKey(FlinkUnityCatalogFactory.ENDPOINT.key()));
      Preconditions.checkArgument(options.containsKey(FlinkUnityCatalogFactory.TOKEN.key()));
      deltaSink =
          DeltaSink.builder()
              .withFlinkSchema(rowType)
              .withTableId(tableId.asSummaryString())
              .withEndpoint(options.get(FlinkUnityCatalogFactory.ENDPOINT.key()))
              .withToken(options.get(FlinkUnityCatalogFactory.TOKEN.key()))
              .withConfigurations(options)
              .build();
    }

    String uid = options.getOrDefault(UID.key(), UUID.randomUUID().toString());
    // There is an 80-chars limit to operator name. We truncate the prefix to 60 and leave some
    // space to suffix.
    String tableId = deltaSink.getTable().getId();
    String name =
        options.getOrDefault(NAME.key(), tableId.substring(0, Math.min(tableId.length(), 60)));

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
        this.tableId, this.consumedDataType, this.configuredParallelism, this.options);
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
