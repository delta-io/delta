package io.delta.flink.sink.dynamic;

import io.delta.flink.sink.DeltaSink;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.connector.sink.abilities.SupportsPartitioning;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import java.util.Arrays;
import java.util.Map;

public class DeltaDynamicTableSink implements DynamicTableSink, SupportsPartitioning {

    private final DataType consumedDataType;
    private final Integer configuredParallelism;
    private final Map<String, String> options;    // whatever config you need

    public DeltaDynamicTableSink(
            DataType consumedDataType,
            Integer configuredParallelism,
            Map<String, String> options) {
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

        DeltaSink deltaSink = new DeltaSink.Builder()
                .withFlinkSchema(rowType)
                .withTablePath(options.get("table_path"))
                .withPartitionColNames(
                        Arrays.asList(options.getOrDefault(
                                "partitions", "").split(",")))
                .build();

        if (configuredParallelism != null) {
            return SinkV2Provider.of(deltaSink, configuredParallelism);
        } else {
            return SinkV2Provider.of(deltaSink);
        }
    }

    @Override
    public DynamicTableSink copy() {
        return new DeltaDynamicTableSink(this.consumedDataType, this.configuredParallelism, this.options);
    }

    @Override
    public String asSummaryString() {
        return "DeltaDynamicTableSink";
    }

    @Override
    public void applyStaticPartition(Map<String, String> partition) {
        if(!partition.isEmpty()) {
            throw new ValidationException("DeltaSink does not support static partitioning");
        }
    }

    @Override
    public boolean requiresPartitionGrouping(boolean supportsGrouping) {
        return SupportsPartitioning.super.requiresPartitionGrouping(supportsGrouping);
    }
}
