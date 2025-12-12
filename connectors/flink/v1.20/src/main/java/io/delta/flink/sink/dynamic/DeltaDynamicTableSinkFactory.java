package io.delta.flink.sink.dynamic;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;

public class DeltaDynamicTableSinkFactory implements DynamicTableSinkFactory {

  ConfigOption<String> TABLE_PATH =
      ConfigOptions.key("table_path")
          .stringType()
          .noDefaultValue()
          .withDescription("Delta table path for the sink");

  ConfigOption<String> PARTITIONS =
      ConfigOptions.key("partitions")
          .stringType()
          .noDefaultValue()
          .withDescription("Partition column names separated by comma");

  @Override
  public DynamicTableSink createDynamicTableSink(Context context) {
    final FactoryUtil.TableFactoryHelper helper =
        FactoryUtil.createTableFactoryHelper(this, context);

    helper.validate();

    ReadableConfig options = helper.getOptions();
    ResolvedSchema schema = context.getCatalogTable().getResolvedSchema();
    DataType consumedDataType = schema.toPhysicalRowDataType();

    Integer sinkParallelism = options.getOptional(FactoryUtil.SINK_PARALLELISM).orElse(null);

    // TODO Support other Delta properties
    Map<String, String> deltaOptions = new HashMap<>();
    deltaOptions.put(TABLE_PATH.key(), options.get(TABLE_PATH));
    deltaOptions.put(PARTITIONS.key(), options.getOptional(PARTITIONS).orElse(""));

    return new DeltaDynamicTableSink(consumedDataType, sinkParallelism, deltaOptions);
  }

  @Override
  public String factoryIdentifier() {
    return "delta-connector";
  }

  @Override
  public Set<ConfigOption<?>> requiredOptions() {
    return Set.of(TABLE_PATH);
  }

  @Override
  public Set<ConfigOption<?>> optionalOptions() {
    return Set.of(PARTITIONS);
  }
}
