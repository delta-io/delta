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

import io.delta.flink.sink.DeltaSinkConf;
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

  public static ConfigOption<String> TABLE_PATH =
      ConfigOptions.key("table_path")
          .stringType()
          .noDefaultValue()
          .withDescription("Delta table path for the sink");

  public static ConfigOption<String> PARTITIONS =
      ConfigOptions.key("partitions")
          .stringType()
          .noDefaultValue()
          .withDescription("Partition column names separated by comma");

  public static ConfigOption<String> UID =
      ConfigOptions.key("uid")
          .stringType()
          .noDefaultValue()
          .withDescription("UID to be assigned to the sink");

  public static ConfigOption<String> NAME =
      ConfigOptions.key("name").stringType().noDefaultValue().withDescription("Name of the sink");

  @Override
  public DynamicTableSink createDynamicTableSink(Context context) {
    final FactoryUtil.TableFactoryHelper helper =
        FactoryUtil.createTableFactoryHelper(this, context);

    helper.validate();

    ReadableConfig options = helper.getOptions();
    ResolvedSchema schema = context.getCatalogTable().getResolvedSchema();
    DataType consumedDataType = schema.toPhysicalRowDataType();

    Integer sinkParallelism = options.getOptional(FactoryUtil.SINK_PARALLELISM).orElse(null);
    return new DeltaDynamicTableSink(
        context.getObjectIdentifier(), consumedDataType, sinkParallelism, options.toMap());
  }

  public static final String IDENTIFIER = "delta";

  @Override
  public String factoryIdentifier() {
    return IDENTIFIER;
  }

  @Override
  public Set<ConfigOption<?>> requiredOptions() {
    return Set.of();
  }

  @Override
  public Set<ConfigOption<?>> optionalOptions() {
    return Set.of(
        TABLE_PATH,
        PARTITIONS,
        UID,
        NAME,
        FlinkUnityCatalogFactory.ENDPOINT,
        FlinkUnityCatalogFactory.TOKEN,
        FactoryUtil.SINK_PARALLELISM,
        DeltaSinkConf.SCHEMA_EVOLUTION_MODE,
        DeltaSinkConf.FILE_ROLLING_STRATEGY,
        DeltaSinkConf.FILE_ROLLING_SIZE,
        DeltaSinkConf.FILE_ROLLING_COUNT);
  }
}
