/*
 *  Copyright (2026) The Delta Lake Project Authors.
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;

/**
 * Factory for creating {@link DeltaDynamicTableSink} instances from Flink SQL DDL options.
 * Registered via SPI with connector identifier "delta".
 */
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

    Map<String, String> resolvedOptions = new HashMap<>(options.toMap());
    DeltaSinkConf.WriteMode writeMode = options.get(DeltaSinkConf.WRITE_MODE);

    if (writeMode == DeltaSinkConf.WriteMode.UPSERT) {
      Optional<UniqueConstraint> pkConstraint = schema.getPrimaryKey();
      if (pkConstraint.isEmpty()) {
        throw new ValidationException(
            "write.mode = 'upsert' requires a 'PRIMARY KEY (...) NOT ENFORCED' clause on the "
                + "table definition.");
      }
      // Resolve PK column names to 0-based ordinals against the physical schema. Lower layers
      // operate on RowData ordinals; doing the lookup here lets us surface "unknown column"
      // failures as ValidationException at planning time instead of as a writer-construction
      // RuntimeException at job startup.
      List<String> physicalNames = schema.getColumnNames();
      String ordinals =
          pkConstraint.get().getColumns().stream()
              .map(
                  name -> {
                    int idx = physicalNames.indexOf(name);
                    if (idx < 0) {
                      throw new ValidationException(
                          "Primary-key column '"
                              + name
                              + "' does not exist in the table schema "
                              + physicalNames
                              + ".");
                    }
                    return Integer.toString(idx);
                  })
              .collect(Collectors.joining(","));
      // PRIMARY_KEY is an internal wire-format option carrying the resolved ordinals to
      // TaskManagers; it is intentionally not in optionalOptions().
      resolvedOptions.put(DeltaSinkConf.PRIMARY_KEY.key(), ordinals);
    }

    return new DeltaDynamicTableSink(
        context.getObjectIdentifier(), consumedDataType, sinkParallelism, resolvedOptions);
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
        DeltaSinkConf.FILE_ROLLING_COUNT,
        DeltaSinkConf.WRITE_MODE);
  }
}
