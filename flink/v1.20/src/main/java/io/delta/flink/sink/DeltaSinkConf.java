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

package io.delta.flink.sink;

import io.delta.kernel.internal.types.DataTypeJsonSerDe;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import java.io.Serializable;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.types.logical.RowType;

public class DeltaSinkConf implements Serializable {

  public static String FILE_ROLLING_STRATEGY_KEY = "file-rolling-strategy";
  public static String FILE_ROLLING_SIZE_KEY = "file-rolling-size";
  public static String FILE_ROLLING_COUNT_KEY = "file-rolling-count";
  public static String SCHEMA_EVOLUTION_MODE_KEY = "schema-evolution-mode";

  private RowType sinkFlinkSchema;
  private final String sinkSchemaString;
  private final Map<String, String> conf;
  private final SchemaEvolutionPolicy schemaEvolutionPolicy;

  private transient StructType sinkSchema;

  public DeltaSinkConf(StructType sinkSchema, Map<String, String> conf) {
    this.sinkSchemaString = sinkSchema.toJson();
    this.sinkSchema = sinkSchema;
    this.conf = conf;
    switch (conf.getOrDefault(SCHEMA_EVOLUTION_MODE_KEY, "no")) {
      case "newcolumn":
        this.schemaEvolutionPolicy = new NewColumnEvolution();
        break;
      case "no":
      default:
        this.schemaEvolutionPolicy = new NoEvolution();
    }
  }

  public DeltaSinkConf(RowType flinkSchema, Map<String, String> conf) {
    this(Conversions.FlinkToDelta.schema(flinkSchema), conf);
    this.sinkFlinkSchema = flinkSchema;
  }

  public RowType getSinkFlinkSchema() {
    return sinkFlinkSchema;
  }

  public StructType getSinkSchema() {
    if (this.sinkSchema == null) {
      this.sinkSchema = DataTypeJsonSerDe.deserializeStructType(this.sinkSchemaString);
    }
    return this.sinkSchema;
  }

  public FileRollingStrategy createFileRollingStrategy() {
    switch (conf.getOrDefault(FILE_ROLLING_STRATEGY_KEY, "count")) {
      case "size":
        return new SizeRolling(Long.parseLong(conf.getOrDefault(FILE_ROLLING_SIZE_KEY, "-1")));
      case "count":
      default:
        return new CountRolling(Integer.parseInt(conf.getOrDefault(FILE_ROLLING_COUNT_KEY, "-1")));
    }
  }

  public SchemaEvolutionPolicy getSchemaEvolutionPolicy() {
    return schemaEvolutionPolicy;
  }

  public interface SchemaEvolutionPolicy extends Serializable {
    boolean allowEvolve(StructType tableSchema, StructType sinkSchema);
  }

  static class NoEvolution implements SchemaEvolutionPolicy {
    @Override
    public boolean allowEvolve(StructType tableSchema, StructType sinkSchema) {
      return tableSchema.equivalent(sinkSchema);
    }
  }

  static class NewColumnEvolution implements SchemaEvolutionPolicy {
    @Override
    public boolean allowEvolve(StructType tableSchema, StructType sinkSchema) {
      Map<String, StructField> tableFields =
          tableSchema.fields().stream()
              .collect(Collectors.toMap(StructField::getName, Function.identity()));
      // Every field of sink schema must exist in table schema
      return sinkSchema.fields().stream()
          .allMatch(
              field -> {
                StructField tableField = tableFields.getOrDefault(field.getName(), null);
                return tableField != null
                    && tableField.getDataType().equivalent(field.getDataType())
                    && tableField.isNullable() == field.isNullable();
              });
    }
  }

  public interface FileRollingStrategy {
    boolean shouldRoll(RowData row);
  }

  static class SizeRolling implements FileRollingStrategy {

    private long sizeLimit = -1L;
    private long sizeCounter = 0L;

    public SizeRolling(long sizeLimit) {
      this.sizeLimit = sizeLimit;
    }

    @Override
    public boolean shouldRoll(RowData row) {
      if (sizeLimit < 0) {
        return false;
      }
      // For efficiency, only BinaryRowData is supported
      if (row instanceof BinaryRowData) {
        sizeCounter += ((BinaryRowData) row).getSizeInBytes();
      }
      if (sizeCounter >= sizeLimit) {
        sizeCounter = 0;
        return true;
      }
      return false;
    }
  }

  static class CountRolling implements FileRollingStrategy {

    private int countLimit = -1;
    private int counter = 0;

    public CountRolling(int countLimit) {
      this.countLimit = countLimit;
    }

    @Override
    public boolean shouldRoll(RowData row) {
      if (countLimit < 0) {
        return false;
      }
      counter++;
      if (counter >= countLimit) {
        counter = 0;
        return true;
      }
      return false;
    }
  }
}
