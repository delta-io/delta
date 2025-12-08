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
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.types.logical.RowType;

/**
 * Configuration container for {@code DeltaSink}.
 *
 * <p>This class holds:
 *
 * <ul>
 *   <li>The sink schema (both Flink {@link RowType} and Delta {@code StructType})
 *   <li>Per-table sink configurations passed by users
 *   <li>Schema evolution policy selection
 *   <li>File rolling strategy construction
 * </ul>
 */
public class DeltaSinkConf implements Serializable {

  // ----------------------------------------------------------------------
  // Per-table sink-only configuration options (NOT persisted into Delta table)
  // ----------------------------------------------------------------------

  /**
   * File rolling strategy.
   *
   * <p>Supported values:
   *
   * <ul>
   *   <li>{@code "count"}: roll when record count reaches {@link #FILE_ROLLING_COUNT}
   *   <li>{@code "size"}: roll when byte size reaches {@link #FILE_ROLLING_SIZE}
   * </ul>
   */
  public static final ConfigOption<String> FILE_ROLLING_STRATEGY =
      ConfigOptions.key("file_rolling.strategy")
          .stringType()
          .defaultValue("size")
          .withDescription(
              "Rolling strategy for output files. Supported values: 'count' and 'size'.");

  /**
   * File rolling size threshold in bytes.
   *
   * <p>Only effective when {@link #FILE_ROLLING_STRATEGY} = {@code "size"}. A negative value
   * disables size-based rolling.
   */
  public static final ConfigOption<Long> FILE_ROLLING_SIZE =
      ConfigOptions.key("file_rolling.size")
          .longType()
          .defaultValue(104857600L)
          .withDescription(
              "Size threshold in bytes for rolling output files when strategy is 'size'. "
                  + "Negative value disables size-based rolling.");

  /**
   * File rolling record count threshold.
   *
   * <p>Only effective when {@link #FILE_ROLLING_STRATEGY} = {@code "count"}. A negative value
   * disables count-based rolling.
   */
  public static final ConfigOption<Integer> FILE_ROLLING_COUNT =
      ConfigOptions.key("file_rolling.count")
          .intType()
          .defaultValue(-1)
          .withDescription(
              "Record count threshold for rolling output files when strategy is 'count'. "
                  + "Negative value disables count-based rolling.");

  /**
   * Schema evolution mode.
   *
   * <p>Supported values:
   *
   * <ul>
   *   <li>{@code "no"}: do not allow any schema change
   *   <li>{@code "newcolumn"}: allow adding new columns only
   * </ul>
   *
   * <p>Note: The sink does not automatically evolve the table schema. It only validates whether the
   * observed schema change is allowed by this mode.
   */
  public static final ConfigOption<String> SCHEMA_EVOLUTION_MODE =
      ConfigOptions.key("schema_evolution.mode")
          .stringType()
          .defaultValue("no")
          .withDescription(
              "Schema evolution policy: 'no' disallows any change; 'newcolumn' allows adding new "
                  + "columns only.");

  // ----------------------------------------------------------------------
  // State
  // ----------------------------------------------------------------------

  private RowType sinkFlinkSchema;
  private final String sinkSchemaString;
  private final Map<String, String> conf;
  private final Configuration configuration;
  private final SchemaEvolutionPolicy schemaEvolutionPolicy;

  private transient StructType sinkSchema;

  /**
   * Creates a {@link DeltaSinkConf} from a Delta {@code StructType} schema and user-provided
   * configs.
   *
   * @param sinkSchema Delta sink schema
   * @param conf per-table configuration map
   */
  public DeltaSinkConf(StructType sinkSchema, Map<String, String> conf) {
    this.sinkSchemaString = sinkSchema.toJson();
    this.sinkSchema = sinkSchema;
    this.conf = conf;

    // Materialize the raw Map into Flink Configuration for ConfigOption access.
    this.configuration = Configuration.fromMap(conf);

    String mode = configuration.get(SCHEMA_EVOLUTION_MODE).toLowerCase();
    switch (mode) {
      case "newcolumn":
        this.schemaEvolutionPolicy = new NewColumnEvolution();
        break;
      case "no":
      default:
        this.schemaEvolutionPolicy = new NoEvolution();
    }
  }

  /**
   * Creates a {@link DeltaSinkConf} from a Flink {@link RowType} schema and user-provided configs.
   *
   * @param flinkSchema Flink sink schema
   * @param conf per-table configuration map
   */
  public DeltaSinkConf(RowType flinkSchema, Map<String, String> conf) {
    this(Conversions.FlinkToDelta.schema(flinkSchema), conf);
    this.sinkFlinkSchema = flinkSchema;
  }

  /**
   * Returns the Flink sink schema if the instance was constructed with {@link RowType}.
   *
   * @return Flink {@link RowType} sink schema, or {@code null} if constructed from Delta schema
   *     only
   */
  public RowType getSinkFlinkSchema() {
    return sinkFlinkSchema;
  }

  /**
   * Returns the Delta sink schema. If this object was deserialized, the schema will be lazily
   * reconstructed from JSON.
   *
   * @return Delta {@code StructType} schema
   */
  public StructType getSinkSchema() {
    if (this.sinkSchema == null) {
      this.sinkSchema = DataTypeJsonSerDe.deserializeStructType(this.sinkSchemaString);
    }
    return this.sinkSchema;
  }

  /**
   * Creates a {@link FileRollingStrategy} based on the per-table configuration.
   *
   * <p>Default behavior: {@code count} strategy with {@link #FILE_ROLLING_COUNT} = -1 (disabled).
   *
   * @return a file rolling strategy instance
   */
  public FileRollingStrategy createFileRollingStrategy() {
    String strategy = configuration.get(FILE_ROLLING_STRATEGY).toLowerCase();
    switch (strategy) {
      case "count":
        return new CountRolling(configuration.get(FILE_ROLLING_COUNT));
      case "size":
      default:
        return new SizeRolling(configuration.get(FILE_ROLLING_SIZE));
    }
  }

  /**
   * Returns the schema evolution policy chosen by {@link #SCHEMA_EVOLUTION_MODE}.
   *
   * @return schema evolution policy implementation
   */
  public SchemaEvolutionPolicy getSchemaEvolutionPolicy() {
    return schemaEvolutionPolicy;
  }

  /**
   * Returns the raw per-table configuration map.
   *
   * <p>Prefer {@link #getConfiguration()} for reading values via {@link ConfigOption}.
   */
  public Map<String, String> getConf() {
    return conf;
  }

  /**
   * Returns the Flink {@link Configuration} view of the per-table configs.
   *
   * @return configuration
   */
  public Configuration getConfiguration() {
    return configuration;
  }

  // ----------------------------------------------------------------------
  // Schema evolution policy
  // ----------------------------------------------------------------------

  /**
   * Policy interface for validating whether schema evolution is allowed.
   *
   * <p>This is a validation-only mechanism. The sink does not mutate the table schema.
   */
  public interface SchemaEvolutionPolicy extends Serializable {
    /**
     * Returns whether the sink is allowed to proceed given the current table schema and sink
     * schema.
     *
     * @param tableSchema schema read from the target table
     * @param sinkSchema schema implied by the sink/job
     * @return true if the sink can proceed, false otherwise
     */
    boolean allowEvolve(StructType tableSchema, StructType sinkSchema);
  }

  /** Disallows any schema change. Table schema must be equivalent to sink schema. */
  static class NoEvolution implements SchemaEvolutionPolicy {
    @Override
    public boolean allowEvolve(StructType tableSchema, StructType sinkSchema) {
      return tableSchema.equivalent(sinkSchema);
    }
  }

  /**
   * Allows adding new columns in the table schema only.
   *
   * <p>All fields in the sink schema must exist in the table schema with:
   *
   * <ul>
   *   <li>same name
   *   <li>equivalent data type
   *   <li>same nullability
   * </ul>
   *
   * <p>This means the table schema may contain extra fields that the sink does not write yet.
   */
  static class NewColumnEvolution implements SchemaEvolutionPolicy {
    @Override
    public boolean allowEvolve(StructType tableSchema, StructType sinkSchema) {
      Map<String, StructField> tableFields =
          tableSchema.fields().stream()
              .collect(Collectors.toMap(StructField::getName, Function.identity()));

      // Every field of sink schema must exist in table schema with same definition.
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

  // ----------------------------------------------------------------------
  // File rolling
  // ----------------------------------------------------------------------

  /** File rolling strategy that decides when to close the current file and start a new one. */
  public interface FileRollingStrategy {
    /**
     * Returns true if the sink should roll to a new file for the given row.
     *
     * @param row record being written
     * @return true to roll, false to continue writing to the current file
     */
    boolean shouldRoll(RowData row);
  }

  /**
   * Rolls files based on the estimated record size in bytes.
   *
   * <p>Note: For efficiency, only {@link BinaryRowData} contributes to size tracking. If the {@link
   * RowData} implementation is not {@link BinaryRowData}, the rolling decision will not incorporate
   * its size.
   */
  static class SizeRolling implements FileRollingStrategy {

    private final long sizeLimit;
    private long sizeCounter = 0L;

    /**
     * Creates a size-based rolling strategy.
     *
     * @param sizeLimit size threshold in bytes; negative value disables size-based rolling
     */
    public SizeRolling(long sizeLimit) {
      this.sizeLimit = sizeLimit;
    }

    @Override
    public boolean shouldRoll(RowData row) {
      if (sizeLimit < 0) {
        return false;
      }

      // For efficiency, only BinaryRowData is supported.
      if (row instanceof BinaryRowData) {
        sizeCounter += ((BinaryRowData) row).getSizeInBytes();
      }

      if (sizeCounter >= sizeLimit) {
        sizeCounter = 0L;
        return true;
      }
      return false;
    }
  }

  /** Rolls files based on number of records written. */
  static class CountRolling implements FileRollingStrategy {

    private final int countLimit;
    private int counter = 0;

    /**
     * Creates a count-based rolling strategy.
     *
     * @param countLimit record count threshold; negative value disables count-based rolling
     */
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
