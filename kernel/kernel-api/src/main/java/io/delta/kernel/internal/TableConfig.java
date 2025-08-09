/*
 * Copyright (2023) The Delta Lake Project Authors.
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
package io.delta.kernel.internal;

import io.delta.kernel.exceptions.InvalidConfigurationValueException;
import io.delta.kernel.exceptions.UnknownConfigurationException;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.tablefeatures.TableFeatures;
import io.delta.kernel.internal.util.*;
import io.delta.kernel.internal.util.ColumnMapping.ColumnMappingMode;
import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Represents the table properties. Also provides methods to access the property values from the
 * table metadata.
 */
public class TableConfig<T> {

  //////////////////
  // TableConfigs //
  //////////////////

  /**
   * Whether this Delta table is append-only. Files can't be deleted, or values can't be updated.
   */
  public static final TableConfig<Boolean> APPEND_ONLY_ENABLED =
      new TableConfig<>(
          "delta.appendOnly",
          "false",
          Boolean::valueOf,
          value -> true,
          "needs to be a boolean.",
          true);

  /**
   * Enable change data feed output. When enabled, DELETE, UPDATE, and MERGE INTO operations will
   * need to do additional work to output their change data in an efficiently readable format.
   */
  public static final TableConfig<Boolean> CHANGE_DATA_FEED_ENABLED =
      new TableConfig<>(
          "delta.enableChangeDataFeed",
          "false",
          Boolean::valueOf,
          value -> true,
          "needs to be a boolean.",
          true);

  public static final TableConfig<String> CHECKPOINT_POLICY =
      new TableConfig<>(
          "delta.checkpointPolicy",
          "classic",
          v -> v,
          value -> value.equals("classic") || value.equals("v2"),
          "needs to be a string and one of 'classic' or 'v2'.",
          true);

  /** Whether commands modifying this Delta table are allowed to create new deletion vectors. */
  public static final TableConfig<Boolean> DELETION_VECTORS_CREATION_ENABLED =
      new TableConfig<>(
          "delta.enableDeletionVectors",
          "false",
          Boolean::valueOf,
          value -> true,
          "needs to be a boolean.",
          true);

  /**
   * Whether widening the type of an existing column or field is allowed, either manually using
   * ALTER TABLE CHANGE COLUMN or automatically if automatic schema evolution is enabled.
   */
  public static final TableConfig<Boolean> TYPE_WIDENING_ENABLED =
      new TableConfig<>(
          "delta.enableTypeWidening",
          "false",
          Boolean::valueOf,
          value -> true,
          "needs to be a boolean.",
          true);

  /**
   * Indicates whether Row Tracking is enabled on the table. When this flag is turned on, all rows
   * are guaranteed to have Row IDs and Row Commit Versions assigned to them, and writers are
   * expected to preserve them by materializing them to hidden columns in the data files.
   */
  public static final TableConfig<Boolean> ROW_TRACKING_ENABLED =
      new TableConfig<>(
          "delta.enableRowTracking",
          "false",
          Boolean::valueOf,
          value -> true,
          "needs to be a boolean.",
          true);

  /**
   * The shortest duration we have to keep logically deleted data files around before deleting them
   * physically.
   *
   * <p>Note: this value should be large enough:
   *
   * <ul>
   *   <li>It should be larger than the longest possible duration of a job if you decide to run
   *       "VACUUM" when there are concurrent readers or writers accessing the table.
   *   <li>If you are running a streaming query reading from the table, you should make sure the
   *       query doesn't stop longer than this value. Otherwise, the query may not be able to
   *       restart as it still needs to read old files.
   * </ul>
   */
  public static final TableConfig<Long> TOMBSTONE_RETENTION =
      new TableConfig<>(
          "delta.deletedFileRetentionDuration",
          "interval 1 week",
          IntervalParserUtils::safeParseIntervalAsMillis,
          value -> value >= 0,
          "needs to be provided as a calendar interval such as '2 weeks'. Months"
              + " and years are not accepted. You may specify '365 days' for a year instead.",
          true);

  /**
   * How often to checkpoint the delta log? For every N (this config) commits to the log, we will
   * suggest write out a checkpoint file that can speed up the Delta table state reconstruction.
   */
  public static final TableConfig<Integer> CHECKPOINT_INTERVAL =
      new TableConfig<>(
          "delta.checkpointInterval",
          "10",
          Integer::valueOf,
          value -> value > 0,
          "needs to be a positive integer.",
          true);

  /**
   * The shortest duration we have to keep delta/checkpoint files around before deleting them. We
   * can only delete delta files that are before a checkpoint.
   */
  public static final TableConfig<Long> LOG_RETENTION =
      new TableConfig<>(
          "delta.logRetentionDuration",
          "interval 30 days",
          IntervalParserUtils::safeParseIntervalAsMillis,
          value -> true,
          "needs to be provided as a calendar interval such as '2 weeks'. Months "
              + "and years are not accepted. You may specify '365 days' for a year instead.",
          true /* editable */);

  /** Whether to clean up expired checkpoints and delta logs. */
  public static final TableConfig<Boolean> EXPIRED_LOG_CLEANUP_ENABLED =
      new TableConfig<>(
          "delta.enableExpiredLogCleanup",
          "true",
          Boolean::valueOf,
          value -> true,
          "needs to be a boolean.",
          true /* editable */);

  /**
   * This table property is used to track the enablement of the {@code inCommitTimestamps}.
   *
   * <p>When enabled, commit metadata includes a monotonically increasing timestamp that allows for
   * reliable TIMESTAMP AS OF time travel even if filesystem operations change a commit file's
   * modification timestamp.
   */
  public static final TableConfig<Boolean> IN_COMMIT_TIMESTAMPS_ENABLED =
      new TableConfig<>(
          "delta.enableInCommitTimestamps",
          "false", /* default values */
          v -> Boolean.valueOf(v),
          value -> true,
          "needs to be a boolean.",
          true);

  /**
   * This table property is used to track the version of the table at which {@code
   * inCommitTimestamps} were enabled.
   */
  public static final TableConfig<Optional<Long>> IN_COMMIT_TIMESTAMP_ENABLEMENT_VERSION =
      new TableConfig<>(
          "delta.inCommitTimestampEnablementVersion",
          null, /* default values */
          v -> Optional.ofNullable(v).map(Long::valueOf),
          value -> true,
          "needs to be a long.",
          true);

  /**
   * This table property is used to track the timestamp at which {@code inCommitTimestamps} were
   * enabled. More specifically, it is the {@code inCommitTimestamps} of the commit with the version
   * specified in {@link #IN_COMMIT_TIMESTAMP_ENABLEMENT_VERSION}.
   */
  public static final TableConfig<Optional<Long>> IN_COMMIT_TIMESTAMP_ENABLEMENT_TIMESTAMP =
      new TableConfig<>(
          "delta.inCommitTimestampEnablementTimestamp",
          null, /* default values */
          v -> Optional.ofNullable(v).map(Long::valueOf),
          value -> true,
          "needs to be a long.",
          true);

  /** This table property is used to control the column mapping mode. */
  public static final TableConfig<ColumnMappingMode> COLUMN_MAPPING_MODE =
      new TableConfig<>(
          "delta.columnMapping.mode",
          "none", /* default values */
          ColumnMappingMode::fromTableConfig,
          value -> true,
          "Needs to be one of none, id, name.",
          true);

  /** This table property is used to control the maximum column mapping ID. */
  public static final TableConfig<Long> COLUMN_MAPPING_MAX_COLUMN_ID =
      new TableConfig<>(
          "delta.columnMapping.maxColumnId", "0", Long::valueOf, value -> value >= 0, "", false);

  /**
   * Table property that enables modifying the table in accordance with the Delta-Iceberg
   * Compatibility V2 protocol.
   *
   * @see <a
   *     href="https://github.com/delta-io/delta/blob/master/PROTOCOL.md#delta-iceberg-compatibility-v2">
   *     Delta-Iceberg Compatibility V2 Protocol</a>
   */
  public static final TableConfig<Boolean> ICEBERG_COMPAT_V2_ENABLED =
      new TableConfig<>(
          "delta.enableIcebergCompatV2",
          "false",
          Boolean::valueOf,
          value -> true,
          "needs to be a boolean.",
          true);

  /**
   * Table property that enables modifying the table in accordance with the Delta-Iceberg
   * Compatibility V3 protocol. TODO: add the delta protocol link once updated
   * [https://github.com/delta-io/delta/issues/4574]
   */
  public static final TableConfig<Boolean> ICEBERG_COMPAT_V3_ENABLED =
      new TableConfig<>(
          "delta.enableIcebergCompatV3",
          "false",
          Boolean::valueOf,
          value -> true,
          "needs to be a boolean.",
          true);

  /**
   * The number of columns to collect stats on for data skipping. A value of -1 means collecting
   * stats for all columns.
   *
   * <p>For Struct types, all leaf fields count individually toward this limit in depth-first order.
   * For example, if a table has columns a, b.c, b.d, and e, then the first three indexed columns
   * would be a, b.c, and b.d. Map and array types are not supported for statistics collection.
   */
  public static final TableConfig<Integer> DATA_SKIPPING_NUM_INDEXED_COLS =
      new TableConfig<>(
          "delta.dataSkippingNumIndexedCols",
          "32",
          Integer::valueOf,
          value -> value >= -1,
          "needs to be larger than or equal to -1.",
          true);

  /**
   * Table property that enables modifying the table in accordance with the Delta-Iceberg Writer
   * Compatibility V1 ({@code icebergCompatWriterV1}) protocol.
   */
  public static final TableConfig<Boolean> ICEBERG_WRITER_COMPAT_V1_ENABLED =
      new TableConfig<>(
          "delta.enableIcebergWriterCompatV1",
          "false",
          Boolean::valueOf,
          value -> true,
          "needs to be a boolean.",
          true);

  /**
   * Table property that enables modifying the table in accordance with the Delta-Iceberg Writer
   * Compatibility V3 ({@code icebergCompatWriterV3}) protocol. V2 is skipped to align with the
   * iceberg v3 spec.
   */
  public static final TableConfig<Boolean> ICEBERG_WRITER_COMPAT_V3_ENABLED =
      new TableConfig<>(
          "delta.enableIcebergWriterCompatV3",
          "false",
          Boolean::valueOf,
          value -> true,
          "needs to be a boolean.",
          true);

  public static class UniversalFormats {

    /**
     * The value that enables uniform exports to Iceberg for {@linkplain
     * TableConfig#UNIVERSAL_FORMAT_ENABLED_FORMATS}.
     *
     * <p>{@link #ICEBERG_COMPAT_V2_ENABLED but also be set to true} to fully enable this feature.
     */
    public static final String FORMAT_ICEBERG = "iceberg";
    /**
     * The value to use to enable uniform exports to Hudi for {@linkplain
     * TableConfig#UNIVERSAL_FORMAT_ENABLED_FORMATS}.
     */
    public static final String FORMAT_HUDI = "hudi";
  }

  private static final Collection<String> ALLOWED_UNIFORM_FORMATS =
      Collections.unmodifiableList(
          Arrays.asList(UniversalFormats.FORMAT_HUDI, UniversalFormats.FORMAT_ICEBERG));

  /** Table config that allows for translation of Delta metadata to other table formats metadata. */
  public static final TableConfig<Set<String>> UNIVERSAL_FORMAT_ENABLED_FORMATS =
      new TableConfig<>(
          "delta.universalFormat.enabledFormats",
          null,
          TableConfig::parseStringSet,
          value -> ALLOWED_UNIFORM_FORMATS.containsAll(value),
          String.format("each value must in the the set: %s", ALLOWED_UNIFORM_FORMATS),
          true);

  /**
   * Table property that enables modifying the table in accordance with the Delta-Variant Shredding
   * Preview protocol.
   *
   * @see <a
   *     href="https://github.com/delta-io/delta/blob/master/protocol_rfcs/variant-shredding.md">
   *     Delta-Variant Shredding Protocol</a>
   */
  public static final TableConfig<Boolean> VARIANT_SHREDDING_ENABLED =
      new TableConfig<>(
          "delta.enableVariantShredding",
          "false",
          Boolean::valueOf,
          value -> true,
          "needs to be a boolean.",
          true);

  public static final TableConfig<String> MATERIALIZED_ROW_ID_COLUMN_NAME =
      new TableConfig<>(
          "delta.rowTracking.materializedRowIdColumnName",
          null,
          v -> v,
          value -> true,
          "need to be a string.",
          false);

  public static final TableConfig<String> MATERIALIZED_ROW_COMMIT_VERSION_COLUMN_NAME =
      new TableConfig<>(
          "delta.rowTracking.materializedRowCommitVersionColumnName",
          null,
          v -> v,
          value -> true,
          "need to be a string.",
          false);

  /** All the valid properties that can be set on the table. */
  private static final Map<String, TableConfig<?>> VALID_PROPERTIES =
      Collections.unmodifiableMap(
          new HashMap<String, TableConfig<?>>() {
            {
              addConfig(this, APPEND_ONLY_ENABLED);
              addConfig(this, CHANGE_DATA_FEED_ENABLED);
              addConfig(this, CHECKPOINT_POLICY);
              addConfig(this, DELETION_VECTORS_CREATION_ENABLED);
              addConfig(this, TYPE_WIDENING_ENABLED);
              addConfig(this, ROW_TRACKING_ENABLED);
              addConfig(this, LOG_RETENTION);
              addConfig(this, EXPIRED_LOG_CLEANUP_ENABLED);
              addConfig(this, TOMBSTONE_RETENTION);
              addConfig(this, CHECKPOINT_INTERVAL);
              addConfig(this, IN_COMMIT_TIMESTAMPS_ENABLED);
              addConfig(this, IN_COMMIT_TIMESTAMP_ENABLEMENT_VERSION);
              addConfig(this, IN_COMMIT_TIMESTAMP_ENABLEMENT_TIMESTAMP);
              addConfig(this, COLUMN_MAPPING_MODE);
              addConfig(this, ICEBERG_COMPAT_V2_ENABLED);
              addConfig(this, ICEBERG_COMPAT_V3_ENABLED);
              addConfig(this, ICEBERG_WRITER_COMPAT_V1_ENABLED);
              addConfig(this, ICEBERG_WRITER_COMPAT_V3_ENABLED);
              addConfig(this, COLUMN_MAPPING_MAX_COLUMN_ID);
              addConfig(this, DATA_SKIPPING_NUM_INDEXED_COLS);
              addConfig(this, UNIVERSAL_FORMAT_ENABLED_FORMATS);
              addConfig(this, MATERIALIZED_ROW_ID_COLUMN_NAME);
              addConfig(this, MATERIALIZED_ROW_COMMIT_VERSION_COLUMN_NAME);
            }
          });

  ///////////////////////////
  // Static Helper Methods //
  ///////////////////////////

  /**
   * Validates that the given new properties that the txn is trying to update in table. Properties
   * that have `delta.` prefix in the key name should be in valid list and are editable. The caller
   * is expected to store the returned properties in the table metadata after further validation
   * from a protocol point of view. The returned properties will have the key's case normalized as
   * defined in its {@link TableConfig}.
   *
   * @param newProperties the properties to validate
   * @throws InvalidConfigurationValueException if any of the properties are invalid
   * @throws UnknownConfigurationException if any of the properties are unknown
   */
  public static Map<String, String> validateAndNormalizeDeltaProperties(
      Map<String, String> newProperties) {
    Map<String, String> validatedProperties = new HashMap<>();
    for (Map.Entry<String, String> kv : newProperties.entrySet()) {
      String key = kv.getKey().toLowerCase(Locale.ROOT);
      String value = kv.getValue();

      boolean isTableFeatureOverrideKey =
          key.startsWith(TableFeatures.SET_TABLE_FEATURE_SUPPORTED_PREFIX);
      boolean isTableConfigKey = key.startsWith("delta.");
      // TableFeature override properties validation is handled separately in TransactionBuilder.
      boolean shouldValidateProperties = isTableConfigKey && !isTableFeatureOverrideKey;
      if (shouldValidateProperties) {
        // If it is a delta table property, make sure it is a supported property and editable
        if (!VALID_PROPERTIES.containsKey(key)) {
          throw DeltaErrors.unknownConfigurationException(kv.getKey());
        }

        TableConfig<?> tableConfig = VALID_PROPERTIES.get(key);
        if (!tableConfig.editable) {
          throw DeltaErrors.cannotModifyTableProperty(kv.getKey());
        }

        tableConfig.validate(value);
        validatedProperties.put(tableConfig.getKey(), value);
      } else {
        // allow unknown properties to be set (and preserve their original case!)
        validatedProperties.put(kv.getKey(), value);
      }
    }
    return validatedProperties;
  }

  private static void addConfig(HashMap<String, TableConfig<?>> configs, TableConfig<?> config) {
    configs.put(config.getKey().toLowerCase(Locale.ROOT), config);
  }

  /////////////////////////////
  // Member Fields / Methods //
  /////////////////////////////

  private final String key;
  private final String defaultValue;
  private final Function<String, T> fromString;
  private final Predicate<T> validator;
  private final boolean editable;
  private final String helpMessage;

  private TableConfig(
      String key,
      String defaultValue,
      Function<String, T> fromString,
      Predicate<T> validator,
      String helpMessage,
      boolean editable) {
    this.key = key;
    this.defaultValue = defaultValue;
    this.fromString = fromString;
    this.validator = validator;
    this.helpMessage = helpMessage;
    this.editable = editable;
  }

  /**
   * Returns the value of the table property from the given metadata.
   *
   * @param metadata the table metadata
   * @return the value of the table property
   */
  public T fromMetadata(Metadata metadata) {
    return fromMetadata(metadata.getConfiguration());
  }

  /**
   * Returns the value of the table property from the given configuration.
   *
   * @param configuration the table configuration
   * @return the value of the table property
   */
  public T fromMetadata(Map<String, String> configuration) {
    String value = configuration.getOrDefault(key, defaultValue);
    validate(value);
    return fromString.apply(value);
  }

  /**
   * Returns the key of the table property.
   *
   * @return the key of the table property
   */
  public String getKey() {
    return key;
  }

  private void validate(String value) {
    T parsedValue = fromString.apply(value);
    if (!validator.test(parsedValue)) {
      throw DeltaErrors.invalidConfigurationValueException(key, value, helpMessage);
    }
  }

  private static Set<String> parseStringSet(String value) {
    if (value == null || value.isEmpty()) {
      return Collections.emptySet();
    }
    String[] formats = value.split(",");
    Set<String> config = new HashSet<>();

    for (String format : formats) {
      config.add(format.trim());
    }
    return config;
  }
}
