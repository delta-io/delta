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

  /*
   * This table property is used to track the commit-coordinator name for this table. If this
   * property is not set, the table will be considered as file system table and commits will be
   * done via atomically publishing the commit file.
   */
  public static final TableConfig<Optional<String>> COORDINATED_COMMITS_COORDINATOR_NAME =
      new TableConfig<>(
          "delta.coordinatedCommits.commitCoordinator-preview",
          null, /* default values */
          Optional::ofNullable,
          value -> true,
          "The commit-coordinator name for this table. This is used to determine "
              + "which implementation of commit-coordinator to use when committing "
              + "to this table. If this property is not set, the table will be "
              + "considered as file system table and commits will be done via "
              + "atomically publishing the commit file.",
          true);

  /*
   * This table property is used to track the configuration properties for the commit coordinator
   * which is needed to build the commit coordinator client.
   */
  public static final TableConfig<Map<String, String>> COORDINATED_COMMITS_COORDINATOR_CONF =
      new TableConfig<>(
          "delta.coordinatedCommits.commitCoordinatorConf-preview",
          null, /* default values */
          JsonUtils::parseJSONKeyValueMap,
          value -> true,
          "A string-to-string map of configuration properties for the"
              + " coordinated commits-coordinator.",
          true);

  /*
   * This property is used by the commit coordinator to uniquely identify and manage the table
   * internally.
   */
  public static final TableConfig<Map<String, String>> COORDINATED_COMMITS_TABLE_CONF =
      new TableConfig<>(
          "delta.coordinatedCommits.tableConf-preview",
          null, /* default values */
          JsonUtils::parseJSONKeyValueMap,
          value -> true,
          "A string-to-string map of configuration properties for"
              + "  describing the table to commit-coordinator.",
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

  /** All the valid properties that can be set on the table. */
  private static final Map<String, TableConfig<?>> VALID_PROPERTIES =
      Collections.unmodifiableMap(
          new HashMap<String, TableConfig<?>>() {
            {
              addConfig(this, TOMBSTONE_RETENTION);
              addConfig(this, CHECKPOINT_INTERVAL);
              addConfig(this, IN_COMMIT_TIMESTAMPS_ENABLED);
              addConfig(this, IN_COMMIT_TIMESTAMP_ENABLEMENT_VERSION);
              addConfig(this, IN_COMMIT_TIMESTAMP_ENABLEMENT_TIMESTAMP);
              addConfig(this, COORDINATED_COMMITS_COORDINATOR_NAME);
              addConfig(this, COORDINATED_COMMITS_COORDINATOR_CONF);
              addConfig(this, COORDINATED_COMMITS_TABLE_CONF);
              addConfig(this, COLUMN_MAPPING_MODE);
              addConfig(this, ICEBERG_COMPAT_V2_ENABLED);
              addConfig(this, COLUMN_MAPPING_MAX_COLUMN_ID);
            }
          });

  ///////////////////////////
  // Static Helper Methods //
  ///////////////////////////

  /**
   * Validates that the given properties have the delta prefix in the key name, and they are in the
   * set of valid properties. The caller should get the validated configurations and store the case
   * of the property name defined in TableConfig.
   *
   * @param configurations the properties to validate
   * @throws InvalidConfigurationValueException if any of the properties are invalid
   * @throws UnknownConfigurationException if any of the properties are unknown
   */
  public static Map<String, String> validateProperties(Map<String, String> configurations) {
    Map<String, String> validatedConfigurations = new HashMap<>();
    for (Map.Entry<String, String> kv : configurations.entrySet()) {
      String key = kv.getKey().toLowerCase(Locale.ROOT);
      String value = kv.getValue();
      if (key.startsWith("delta.") && VALID_PROPERTIES.containsKey(key)) {
        // Disable `columnMapping` and `icebergCompatV2` for Delta 3.3 release
        // There are a couple of pieces missing from fully supporting these features
        // 1. Data path PR is not yet merged (delta-io/delta#3475), waiting on the
        //    expression fixup which is going to affect how column mapping is done.
        // 2. Auto enabling of the protocol version based on the current table version and
        //    column mapping/iceberg compat v2 enabled.
        if ((key.equalsIgnoreCase(COLUMN_MAPPING_MODE.getKey())
                || key.equalsIgnoreCase(ICEBERG_COMPAT_V2_ENABLED.getKey()))
            &&
            // Ignore the check if tests are running
            !Boolean.getBoolean("ENABLE_COLUMN_MAPPING_TESTS")) {
          throw DeltaErrors.unknownConfigurationException(kv.getKey());
        }

        TableConfig<?> tableConfig = VALID_PROPERTIES.get(key);
        if (tableConfig.editable) {
          tableConfig.validate(value);
          validatedConfigurations.put(tableConfig.getKey(), value);
        } else {
          throw DeltaErrors.cannotModifyTableProperty(kv.getKey());
        }
      } else {
        throw DeltaErrors.unknownConfigurationException(kv.getKey());
      }
    }
    return validatedConfigurations;
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
}
