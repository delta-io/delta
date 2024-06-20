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

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;

import io.delta.kernel.exceptions.InvalidConfigurationValueException;
import io.delta.kernel.exceptions.UnknownConfigurationException;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.util.IntervalParserUtils;

/**
 * Represents the table properties. Also provides methods to access the property values
 * from the table metadata.
 */
public class TableConfig<T> {
    /**
     * The shortest duration we have to keep logically deleted data files around before deleting
     * them physically.
     *
     * Note: this value should be large enough:
     * <ul>
     *     <li>It should be larger than the longest possible duration of a job if you decide to
     *     run "VACUUM" when there are concurrent readers or writers accessing the table.</li>
     *     <li>If you are running a streaming query reading from the table, you should make sure
     *     the query doesn't stop longer than this value. Otherwise, the query may not be able to
     *     restart as it still needs to read old files.</li>
     * </ul>
     */
    public static final TableConfig<Long> TOMBSTONE_RETENTION = new TableConfig<>(
            "delta.deletedFileRetentionDuration",
            "interval 1 week",
            IntervalParserUtils::safeParseIntervalAsMillis,
            value -> value >= 0,
            "needs to be provided as a calendar interval such as '2 weeks'. Months" +
                    " and years are not accepted. You may specify '365 days' for a year instead."
    );

    /**
     * How often to checkpoint the delta log? For every N (this config) commits to the log, we will
     * suggest write out a checkpoint file that can speed up the Delta table state reconstruction.
     */
    public static final TableConfig<Integer> CHECKPOINT_INTERVAL = new TableConfig<>(
            "delta.checkpointInterval",
            "10",
            Integer::valueOf,
            value -> value > 0,
            "needs to be a positive integer."
    );

    /**
     * All the valid properties that can be set on the table.
     */
    private static final HashMap<String, TableConfig> validProperties = new HashMap<>();

    /**
     * This table property is used to track the enablement of the {@code inCommitTimestamps}.
     * <p>
     * When enabled, commit metadata includes a monotonically increasing timestamp that allows for
     * reliable TIMESTAMP AS OF time travel even if filesystem operations change a commit file's
     * modification timestamp.
     */
    public static final TableConfig<Boolean> IN_COMMIT_TIMESTAMPS_ENABLED = new TableConfig<>(
            "delta.enableInCommitTimestamps-preview",
            "false", /* default values */
            Boolean::valueOf,
            value -> true,
            "needs to be a boolean."
    );

    /**
     * This table property is used to track the version of the table at which
     * {@code inCommitTimestamps} were enabled.
     */
    public static final TableConfig<Optional<Long>> IN_COMMIT_TIMESTAMP_ENABLEMENT_VERSION =
            new TableConfig<>(
                    "delta.inCommitTimestampEnablementVersion-preview",
                    null, /* default values */
                    v -> Optional.ofNullable(v).map(Long::valueOf),
                    value -> true,
                    "needs to be a long."
            );

    /**
     * This table property is used to track the timestamp at which {@code inCommitTimestamps} were
     * enabled. More specifically, it is the {@code inCommitTimestamps} of the commit with the
     * version specified in {@link #IN_COMMIT_TIMESTAMP_ENABLEMENT_VERSION}.
     */
    public static final TableConfig<Optional<Long>> IN_COMMIT_TIMESTAMP_ENABLEMENT_TIMESTAMP =
            new TableConfig<>(
                    "delta.inCommitTimestampEnablementTimestamp-preview",
                    null, /* default values */
                    v -> Optional.ofNullable(v).map(Long::valueOf),
                    value -> true,
                    "needs to be a long."
            );

    private final String key;
    private final String defaultValue;
    private final Function<String, T> fromString;
    private final Predicate<T> validator;
    private final String helpMessage;

    private static void addConfig(Map<String, TableConfig> configs, TableConfig config) {
        configs.put(config.getKey().toLowerCase(Locale.ROOT), config);
    }

    static {
        addConfig(validProperties, TOMBSTONE_RETENTION);
        addConfig(validProperties, CHECKPOINT_INTERVAL);
        addConfig(validProperties, IN_COMMIT_TIMESTAMPS_ENABLED);
        addConfig(validProperties, IN_COMMIT_TIMESTAMP_ENABLEMENT_VERSION);
        addConfig(validProperties, IN_COMMIT_TIMESTAMP_ENABLEMENT_TIMESTAMP);
    }

    private TableConfig(
            String key,
            String defaultValue,
            Function<String, T> fromString,
            Predicate<T> validator,
            String helpMessage) {
        this.key = key;
        this.defaultValue = defaultValue;
        this.fromString = fromString;
        this.validator = validator;
        this.helpMessage = helpMessage;
    }

    /**
     * Returns the value of the table property from the given metadata.
     *
     * @param metadata the table metadata
     * @return the value of the table property
     */
    public T fromMetadata(Metadata metadata) {
        String value = metadata.getConfiguration().getOrDefault(key, defaultValue);
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

    /**
     * Validates that the given properties have the delta prefix in the key name, and they are in
     * the set of valid properties. The caller should get the validated configurations and store the
     * case of the property name defined in TableConfig.
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
            if (key.startsWith("delta.")) {
                TableConfig tableConfig = validProperties.get(key);
                if (tableConfig != null) {
                    tableConfig.validate(value);
                    validatedConfigurations.put(tableConfig.getKey(), value);
                } else {
                    throw DeltaErrors.unknownConfigurationException(key);
                }
            } else {
                throw DeltaErrors.unknownConfigurationException(key);
            }
        }
        return validatedConfigurations;
    }

    private void validate(String value) {
        T parsedValue = fromString.apply(value);
        if (!validator.test(parsedValue)) {
            throw DeltaErrors.invalidConfigurationValueException(key, value, helpMessage);
        }
    }
}
