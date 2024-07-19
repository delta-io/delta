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

import java.io.IOException;
import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Predicate;

import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.exceptions.InvalidConfigurationValueException;
import io.delta.kernel.exceptions.UnknownConfigurationException;
import io.delta.kernel.types.MapType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.util.IntervalParserUtils;
import io.delta.kernel.internal.util.VectorUtils;
import static io.delta.kernel.internal.util.InternalUtils.getSingularElement;
import static io.delta.kernel.internal.util.InternalUtils.singletonStringColumnVector;

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
            (engineOpt, v) -> IntervalParserUtils.safeParseIntervalAsMillis(v),
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
            (engineOpt, v) -> Integer.valueOf(v),
            value -> value > 0,
            "needs to be a positive integer."
    );

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
            (engineOpt, v) -> Boolean.valueOf(v),
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
                    (engineOpt, v) -> Optional.ofNullable(v).map(Long::valueOf),
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
                    (engineOpt, v) -> Optional.ofNullable(v).map(Long::valueOf),
                    value -> true,
                    "needs to be a long."
            );

    /*
     * This table property is used to track the commit-coordinator name for this table. If this
     * property is not set, the table will be considered as file system table and commits will be
     * done via atomically publishing the commit file.
     */
    public static final TableConfig<Optional<String>> COORDINATED_COMMITS_COORDINATOR_NAME =
            new TableConfig<>(
                    "delta.coordinatedCommits.commitCoordinator-preview",
                    null, /* default values */
                    (engineOpt, v) -> Optional.ofNullable(v),
                    value -> true,
                    "The commit-coordinator name for this table. This is used to determine " +
                            "which implementation of commit-coordinator to use when committing " +
                            "to this table. If this property is not set, the table will be " +
                            "considered as file system table and commits will be done via " +
                            "atomically publishing the commit file."
            );

    /*
     * This table property is used to track the configuration properties for the commit coordinator
     * which is needed to build the commit coordinator client.
     */
    public static final TableConfig<Map<String, String>> COORDINATED_COMMITS_COORDINATOR_CONF =
            new TableConfig<>(
                    "delta.coordinatedCommits.commitCoordinatorConf-preview",
                    null, /* default values */
                    (engineOpt, v) -> {
                        Engine engine = engineOpt.orElseThrow(
                                () -> new IllegalStateException("Engine is not provided"));
                        return parseJSONKeyValueMap(engine, v);
                    },
                    value -> true,
                    "A string-to-string map of configuration properties for the" +
                            " coordinated commits-coordinator."
            );

    /*
     * This property is used by the commit coordinator to uniquely identify and manage the table
     * internally.
     */
    public static final TableConfig<Map<String, String>> COORDINATED_COMMITS_TABLE_CONF =
            new TableConfig<>(
                    "delta.coordinatedCommits.tableConf-preview",
                    null, /* default values */
                    (engineOpt, v) -> {
                        Engine engine = engineOpt.orElseThrow(
                                () -> new IllegalStateException("Engine is not provided"));
                        return parseJSONKeyValueMap(engine, v);
                    },
                    value -> true,
                    "A string-to-string map of configuration properties for" +
                            "  describing the table to commit-coordinator."
            );

    /**
     * All the valid properties that can be set on the table.
     */
    private static final Map<String, TableConfig<?>> VALID_PROPERTIES = Collections.unmodifiableMap(
            new HashMap<String, TableConfig<?>>() {{
                addConfig(this, TOMBSTONE_RETENTION);
                addConfig(this, CHECKPOINT_INTERVAL);
                addConfig(this, IN_COMMIT_TIMESTAMPS_ENABLED);
                addConfig(this, IN_COMMIT_TIMESTAMP_ENABLEMENT_VERSION);
                addConfig(this, IN_COMMIT_TIMESTAMP_ENABLEMENT_TIMESTAMP);
                addConfig(this, COORDINATED_COMMITS_COORDINATOR_NAME);
                addConfig(this, COORDINATED_COMMITS_COORDINATOR_CONF);
                addConfig(this, COORDINATED_COMMITS_TABLE_CONF);
            }}
    );

    private final String key;
    private final String defaultValue;
    private final BiFunction<Optional<Engine>, String, T> fromString;
    private final Predicate<T> validator;
    private final String helpMessage;

    private TableConfig(
            String key,
            String defaultValue,
            BiFunction<Optional<Engine>, String, T> fromString,
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
     * @param engine {@link Engine} instance.
     * @param metadata the table metadata
     * @return the value of the table property
     */
    public T fromMetadata(Engine engine, Metadata metadata) {
        String value = metadata.getConfiguration().getOrDefault(key, defaultValue);
        validate(Optional.of(engine), value);
        return fromString.apply(Optional.of(engine), value);
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
    public static Map<String, String> validateProperties(
            Engine engine, Map<String, String> configurations) {
        Map<String, String> validatedConfigurations = new HashMap<>();
        for (Map.Entry<String, String> kv : configurations.entrySet()) {
            String key = kv.getKey().toLowerCase(Locale.ROOT);
            String value = kv.getValue();
            if (key.startsWith("delta.")) {
                TableConfig<?> tableConfig = VALID_PROPERTIES.get(key);
                if (tableConfig != null) {
                    tableConfig.validate(Optional.of(engine), value);
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

    public static Boolean isICTEnabled(Engine engine, Metadata metadata) {
        return IN_COMMIT_TIMESTAMPS_ENABLED.fromMetadata(engine, metadata);
    }

    private void validate(Optional<Engine> engineOpt, String value) {
        T parsedValue = fromString.apply(engineOpt, value);
        if (!validator.test(parsedValue)) {
            throw DeltaErrors.invalidConfigurationValueException(key, value, helpMessage);
        }
    }

    private static void addConfig(HashMap<String, TableConfig<?>> configs, TableConfig<?> config) {
        configs.put(config.getKey().toLowerCase(Locale.ROOT), config);
    }

    /**
     * Parse the given JSON string into a map of key-value pairs. The JSON string should be in the
     * format of """{"key1": "value1", "key2": "value2", ...}""" whose keys and values are strings.
     *
     * @param engine the engine instance
     * @param jsonString the JSON string to parse
     * @return the map of key-value pairs
     */
    private static Map<String, String> parseJSONKeyValueMap(Engine engine, String jsonString) {
        if (jsonString == null) {
            return Collections.emptyMap();
        }
        // By adding the top-level key "config", the schema is fixed with a single column "config"
        // of type MapType(StringType, StringType). In this way, we can parse the JSON string into
        // MapType(StringType, StringType) using existing engine.getJsonHandler().parseJson API.
        StructType schema = new StructType()
                .add("config", new MapType(StringType.STRING, StringType.STRING, true));
        CloseableIterator<Row> batchRows = engine.getJsonHandler().parseJson(
                singletonStringColumnVector(String.format("{\"config\": %s}", jsonString)),
                schema,
                Optional.empty()
        ).getRows();
        try {
            Row row = getSingularElement(batchRows).orElseThrow(
                    () -> new IllegalStateException("Iterator should not be empty"));
            assert !row.isNullAt(0);
            return VectorUtils.toJavaMap(row.getMap(0));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
