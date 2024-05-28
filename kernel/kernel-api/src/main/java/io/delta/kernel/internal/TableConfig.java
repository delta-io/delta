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

import java.util.function.Function;
import java.util.function.Predicate;

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

    public static final TableConfig<Long> LOG_RETENTION = new TableConfig<>(
            "delta.logRetentionDuration",
            "interval 30 days",
            IntervalParserUtils::safeParseIntervalAsMillis,
            value -> value >= 0,
            "needs to be provided as a calendar interval such as '2 weeks'. Months "
                + "and years are not accepted. You may specify '365 days' for a year instead."
        );

    public static final TableConfig<Boolean> ENABLE_EXPIRED_LOG_CLEANUP = new TableConfig<>(
            "delta.enableExpiredLogCleanup",
            "true",
        Boolean::valueOf,
            value -> true,
            "needs to be a boolean."
    );

    public static final TableConfig<Boolean> IS_APPEND_ONLY = new TableConfig<>(
        "delta.appendOnly",
        "false",
        Boolean::valueOf,
        value -> true,
        "needs to be a boolean."
    );

    private final String key;
    private final String defaultValue;
    private final Function<String, T> fromString;
    private final Predicate<T> validator;
    private final String helpMessage;

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

    public String key() {
        return key;
    }

    /**
     * Returns the value of the table property from the given metadata.
     *
     * @param metadata the table metadata
     * @return the value of the table property
     */
    public T fromMetadata(Metadata metadata) {
        T value = fromString.apply(metadata.getConfiguration().getOrDefault(key, defaultValue));
        if (!validator.test(value)) {
            throw new IllegalArgumentException(
                    String.format("Invalid value for table property '%s': '%s'. %s",
                            key, value, helpMessage));
        }
        return value;
    }
}

