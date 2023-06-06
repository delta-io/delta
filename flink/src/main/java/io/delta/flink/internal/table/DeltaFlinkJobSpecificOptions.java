/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.delta.flink.internal.table;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.delta.flink.source.internal.DeltaSourceOptions;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.core.fs.Path;
import org.apache.hadoop.conf.Configuration;

/**
 * This class contains Flink job-specific options for {@link io.delta.flink.source.DeltaSource} and
 * {@link io.delta.flink.sink.DeltaSink} that are relevant for Table API. For Table API, this
 * options can be set only using Flink, dynamic table options from DML/DQL query level, for
 * example:
 * <pre>{@code
 *    SELECT * FROM my_delta_source_table /*+ OPTIONS(‘mode' = 'streaming')
 *  }</pre>
 * Flink job-specific options are not stored in metastore nor in Delta Log. Their scope is single
 * Flink Job (DML/DQL query) only.
 *
 * <p>In practice this class will contain options from
 * {@link io.delta.flink.source.internal.DeltaSourceOptions} and
 * {@link io.delta.flink.sink.internal.DeltaSinkOptions} + extra ones like MODE.
 */
public class DeltaFlinkJobSpecificOptions {

    /**
     * Option to specify if SELECT query should be bounded (read only Delta Snapshot) or should
     * continuously monitor Delta table for new changes.
     */
    public static final ConfigOption<QueryMode> MODE =
        ConfigOptions.key("mode")
            .enumType(QueryMode.class)
            .defaultValue(QueryMode.BATCH);

    /**
     * Set of allowed job-specific options for SELECT statements that can be passed used Flink's
     * query hint.
     */
    public static final Set<String> SOURCE_JOB_OPTIONS = Stream.of(
        MODE.key(),
        DeltaSourceOptions.VERSION_AS_OF.key(),
        DeltaSourceOptions.TIMESTAMP_AS_OF.key(),
        DeltaSourceOptions.STARTING_VERSION.key(),
        DeltaSourceOptions.STARTING_TIMESTAMP.key(),
        DeltaSourceOptions.UPDATE_CHECK_INTERVAL.key(),
        DeltaSourceOptions.UPDATE_CHECK_INITIAL_DELAY.key(),
        DeltaSourceOptions.IGNORE_DELETES.key(),
        DeltaSourceOptions.IGNORE_CHANGES.key()
    ).collect(Collectors.toSet());

    /**
     * Expected values for {@link DeltaFlinkJobSpecificOptions#MODE} job specific option. Based on
     * this value, proper Delta source builder instance (DeltaSource.forBoundedRowData or
     * DeltaSource.forContinuousRowData) will be created. Flink will automatically convert string
     * value from dynamic table option from DML/DQL query and convert to QueryMode value. The value
     * is case-insensitive.
     */
    public enum QueryMode {

        /**
         * Used to created Bounded Delta Source -
         * {@link io.delta.flink.source.DeltaSource#forBoundedRowData(Path, Configuration)}
         */
        BATCH("batch"),

        /**
         * Used to created continuous Delta Source -
         * {@link io.delta.flink.source.DeltaSource#forContinuousRowData(Path, Configuration)}
         */
        STREAMING("streaming");

        private final String name;

        QueryMode(String name) {
            this.name = name;
        }

        @Override
        public String toString() {
            return name;
        }
    }
}
