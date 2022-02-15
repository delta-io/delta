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

package io.delta.flink.sink;

import java.util.UUID;

import io.delta.flink.sink.internal.DeltaSinkBuilderInternal;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.ParquetWriterFactory;
import org.apache.flink.formats.parquet.utils.SerializableConfiguration;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.CheckpointRollingPolicy;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.hadoop.conf.Configuration;

/**
 * A builder class for {@link DeltaSink}.
 * <p>
 * For most common use cases use {@link DeltaSink#forRowData} utility method to instantiate the
 * sink. This builder should be used only if you need to provide custom writer factory instance
 * or configure some low level settings for the sink.
 * <p>
 * Example how to use this class for the stream of {@link RowData}:
 * <pre>
 *     RowType rowType = ...;
 *     Configuration conf = new Configuration();
 *     conf.set("parquet.compression", "SNAPPY");
 *     ParquetWriterFactory&lt;RowData&gt; writerFactory =
 *         ParquetRowDataBuilder.createWriterFactory(rowType, conf, true);
 *
 *     DeltaSinkBuilder&lt;RowData&gt; sinkBuilder = new DeltaSinkBuilder(
 *         basePath,
 *         conf,
 *         bucketCheckInterval,
 *         writerFactory,
 *         new BasePathBucketAssigner&lt;&gt;(),
 *         OnCheckpointRollingPolicy.build(),
 *         OutputFileConfig.builder().withPartSuffix(".snappy.parquet").build(),
 *         appId,
 *         rowType,
 *         mergeSchema
 *     );
 *
 *     DeltaSink&lt;RowData&gt; sink = sinkBuilder.build();
 *
 * </pre>
 *
 * @param <IN> The type of input elements.
 */
public class DeltaSinkBuilder<IN> extends DeltaSinkBuilderInternal<IN> {

    private static String generateNewAppId() {
        return UUID.randomUUID().toString();
    }

    protected DeltaSinkBuilder(
        Path basePath,
        Configuration conf,
        ParquetWriterFactory<IN> writerFactory,
        BucketAssigner<IN, String> assigner,
        CheckpointRollingPolicy<IN, String> policy,
        RowType rowType,
        boolean mergeSchema) {
        this(
            basePath,
            conf,
            DEFAULT_BUCKET_CHECK_INTERVAL,
            writerFactory,
            assigner,
            policy,
            OutputFileConfig.builder().withPartSuffix(".snappy.parquet").build(),
            generateNewAppId(),
            rowType,
            mergeSchema
        );
    }

    /**
     * Creates instance of the builder for {@link DeltaSink}.
     *
     * @param basePath            path to a Delta table
     * @param conf                Hadoop's conf object
     * @param bucketCheckInterval interval (in milliseconds) for triggering
     *                            {@link Sink.ProcessingTimeService} within internal
     *                            {@code io.delta.flink.sink.internal.writer.DeltaWriter} instance
     * @param writerFactory       a factory that in runtime is used to create instances of
     *                            {@link org.apache.flink.api.common.serialization.BulkWriter}
     * @param assigner            {@link BucketAssigner} used with a Delta sink to determine the
     *                            bucket each incoming element should be put into
     * @param policy              instance of {@link CheckpointRollingPolicy} which rolls on every
     *                            checkpoint by default
     * @param outputFileConfig    part file name configuration. This allow to define a prefix and a
     *                            suffix to the part file name.
     * @param appId               unique identifier of the Flink application that will be used as a
     *                            part of transactional id in Delta's transactions. It is crucial
     *                            for this value to be unique across all applications committing to
     *                            a given Delta table
     * @param rowType             Flink's logical type to indicate the structure of the events in
     *                            the stream
     * @param mergeSchema         indicator whether we should try to update table's schema with
     *                            stream's schema in case those will not match. The update is not
     *                            guaranteed as there will be still some checks performed whether
     *                            the updates to the schema are compatible.
     */
    public DeltaSinkBuilder(
        Path basePath,
        Configuration conf,
        long bucketCheckInterval,
        ParquetWriterFactory<IN> writerFactory,
        BucketAssigner<IN, String> assigner,
        CheckpointRollingPolicy<IN, String> policy,
        OutputFileConfig outputFileConfig,
        String appId,
        RowType rowType,
        boolean mergeSchema) {
        super(
            basePath,
            conf,
            bucketCheckInterval,
            writerFactory,
            assigner,
            policy,
            outputFileConfig,
            appId,
            rowType,
            mergeSchema
        );
    }

    /**
     * Sets the sink's option whether in case of any differences between stream's schema and Delta
     * table's schema we should try to update it during commit to the
     * {@link io.delta.standalone.DeltaLog}. The update is not guaranteed as there will be some
     * compatibility checks performed.
     *
     * @param mergeSchema indicator whether we should try to update table's schema with stream's
     *                    schema in case those will not match. The update is not guaranteed as there
     *                    will be still some checks performed whether the updates to the schema are
     *                    compatible.
     * @return builder for {@link DeltaSink}
     */
    public DeltaSinkBuilder<IN> withMergeSchema(final boolean mergeSchema) {
        super.withMergeSchema(mergeSchema);
        return this;
    }

    protected Path getTableBasePath() {
        return super.getTableBasePath();
    }

    protected String getAppId() {
        return super.getAppId();
    }

    protected SerializableConfiguration getSerializableConfiguration() {
        return super.getSerializableConfiguration();
    }

    ///////////////////////////////////////////////////////////////////////////
    // FileSink-specific methods
    ///////////////////////////////////////////////////////////////////////////

    /**
     * Sets bucket assigner responsible for mapping events to its partitions.
     *
     * @param assigner bucket assigner instance for this sink
     * @return builder for {@link DeltaSink}
     */
    public DeltaSinkBuilder<IN> withBucketAssigner(BucketAssigner<IN, String> assigner) {
        super.withBucketAssigner(assigner);
        return this;
    }

    /**
     * Creates the actual sink.
     *
     * @return constructed {@link DeltaSink} object
     */
    public DeltaSink<IN> build() {
        return new DeltaSink<>(this);
    }

    /**
     * Default builder for {@link DeltaSink}.
     */
    static final class DefaultDeltaFormatBuilder<IN> extends DeltaSinkBuilder<IN> {

        private static final long serialVersionUID = 2818087325120827526L;

        DefaultDeltaFormatBuilder(
            Path basePath,
            final Configuration conf,
            ParquetWriterFactory<IN> writerFactory,
            BucketAssigner<IN, String> assigner,
            CheckpointRollingPolicy<IN, String> policy,
            RowType rowType,
            boolean mergeSchema) {
            super(basePath, conf, writerFactory, assigner, policy, rowType, mergeSchema);
        }
    }
}
