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

import java.io.IOException;
import java.io.Serializable;
import java.util.UUID;

import io.delta.flink.sink.internal.committables.DeltaCommittable;
import io.delta.flink.sink.internal.committables.DeltaCommittableSerializer;
import io.delta.flink.sink.internal.committables.DeltaGlobalCommittable;
import io.delta.flink.sink.internal.committables.DeltaGlobalCommittableSerializer;
import io.delta.flink.sink.internal.committer.DeltaCommitter;
import io.delta.flink.sink.internal.committer.DeltaGlobalCommitter;
import io.delta.flink.sink.internal.writer.DeltaWriter;
import io.delta.flink.sink.internal.writer.DeltaWriterBucketState;
import io.delta.flink.sink.internal.writer.DeltaWriterBucketStateSerializer;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.formats.parquet.ParquetWriterFactory;
import org.apache.flink.formats.parquet.utils.SerializableConfiguration;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.DeltaBulkBucketWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.CheckpointRollingPolicy;
import org.apache.flink.table.types.logical.RowType;
import org.apache.hadoop.conf.Configuration;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A builder class for {@link DeltaSink}.
 * <p>
 * Most of the logic for this class was sourced from
 * {@link org.apache.flink.connector.file.sink.FileSink.BulkFormatBuilder} as the behaviour is very
 * similar. The main difference is that this {@link DeltaSinkBuilder} was extended with DeltaLake's
 * specific parts that are explicitly marked in the implementation below.
 *
 * @param <IN> The type of input elements.
 */
public class DeltaSinkBuilder<IN> implements Serializable {

    private static final long serialVersionUID = 7493169281026370228L;

    protected static final long DEFAULT_BUCKET_CHECK_INTERVAL = 60L * 1000L;

    private static String generateNewAppId() {
        return UUID.randomUUID().toString();
    }

    ///////////////////////////////////////////////////////////////////////////
    // DeltaLake-specific fields
    ///////////////////////////////////////////////////////////////////////////

    /**
     * Delta table's root path
     */
    private final Path tableBasePath;

    /**
     * Flink's logical type to indicate the structure of the events in the stream
     */
    private final RowType rowType;

    /**
     * Unique identifier of the current Flink's app. Value from this builder will be read
     * only during the fresh start of the application. For restarts or failure recovery
     * it will be resolved from the snapshoted state.
     */
    private final String appId;

    /**
     * Indicator whether we should try to update table's schema with stream's schema in case
     * those will not match. The update is not guaranteed as there will be still some checks
     * performed whether the updates to the schema are compatible.
     */
    private boolean shouldTryUpdateSchema;

    /**
     * Serializable wrapper for {@link Configuration} object
     */
    private final SerializableConfiguration serializableConfiguration;

    ///////////////////////////////////////////////////////////////////////////
    // FileSink-specific fields
    ///////////////////////////////////////////////////////////////////////////

    /**
     * Interval for triggering {@link Sink.ProcessingTimeService} within {@link DeltaWriter}
     * instance.
     * <p>
     * In some scenarios, the open buckets are required to change based on time. In these cases,
     * the user can specify a bucketCheckInterval and the sink will check
     * periodically and roll the part file if the specified rolling policy says so.
     */
    private final long bucketCheckInterval;

    private final ParquetWriterFactory<IN> writerFactory;

    private BucketAssigner<IN, String> bucketAssigner;

    private CheckpointRollingPolicy<IN, String> rollingPolicy;

    private OutputFileConfig outputFileConfig;

    protected DeltaSinkBuilder(
        Path basePath,
        Configuration conf,
        ParquetWriterFactory<IN> writerFactory,
        BucketAssigner<IN, String> assigner,
        CheckpointRollingPolicy<IN, String> policy,
        RowType rowType,
        boolean shouldTryUpdateSchema) {
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
            shouldTryUpdateSchema
        );
    }

    protected DeltaSinkBuilder(
        Path basePath,
        Configuration conf,
        long bucketCheckInterval,
        ParquetWriterFactory<IN> writerFactory,
        BucketAssigner<IN, String> assigner,
        CheckpointRollingPolicy<IN, String> policy,
        OutputFileConfig outputFileConfig,
        String appId,
        RowType rowType,
        boolean shouldTryUpdateSchema) {
        this.tableBasePath = checkNotNull(basePath);
        this.serializableConfiguration = new SerializableConfiguration(checkNotNull(conf));
        this.bucketCheckInterval = bucketCheckInterval;
        this.writerFactory = writerFactory;
        this.bucketAssigner = checkNotNull(assigner);
        this.rollingPolicy = checkNotNull(policy);
        this.outputFileConfig = checkNotNull(outputFileConfig);
        this.appId = appId;
        this.rowType = rowType;
        this.shouldTryUpdateSchema = shouldTryUpdateSchema;
    }

    /**
     * Sets the sink's option whether in case of any differences between stream's schema and Delta
     * table's schema we should try to update it during commit to the
     * {@link io.delta.standalone.DeltaLog}. The update is not guaranteed as there will be some
     * compatibility checks performed.
     *
     * @param shouldTryUpdateSchema whether we should try to update table's schema with stream's
     *                              schema in case those will not match. See
     *                              {@link DeltaSinkBuilder#shouldTryUpdateSchema} for details.
     * @return builder for {@link DeltaSink}
     */
    public DeltaSinkBuilder<IN> withShouldTryUpdateSchema(final boolean shouldTryUpdateSchema) {
        this.shouldTryUpdateSchema = shouldTryUpdateSchema;
        return this;
    }

    DeltaCommitter createCommitter() throws IOException {
        return new DeltaCommitter(createBucketWriter());
    }

    DeltaGlobalCommitter createGlobalCommitter() {
        return new DeltaGlobalCommitter(
            serializableConfiguration.conf(), tableBasePath, rowType, shouldTryUpdateSchema);
    }

    Path getTableBasePath() {
        return tableBasePath;
    }

    String getAppId() {
        return appId;
    }

    SerializableConfiguration getSerializableConfiguration() {
        return serializableConfiguration;
    }

    ///////////////////////////////////////////////////////////////////////////
    // FileSink-specific methods
    ///////////////////////////////////////////////////////////////////////////

    public DeltaSinkBuilder<IN> withBucketAssigner(BucketAssigner<IN, String> assigner) {
        this.bucketAssigner = checkNotNull(assigner);
        return this;
    }

    public DeltaSinkBuilder<IN> withRollingPolicy(
        CheckpointRollingPolicy<IN, String> rollingPolicy) {
        this.rollingPolicy = checkNotNull(rollingPolicy);
        return this;
    }

    public DeltaSinkBuilder<IN> withOutputFileConfig(final OutputFileConfig outputFileConfig) {
        this.outputFileConfig = outputFileConfig;
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

    DeltaWriter<IN> createWriter(Sink.InitContext context,
                                 String appId,
                                 long nextCheckpointId) throws IOException {
        return new DeltaWriter<>(
            tableBasePath,
            bucketAssigner,
            createBucketWriter(),
            rollingPolicy,
            outputFileConfig,
            context.getProcessingTimeService(),
            bucketCheckInterval,
            appId,
            nextCheckpointId);
    }

    SimpleVersionedSerializer<DeltaWriterBucketState> getWriterStateSerializer()
        throws IOException {
        return new DeltaWriterBucketStateSerializer();
    }

    SimpleVersionedSerializer<DeltaCommittable> getCommittableSerializer()
        throws IOException {
        BucketWriter<IN, String> bucketWriter = createBucketWriter();

        return new DeltaCommittableSerializer(
            bucketWriter.getProperties().getPendingFileRecoverableSerializer());
    }

    SimpleVersionedSerializer<DeltaGlobalCommittable> getGlobalCommittableSerializer()
        throws IOException {
        BucketWriter<IN, String> bucketWriter = createBucketWriter();

        return new DeltaGlobalCommittableSerializer(
            bucketWriter.getProperties().getPendingFileRecoverableSerializer());
    }

    private DeltaBulkBucketWriter<IN, String> createBucketWriter() throws IOException {
        return new DeltaBulkBucketWriter<>(
            FileSystem.get(tableBasePath.toUri()).createRecoverableWriter(), writerFactory);
    }

    /**
     * Default builder for {@link DeltaSink}.
     */
    public static final class DefaultDeltaFormatBuilder<IN> extends DeltaSinkBuilder<IN> {

        private static final long serialVersionUID = 2818087325120827526L;

        DefaultDeltaFormatBuilder(
            Path basePath,
            final Configuration conf,
            ParquetWriterFactory<IN> writerFactory,
            BucketAssigner<IN, String> assigner,
            CheckpointRollingPolicy<IN, String> policy,
            RowType rowType,
            boolean shouldTryUpdateSchema) {
            super(basePath, conf, writerFactory, assigner, policy, rowType, shouldTryUpdateSchema);
        }
    }
}
