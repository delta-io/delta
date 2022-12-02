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

package io.delta.flink.sink.internal;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import io.delta.flink.sink.internal.committables.DeltaCommittable;
import io.delta.flink.sink.internal.committables.DeltaGlobalCommittable;
import io.delta.flink.sink.internal.writer.DeltaWriter;
import io.delta.flink.sink.internal.writer.DeltaWriterBucketState;
import org.apache.flink.api.connector.sink.Committer;
import org.apache.flink.api.connector.sink.GlobalCommitter;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.util.FlinkRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.apache.flink.util.Preconditions.checkNotNull;

import io.delta.standalone.DeltaLog;

/**
 * A unified sink that emits its input elements to file system files within buckets using
 * Parquet format and commits those files to the {@link DeltaLog}. This sink achieves exactly-once
 * semantics for both {@code BATCH} and {@code STREAMING}.
 * <p>
 * Behaviour of this sink splits down upon two phases. The first phase takes place between
 * application's checkpoints when records are being flushed to files (or appended to writers'
 * buffers) where the behaviour is almost identical as in case of
 * {@link org.apache.flink.connector.file.sink.FileSink}.
 * <p>
 * Next during the checkpoint phase files are "closed" (renamed) by the independent instances of
 * {@code io.delta.flink.sink.internal.committer.DeltaCommitter} that behave very similar
 * to {@link org.apache.flink.connector.file.sink.committer.FileCommitter}.
 * When all the parallel committers are done, then all the files are committed at once by
 * single-parallelism {@code io.delta.flink.sink.internal.committer.DeltaGlobalCommitter}.
 * <p>
 * This {@link DeltaSinkInternal} sources many specific implementations from the
 * {@link org.apache.flink.connector.file.sink.FileSink} so for most of the low level behaviour one
 * may refer to the docs from this module. The most notable differences to the FileSinks are:
 * <ul>
 *  <li>tightly coupling DeltaSink to the Bulk-/ParquetFormat</li>
 *  <li>extending committable information with files metadata (name, size, rows, last update
 *      timestamp)</li>
 *  <li>providing DeltaLake-specific behaviour which is mostly contained in the
 *      {@code io.delta.flink.sink.internal.committer.DeltaGlobalCommitter} implementing the commit
 *      to the {@link DeltaLog} at the final stage of each checkpoint.</li>
 * </ul>
 *
 * @param <IN> Type of the elements in the input of the sink that are also the elements to be
 *             written to its output
 * @implNote This sink sources many methods and solutions from
 * {@link org.apache.flink.connector.file.sink.FileSink} implementation simply by copying the
 * code since it was not possible to directly reuse those due to some access specifiers, use of
 * generics and need to provide some internal workarounds compared to the FileSink. To make it
 * explicit which methods are directly copied from FileSink we use `FileSink-specific methods`
 * comment marker inside class files to decouple DeltaLake's specific code from parts borrowed
 * from FileSink.
 */
public class DeltaSinkInternal<IN>
    implements Sink<IN, DeltaCommittable, DeltaWriterBucketState, DeltaGlobalCommittable> {

    private static final Logger LOG = LoggerFactory.getLogger(DeltaSinkInternal.class);

    private final DeltaSinkBuilder<IN> sinkBuilder;

    protected DeltaSinkInternal(DeltaSinkBuilder<IN> sinkBuilder) {
        this.sinkBuilder = checkNotNull(sinkBuilder);
    }

    /**
     * This method creates the {@link SinkWriter} instance that will be responsible for passing
     * incoming stream events to the correct bucket writer and then flushed to the underlying files.
     * <p>
     * The logic for resolving constructor params differ depending on whether any previous writer's
     * states were provided.
     * If there are no previous states then we assume that this is a fresh start of the app and set
     * next checkpoint id in {@code io.delta.flink.sink.internal.writer.DeltaWriter} to 1 and app id
     * is taken from the {@link DeltaSinkBuilder#getAppId} what guarantees us that each
     * writer will get the same value. In other case, if we are provided by the Flink framework
     * with some previous writers' states then we use those to restore values of appId and
     * nextCheckpointId.
     *
     * @param context {@link SinkWriter} init context object
     * @param states  restored states of the writers. Will be empty collection for fresh start.
     * @return new {@link SinkWriter} object
     * @throws IOException When the recoverable writer cannot be instantiated.
     */
    @Override
    public SinkWriter<IN, DeltaCommittable, DeltaWriterBucketState> createWriter(
        InitContext context,
        List<DeltaWriterBucketState> states
    ) throws IOException {
        String appId = restoreOrCreateAppId(states);
        long checkpointId = context.getRestoredCheckpointId().orElse(1);
        DeltaWriter<IN> writer = sinkBuilder.createWriter(context, appId, checkpointId);
        writer.initializeState(states);
        LOG.info("Created new writer for: " +
                    "appId=" + appId +
                    " checkpointId=" + checkpointId
        );
        return writer;
    }

    /**
     * Restores application's id snapshotted in any of the {@link DeltaWriter}s' states or gets
     * new one from the builder in case there is no previous states.
     * <p>
     * In order to gurantee the idempotency of the GlobalCommitter we need unique identifier of the
     * app. We obtain it with simple logic: if it's the first run of the application (so no restart
     * from snapshot or failure recovery happened and the writer's state is empty) then assign appId
     * to a newly generated UUID that will be further stored in the state of each writer.
     * Alternatively if the writer's states are not empty then we resolve appId from one of the
     * restored states.
     *
     * @param states restored list of writer's buckets states that include previously generated
     *               appId
     * @return newly created or resolved from restored writer's states unique identifier of the app.
     */
    private String restoreOrCreateAppId(List<DeltaWriterBucketState> states) {
        if (states.isEmpty()) {
            return sinkBuilder.getAppId();
        }
        return states.get(0).getAppId();
    }

    @Override
    public Optional<SimpleVersionedSerializer<DeltaWriterBucketState>>
        getWriterStateSerializer() {
        try {
            return Optional.of(sinkBuilder.getWriterStateSerializer());
        } catch (IOException e) {
            throw new FlinkRuntimeException("Could not create writer state serializer.", e);
        }
    }

    @Override
    public Optional<Committer<DeltaCommittable>> createCommitter() throws IOException {
        return Optional.of(sinkBuilder.createCommitter());
    }

    @Override
    public Optional<SimpleVersionedSerializer<DeltaCommittable>>
        getCommittableSerializer() {
        try {
            return Optional.of(sinkBuilder.getCommittableSerializer());
        } catch (IOException e) {
            throw new FlinkRuntimeException("Could not create committable serializer.", e);
        }
    }

    @Override
    public Optional<GlobalCommitter<DeltaCommittable, DeltaGlobalCommittable>>
        createGlobalCommitter() {
        return Optional.of(sinkBuilder.createGlobalCommitter());
    }

    @Override
    public Optional<SimpleVersionedSerializer<DeltaGlobalCommittable>>
        getGlobalCommittableSerializer() {
        try {
            return Optional.of(sinkBuilder.getGlobalCommittableSerializer());
        } catch (IOException e) {
            throw new FlinkRuntimeException("Could not create committable serializer.", e);
        }
    }
}
