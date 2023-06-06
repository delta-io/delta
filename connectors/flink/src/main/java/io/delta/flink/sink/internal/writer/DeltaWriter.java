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

package io.delta.flink.sink.internal.writer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

import io.delta.flink.sink.internal.DeltaBucketAssigner;
import io.delta.flink.sink.internal.committables.DeltaCommittable;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.connector.file.sink.writer.FileWriter;
import org.apache.flink.core.fs.Path;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.DeltaBulkBucketWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.CheckpointRollingPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A {@link SinkWriter} implementation for {@link io.delta.flink.sink.DeltaSink}.
 *
 * <p>
 * It writes data to and manages the different active {@link DeltaWriterBucket buckets} in the
 * {@link io.delta.flink.sink.DeltaSink}.
 * <p>
 * Most of the logic for this class was sourced from {@link FileWriter} as the behaviour is very
 * similar. The main differences are use of custom implementations for some member classes and also
 * managing {@link io.delta.standalone.DeltaLog} transactional ids: {@link DeltaWriter#appId} and
 * {@link DeltaWriter#nextCheckpointId}.
 * <p>
 * Lifecycle of instances of this class is as follows:
 * <ol>
 *     <li>Every instance is being created via
 *         {@link io.delta.flink.sink.DeltaSink#createWriter} method</li>
 *     <li>Writers' life span is the same as the application's (unless the worker node gets
 *         unresponding and the job manager needs to create a new instance to satisfy the
 *         parallelism)</li>
 *     <li>Number of instances are managed globally by a job manager and this number is equal to the
 *         parallelism of the sink.
 *         @see <a href="https://nightlies.apache.org/flink/flink-docs-master/docs/dev/datastream/execution/parallel/" target="_blank">Flink's parallel execution</a></li>
 * </ol>
 *
 * @param <IN> The type of input elements.
 */
public class DeltaWriter<IN> implements SinkWriter<IN, DeltaCommittable, DeltaWriterBucketState>,
                                            Sink.ProcessingTimeService.ProcessingTimeCallback {

    private static final Logger LOG = LoggerFactory.getLogger(DeltaWriter.class);

    public static final String RECORDS_OUT_METRIC_NAME = "DeltaSinkRecordsOut";

    /**
     * Value used as a bucket id for noop bucket states. It will be used to snapshot and indicate
     * the writer's states with no active buckets.
     */
    public static final String NOOP_WRITER_STATE = "<noop-writer-state>";

    ///////////////////////////////////////////////////////////////////////////
    // DeltaSink-specific fields
    ///////////////////////////////////////////////////////////////////////////

    /**
     * Unique identifier of the application that will be passed as part of committables' information
     * during {@link DeltaWriter#prepareCommit} method. It's also snapshotted as a part of the
     * writer's state in order to support failure recovery and provide exactly-once delivery
     * guarantee. This value will be unique to a streaming job as long as it is being restarted
     * using checkpoint/savepoint information.
     */
    private final String appId;

    /**
     * Unique identifier of a checkpoint interval. It's necessary to maintain and increment this
     * value inside writer as this needs to be passed as a part of committables' information during
     * {@link DeltaWriter#prepareCommit} method. Its value is always incremented by one after
     * generating set of committables for given checkpoint interval. For a fresh start of an
     * application it always starts with the value of "1".
     *
     * @implNote The checkpointId we forward from the writer might not be fully accurate. It is
     * possible that {@link DeltaWriter#prepareCommit} is called without a following checkpoint i.e.
     * if the pipeline finishes or for batch executions. For this reason this value might not
     * reflect exact current checkpointID for the Job, and it is advised to use it as a general
     * monitoring tool, for example logs.
     */
    private long nextCheckpointId;

    ///////////////////////////////////////////////////////////////////////////
    // FileSink-specific fields
    ///////////////////////////////////////////////////////////////////////////

    ///////////////////////////////////////////////////
    // configuration fields
    ///////////////////////////////////////////////////

    private final DeltaBulkBucketWriter<IN, String> bucketWriter;

    private final CheckpointRollingPolicy<IN, String> rollingPolicy;

    private final Path basePath;

    private final BucketAssigner<IN, String> bucketAssigner;

    private final Sink.ProcessingTimeService processingTimeService;

    private final long bucketCheckInterval;

    ///////////////////////////////////////////////////
    // runtime fields
    ///////////////////////////////////////////////////

    private final Map<String, DeltaWriterBucket<IN>> activeBuckets;

    private final BucketerContext bucketerContext;

    private final OutputFileConfig outputFileConfig;

    ///////////////////////////////////////////////////
    // metrics
    ///////////////////////////////////////////////////

    /**
     * Metric group for the current sink.
     */
    private final MetricGroup metricGroup;

    /**
     * Counter for how many records were processed by the sink.
     *
     * NOTE: it is not the same as how many records were written to the actual file
     */
    private final Counter recordsOutCounter;

    /**
     * A constructor creating a new empty bucket (DeltaLake table's partitions) manager.
     *
     * @param basePath              The base path for the table
     * @param bucketAssigner        The {@link BucketAssigner} provided by the user. It is advised
     *                              to use {@link DeltaBucketAssigner} however users are
     *                              allowed to use any custom implementation of bucketAssigner. The
     *                              only requirement for correctness is to follow DeltaLake's style
     *                              of table partitioning.
     * @param bucketWriter          The {@link DeltaBulkBucketWriter} to be used when writing data.
     * @param rollingPolicy         The {@link CheckpointRollingPolicy} as specified by the user.
     * @param outputFileConfig      The {@link OutputFileConfig} to configure the options for output
     *                              files.
     * @param processingTimeService The {@link Sink.ProcessingTimeService} that allows to get the
     *                              current processing time and register timers that will execute
     *                              the given Sink.ProcessingTimeService.ProcessingTimeCallback when
     *                              firing.
     * @param metricGroup           metric group object for the current Sink
     * @param bucketCheckInterval   interval for invoking the {@link Sink.ProcessingTimeService}'s
     *                              callback.
     * @param appId                 Unique identifier of the current Flink app. This identifier
     *                              needs to be constant across all app's restarts to guarantee
     *                              idempotent writes/commits to the DeltaLake's table.
     * @param nextCheckpointId      Identifier of the next checkpoint interval to be committed.
     *                              During DeltaLog's commit phase it will be used to group
     *                              committable objects.
     */
    public DeltaWriter(
        final Path basePath,
        final BucketAssigner<IN, String> bucketAssigner,
        final DeltaBulkBucketWriter<IN, String> bucketWriter,
        final CheckpointRollingPolicy<IN, String> rollingPolicy,
        final OutputFileConfig outputFileConfig,
        final Sink.ProcessingTimeService processingTimeService,
        final MetricGroup metricGroup,
        final long bucketCheckInterval,
        final String appId,
        final long nextCheckpointId) {

        this.basePath = checkNotNull(basePath);
        this.bucketAssigner = checkNotNull(bucketAssigner);
        this.bucketWriter = checkNotNull(bucketWriter);
        this.rollingPolicy = checkNotNull(rollingPolicy);

        this.outputFileConfig = checkNotNull(outputFileConfig);

        this.activeBuckets = new HashMap<>();
        this.bucketerContext = new BucketerContext();

        this.processingTimeService = checkNotNull(processingTimeService);

        this.metricGroup = metricGroup;
        this.recordsOutCounter = metricGroup.counter(RECORDS_OUT_METRIC_NAME);

        checkArgument(
            bucketCheckInterval > 0,
            "Bucket checking interval for processing time should be positive.");
        this.bucketCheckInterval = bucketCheckInterval;
        this.appId = appId;
        this.nextCheckpointId = nextCheckpointId;
    }

    /**
     * Prepares the writer's state to be snapshotted between checkpoint intervals.
     * <p>
     *
     * @implNote This method behaves in the similar way as
     * {@link org.apache.flink.connector.file.sink.writer.FileWriter#snapshotState}
     * except that it uses custom {@link DeltaWriterBucketState} and {@link DeltaWriterBucket}
     * implementations. Custom implementation are needed in order to extend the committables'
     * information with metadata of written files and also to customize the state that is being
     * snapshotted during checkpoint phase.
     * <p>
     * Additionally, it implements snapshotting writer's states even in case when there are no
     * active buckets (which may be not such a rare case e.g. when checkpoint interval will be very
     * short and the writer will not receive any data during this interval then it will mark the
     * buckets as inactive). This behaviour is needed for delta-specific case when we want to retain
     * the same application id within all app restarts / recreation writers' states from snapshot.
     */
    @Override
    public List<DeltaWriterBucketState> snapshotState() {
        checkState(bucketWriter != null, "sink has not been initialized");

        List<DeltaWriterBucketState> states = new ArrayList<>();
        for (DeltaWriterBucket<IN> bucket : activeBuckets.values()) {
            states.add(bucket.snapshotState(appId));
        }

        if (states.isEmpty()) {
            // we still need to snapshot transactional ids (appId) even though
            // there are no active buckets in the writer.
            states.add(
                new DeltaWriterBucketState(NOOP_WRITER_STATE, basePath, appId)
            );
        }
        return states;
    }

    private void incrementNextCheckpointId() {
        nextCheckpointId += 1;
    }

    long getNextCheckpointId() {
        return nextCheckpointId;
    }

    ///////////////////////////////////////////////////////////////////////////
    // FileSink-specific methods
    ///////////////////////////////////////////////////////////////////////////

    /**
     * A proxy method that forwards the incoming event to the correct {@link DeltaWriterBucket}
     * instance.
     *
     * @param element incoming stream event
     * @param context context for getting additional data about input event
     * @implNote This method behaves in the same way as
     * {@link org.apache.flink.connector.file.sink.writer.FileWriter#write}
     * except that it uses custom {@link DeltaWriterBucket} implementation.
     */
    @Override
    public void write(IN element, Context context) throws IOException {
        bucketerContext.update(
            context.timestamp(),
            context.currentWatermark(),
            processingTimeService.getCurrentProcessingTime());

        final String bucketId = bucketAssigner.getBucketId(element, bucketerContext);
        final DeltaWriterBucket<IN> bucket = getOrCreateBucketForBucketId(bucketId);
        bucket.write(element, processingTimeService.getCurrentProcessingTime());
        recordsOutCounter.inc();
    }

    /**
     * This method prepares committables objects that will be passed to
     * {@link io.delta.flink.sink.internal.committer.DeltaCommitter} and
     * {@link io.delta.flink.sink.internal.committer.DeltaGlobalCommitter} to finalize the
     * checkpoint interval and commit written files.
     *
     * @implNote This method behaves in the same way as
     * {@link org.apache.flink.connector.file.sink.writer.FileWriter#prepareCommit}
     * except that it uses custom {@link DeltaWriterBucket} implementation and
     * also increments the {@link DeltaWriter#nextCheckpointId} counter.
     */
    @Override
    public List<DeltaCommittable> prepareCommit(boolean flush) throws IOException {
        List<DeltaCommittable> committables = new ArrayList<>();

        // Every time before we prepare commit, we first check and remove the inactive
        // buckets. Checking the activeness right before pre-committing avoid re-creating
        // the bucket every time if the bucket use OnCheckpointingRollingPolicy.
        Iterator<Map.Entry<String, DeltaWriterBucket<IN>>> activeBucketIter =
            activeBuckets.entrySet().iterator();
        while (activeBucketIter.hasNext()) {
            Map.Entry<String, DeltaWriterBucket<IN>> entry = activeBucketIter.next();
            if (!entry.getValue().isActive()) {
                activeBucketIter.remove();
            } else {
                committables.addAll(entry.getValue().prepareCommit(flush, appId, nextCheckpointId));
            }
        }

        incrementNextCheckpointId();
        return committables;
    }

    /**
     * Initializes the state from snapshotted {@link DeltaWriterBucketState}.
     *
     * @param bucketStates the state holding recovered state about active buckets.
     * @throws IOException if anything goes wrong during retrieving the state or
     *                     restoring/committing of any in-progress/pending part files
     * @implNote This method behaves in the same way as
     * {@link org.apache.flink.connector.file.sink.writer.FileWriter#initializeState}
     * except that it uses custom {@link DeltaWriterBucketState} and {@link DeltaWriterBucket}
     * implementations.
     * Additionally, it skips restoring the bucket in case of bucket id equal to the value of
     * {@link DeltaWriter#NOOP_WRITER_STATE}.
     */
    public void initializeState(List<DeltaWriterBucketState> bucketStates) throws IOException {
        checkNotNull(bucketStates, "The retrieved state was null.");

        for (DeltaWriterBucketState state : bucketStates) {
            String bucketId = state.getBucketId();
            if (bucketId.equals(NOOP_WRITER_STATE)) {
                // nothing to restore
                continue;
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("Restoring: {}", state);
            }

            DeltaWriterBucket<IN> restoredBucket =
                DeltaWriterBucket.DeltaWriterBucketFactory.restoreBucket(
                    bucketWriter, rollingPolicy, state, outputFileConfig, metricGroup);

            updateActiveBucketId(bucketId, restoredBucket);
        }

        registerNextBucketInspectionTimer();
    }

    /**
     * This method either initializes new bucket or merges its state with the existing one.
     * <p>
     * It is run only during creation of the {@link DeltaWriter} when received some previous states.
     *
     * @implNote This method behaves in the same way as
     * {@link org.apache.flink.connector.file.sink.writer.FileWriter}#updateActiveBucketId
     * except that it uses custom {@link DeltaWriterBucket} implementation.
     */
    private void updateActiveBucketId(String bucketId,
                                      DeltaWriterBucket<IN> restoredBucket)
        throws IOException {
        final DeltaWriterBucket<IN> bucket = activeBuckets.get(bucketId);
        if (bucket != null) {
            bucket.merge(restoredBucket);
        } else {
            activeBuckets.put(bucketId, restoredBucket);
        }
    }

    /**
     * This method returns {@link DeltaWriterBucket} by either creating it or resolving from
     * existing instances for given writer.
     *
     * @implNote This method behaves in the same way as
     * {@link org.apache.flink.connector.file.sink.writer.FileWriter}#getOrCreateBucketForBucketId
     * except that it uses custom {@link DeltaWriterBucket} implementation.
     */
    private DeltaWriterBucket<IN> getOrCreateBucketForBucketId(String bucketId) {
        DeltaWriterBucket<IN> bucket = activeBuckets.get(bucketId);
        if (bucket == null) {
            final Path bucketPath = assembleBucketPath(bucketId);
            bucket = DeltaWriterBucket.DeltaWriterBucketFactory.getNewBucket(
                bucketId,
                bucketPath,
                bucketWriter,
                rollingPolicy,
                outputFileConfig,
                metricGroup);

            activeBuckets.put(bucketId, bucket);
        }
        return bucket;
    }

    /**
     * Method for closing the writer, that it to say to dispose any in progress files.
     *
     * @implNote This method behaves in the same way as
     * {@link org.apache.flink.connector.file.sink.writer.FileWriter#close}
     * except that it uses custom {@link DeltaWriterBucket} implementation.
     */
    @Override
    public void close() {
        if (activeBuckets != null) {
            activeBuckets.values().forEach(DeltaWriterBucket::disposePartFile);
        }
    }

    /**
     * Resolves full filesystem's path to the bucket with given id.
     *
     * @implNote This method behaves in the same way as
     * {@link org.apache.flink.connector.file.sink.writer.FileWriter}#assembleBucketPath.
     */
    private Path assembleBucketPath(String bucketId) {
        if ("".equals(bucketId)) {
            return basePath;
        }
        return new Path(basePath, bucketId);
    }

    /**
     * Method for getting current processing time ahd register timers.
     * <p>
     * This method could be used e.g. to apply custom rolling file behaviour.
     *
     * @implNote This method behaves in the same way as
     * {@link org.apache.flink.connector.file.sink.writer.FileWriter#onProcessingTime}
     * except that it uses custom {@link DeltaWriterBucket} implementation.
     */
    @Override
    public void onProcessingTime(long time) throws IOException {
        for (DeltaWriterBucket<IN> bucket : activeBuckets.values()) {
            bucket.onProcessingTime(time);
        }

        registerNextBucketInspectionTimer();
    }

    /**
     * Invokes the given callback at the given timestamp.
     *
     * @implNote This method behaves in the same way as in
     * {@link org.apache.flink.connector.file.sink.writer.FileWriter}
     */
    private void registerNextBucketInspectionTimer() {
        final long nextInspectionTime =
            processingTimeService.getCurrentProcessingTime() + bucketCheckInterval;
        processingTimeService.registerProcessingTimer(nextInspectionTime, this);
    }

    /**
     * The {@link BucketAssigner.Context} exposed to the {@link BucketAssigner#getBucketId(Object,
     * BucketAssigner.Context)} whenever a new incoming element arrives.
     *
     * @implNote This class is implemented in the same way as
     * {@link org.apache.flink.connector.file.sink.writer.FileWriter}.BucketerContext.
     */
    private static final class BucketerContext implements BucketAssigner.Context {

        @Nullable
        private Long elementTimestamp;

        private long currentWatermark;

        private long currentProcessingTime;

        private BucketerContext() {
            this.elementTimestamp = null;
            this.currentWatermark = Long.MIN_VALUE;
            this.currentProcessingTime = Long.MIN_VALUE;
        }

        void update(@Nullable Long elementTimestamp, long watermark, long currentProcessingTime) {
            this.elementTimestamp = elementTimestamp;
            this.currentWatermark = watermark;
            this.currentProcessingTime = currentProcessingTime;
        }

        @Override
        public long currentProcessingTime() {
            return currentProcessingTime;
        }

        @Override
        public long currentWatermark() {
            return currentWatermark;
        }

        @Override
        @Nullable
        public Long timestamp() {
            return elementTimestamp;
        }
    }

    ///////////////////////////////////////////////////////////////////////////
    // Testing Methods
    ///////////////////////////////////////////////////////////////////////////

    @VisibleForTesting
    Map<String, DeltaWriterBucket<IN>> getActiveBuckets() {
        return activeBuckets;
    }
}
