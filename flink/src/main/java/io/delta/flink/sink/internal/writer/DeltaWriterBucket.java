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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import javax.annotation.Nullable;

import io.delta.flink.sink.internal.committables.DeltaCommittable;
import org.apache.flink.core.fs.Path;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.functions.sink.filesystem.DeltaBulkBucketWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.DeltaBulkPartWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.DeltaInProgressPart;
import org.apache.flink.streaming.api.functions.sink.filesystem.DeltaPendingFile;
import org.apache.flink.streaming.api.functions.sink.filesystem.InProgressFileWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.CheckpointRollingPolicy;
import org.apache.flink.table.utils.PartitionPathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Internal implementation for writing the actual events to the underlying files in the correct
 * buckets / partitions.
 *
 * <p>
 * In reference to the Flink's {@link org.apache.flink.api.connector.sink.Sink} topology
 * one of its main components is {@link org.apache.flink.api.connector.sink.SinkWriter}
 * which in case of DeltaSink is implemented as {@link DeltaWriter}. However, to comply
 * with DeltaLake's support for partitioning tables a new component was added in the form
 * of {@link DeltaWriterBucket} that is responsible for handling writes to only one of the
 * buckets (aka partitions). Such bucket writers are managed by {@link DeltaWriter}
 * which works as a proxy between higher order frameworks commands (write, prepareCommit etc.)
 * and actual writes' implementation in {@link DeltaWriterBucket}. Thanks to this solution
 * events within one {@link DeltaWriter} operator received during particular checkpoint interval
 * are always grouped and flushed to the currently opened in-progress file.
 * <p>
 * The implementation was sourced from the {@link org.apache.flink.connector.file.sink.FileSink}
 * that utilizes same concept and implements
 * {@link org.apache.flink.connector.file.sink.writer.FileWriter} with its FileWriterBucket
 * implementation.
 * All differences between DeltaSink's and FileSink's writer buckets are explained in particular
 * method's below.
 * <p>
 * Lifecycle of instances of this class is as follows:
 * <ol>
 *     <li>Every instance is being created via {@link DeltaWriter#write} method whenever writer
 *         receives first event that belongs to the bucket represented by given
 *         {@link DeltaWriterBucket} instance. Or in case of non-partitioned tables whenever writer
 *         receives the very first event as in such cases there is only one
 *         {@link DeltaWriterBucket} representing the root path of the table</li>
 *     <li>{@link DeltaWriter} instance can create zero, one or multiple instances of
 *         {@link DeltaWriterBucket} during one checkpoint interval. It creates none if it hasn't
 *         received any events (thus didn't have to create buckets for them). It creates one when it
 *         has received events belonging only to one bucket (same if the table is not partitioned).
 *         Finally, it creates multiple when it has received events belonging to more than one
 *         bucket.</li>
 *     <li>Life span of one {@link DeltaWriterBucket} may hold through one or more checkpoint
 *         intervals. It remains "active" as long as it receives data. If e.g. for given checkpoint
 *         interval an instance of {@link DeltaWriter} hasn't received any events belonging to given
 *         bucket, then {@link DeltaWriterBucket} representing this bucket is de-listed from the
 *         writer's internal bucket's iterator. If in future checkpoint interval given
 *         {@link DeltaWriter} will receive some more events for given bucket then it will create
 *         new instance of {@link DeltaWriterBucket} representing this bucket.
 *         </li>
 * </ol>
 *
 * @param <IN> The type of input elements.
 */
public class DeltaWriterBucket<IN> {

    private static final Logger LOG = LoggerFactory.getLogger(DeltaWriterBucket.class);

    public static final String RECORDS_WRITTEN_METRIC_NAME = "DeltaSinkRecordsWritten";

    public static final String BYTES_WRITTEN_METRIC_NAME = "DeltaSinkBytesWritten";

    private final String bucketId;

    private final Path bucketPath;

    private final OutputFileConfig outputFileConfig;

    private final String uniqueId;

    private final DeltaBulkBucketWriter<IN, String> bucketWriter;

    private final CheckpointRollingPolicy<IN, String> rollingPolicy;

    private final List<DeltaPendingFile> pendingFiles = new ArrayList<>();

    private final LinkedHashMap<String, String> partitionSpec;

    private long partCounter;

    private long inProgressPartRecordCount;

    @Nullable
    private DeltaInProgressPart<IN> deltaInProgressPart;

    /**
     * Counter for how many records were written to the files on the underlying file system.
     */
    private final Counter recordsWrittenCounter;

    /**
     * Counter for how many bytes were written to the files on the underlying file system.
     */
    private final Counter bytesWrittenCounter;

    /**
     * Constructor to create a new empty bucket.
     */
    private DeltaWriterBucket(
        String bucketId,
        Path bucketPath,
        DeltaBulkBucketWriter<IN, String> bucketWriter,
        CheckpointRollingPolicy<IN, String> rollingPolicy,
        OutputFileConfig outputFileConfig,
        MetricGroup metricGroup) {
        this.bucketId = checkNotNull(bucketId);
        this.bucketPath = checkNotNull(bucketPath);
        this.bucketWriter = checkNotNull(bucketWriter);
        this.rollingPolicy = checkNotNull(rollingPolicy);
        this.outputFileConfig = checkNotNull(outputFileConfig);

        this.partitionSpec = PartitionPathUtils.extractPartitionSpecFromPath(this.bucketPath);
        this.uniqueId = UUID.randomUUID().toString();
        this.partCounter = 0;
        this.inProgressPartRecordCount = 0;

        this.recordsWrittenCounter = metricGroup.counter(RECORDS_WRITTEN_METRIC_NAME);
        this.bytesWrittenCounter = metricGroup.counter(BYTES_WRITTEN_METRIC_NAME);
    }

    /**
     * Constructor to restore a bucket from checkpointed state.
     */
    private DeltaWriterBucket(
        DeltaBulkBucketWriter<IN, String> partFileFactory,
        CheckpointRollingPolicy<IN, String> rollingPolicy,
        DeltaWriterBucketState bucketState,
        OutputFileConfig outputFileConfig,
        MetricGroup metricGroup) {

        this(
            bucketState.getBucketId(),
            bucketState.getBucketPath(),
            partFileFactory,
            rollingPolicy,
            outputFileConfig,
            metricGroup);
    }

    /**
     * @implNote This method behaves in the similar way as
     * org.apache.flink.connector.file.sink.writer.FileWriterBucket#prepareCommit
     * except that:
     * <ol>
     *   <li>it uses custom {@link DeltaInProgressPart} implementation in order to carry additional
     *       file's metadata that will be used during global commit phase</li>
     *   <li>it adds transactional identifier for current checkpoint interval (appId + checkpointId)
     *       to the committables</li>
     *   <li>it does not handle any in progress files to cleanup as it's supposed to always roll
     *       part files on checkpoint which is also the default behaviour for bulk formats in
     *       {@link org.apache.flink.connector.file.sink.FileSink} as well. The reason why its
     *       needed for FileSink is that it also provides support for row wise formats which is not
     *       required in case of DeltaSink.</li>
     * </ol>
     */
    List<DeltaCommittable> prepareCommit(boolean flush,
                                         String appId,
                                         long checkpointId) throws IOException {
        if (deltaInProgressPart != null) {
            if (rollingPolicy.shouldRollOnCheckpoint(deltaInProgressPart.getBulkPartWriter())
                || flush) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug(
                        "Closing in-progress part file for bucket id={} on checkpoint.",
                        bucketId);
                }

                closePartFile();
            } else {
                throw new RuntimeException(
                    "Unexpected behaviour. Delta writers should always roll part files " +
                        "on checkpoint. To resolve this issue verify behaviour of your" +
                        " rolling policy.");
            }
        }

        List<DeltaCommittable> committables = new ArrayList<>();
        pendingFiles.forEach(pendingFile -> committables.add(
            new DeltaCommittable(pendingFile, appId, checkpointId)));
        pendingFiles.clear();

        return committables;
    }

    /**
     * This method is responsible for snapshotting state of the bucket writer. The writer's
     * state snapshot can be further used to recover from failure or from manual Flink's app
     * snapshot.
     * <p>
     * Since the writer is supposed to always roll part files on checkpoint then there is not
     * much state to snapshot and recover from except bucket metadata (id and path) and also
     * unique identifier for the application that the writer is part of.
     *
     * @param appId        unique identifier of the Flink app that needs to be retained within all
     *                     app restarts
     * @return snapshot of the current bucket writer's state
     */
    DeltaWriterBucketState snapshotState(String appId) {
        return new DeltaWriterBucketState(bucketId, bucketPath, appId);
    }

    /**
     * Method responsible for "closing" previous in-progress file and "opening" new one to be
     * written to.
     *
     * @param currentTime current processing time
     * @return new in progress part instance representing part file that the writer will start
     * write data to
     * @throws IOException Thrown if the writer cannot be opened, or if the output stream throws an
     *                     exception.
     * @implNote This method behaves in the similar way as
     * org.apache.flink.connector.file.sink.writer.FileWriterBucket#rollPartFile
     * except that it uses custom implementation to represent the in-progress part file.
     * See {@link DeltaInProgressPart} for details.
     */
    private DeltaInProgressPart<IN> rollPartFile(long currentTime) throws IOException {
        closePartFile();

        final Path partFilePath = assembleNewPartPath();

        if (LOG.isDebugEnabled()) {
            LOG.debug(
                "Opening new part file \"{}\" for bucket id={}.",
                partFilePath.getName(),
                bucketId);
        }

        DeltaBulkPartWriter<IN, String> fileWriter =
            (DeltaBulkPartWriter<IN, String>) bucketWriter.openNewInProgressFile(
                bucketId, partFilePath, currentTime);

        if (LOG.isDebugEnabled()) {
            LOG.debug(
                "Successfully opened new part file \"{}\" for bucket id={}.",
                partFilePath.getName(),
                bucketId);
        }

        return new DeltaInProgressPart<>(partFilePath.getName(), fileWriter);
    }

    /**
     * Method responsible for "closing" currently opened in-progress file and appending new
     * {@link DeltaPendingFile} instance to {@link DeltaWriterBucket#pendingFiles}. Those pending
     * files during commit will become critical part of committables information passed to both
     * types of committers.
     *
     * @throws IOException Thrown if the encoder cannot be flushed, or if the output stream throws
     *                     an exception.
     * @implNote This method behaves in the similar way as
     * org.apache.flink.connector.file.sink.writer.FileWriterBucket#closePartFile
     * however it adds some implementation details.
     * <ol>
     *   <li>it uses custom {@link DeltaInProgressPart} implementation in order to be able to
     *       explicitly close the internal file writer what allows to get the actual file size. It
     *       is necessary as original implementation of {@link InProgressFileWriter} used by
     *       {@link org.apache.flink.connector.file.sink.FileSink} does not provide us with correct
     *       file size because for bulk formats it shows the file size before flushing the internal
     *       buffer,
     *   <li>it enriches the {@link DeltaPendingFile} with closed file's metadata
     *   <li>it resets the counter for currently opened part file
     * </ol>
     */
    private void closePartFile() throws IOException {
        if (deltaInProgressPart != null) {
            // we need to close the writer explicitly before calling closeForCommit() in order to
            // get the actual file size
            deltaInProgressPart.getBulkPartWriter().closeWriter();
            long fileSize = deltaInProgressPart.getBulkPartWriter().getSize();
            InProgressFileWriter.PendingFileRecoverable pendingFileRecoverable =
                deltaInProgressPart.getBulkPartWriter().closeForCommit();

            DeltaPendingFile pendingFile = new DeltaPendingFile(
                partitionSpec,
                deltaInProgressPart.getFileName(),
                pendingFileRecoverable,
                this.inProgressPartRecordCount,
                fileSize,
                deltaInProgressPart.getBulkPartWriter().getLastUpdateTime()
            );
            pendingFiles.add(pendingFile);
            deltaInProgressPart = null;
            inProgressPartRecordCount = 0;

            recordsWrittenCounter.inc(pendingFile.getRecordCount());
            bytesWrittenCounter.inc(fileSize);
        }
    }

    ///////////////////////////////////////////////////////////////////////////
    // FileSink-specific methods
    ///////////////////////////////////////////////////////////////////////////

    /**
     * Writes received element to the actual writer's buffer.
     *
     * @implNote This method behaves in the same way as
     * org.apache.flink.connector.file.sink.writer.FileWriterBucket#write
     * except that it uses custom {@link DeltaInProgressPart} implementation and also
     * counts the events written to the currently opened part file.
     */
    void write(IN element, long currentTime) throws IOException {
        if (deltaInProgressPart == null || rollingPolicy.shouldRollOnEvent(
            deltaInProgressPart.getBulkPartWriter(), element)) {
            if (LOG.isDebugEnabled()) {
                LOG.debug(
                    "Opening new part file for bucket id={} due to element {}.",
                    bucketId,
                    element);
            }
            deltaInProgressPart = rollPartFile(currentTime);
        }

        deltaInProgressPart.getBulkPartWriter().write(element, currentTime);
        ++inProgressPartRecordCount;
    }

    /**
     * Merges two states of the same bucket.
     * <p>
     * This method is run only when creating new writer based on existing previous states. If the
     * restored states will contain inputs for the same bucket them we merge those.
     *
     * @param bucket another state representing the same bucket as the current instance
     * @throws IOException when I/O error occurs
     */
    void merge(final DeltaWriterBucket<IN> bucket) throws IOException {
        checkNotNull(bucket);
        checkState(Objects.equals(bucket.bucketPath, bucketPath));

        bucket.closePartFile();
        pendingFiles.addAll(bucket.pendingFiles);

        if (LOG.isDebugEnabled()) {
            LOG.debug("Merging buckets for bucket id={}", bucketId);
        }
    }

    public boolean isActive() {
        return deltaInProgressPart != null || pendingFiles.size() > 0;
    }

    /**
     * Method for getting current processing time and (optionally) apply roll file behaviour.
     * <p>
     * This method could be used e.g. to apply custom rolling file behaviour.
     *
     * @implNote This method behaves in the same way as
     * {@link org.apache.flink.connector.file.sink.writer.FileWriter#onProcessingTime}
     * except that it uses custom {@link DeltaWriterBucket} implementation.
     */
    void onProcessingTime(long timestamp) throws IOException {
        if (deltaInProgressPart != null
            && rollingPolicy.shouldRollOnProcessingTime(
            deltaInProgressPart.getBulkPartWriter(), timestamp)) {
            InProgressFileWriter<IN, String> inProgressPart =
                deltaInProgressPart.getBulkPartWriter();
            if (LOG.isDebugEnabled()) {
                LOG.debug(
                    "Bucket {} closing in-progress part file for part file id={} due to " +
                        "processing time rolling policy (in-progress file created @ {}," +
                        " last updated @ {} and current time is {}).",
                    bucketId,
                    uniqueId,
                    inProgressPart.getCreationTime(),
                    inProgressPart.getLastUpdateTime(),
                    timestamp);
            }

            closePartFile();
        }
    }

    /**
     * Constructor a new PartPath and increment the partCounter.
     */
    private Path assembleNewPartPath() {
        long currentPartCounter = partCounter++;
        return new Path(
            bucketPath,
            outputFileConfig.getPartPrefix()
                + '-'
                + uniqueId
                + '-'
                + currentPartCounter
                + outputFileConfig.getPartSuffix());
    }

    void disposePartFile() {
        if (deltaInProgressPart != null) {
            deltaInProgressPart.getBulkPartWriter().dispose();
        }
    }

    ///////////////////////////////////////////////////////////////////////////
    // Static Factory
    ///////////////////////////////////////////////////////////////////////////

    public static class DeltaWriterBucketFactory {
        static <IN> DeltaWriterBucket<IN> getNewBucket(
            final String bucketId,
            final Path bucketPath,
            final DeltaBulkBucketWriter<IN, String> bucketWriter,
            final CheckpointRollingPolicy<IN, String> rollingPolicy,
            final OutputFileConfig outputFileConfig,
            final MetricGroup metricGroup) {
            return new DeltaWriterBucket<>(
                bucketId, bucketPath, bucketWriter, rollingPolicy, outputFileConfig, metricGroup);
        }

        static <IN> DeltaWriterBucket<IN> restoreBucket(
            final DeltaBulkBucketWriter<IN, String> bucketWriter,
            final CheckpointRollingPolicy<IN, String> rollingPolicy,
            final DeltaWriterBucketState bucketState,
            final OutputFileConfig outputFileConfig,
            final MetricGroup metricGroup) {
            return new DeltaWriterBucket<>(
                bucketWriter, rollingPolicy, bucketState, outputFileConfig, metricGroup);
        }
    }
}
