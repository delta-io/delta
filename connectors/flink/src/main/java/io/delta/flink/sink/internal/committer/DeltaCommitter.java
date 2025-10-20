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

package io.delta.flink.sink.internal.committer;

import java.io.IOException;
import java.util.Collection;

import io.delta.flink.sink.DeltaSink;
import io.delta.flink.sink.internal.committables.DeltaCommittable;
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Committer implementation for {@link DeltaSink}.
 *
 * <p>FLINK 2.0 STATUS:
 * This class has been migrated to use the new Flink 2.0 Committer API with CommitRequest pattern.
 *
 * <p>CURRENT FUNCTIONALITY:
 * This committer is responsible for taking staged part-files (files in "pending" state)
 * created by {@link io.delta.flink.sink.internal.writer.DeltaWriter} and putting them
 * in "finished" state by renaming them to remove in-progress markers.
 *
 * <p>IMPORTANT - INCOMPLETE MIGRATION WARNING:
 * In Flink 1.x, there were two commit phases:
 * <ol>
 *   <li>Local commit (this class) - renames temp files to finalize them</li>
 *   <li>Global commit ({@link DeltaGlobalCommitter}) - commits finalized files to DeltaLog</li>
 * </ol>
 *
 * In Flink 2.0, the GlobalCommitter interface was removed. Currently:
 * <ul>
 *   <li>✅ Local file commits work correctly (implemented in this class)</li>
 *   <li>❌ DeltaLog commits are NOT YET IMPLEMENTED for Flink 2.0</li>
 * </ul>
 *
 * <p>TODO - CRITICAL FOR PRODUCTION USE:
 * To make this sink production-ready for Flink 2.0, this class needs to be extended to:
 * 1. After renaming files, aggregate committables by checkpoint ID
 * 2. Commit aggregated data to DeltaLog with exactly-once semantics
 * 3. Use appId + checkpointId for idempotent commits (prevent duplicate commits)
 * 4. Handle schema evolution and validation
 * 5. Implement proper failure recovery and retry logic
 *
 * See {@link DeltaGlobalCommitter} for the complete DeltaLog commit logic that needs integration.
 *
 * <p>IMPLEMENTATION NOTES:
 * Based on {@link org.apache.flink.connector.file.sink.committer.FileCommitter}.
 * Key differences from FileSink's FileCommitter:
 * <ul>
 *   <li>Uses {@link DeltaCommittable} instead of FileSinkCommittable</li>
 *   <li>Simplified commit behavior - always rolls all in-progress files in prepareCommit()</li>
 * </ul>
 *
 * @see DeltaGlobalCommitter for DeltaLog commit logic (Flink 1.x reference)
 */
public class DeltaCommitter implements Committer<DeltaCommittable> {

    private static final Logger LOG = LoggerFactory.getLogger(DeltaCommitter.class);

    ///////////////////////////////////////////////////////////////////////////
    // FileSink-specific
    ///////////////////////////////////////////////////////////////////////////

    private final BucketWriter<?, ?> bucketWriter;

    public DeltaCommitter(BucketWriter<?, ?> bucketWriter) {
        this.bucketWriter = checkNotNull(bucketWriter);
    }

    /**
     * This method is responsible for "committing" files locally.
     * <p>
     * "Local" commit in our case means the same as in
     * {@link org.apache.flink.connector.file.sink.committer.FileCommitter#commit}, namely it's
     * the simple process of renaming the hidden file to make it visible and removing from the name
     * some 'in-progress file' marker. For details see internal interfaces in
     * {@link org.apache.flink.streaming.api.functions.sink.filesystem.BucketWriter}.
     * <p>
     * Flink 2.0 changed the commit signature to use Collection<CommitRequest<T>> instead of List<T>
     *
     * @param commitRequests collection of commit requests. May contain committables from multiple
     *                       checkpoint intervals
     * @throws IOException if committing files (e.g. I/O errors occurs)
     */
    @Override
    public void commit(Collection<CommitRequest<DeltaCommittable>> commitRequests)
            throws IOException {
        for (CommitRequest<DeltaCommittable> request : commitRequests) {
            DeltaCommittable committable = request.getCommittable();
            LOG.info("Committing delta committable locally: " +
                "appId=" + committable.getAppId() +
                " checkpointId=" + committable.getCheckpointId() +
                " deltaPendingFile=" + committable.getDeltaPendingFile()
            );
            try {
                bucketWriter.recoverPendingFile(committable.getDeltaPendingFile().getPendingFile())
                    .commitAfterRecovery();
                // Signal success
                request.signalAlreadyCommitted();
            } catch (Exception e) {
                // Signal failure with retry
                request.retryLater();
                throw e;
            }
        }
    }

    @Override
    public void close() {
    }
}
