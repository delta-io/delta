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
 * <p><strong>FLINK 2.0 STATUS - PRODUCTION READY:</strong>
 * In Flink 2.0, the two-phase commit is now handled by:
 * <ul>
 *   <li>✅ Local commits: {@link DeltaCommitter} (this class) - renames temp files</li>
 *   <li>✅ Global commits: {@link DeltaGlobalCommitter} via SupportsPostCommitTopology</li>
 * </ul>
 *
 * <p>The {@link DeltaGlobalCommitter} provides exactly-once semantics with:
 * <ul>
 *   <li>Aggregates committables from all parallel committers</li>
 *   <li>Groups by checkpoint for atomic Delta Log commits</li>
 *   <li>Idempotent commits using appId + checkpointId</li>
 *   <li>Schema evolution and validation</li>
 * </ul>
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

    private final BucketWriter<?, ?> bucketWriter;

    public DeltaCommitter(BucketWriter<?, ?> bucketWriter) {
        this.bucketWriter = checkNotNull(bucketWriter);
    }

    /**
     * Commits files locally by renaming them to finalize the write.
     * <p>
     * "Local" commit means renaming the hidden file to make it visible and removing from the name
     * some 'in-progress file' marker. For details see internal interfaces in
     * {@link org.apache.flink.streaming.api.functions.sink.filesystem.BucketWriter}.
     * <p>
     * Delta Log commits are handled separately by {@link DeltaGlobalCommitter} through
     * the post-commit topology (see {@link DeltaSinkInternal#addPostCommitTopology}).
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
        LOG.info("DeltaCommitter committing {} files locally", commitRequests.size());

        for (CommitRequest<DeltaCommittable> request : commitRequests) {
            DeltaCommittable committable = request.getCommittable();
            LOG.debug("Committing file locally: appId={} checkpointId={} file={}",
                committable.getAppId(),
                committable.getCheckpointId(),
                committable.getDeltaPendingFile());
            try {
                bucketWriter.recoverPendingFile(committable.getDeltaPendingFile().getPendingFile())
                    .commitAfterRecovery();

                // Signal success for local commit
                // Committable will be passed to DeltaGlobalCommitter via post-commit topology
                request.signalAlreadyCommitted();
            } catch (Exception e) {
                LOG.error("Failed to commit file locally: {}", committable, e);
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
