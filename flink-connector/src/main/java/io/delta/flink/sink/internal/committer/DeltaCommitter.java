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
import java.util.Collections;
import java.util.List;

import io.delta.flink.sink.DeltaSink;
import io.delta.flink.sink.internal.committables.DeltaCommittable;
import io.delta.flink.sink.internal.writer.DeltaWriter;
import org.apache.flink.api.connector.sink.Committer;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Committer implementation for {@link DeltaSink}.
 *
 * <p>This committer is responsible for taking staged part-files, i.e. part-files in "pending"
 * state, created by the {@link io.delta.flink.sink.internal.writer.DeltaWriter}
 * and put them in "finished" state ready to be committed to the DeltaLog during "global" commit.
 *
 * <p> This class behaves almost in the same way as its equivalent
 * {@link org.apache.flink.connector.file.sink.committer.FileCommitter}
 * in the {@link FileSink}. The only differences are:
 *
 * <ol>
 *   <li>use of the {@link DeltaCommittable} instead of
 *       {@link org.apache.flink.connector.file.sink.FileSinkCommittable}</li>
 *   <li>some simplifications for the committable's internal information and commit behaviour.
 *       In particular in {@link DeltaCommitter#commit} method we do not take care of any inprogress
 *       file's state (as opposite to
 *       {@link org.apache.flink.connector.file.sink.committer.FileCommitter#commit}
 *       because in {@link DeltaWriter#prepareCommit} we always roll all of the in-progress files.
 *       Valid note here is that's also the default {@link FileSink}'s behaviour for all of the
 *       bulk formats (Parquet included).</li>
 * </ol>
 * <p>
 * Lifecycle of instances of this class is as follows:
 * <ol>
 *     <li>Instances of this class are being created during a commit stage</li>
 *     <li>For every {@link DeltaWriter} object there is only one of corresponding
 *         {@link DeltaCommitter} created, thus the number of created instances is equal to the
 *         parallelism of the application's sink</li>
 *     <li>Every instance exists only during given commit stage after finishing particular
 *         checkpoint interval. Despite being bundled to a finish phase of a checkpoint interval
 *         a single instance of {@link DeltaCommitter} may process committables from multiple
 *         checkpoints intervals (it happens e.g. when there was a app's failure and Flink has
 *         recovered committables from previous commit stage to be re-committed.</li>
 * </ol>
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
     *
     * @param committables list of committables. May contain committables from multiple checkpoint
     *                     intervals
     * @return always empty list as we do not allow or expect any retry behaviour
     * @throws IOException if committing files (e.g. I/O errors occurs)
     */
    @Override
    public List<DeltaCommittable> commit(
        List<DeltaCommittable> committables) throws IOException {
        for (DeltaCommittable committable : committables) {
            LOG.info("Committing delta committable locally: " +
                "appId=" + committable.getAppId() +
                " checkpointId=" + committable.getCheckpointId() +
                " deltaPendingFile=" + committable.getDeltaPendingFile()
            );
            bucketWriter.recoverPendingFile(committable.getDeltaPendingFile().getPendingFile())
                .commitAfterRecovery();
        }
        return Collections.emptyList();
    }

    @Override
    public void close() {
    }
}
