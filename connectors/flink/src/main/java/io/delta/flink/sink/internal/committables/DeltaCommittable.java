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

package io.delta.flink.sink.internal.committables;

import java.io.Serializable;

import org.apache.flink.streaming.api.functions.sink.filesystem.DeltaPendingFile;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Committable object that carries the information about files written to the file system
 * during particular checkpoint interval.
 * <p>
 * As {@link io.delta.flink.sink.DeltaSink} implements both
 * {@link org.apache.flink.api.connector.sink.Committer} and
 * {@link org.apache.flink.api.connector.sink.GlobalCommitter} and
 * then its committable must provide all metadata for committing data on both levels.
 * <p>
 * In order to commit data during {@link org.apache.flink.api.connector.sink.Committer#commit}
 * information carried inside {@link DeltaPendingFile} are used. Next during
 * {@link org.apache.flink.api.connector.sink.GlobalCommitter#commit} we are using both:
 * metadata carried inside {@link DeltaPendingFile} and also transactional identifier constructed by
 * application's unique id and checkpoint interval's id.
 * <p>
 * Lifecycle of instances of this class is as follows:
 * <ol>
 *     <li>Every instance is created in
 *         {@link io.delta.flink.sink.internal.writer.DeltaWriterBucket#prepareCommit}
 *         method during a pre-commit phase.</li>
 *     <li>When certain checkpointing barriers are reached then generated committables are
 *         snapshotted along with the rest of the application's state.
 *         See Flink's docs for details
 *         @see <a href="https://nightlies.apache.org/flink/flink-docs-master/docs/learn-flink/fault_tolerance/#how-does-state-snapshotting-work" target="_blank">here</a></li>
 *     <li>During commit phase every committable is first delivered to
 *         {@link io.delta.flink.sink.internal.committer.DeltaCommitter#commit}
 *         and then to
 *         {@link io.delta.flink.sink.internal.committer.DeltaGlobalCommitter#combine}
 *         methods when they are being committed.</li>
 *     <li>If there's any failure of the app's execution then Flink may recover previously generated
 *         set of committables that may have not been committed. In such cases those recovered
 *         committables will be again passed to the committers' instance along with the new
 *         committables from the next checkpoint interval.</li>
 *     <li>If checkpoint was successfull then committables from the given checkpoint interval are
 *         no longer recovered and exist only in the previously snapshotted states.</li>
 * </ol>
 */
public class DeltaCommittable implements Serializable {

    private final DeltaPendingFile deltaPendingFile;

    /**
     * Unique identifier of the application used for interacting with
     * {@link io.delta.standalone.DeltaLog} and for identifying previous table's versions committed
     * by this application.
     */
    private final String appId;

    /**
     * Unique identifier of the current checkpoint interval. It's necessary to carry this as a part
     * of committable information in order to guarantee idempotent behaviour of
     * {@link io.delta.flink.sink.internal.committer.DeltaGlobalCommitter#commit}.
     */
    private final long checkpointId;

    public DeltaCommittable(
            DeltaPendingFile deltaPendingFile,
            String appId,
            long checkpointId) {
        this.deltaPendingFile = checkNotNull(deltaPendingFile);
        this.appId = appId;
        this.checkpointId = checkpointId;
    }

    public DeltaPendingFile getDeltaPendingFile() {
        return deltaPendingFile;
    }

    public long getCheckpointId() {
        return checkpointId;
    }

    public String getAppId() {
        return appId;
    }
}
