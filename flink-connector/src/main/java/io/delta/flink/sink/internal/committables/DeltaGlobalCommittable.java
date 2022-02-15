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

import java.util.List;

import io.delta.flink.sink.internal.logging.Logging;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Simple wrapper class for a collection of {@link DeltaCommittable} instances.
 * <p>
 * This class is provided to comply with the
 * {@link org.apache.flink.api.connector.sink.GlobalCommitter}
 * interfaces' structure. It's only purpose is to wrap {@link DeltaCommittable} collection during
 * {@link io.delta.flink.sink.internal.committer.DeltaGlobalCommitter#combine} method
 * that will be further flattened and processed inside
 * {@link io.delta.flink.sink.internal.committer.DeltaGlobalCommitter#commit} method.
 * <p>
 * Lifecycle of instances of this class is as follows:
 * <ol>
 *     <li>Every instance is created in
 *         {@link io.delta.flink.sink.internal.committer.DeltaGlobalCommitter#combine}
 *         method during a global commit phase.</li>
 *     <li>When certain checkpointing barriers are reached then generated committables are
 *         snapshotted along with the rest of the application's state.
 *         See Flink's docs for details
 *         @see <a href="https://nightlies.apache.org/flink/flink-docs-master/docs/learn-flink/fault_tolerance/#how-does-state-snapshotting-work" target="_blank">here</a></li>
 *     <li>Every {@link DeltaGlobalCommittable} instance is delivered to
 *         {@link io.delta.flink.sink.internal.committer.DeltaGlobalCommitter#combine}
 *         method when they are being committed to a {@link io.delta.standalone.DeltaLog}.</li>
 *     <li>If there's any failure of the app's execution then Flink may recover previously generated
 *         set of committables that may have not been committed. In such cases those recovered
 *         committables will be again passed to the
 *         {@link org.apache.flink.api.connector.sink.GlobalCommitter} instance along with the new
 *         set of committables from the next checkpoint interval.</li>
 *     <li>If checkpoint was successfull then committables from the given checkpoint interval are
 *         no longer recovered and exist only in the previously snapshotted states.</li>
 * </ol>
 */
public class DeltaGlobalCommittable implements Logging {

    private final List<DeltaCommittable> deltaCommittables;

    public DeltaGlobalCommittable(List<DeltaCommittable> deltaCommittables) {
        for (DeltaCommittable committable : deltaCommittables) {
            logInfo("Creating global committable object with committable for: " +
                "appId=" + committable.getAppId() +
                " checkpointId=" + committable.getCheckpointId() +
                " deltaPendingFile=" + committable.getDeltaPendingFile()
            );
        }
        this.deltaCommittables = checkNotNull(deltaCommittables);
    }

    public List<DeltaCommittable> getDeltaCommittables() {
        return deltaCommittables;
    }
}
