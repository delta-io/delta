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

package org.apache.flink.connector.delta.sink.committer;

import java.util.Collections;
import java.util.List;

import org.apache.flink.api.connector.sink.GlobalCommitter;
import org.apache.flink.connector.delta.sink.committables.DeltaCommittable;
import org.apache.flink.connector.delta.sink.committables.DeltaGlobalCommittable;

/**
 * A {@link GlobalCommitter} implementation for
 * {@link org.apache.flink.connector.delta.sink.DeltaSink}.
 * <p>
 * It commits written files to the DeltaLog and provides exactly once semantics by guaranteeing
 * idempotence behaviour of the commit phase. It means that when given the same set of
 * {@link DeltaCommittable} objects (that contain metadata about written files along with unique
 * identifier of the given Flink's job and checkpoint id) it will never commit them multiple times.
 * Such behaviour is achieved by constructing transactional id using mentioned app identifier and
 * checkpointId.
 * <p>
 * Lifecycle of instances of this class is as follows:
 * <ol>
 *     <li>Instances of this class are being created during a (global) commit stage</li>
 *     <li>For given commit stage there is only one singleton instance of
 *         {@link DeltaGlobalCommitter}</li>
 *     <li>Every instance exists only during given commit stage after finishing particular
 *         checkpoint interval. Despite being bundled to a finish phase of a checkpoint interval
 *         a single instance of {@link DeltaGlobalCommitter} may process committables from multiple
 *         checkpoints intervals (it happens e.g. when there was a app's failure and Flink has
 *         recovered committables from previous commit stage to be re-committed.</li>
 * </ol>
 */
public class DeltaGlobalCommitter
        implements GlobalCommitter<DeltaCommittable, DeltaGlobalCommittable> {

    public DeltaGlobalCommitter() {
    }

    @Override
    public List<DeltaGlobalCommittable> filterRecoveredCommittables(
            List<DeltaGlobalCommittable> globalCommittables) {
        return globalCommittables;
    }

    @Override
    public DeltaGlobalCommittable combine(List<DeltaCommittable> committables) {
        return new DeltaGlobalCommittable(committables);
    }

    @Override
    public List<DeltaGlobalCommittable> commit(List<DeltaGlobalCommittable> globalCommittables) {
        return Collections.emptyList();
    }

    @Override
    public void endOfInput() {
    }

    @Override
    public void close() {
    }
}
