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

package org.apache.flink.connector.delta.sink.writer;

import org.apache.flink.annotation.Internal;

/**
 * State of a {@link DeltaWriterBucket} that will become part of each application's
 * snapshot created during pre-commit phase of a checkpoint process or manually on demand
 * by the user.
 * see fault_tolerance/#state-snapshots section on
 * @see <a href="https://ci.apache.org/projects/flink/flink-docs-master/docs/learn-flink/" target="_blank">this page</a>
 *
 * <p>
 * This class is partially inspired by
 * {@link org.apache.flink.connector.file.sink.writer.FileWriterBucketState}
 * but with some modifications like:
 * <ol>
 *   <li>removed snapshotting in-progress file's state because
 *       {@link org.apache.flink.connector.delta.sink.DeltaSink} is supposed to always roll part
 *       files on checkpoint so there is no need to recover any in-progress files' states
 *   <li>extends the state by adding application's unique identifier to guarantee the idempotent
 *       file writes and commits to the {@link io.delta.standalone.DeltaLog}
 * </ol>
 */
@Internal
public class DeltaWriterBucketState {

    public DeltaWriterBucketState() {}
}
