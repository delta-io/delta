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

package org.apache.flink.connector.delta.sink.committables;

import java.io.Serializable;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.api.functions.sink.filesystem.DeltaPendingFile;

/**
 * Committable object that carries the information about files written to the file system
 * during particular checkpoint interval.
 * <p>
 * As {@link org.apache.flink.connector.delta.sink.DeltaSink} implements both
 * {@link org.apache.flink.api.connector.sink.Committer} and
 * {@link org.apache.flink.api.connector.sink.GlobalCommitter} and
 * then its committable must provide all metadata for committing data on both levels.
 * <p>
 * In order to commit data during {@link org.apache.flink.api.connector.sink.Committer#commit}
 * information carried inside {@link DeltaPendingFile} are used. Next during
 * {@link org.apache.flink.api.connector.sink.GlobalCommitter#commit} we are using both:
 * metadata carried inside {@link DeltaPendingFile} and also transactional identifier constructed by
 * application's unique id and checkpoint interval's id.
 */
@Internal
public class DeltaCommittable implements Serializable {

    public DeltaCommittable() {}
}
