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

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.flink.api.connector.sink.Committer;
import org.apache.flink.connector.delta.sink.DeltaSink;
import org.apache.flink.connector.delta.sink.committables.DeltaCommittable;
import org.apache.flink.connector.delta.sink.writer.DeltaWriter;
import org.apache.flink.connector.file.sink.FileSink;

/**
 * Committer implementation for {@link DeltaSink}.
 *
 * <p>This committer is responsible for taking staged part-files, i.e. part-files in "pending"
 * state, created by the {@link org.apache.flink.connector.delta.sink.writer.DeltaWriter}
 * and put them in "finished" state ready to be committed to the DeltaLog during "global" commit.
 *
 * <p> This class behaves almost in the same way as its equivalent
 * {@link org.apache.flink.connector.file.sink.committer.FileCommitter}
 * in the {@link FileSink}. The only differences are:
 *
 * <ol>
 *   <li>use of the {@link DeltaCommittable} instead of
 *       {@link org.apache.flink.connector.file.sink.FileSinkCommittable}
 *   <li>some simplifications for the committable's internal information and commit behaviour.
 *       In particular in {@link DeltaCommitter#commit} method we do not take care of any inprogress
 *       file's state (as opposite to
 *       {@link org.apache.flink.connector.file.sink.committer.FileCommitter#commit}
 *       because in {@link DeltaWriter#prepareCommit} we always roll all of the in-progress files.
 *       Valid note here is that's also the default {@link FileSink}'s behaviour for all of the
 *       bulk formats (Parquet included).
 * </ol>
 */
public class DeltaCommitter implements Committer<DeltaCommittable> {

    ///////////////////////////////////////////////////////////////////////////
    // FileSink-specific
    ///////////////////////////////////////////////////////////////////////////

    public DeltaCommitter() {
    }

    @Override
    public List<DeltaCommittable> commit(List<DeltaCommittable> committables) throws IOException {
        return Collections.emptyList();
    }

    @Override
    public void close() {
    }
}
