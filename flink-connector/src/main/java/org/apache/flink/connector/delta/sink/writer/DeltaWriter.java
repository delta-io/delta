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

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.connector.delta.sink.committables.DeltaCommittable;
import org.apache.flink.connector.file.sink.writer.FileWriter;

/**
 * A {@link SinkWriter} implementation for {@link org.apache.flink.connector.delta.sink.DeltaSink}.
 *
 * <p>It writes data to and manages the different active {@link DeltaWriterBucket buckets} in the
 * {@link org.apache.flink.connector.delta.sink.DeltaSink}.
 * <p>
 * Most of the logic for this class was sourced from {@link FileWriter} as the behaviour is very
 * similar. The main differences are use of custom implementations for some member classes and also
 * managing {@link io.delta.standalone.DeltaLog} transactional ids.
 *
 * @param <IN> The type of input elements.
 */
@Internal
public class DeltaWriter<IN>
        implements SinkWriter<IN, DeltaCommittable, DeltaWriterBucketState>,
        Sink.ProcessingTimeService.ProcessingTimeCallback {

    public DeltaWriter() {
    }

    @Override
    public List<DeltaWriterBucketState> snapshotState() {
        return Collections.emptyList();
    }

    ///////////////////////////////////////////////////////////////////////////
    // FileSink-specific methods
    ///////////////////////////////////////////////////////////////////////////

    @Override
    public void write(IN element, Context context) throws IOException {
    }

    @Override
    public List<DeltaCommittable> prepareCommit(boolean flush) throws IOException {
        return Collections.emptyList();
    }

    @Override
    public void close() {
    }

    @Override
    public void onProcessingTime(long time) throws IOException {
    }
}
