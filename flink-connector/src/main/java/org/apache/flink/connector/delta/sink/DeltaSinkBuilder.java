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

package org.apache.flink.connector.delta.sink;

import java.io.IOException;
import java.io.Serializable;

import org.apache.flink.connector.delta.sink.committables.DeltaCommittable;
import org.apache.flink.connector.delta.sink.committables.DeltaCommittableSerializer;
import org.apache.flink.connector.delta.sink.committables.DeltaGlobalCommittable;
import org.apache.flink.connector.delta.sink.committables.DeltaGlobalCommittableSerializer;
import org.apache.flink.connector.delta.sink.committer.DeltaCommitter;
import org.apache.flink.connector.delta.sink.committer.DeltaGlobalCommitter;
import org.apache.flink.connector.delta.sink.writer.DeltaWriter;
import org.apache.flink.connector.delta.sink.writer.DeltaWriterBucketState;
import org.apache.flink.connector.delta.sink.writer.DeltaWriterBucketStateSerializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.hadoop.conf.Configuration;

/**
 * A builder class for {@link DeltaSink}.
 * <p>
 * Most of the logic for this class was sourced from
 * {@link org.apache.flink.connector.file.sink.FileSink.BulkFormatBuilder} as the behaviour is very
 * similar. The main difference is that this {@link DeltaSinkBuilder} was extended with DeltaLake's
 * specific parts that are explicitly marked in the implementation below.
 *
 * @param <IN> The type of input elements.
 */
public class DeltaSinkBuilder<IN> implements Serializable {

    private static final long serialVersionUID = 7493169281026370228L;

    protected DeltaSinkBuilder() {
    }

    DeltaCommitter createCommitter() throws IOException {
        return new DeltaCommitter();
    }

    DeltaGlobalCommitter createGlobalCommitter() {
        return new DeltaGlobalCommitter();
    }

    ///////////////////////////////////////////////////////////////////////////
    // FileSink-specific methods
    ///////////////////////////////////////////////////////////////////////////

    /**
     * Creates the actual sink.
     */
    public DeltaSink<IN> build() {
        return new DeltaSink<>(this);
    }

    DeltaWriter<IN> createWriter() throws IOException {
        return new DeltaWriter<>();
    }

    SimpleVersionedSerializer<DeltaWriterBucketState> getWriterStateSerializer()
            throws IOException {
        return new DeltaWriterBucketStateSerializer();
    }

    SimpleVersionedSerializer<DeltaCommittable> getCommittableSerializer()
            throws IOException {
        return new DeltaCommittableSerializer();
    }

    SimpleVersionedSerializer<DeltaGlobalCommittable> getGlobalCommittableSerializer()
            throws IOException {
        return new DeltaGlobalCommittableSerializer();
    }

    /**
     * Default builder for {@link DeltaSink}.
     */
    public static final class DefaultDeltaFormatBuilder<IN> extends DeltaSinkBuilder<IN> {

        private static final long serialVersionUID = 2818087325120827526L;

        DefaultDeltaFormatBuilder(Path basePath, final Configuration conf) {
            super();
        }
    }
}
