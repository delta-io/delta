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
import org.apache.flink.connector.delta.sink.committables.DeltaCommittable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Internal implementation for writing the actual events to the underlying files in the correct
 * buckets / partitions.
 *
 * <p>
 * In reference to the Flink's {@link org.apache.flink.api.connector.sink.Sink} topology
 * one of its main components is {@link org.apache.flink.api.connector.sink.SinkWriter}
 * which in case of DeltaSink is implemented as {@link DeltaWriter}. However, to comply
 * with DeltaLake's support for partitioning tables a new component was added in the form
 * of {@link DeltaWriterBucket} that is responsible for handling writes to only one of the
 * buckets (aka partitions). Such bucket writers are managed by {@link DeltaWriter}
 * which works as a proxy between higher order frameworks commands (write, prepareCommit etc.)
 * and actual writes' implementation in {@link DeltaWriterBucket}. Thanks to this solution
 * events within one {@link DeltaWriter} operator received during particular checkpoint interval
 * are always grouped and flushed to the currently opened in-progress file.
 * <p>
 * The implementation was sourced from the {@link org.apache.flink.connector.file.sink.FileSink}
 * that utilizes same concept and implements
 * {@link org.apache.flink.connector.file.sink.writer.FileWriter} with its FileWriterBucket
 * implementation.
 * All differences between DeltaSink's and FileSink's writer buckets are explained in particular
 * method's below.
 *
 * @param <IN> The type of input elements.
 */
@Internal
class DeltaWriterBucket<IN> {

    private static final Logger LOG = LoggerFactory.getLogger(DeltaWriterBucket.class);

    /**
     * Constructor to create a new empty bucket.
     */
    private DeltaWriterBucket() throws IOException {
    }

    List<DeltaCommittable> prepareCommit(boolean flush) throws IOException {
        return Collections.emptyList();
    }

    DeltaWriterBucketState snapshotState() {
        return new DeltaWriterBucketState();
    }

    ///////////////////////////////////////////////////////////////////////////
    // FileSink-specific methods
    ///////////////////////////////////////////////////////////////////////////

    void write(IN element, long currentTime) throws IOException {
    }

    ///////////////////////////////////////////////////////////////////////////
    // Static Factory
    ///////////////////////////////////////////////////////////////////////////

    public static class DeltaWriterBucketFactory {
        static <IN> DeltaWriterBucket<IN> getNewBucket() throws IOException {
            return new DeltaWriterBucket<>();
        }

        static <IN> DeltaWriterBucket<IN> restoreBucket()
                throws IOException {
            return new DeltaWriterBucket<>();
        }
    }
}
