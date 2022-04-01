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

package org.apache.flink.streaming.api.functions.sink.filesystem;

import java.io.IOException;

import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.RecoverableFsDataOutputStream;
import org.apache.flink.core.fs.RecoverableWriter;
import org.apache.flink.util.Preconditions;

/**
 * A factory that creates {@link DeltaBulkPartWriter DeltaBulkPartWriters}.
 * <p>
 * This class is provided as a part of workaround for getting actual file size.
 * <p>
 * Compared to its original version {@link BulkPartWriter} it changes only the return types
 * for methods {@link DeltaBulkBucketWriter#resumeFrom} and {@link DeltaBulkBucketWriter#openNew} to
 * a custom implementation of {@link BulkPartWriter} that is {@link DeltaBulkPartWriter}.
 *
 * @param <IN>       The type of input elements.
 * @param <BucketID> The type of bucket identifier
 */
public class DeltaBulkBucketWriter<IN, BucketID> extends BulkBucketWriter<IN, BucketID> {

    private final BulkWriter.Factory<IN> writerFactory;

    public DeltaBulkBucketWriter(final RecoverableWriter recoverableWriter,
                                 BulkWriter.Factory<IN> writerFactory)
        throws IOException {
        super(recoverableWriter, writerFactory);
        this.writerFactory = writerFactory;
    }

    ///////////////////////////////////////////////////////////////////////////
    // FileSink-specific
    ///////////////////////////////////////////////////////////////////////////

    @Override
    public DeltaBulkPartWriter<IN, BucketID> resumeFrom(
        final BucketID bucketId,
        final RecoverableFsDataOutputStream stream,
        final RecoverableWriter.ResumeRecoverable resumable,
        final long creationTime)
        throws IOException {
        Preconditions.checkNotNull(stream);
        Preconditions.checkNotNull(resumable);

        final BulkWriter<IN> writer = writerFactory.create(stream);

        return new DeltaBulkPartWriter<>(bucketId, stream, writer, creationTime);
    }

    @Override
    public DeltaBulkPartWriter<IN, BucketID> openNew(
        final BucketID bucketId,
        final RecoverableFsDataOutputStream stream,
        final Path path,
        final long creationTime)
        throws IOException {

        Preconditions.checkNotNull(stream);
        Preconditions.checkNotNull(path);

        final BulkWriter<IN> writer = writerFactory.create(stream);
        return new DeltaBulkPartWriter<>(bucketId, stream, writer, creationTime);
    }
}
