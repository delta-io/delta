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
import org.apache.flink.core.fs.RecoverableFsDataOutputStream;
import org.apache.flink.util.Preconditions;

/**
 * This class is an implementation of {@link InProgressFileWriter} for writing elements to a part
 * using {@link BulkPartWriter}. This also implements the {@link PartFileInfo}.
 * <p>
 * An instance of this class represents one in-progress files that is currently "opened" by one of
 * the {@link org.apache.flink.connector.delta.sink.writer.DeltaWriterBucket} instance.
 * <p>
 * It's provided as a workaround for getting actual size of in-progress file right before
 * transitioning it to a pending state ("closing").
 * <p>
 * The changed behaviour compared to the original {@link BulkPartWriter} includes
 * adding {@link DeltaBulkPartWriter#closeWriter} method which is called first during
 * "close" operation for in-progress file. After calling it we can safely get the
 * actual file size and then call {@link DeltaBulkPartWriter#closeForCommit()} method.
 * <p>
 * This workaround is needed because for Parquet format the writer's buffer needs
 * to be explicitly flushed before getting the file size (and there is also no easy why to track
 * the bytes send to the writer). If such a flush will not be performed then
 * {@link PartFileInfo#getSize} will show file size without considering data buffered in writer's
 * memory (which in most cases are all the events consumed within given checkpoint interval).
 * <p>
 * Lifecycle of instances of this class is as follows:
 * <ol>
 *     <li>Since it's a class member of {@link DeltaInProgressPart} it shares its life span as
 *         well</li>
 *     <li>Instances of this class are being created inside
 *         {@link org.apache.flink.connector.delta.sink.writer.DeltaWriterBucket#rollPartFile}
 *         method every time a bucket processes the first event or if the previously opened file
 *         met conditions for rolling (e.g. size threshold)</li>
 *     <li>Its life span holds as long as the underlying file stays in an in-progress state (so
 *         until it's "rolled"), but no longer then single checkpoint interval.</li>
 *     <li>During pre-commit phase every existing {@link DeltaInProgressPart} instance is
 *         automatically transformed ("rolled") into a {@link DeltaPendingFile} instance</li>
 * </ol>
 *
 * @param <IN>       The type of input elements.
 * @param <BucketID> The type of bucket identifier
 */
public class DeltaBulkPartWriter<IN, BucketID>
    extends OutputStreamBasedPartFileWriter<IN, BucketID> {

    private final BulkWriter<IN> writer;

    private boolean closed = false;

    public DeltaBulkPartWriter(
        final BucketID bucketId,
        final RecoverableFsDataOutputStream currentPartStream,
        final BulkWriter<IN> writer,
        final long creationTime) {
        super(bucketId, currentPartStream, creationTime);
        this.writer = Preconditions.checkNotNull(writer);
    }

    public void closeWriter() throws IOException {
        writer.flush();
        writer.finish();
        closed = true;
    }

    ///////////////////////////////////////////////////////////////////////////
    // FileSink-specific
    ///////////////////////////////////////////////////////////////////////////

    @Override
    public void write(IN element, long currentTime) throws IOException {
        writer.addElement(element);
        markWrite(currentTime);
    }

    @Override
    public InProgressFileRecoverable persist() {
        throw new UnsupportedOperationException(
            "Bulk Part Writers do not support \"pause and resume\" operations.");
    }

    @Override
    public PendingFileRecoverable closeForCommit() throws IOException {
        if (!closed) {
            closeWriter();
        }
        return super.closeForCommit();
    }
}
