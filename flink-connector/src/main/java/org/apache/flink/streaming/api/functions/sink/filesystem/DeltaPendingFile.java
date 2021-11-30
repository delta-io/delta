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

import org.apache.flink.core.io.SimpleVersionedSerialization;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

/**
 * Wrapper class for {@link InProgressFileWriter.PendingFileRecoverable} object.
 * This class carries the internal committable information to be used during the checkpoint/commit
 * phase.
 * <p>
 * As similar to {@link org.apache.flink.connector.file.sink.FileSink} we need to carry
 * {@link InProgressFileWriter.PendingFileRecoverable} information to perform "local" commit
 * on file that the sink has written data to. However, as opposite to mentioned FileSink,
 * in DeltaSink we need to perform also "global" commit to the {@link io.delta.standalone.DeltaLog}
 * and for that additional file metadata must be provided. Hence, this class provides the required
 * information for both types of commits by wrapping pending file and attaching file's metadata.
 * <p>
 * Lifecycle of instances of this class is as follows:
 * <ol>
 *     <li>Instances of this class are being created inside
 *         {@link org.apache.flink.connector.delta.sink.writer.DeltaWriterBucket#closePartFile}
 *         method every time when any in-progress is called to be closed. This happens either when
 *         some conditions for closing are met or at the end of every checkpoint interval during a
 *         pre-commit phase when we are closing all the open files in all buckets</li>
 *     <li>Its life span holds only until the end of a checkpoint interval</li>
 *     <li>During pre-commit phase (and after closing every in-progress files) every existing
 *         {@link DeltaPendingFile} instance is automatically transformed into a
 *         {@link org.apache.flink.connector.delta.sink.committables.DeltaCommittable} instance</li>
 * </ol>
 */
public class DeltaPendingFile {

    private final String fileName;

    private final InProgressFileWriter.PendingFileRecoverable pendingFile;

    private final long recordCount;

    private final long fileSize;

    private final long lastUpdateTime;

    public DeltaPendingFile(String fileName,
                            InProgressFileWriter.PendingFileRecoverable pendingFile,
                            long recordCount,
                            long fileSize,
                            long lastUpdateTime) {
        this.fileName = fileName;
        this.pendingFile = pendingFile;
        this.fileSize = fileSize;
        this.recordCount = recordCount;
        this.lastUpdateTime = lastUpdateTime;
    }

    public String getFileName() {
        return fileName;
    }

    public InProgressFileWriter.PendingFileRecoverable getPendingFile() {
        return pendingFile;
    }

    public long getFileSize() {
        return fileSize;
    }

    public long getRecordCount() {
        return recordCount;
    }

    public long getLastUpdateTime() {
        return lastUpdateTime;
    }

    ///////////////////////////////////////////////////////////////////////////
    // serde utils
    ///////////////////////////////////////////////////////////////////////////

    public static void serialize(
        DeltaPendingFile deltaPendingFile,
        DataOutputView dataOutputView,
        SimpleVersionedSerializer<InProgressFileWriter.PendingFileRecoverable>
            pendingFileSerializer) throws IOException {
        assert deltaPendingFile.getFileName() != null;
        assert deltaPendingFile.getPendingFile() != null;

        dataOutputView.writeUTF(deltaPendingFile.getFileName());
        dataOutputView.writeLong(deltaPendingFile.getRecordCount());
        dataOutputView.writeLong(deltaPendingFile.getFileSize());
        dataOutputView.writeLong(deltaPendingFile.getLastUpdateTime());

        SimpleVersionedSerialization.writeVersionAndSerialize(
            pendingFileSerializer,
            deltaPendingFile.getPendingFile(),
            dataOutputView
        );
    }

    public static DeltaPendingFile deserialize(
        DataInputView dataInputView,
        SimpleVersionedSerializer<InProgressFileWriter.PendingFileRecoverable>
            pendingFileSerializer) throws IOException {

        String pendingFileName = dataInputView.readUTF();
        long pendingFileRecordCount = dataInputView.readLong();
        long pendingFileSize = dataInputView.readLong();
        long lastUpdateTime = dataInputView.readLong();
        InProgressFileWriter.PendingFileRecoverable pendingFile =
            SimpleVersionedSerialization.readVersionAndDeSerialize(
                pendingFileSerializer, dataInputView);
        return new DeltaPendingFile(
            pendingFileName,
            pendingFile,
            pendingFileRecordCount,
            pendingFileSize,
            lastUpdateTime);
    }
}
