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
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

import io.delta.flink.sink.internal.committables.DeltaCommittable;
import org.apache.flink.core.io.SimpleVersionedSerialization;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.table.utils.PartitionPathUtils;

import io.delta.standalone.actions.AddFile;

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
 *         {@link io.delta.flink.sink.internal.writer.DeltaWriterBucket#closePartFile}
 *         method every time when any in-progress is called to be closed. This happens either when
 *         some conditions for closing are met or at the end of every checkpoint interval during a
 *         pre-commit phase when we are closing all the open files in all buckets</li>
 *     <li>Its life span holds only until the end of a checkpoint interval</li>
 *     <li>During pre-commit phase (and after closing every in-progress files) every existing
 *         {@link DeltaPendingFile} instance is automatically transformed into a
 *         {@link DeltaCommittable} instance</li>
 * </ol>
 */
public class DeltaPendingFile {

    private final LinkedHashMap<String, String> partitionSpec;

    private final String fileName;

    private final InProgressFileWriter.PendingFileRecoverable pendingFile;

    private final long recordCount;

    private final long fileSize;

    private final long lastUpdateTime;

    public DeltaPendingFile(LinkedHashMap<String, String> partitionSpec,
                            String fileName,
                            InProgressFileWriter.PendingFileRecoverable pendingFile,
                            long recordCount,
                            long fileSize,
                            long lastUpdateTime) {
        this.partitionSpec = partitionSpec;
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

    public LinkedHashMap<String, String> getPartitionSpec() {
        return new LinkedHashMap<>(partitionSpec);
    }

    /**
     * Converts {@link DeltaPendingFile} object to a {@link AddFile} object
     *
     * @return {@link AddFile} object generated from input
     */
    public AddFile toAddFile() {
        LinkedHashMap<String, String> partitionSpec = this.getPartitionSpec();
        long modificationTime = this.getLastUpdateTime();
        String filePath = PartitionPathUtils.generatePartitionPath(partitionSpec) +
            this.getFileName();
        return new AddFile(
            filePath,
            partitionSpec,
            this.getFileSize(),
            modificationTime,
            true, // dataChange
            null,
            null);
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

        dataOutputView.writeInt(deltaPendingFile.getPartitionSpec().size());
        for (Map.Entry<String, String> entry : deltaPendingFile.getPartitionSpec().entrySet()) {
            dataOutputView.writeUTF(entry.getKey());
            dataOutputView.writeUTF(entry.getValue());
        }

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
        LinkedHashMap<String, String> partitionSpec = new LinkedHashMap<>();
        int partitionSpecEntriesCount = dataInputView.readInt();
        for (int i = 0; i < partitionSpecEntriesCount; i++) {
            partitionSpec.put(dataInputView.readUTF(), dataInputView.readUTF());
        }

        String pendingFileName = dataInputView.readUTF();
        long pendingFileRecordCount = dataInputView.readLong();
        long pendingFileSize = dataInputView.readLong();
        long lastUpdateTime = dataInputView.readLong();
        InProgressFileWriter.PendingFileRecoverable pendingFile =
            SimpleVersionedSerialization.readVersionAndDeSerialize(
                pendingFileSerializer, dataInputView);
        return new DeltaPendingFile(
            partitionSpec,
            pendingFileName,
            pendingFile,
            pendingFileRecordCount,
            pendingFileSize,
            lastUpdateTime);
    }

    @Override
    public String toString() {
        String partitionSpecString = partitionSpec.keySet().stream()
            .map(key -> key + "=" + partitionSpec.get(key))
            .collect(Collectors.joining(", ", "{", "}"));
        return "DeltaPendingFile(" +
            "fileName=" + fileName +
            " lastUpdateTime=" + lastUpdateTime +
            " fileSize=" + fileSize +
            " recordCount=" + recordCount +
            " partitionSpec=" + partitionSpecString +
            ")";
    }
}
