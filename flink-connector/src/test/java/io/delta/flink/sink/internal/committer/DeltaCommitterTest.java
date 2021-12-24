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

package io.delta.flink.sink.internal.committer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;

import io.delta.flink.sink.internal.committables.DeltaCommittable;
import io.delta.flink.sink.internal.committables.DeltaCommittableSerializer;
import io.delta.flink.sink.utils.DeltaSinkTestUtils;
import org.apache.flink.connector.file.sink.utils.FileSinkTestUtils;
import org.apache.flink.connector.file.sink.utils.NoOpBucketWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.InProgressFileWriter;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link DeltaCommitter}.
 * <p>
 * Implementation and testing logic based on
 * {@link org.apache.flink.connector.file.sink.committer.FileCommitterTest}
 */
public class DeltaCommitterTest {

    @Test
    public void testCommitPendingFile() throws Exception {
        // GIVEN
        StubBucketWriter stubBucketWriter = new StubBucketWriter();
        DeltaCommitter deltaCommitter = new DeltaCommitter(stubBucketWriter);

        // WHEN
        DeltaCommittable deltaCommittable =
            new DeltaCommittable(DeltaSinkTestUtils.getTestDeltaPendingFile(), "1", 1);
        List<DeltaCommittable> toRetry =
            deltaCommitter.commit(Collections.singletonList(deltaCommittable));

        // THEN
        assertEquals(1, stubBucketWriter.getRecoveredPendingFiles().size());
        assertTrue(stubBucketWriter.getRecoveredPendingFiles().get(0).isCommitted());
        assertEquals(0, toRetry.size());
    }

    @Test
    public void testCommitMultiple() throws Exception {
        // GIVEN
        StubBucketWriter stubBucketWriter = new StubBucketWriter();
        DeltaCommitter deltaCommitter = new DeltaCommitter(stubBucketWriter);

        // WHEN
        List<DeltaCommittable> committables = Arrays.asList(
            new DeltaCommittable(DeltaSinkTestUtils.getTestDeltaPendingFile(), "1", 1),
            new DeltaCommittable(DeltaSinkTestUtils.getTestDeltaPendingFile(), "1", 1),
            new DeltaCommittable(DeltaSinkTestUtils.getTestDeltaPendingFile(), "1", 1)
        );
        List<DeltaCommittable> toRetry =
            deltaCommitter.commit(committables);

        // THEN
        assertEquals(3, stubBucketWriter.getRecoveredPendingFiles().size());
        stubBucketWriter
            .getRecoveredPendingFiles()
            .forEach(pendingFile -> assertTrue(pendingFile.isCommitted()));
        assertEquals(0, toRetry.size());
    }

    @Test
    public void testCommittableWithPendingFileForNonPartitionedTable() throws IOException {
        // GIVEN
        LinkedHashMap<String, String> partitionSpec = new LinkedHashMap<>();
        DeltaCommittable committable =
            DeltaSinkTestUtils.getTestDeltaCommittableWithPendingFile(partitionSpec);

        // WHEN
        DeltaCommittable deserialized = serializeAndDeserialize(committable);

        // THEN
        DeltaSinkTestUtils.validateDeltaCommittablesEquality(
            committable, deserialized, partitionSpec);
    }

    @Test
    public void testCommittableWithPendingFileForPartitionedTable() throws IOException {
        // GIVEN
        LinkedHashMap<String, String> partitionSpec = new LinkedHashMap<>();
        partitionSpec.put("col1", "val1");
        partitionSpec.put("col2", "val2");

        DeltaCommittable committable =
            DeltaSinkTestUtils.getTestDeltaCommittableWithPendingFile(partitionSpec);

        // WHEN
        DeltaCommittable deserialized = serializeAndDeserialize(committable);

        // THEN
        DeltaSinkTestUtils.validateDeltaCommittablesEquality(
            committable, deserialized, partitionSpec);
    }

    ///////////////////////////////////////////////////////////////////////////
    // Mock Classes
    ///////////////////////////////////////////////////////////////////////////

    private static class RecordingPendingFile implements BucketWriter.PendingFile {
        private boolean committed;

        @Override
        public void commit() {
            commitAfterRecovery();
        }

        @Override
        public void commitAfterRecovery() {
            committed = true;
        }

        public boolean isCommitted() {
            return committed;
        }
    }

    private static class StubBucketWriter extends NoOpBucketWriter {
        private final List<RecordingPendingFile> recoveredPendingFiles = new ArrayList<>();

        @Override
        public PendingFile recoverPendingFile(
            InProgressFileWriter.PendingFileRecoverable pendingFileRecoverable) {
            RecordingPendingFile pendingFile = new RecordingPendingFile();
            recoveredPendingFiles.add(pendingFile);
            return pendingFile;
        }

        public List<RecordingPendingFile> getRecoveredPendingFiles() {
            return recoveredPendingFiles;
        }
    }

    ///////////////////////////////////////////////////
    // serde test utils
    ///////////////////////////////////////////////////

    private DeltaCommittable serializeAndDeserialize(DeltaCommittable committable)
        throws IOException {
        DeltaCommittableSerializer serializer =
            new DeltaCommittableSerializer(
                new FileSinkTestUtils.SimpleVersionedWrapperSerializer<>(
                    FileSinkTestUtils.TestPendingFileRecoverable::new));
        byte[] data = serializer.serialize(committable);
        return serializer.deserialize(serializer.getVersion(), data);
    }
}
