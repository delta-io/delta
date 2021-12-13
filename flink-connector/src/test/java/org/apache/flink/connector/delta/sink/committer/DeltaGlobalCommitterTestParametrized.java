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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.flink.connector.delta.sink.SchemaConverter;
import org.apache.flink.connector.delta.sink.committables.DeltaCommittable;
import org.apache.flink.connector.delta.sink.committables.DeltaGlobalCommittable;
import org.apache.flink.connector.delta.sink.utils.DeltaSinkTestUtils;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.types.logical.RowType;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import io.delta.standalone.DeltaLog;
import io.delta.standalone.Snapshot;
import io.delta.standalone.actions.AddFile;
import io.delta.standalone.data.CloseableIterator;

/**
 * Tests for {@link DeltaGlobalCommitter}.
 */
@RunWith(Parameterized.class)
public class DeltaGlobalCommitterTestParametrized {

    @ClassRule
    public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

    @Parameterized.Parameters(
        name = "shouldTryUpdateSchema = {0}, " +
            "initializeTableBeforeCommit = {1}"
    )
    public static Collection<Object[]> params() {
        return Arrays.asList(
            // shouldTryUpdateSchema, initializeTableBeforeCommit
            new Object[]{false, false},
            new Object[]{false, true},
            new Object[]{true, false},
            new Object[]{true, true}
        );
    }

    @Parameterized.Parameter(0)
    public boolean shouldTryUpdateSchema;

    @Parameterized.Parameter(1)
    public boolean initializeTableBeforeCommit;

    private RowType rowTypeToCommit;

    private Path tablePath;
    private DeltaLog deltaLog;

    @Before
    public void setup() throws IOException {
        tablePath = new Path(TEMPORARY_FOLDER.newFolder().toURI());
        if (initializeTableBeforeCommit) {
            DeltaSinkTestUtils.initTestForNonPartitionedTable(
                tablePath.getPath());
        }
        deltaLog = DeltaLog.forTable(DeltaSinkTestUtils.getHadoopConf(), tablePath.getPath());
        rowTypeToCommit = shouldTryUpdateSchema ?
            DeltaSinkTestUtils.addNewColumnToSchema(DeltaSinkTestUtils.TEST_ROW_TYPE) :
            DeltaSinkTestUtils.TEST_ROW_TYPE;
    }

    @Test
    public void testCommitToDeltaTableInAppendMode() {
        //GIVEN
        DeltaGlobalCommitter globalCommitter = new DeltaGlobalCommitter(
            DeltaSinkTestUtils.getHadoopConf(),
            tablePath,
            rowTypeToCommit,
            shouldTryUpdateSchema);
        List<DeltaCommittable> deltaCommittables =
            DeltaSinkTestUtils.getListOfDeltaCommittables(3);
        List<DeltaGlobalCommittable> globalCommittables =
            Collections.singletonList(new DeltaGlobalCommittable(deltaCommittables));

        // WHEN
        globalCommitter.commit(globalCommittables);

        // THEN
        validateCurrentSnapshotState(deltaCommittables.size());
        validateCurrentTableFiles(deltaLog.update());
    }

    private void validateCurrentSnapshotState(int numFilesAdded) {
        int initialTableFilesCount = 0;
        if (initializeTableBeforeCommit) {
            initialTableFilesCount = deltaLog.snapshot().getAllFiles().size();
        }
        int expectedTableVersionAfterUpdate = initializeTableBeforeCommit ? 1 : 0;
        Snapshot snapshot = deltaLog.update();
        assertEquals(snapshot.getVersion(), expectedTableVersionAfterUpdate);
        assertEquals(snapshot.getAllFiles().size(), numFilesAdded + initialTableFilesCount);
        assertEquals(deltaLog.snapshot().getMetadata().getSchema().toJson(),
            SchemaConverter.toDeltaDataType(rowTypeToCommit).toJson());
    }

    private void validateCurrentTableFiles(
        Snapshot snapshot) {
        CloseableIterator<AddFile> filesIterator = snapshot.scan().getFiles();
        while (filesIterator.hasNext()) {
            AddFile addFile = filesIterator.next();
            assertTrue(addFile.getSize() > 0);
        }
    }
}
