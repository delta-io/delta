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
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.regex.Pattern;

import io.delta.flink.sink.internal.SchemaConverter;
import io.delta.flink.sink.internal.committables.DeltaCommittable;
import io.delta.flink.sink.internal.committables.DeltaGlobalCommittable;
import io.delta.flink.sink.utils.DeltaSinkTestUtils;
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
import io.delta.standalone.actions.CommitInfo;
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
        "initializeTableBeforeCommit = {1}, " +
        "partitionSpec = {2}, "
    )
    public static Collection<Object[]> params() {
        return Arrays.asList(
            // shouldTryUpdateSchema, initializeTableBeforeCommit, partitionSpec
            new Object[]{false, false, DeltaSinkTestUtils.getEmptyTestPartitionSpec()},
            new Object[]{false, false, DeltaSinkTestUtils.getTestPartitionSpec()},
            new Object[]{false, true, DeltaSinkTestUtils.getEmptyTestPartitionSpec()},
            new Object[]{false, true, DeltaSinkTestUtils.getTestPartitionSpec()},
            new Object[]{true, false, DeltaSinkTestUtils.getEmptyTestPartitionSpec()},
            new Object[]{true, true, DeltaSinkTestUtils.getEmptyTestPartitionSpec()},
            new Object[]{true, false, DeltaSinkTestUtils.getTestPartitionSpec()},
            new Object[]{true, true, DeltaSinkTestUtils.getTestPartitionSpec()}
        );
    }

    @Parameterized.Parameter(0)
    public boolean shouldTryUpdateSchema;

    @Parameterized.Parameter(1)
    public boolean initializeTableBeforeCommit;

    @Parameterized.Parameter(2)
    public LinkedHashMap<String, String> partitionSpec;

    private RowType rowTypeToCommit;

    private Path tablePath;
    private DeltaLog deltaLog;

    @Before
    public void setup() throws IOException {
        tablePath = new Path(TEMPORARY_FOLDER.newFolder().toURI());
        if (initializeTableBeforeCommit) {
            if (partitionSpec.isEmpty()) {
                DeltaSinkTestUtils.initTestForNonPartitionedTable(
                    tablePath.getPath());
            } else {
                DeltaSinkTestUtils.initTestForPartitionedTable(tablePath.getPath());
            }
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
            DeltaSinkTestUtils.getListOfDeltaCommittables(3, partitionSpec);
        List<DeltaGlobalCommittable> globalCommittables =
            Collections.singletonList(new DeltaGlobalCommittable(deltaCommittables));

        // WHEN
        globalCommitter.commit(globalCommittables);

        // THEN
        validateCurrentSnapshotState(deltaCommittables.size());
        validateCurrentTableFiles(deltaLog.update());
        validateEngineInfo(deltaLog);
    }

    private void validateEngineInfo(DeltaLog deltaLog){
        CommitInfo commitInfo = deltaLog.getCommitInfoAt(deltaLog.snapshot().getVersion());
        String engineInfo = commitInfo.getEngineInfo().orElse("");

        // pattern to match for instance: "flink-engine/1.14.0-flink-delta-connector/0.3.0"
        String expectedEngineInfoPattern =
            "flink-engine/[0-9]+\\.[0-9]+\\.[0-9]+-flink-delta-connector/[0-9]+\\.[0-9]+\\.[0-9]+";
        assertTrue(Pattern.compile(expectedEngineInfoPattern).matcher(engineInfo).find());
    }

    private void validateCurrentSnapshotState(int numFilesAdded) {
        int initialTableFilesCount = 0;
        if (initializeTableBeforeCommit) {
            initialTableFilesCount = deltaLog.snapshot().getAllFiles().size();
        }
        int expectedTableVersionAfterUpdate = initializeTableBeforeCommit ? 1 : 0;
        List<String> partitionColumns = new ArrayList<>(partitionSpec.keySet());
        Snapshot snapshot = deltaLog.update();
        assertEquals(snapshot.getVersion(), expectedTableVersionAfterUpdate);
        assertEquals(snapshot.getAllFiles().size(), numFilesAdded + initialTableFilesCount);
        assertEquals(deltaLog.snapshot().getMetadata().getSchema().toJson(),
            SchemaConverter.toDeltaDataType(rowTypeToCommit).toJson());
        assertEquals(snapshot.getMetadata().getPartitionColumns(), partitionColumns);
    }

    private void validateCurrentTableFiles(Snapshot snapshot) {
        CloseableIterator<AddFile> filesIterator = snapshot.scan().getFiles();
        while (filesIterator.hasNext()) {
            AddFile addFile = filesIterator.next();
            assertEquals(addFile.getPartitionValues(), partitionSpec);
            assertTrue(addFile.getSize() > 0);
        }
    }
}
