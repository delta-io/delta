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

package io.delta.flink.sink.internal.writer;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.delta.flink.sink.internal.committables.DeltaCommittable;
import io.delta.flink.sink.internal.committer.DeltaCommitter;
import io.delta.flink.sink.utils.DeltaSinkTestUtils;
import io.delta.flink.utils.TestParquetReader;
import org.apache.flink.core.fs.Path;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.PartFileInfo;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.CheckpointRollingPolicy;
import org.apache.flink.table.data.RowData;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link DeltaWriterBucket}.
 */
public class DeltaWriterBucketTest {

    @ClassRule
    public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();
    private static final String BUCKET_ID = "testing-bucket";
    private static final String APP_ID = "1";

    private final Map<String, Counter> testCounters = new HashMap<>();

    @Test
    public void testOnCheckpointNoPendingRecoverable() throws IOException {
        // GIVEN
        File outDir = TEMP_FOLDER.newFolder();
        Path bucketPath = new Path(outDir.toURI());
        DeltaWriterBucket<RowData> bucketWriter = getBucketWriter(bucketPath);

        // WHEN
        List<DeltaCommittable> deltaCommittables = onCheckpointActions(
            bucketWriter,
            bucketPath,
            false // doCommit
        );

        // THEN
        assertEquals(0, deltaCommittables.size());
    }

    @Test
    public void testOnSingleCheckpointInterval() throws IOException {
        // GIVEN
        File outDir = TEMP_FOLDER.newFolder();
        Path bucketPath = new Path(outDir.toURI());
        int rowsCount = 2;
        List<RowData> testRows = DeltaSinkTestUtils.getTestRowData(rowsCount);

        DeltaWriterBucket<RowData> bucketWriter = getBucketWriter(bucketPath);

        // WHEN
        writeData(bucketWriter, testRows);
        List<DeltaCommittable> deltaCommittables = onCheckpointActions(
            bucketWriter,
            bucketPath,
            true // doCommit
        );

        // THEN
        assertEquals(deltaCommittables.size(), 1);
        int writtenRecordsCount = getWrittenRecordsCount(deltaCommittables, bucketPath);
        assertEquals(rowsCount, writtenRecordsCount);
    }

    @Test
    public void testOnMultipleCheckpointIntervals() throws IOException {
        // GIVEN
        File outDir = TEMP_FOLDER.newFolder();
        Path bucketPath = new Path(outDir.toURI());
        int rowsCount = 2;
        List<RowData> testRows = DeltaSinkTestUtils.getTestRowData(rowsCount);

        DeltaWriterBucket<RowData> bucketWriter = getBucketWriter(bucketPath);

        // WHEN
        writeData(bucketWriter, testRows);
        List<DeltaCommittable> deltaCommittables1 = onCheckpointActions(
            bucketWriter,
            bucketPath,
            true // doCommit
        );

        writeData(bucketWriter, testRows);
        List<DeltaCommittable> deltaCommittables2 = onCheckpointActions(
            bucketWriter,
            bucketPath,
            true // doCommit
        );

        // THEN
        assertEquals(deltaCommittables1.size(), 1);
        assertEquals(deltaCommittables2.size(), 1);
        List<DeltaCommittable> combinedCommittables =
            Stream.concat(deltaCommittables1.stream(), deltaCommittables2.stream())
                .collect(Collectors.toList());
        int writtenRecordsCount = getWrittenRecordsCount(combinedCommittables, bucketPath);
        assertEquals(rowsCount * 2, writtenRecordsCount);
    }

    /**
     * This test forces one of the pending file to be rolled before checkpoint and then validates
     * that more than one committable (corresponding to one written file) have been generated.
     */
    @Test
    public void testCheckpointWithMultipleRolledFiles() throws IOException {
        // GIVEN
        File outDir = TEMP_FOLDER.newFolder();
        Path bucketPath = new Path(outDir.toURI());
        int rowsCount = 4;
        List<RowData> testRows = DeltaSinkTestUtils.getTestRowData(rowsCount);

        DeltaWriterBucket<RowData> bucketWriter = getBucketWriter(
            bucketPath,
            new TestForcedRollFilePolicy());

        // WHEN
        // writing 4 rows, while only the second one should force rolling
        writeData(bucketWriter, testRows);
        List<DeltaCommittable> deltaCommittables = onCheckpointActions(
            bucketWriter,
            bucketPath,
            true // doCommit
        );

        // THEN
        assertEquals(
            "Two files should have been rolled during tested checkpoint interval",
            deltaCommittables.size(),
            2
        );
        int writtenRecordsCount = getWrittenRecordsCount(deltaCommittables, bucketPath);
        assertEquals(rowsCount, writtenRecordsCount);
    }

    @Test(expected = FileNotFoundException.class)
    public void testCannotReadUncommittedFiles() throws IOException {
        // GIVEN
        File outDir = TEMP_FOLDER.newFolder();
        Path bucketPath = new Path(outDir.toURI());
        int rowsCount = 2;
        List<RowData> testRows = DeltaSinkTestUtils.getTestRowData(rowsCount);

        DeltaWriterBucket<RowData> bucketWriter = getBucketWriter(bucketPath);

        // WHEN
        writeData(bucketWriter, testRows);
        List<DeltaCommittable> deltaCommittables = onCheckpointActions(
            bucketWriter,
            bucketPath,
            false // doCommit
        );

        // THEN
        assertEquals(deltaCommittables.size(), 1);
        getWrittenRecordsCount(deltaCommittables, bucketPath);
    }

    @Test
    public void testMetrics() throws Exception {
        // GIVEN
        File outDir = TEMP_FOLDER.newFolder();
        Path bucketPath = new Path(outDir.toURI());
        int rowsCount = 2;
        List<RowData> testRows = DeltaSinkTestUtils.getTestRowData(rowsCount);

        DeltaWriterBucket<RowData> bucketWriter = getBucketWriter(bucketPath);

        // WHEN
        writeData(bucketWriter, testRows);
        List<DeltaCommittable> deltaCommittables = onCheckpointActions(
            bucketWriter,
            bucketPath,
            false // doCommit
        );

        // THEN
        assertEquals(
            rowsCount,
            testCounters.get(DeltaWriterBucket.RECORDS_WRITTEN_METRIC_NAME).getCount());
        assertTrue(testCounters.get(DeltaWriterBucket.BYTES_WRITTEN_METRIC_NAME).getCount() > 0);
    }

    ///////////////////////////////////////////////////////////////////////////
    // Utility Methods
    ///////////////////////////////////////////////////////////////////////////

    private DeltaWriterBucket<RowData> getBucketWriter(
        Path bucketPath,
        CheckpointRollingPolicy<RowData, String> rollingPolicy) throws IOException {

        // need to mock the metric group here since it's complicated to initialize a Flink's
        // MetricGroup without the context object
        MetricGroup metricGroupMock = Mockito.mock(MetricGroup.class);
        Mockito.when(metricGroupMock.counter(Mockito.anyString())).thenAnswer(
            invocation -> {
                String metricName = invocation.getArgument(0, String.class);
                if (!testCounters.containsKey(metricName)) {
                    testCounters.put(metricName, new SimpleCounter());
                }
                return testCounters.get(metricName);
            });

        return DeltaWriterBucket.DeltaWriterBucketFactory.getNewBucket(
            BUCKET_ID,
            bucketPath,
            DeltaSinkTestUtils.createBucketWriter(bucketPath),
            rollingPolicy,
            OutputFileConfig.builder().withPartSuffix(".snappy.parquet").build(),
            metricGroupMock
        );
    }

    private DeltaWriterBucket<RowData> getBucketWriter(Path bucketPath) throws IOException {
        return getBucketWriter(bucketPath, DeltaSinkTestUtils.ON_CHECKPOINT_ROLLING_POLICY);
    }

    private static List<DeltaCommittable> onCheckpointActions(DeltaWriterBucket<RowData> bucket,
                                                              Path bucketPath,
                                                              boolean doCommit) throws IOException {
        List<DeltaCommittable> deltaCommittables = bucket.prepareCommit(
                        false, // flush
                        APP_ID,
                        1);
        DeltaWriterBucketState bucketState = bucket.snapshotState(APP_ID);

        assertEquals(BUCKET_ID, bucketState.getBucketId());
        assertEquals(bucketPath, bucketState.getBucketPath());

        if (doCommit) {
            new DeltaCommitter(
                DeltaSinkTestUtils.createBucketWriter(bucketPath)).commit(deltaCommittables);
        }
        return deltaCommittables;
    }

    private static void writeData(DeltaWriterBucket<RowData> bucket,
                                  List<RowData> rows) {
        rows.forEach(rowData -> {
            try {
                bucket.write(rowData, 0);
            } catch (IOException e) {
                throw new RuntimeException("Writing to the bucket failed");
            }
        });
    }

    private static int getWrittenRecordsCount(List<DeltaCommittable> committables,
                                              Path bucketPath) throws IOException {
        int writtenRecordsCount = 0;
        for (DeltaCommittable committable : committables) {
            Path filePath = new Path(bucketPath, committable.getDeltaPendingFile().getFileName());
            writtenRecordsCount +=
                TestParquetReader.parseAndCountRecords(
                    filePath,
                    DeltaSinkTestUtils.TEST_ROW_TYPE,
                    DeltaSinkTestUtils.TEST_ROW_TYPE_CONVERTER
                );
        }
        return writtenRecordsCount;
    }

    private static class TestForcedRollFilePolicy extends CheckpointRollingPolicy<RowData, String> {

        /**
         * Forcing second row to roll current in-progress file.
         * See {@link DeltaSinkTestUtils#getTestRowData} for reference on the incrementing logic of
         * the test rows.
         */
        @Override
        public boolean shouldRollOnEvent(PartFileInfo<String> partFileState, RowData element) {
            return element.getString(0).toString().equals("1");
        }

        @Override
        public boolean shouldRollOnProcessingTime(PartFileInfo<String> partFileState,
                                                  long currentTime) {
            return false;
        }
    }
}
