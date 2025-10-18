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
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;

import io.delta.flink.sink.internal.DeltaBucketAssigner;
import io.delta.flink.sink.internal.DeltaPartitionComputer;
import io.delta.flink.sink.internal.committables.DeltaCommittable;
import io.delta.flink.sink.utils.DeltaSinkTestUtils;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.Path;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.BasePathBucketAssigner;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.ExceptionUtils;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link DeltaWriter}.
 * <p>
 */
@RunWith(Parameterized.class)
public class DeltaWriterTest {

    @ClassRule
    public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();
    private static final String APP_ID = "1";

    @Parameterized.Parameters(
        name = "isPartitioned = {0}"
    )
    public static Collection<Object[]> params() {
        return Arrays.asList(
            new Object[]{false},
            new Object[]{true}
        );
    }

    @Parameterized.Parameter()
    public Boolean isPartitioned;

    // counter for the records produced by given test instance
    private int testRecordsCount = 0;

    private final Map<String, Counter> testCounters = new HashMap<>();

    @Test
    public void testPreCommit() throws Exception {
        // GIVEN
        File outDir = TEMP_FOLDER.newFolder();
        Path path = new Path(outDir.toURI());
        int rowsCount = 2;
        List<RowData> testRows = DeltaSinkTestUtils.getTestRowData(rowsCount);
        DeltaWriter<RowData> writer = createNewWriter(path);

        // WHEN
        writeData(writer, testRows);
        List<DeltaCommittable> committables = writer.prepareCommit(false);

        // THEN
        int elementsCount = isPartitioned ? 2 : 1;
        assertEquals(elementsCount, writer.getActiveBuckets().size());
        assertEquals(elementsCount, committables.size());
        assertEquals(writer.getNextCheckpointId(), 2);
    }

    @Test
    public void testSnapshotAndRestore() throws Exception {
        // GIVEN
        File outDir = TEMP_FOLDER.newFolder();
        Path path = new Path(outDir.toURI());
        int rowsCount = 2;
        List<RowData> testRows = DeltaSinkTestUtils.getTestRowData(rowsCount);
        DeltaWriter<RowData> writer = createNewWriter(path);
        int elementsCount = isPartitioned ? 2 : 1;

        // WHEN
        writeData(writer, testRows);
        writer.prepareCommit(false);
        List<DeltaWriterBucketState> states = writer.snapshotState();
        assertEquals(elementsCount, writer.getActiveBuckets().size());
        assertEquals(elementsCount, states.size());

        // THEN
        writer = restoreWriter(path, states);
        assertEquals(elementsCount, writer.getActiveBuckets().size());
    }

    @Test
    public void testMergingForRescaling() throws Exception {
        // GIVEN
        File outDir = TEMP_FOLDER.newFolder();
        Path path = new Path(outDir.toURI());
        int rowsCount = 2;
        List<RowData> testRows = DeltaSinkTestUtils.getTestRowData(rowsCount);
        DeltaWriter<RowData> firstWriter = createNewWriter(path);
        DeltaWriter<RowData> secondWriter = createNewWriter(path);

        // WHEN
        writeData(firstWriter, testRows);
        firstWriter.prepareCommit(false);
        List<DeltaWriterBucketState> firstState = firstWriter.snapshotState();

        writeData(secondWriter, testRows);
        secondWriter.prepareCommit(false);
        List<DeltaWriterBucketState> secondState = secondWriter.snapshotState();

        List<DeltaWriterBucketState> mergedState = new ArrayList<>();
        mergedState.addAll(firstState);
        mergedState.addAll(secondState);
        DeltaWriter<RowData> restoredWriter = restoreWriter(path, mergedState);

        // THEN
        int elementsCount = isPartitioned ? 2 : 1;
        assertEquals(elementsCount, restoredWriter.getActiveBuckets().size());
    }

    @Test
    public void testBucketIsRemovedWhenNotActive() throws Exception {
        // GIVEN
        File outDir = TEMP_FOLDER.newFolder();
        Path path = new Path(outDir.toURI());
        int rowsCount = 2;
        List<RowData> testRows = DeltaSinkTestUtils.getTestRowData(rowsCount);
        DeltaWriter<RowData> writer = createNewWriter(path);
        int elementsCount = isPartitioned ? 2 : 1;

        // WHEN
        writeData(writer, testRows);
        writer.prepareCommit(false);
        writer.snapshotState();
        assertEquals(elementsCount, writer.getActiveBuckets().size());

        // No more records and another call to prepareCommit will make the bucket inactive
        writer.prepareCommit(false);

        // THEN
        assertEquals(0, writer.getActiveBuckets().size());
    }

    /**
     * Just following {@code org.apache.flink.connector.file.sink.writer
     * .FileWriterTest#testContextPassingNormalExecution()}
     * here.
     */
    @Test
    public void testContextPassingNormalExecution() throws Exception {
        testCorrectTimestampPassingInContext(1L, 2L, 3L);
    }

    /**
     * Just following {@code org.apache.flink.connector.file.sink.writer
     * .FileWriterTest#testContextPassingNullTimestamp()}
     * here.
     */
    @Test
    public void testContextPassingNullTimestamp() throws Exception {
        testCorrectTimestampPassingInContext(null, 4L, 5L);
    }

    private void testCorrectTimestampPassingInContext(
        // GIVEN
        Long timestamp, long watermark, long processingTime) throws Exception {
        final File outDir = TEMP_FOLDER.newFolder();
        final Path path = new Path(outDir.toURI());
        List<RowData> testRows = DeltaSinkTestUtils.getTestRowData(1);

        // Create the processing timer service starts from 10.
        ManuallyTriggeredProcessingTimeService processingTimeService =
            new ManuallyTriggeredProcessingTimeService();
        processingTimeService.advanceTo(processingTime);

        DeltaWriter<RowData> writer = createNewWriter(path);
        writer.initializeState(Collections.emptyList());

        // WHEN
        writer.write(testRows.get(0), new ContextImpl(watermark, timestamp));

        // THEN
        // no error - test passed
    }

    @Test
    public void testMetrics() throws Exception {
        // GIVEN
        File outDir = TEMP_FOLDER.newFolder();
        Path path = new Path(outDir.toURI());
        int rowsCount = 5;
        List<RowData> testRows = DeltaSinkTestUtils.getTestRowData(rowsCount);
        DeltaWriter<RowData> writer = createNewWriter(path);

        // WHEN
        writeData(writer, testRows);

        // THEN
        assertEquals(rowsCount, testCounters.get(DeltaWriter.RECORDS_OUT_METRIC_NAME).getCount());
        // no data flushed to the actual files yet
        assertEquals(0, testCounters.get(DeltaWriterBucket.RECORDS_WRITTEN_METRIC_NAME).getCount());
        assertEquals(0, testCounters.get(DeltaWriterBucket.BYTES_WRITTEN_METRIC_NAME).getCount());

        // AND WHEN
        writer.prepareCommit(true);

        // THEN
        // records flushed to the files on the file system
        assertEquals(
            rowsCount,
            testCounters.get(DeltaWriterBucket.RECORDS_WRITTEN_METRIC_NAME).getCount());
        assertTrue(testCounters.get(DeltaWriterBucket.BYTES_WRITTEN_METRIC_NAME).getCount() > 0);
    }

    ///////////////////////////////////////////////////////////////////////////
    // Utility Methods
    ///////////////////////////////////////////////////////////////////////////

    /**
     * Simple partition assigner that assigns data to only two different partitions based on the
     * information whether the test record count is even or uneven.
     *
     * @return test instance of {@link DeltaBucketAssigner}
     */
    public DeltaBucketAssigner<RowData> getTestPartitionAssigner() {
        DeltaPartitionComputer<RowData> partitionComputer =
            (element, context) -> new LinkedHashMap<String, String>() {{
                    put("col1", Integer.toString(testRecordsCount % 2));
                }};
        return new DeltaBucketAssigner<>(partitionComputer);
    }

    private void writeData(DeltaWriter<RowData> writer,
                           List<RowData> rows) {
        rows.forEach(rowData -> {
            try {
                writer.write(rowData, new ContextImpl());
                testRecordsCount += 1;
            } catch (IOException e) {
                throw new RuntimeException("Writing failed");
            }
        });
    }

    private DeltaWriter<RowData> createNewWriter(Path basePath) throws IOException {
        MetricGroup metricGroup =
            new UnregisteredMetricsGroup() {
                @Override
                public Counter counter(String name) {
                    if (!testCounters.containsKey(name)){
                        testCounters.put(name, super.counter(name));
                    }
                    return testCounters.get(name);
                }
            };

        BucketAssigner<RowData, String> bucketAssigner =
            isPartitioned ? getTestPartitionAssigner() : new BasePathBucketAssigner<>();
        return new DeltaWriter<>(
            basePath,
            bucketAssigner,
            DeltaSinkTestUtils.createBucketWriter(basePath),
            DeltaSinkTestUtils.ON_CHECKPOINT_ROLLING_POLICY,
            OutputFileConfig.builder().withPartSuffix(".snappy.parquet").build(),
            new ManuallyTriggeredProcessingTimeService(),
            metricGroup,
            10,
            APP_ID,
            1
        );
    }

    /**
     * This is a simplified test method for only restoring the buckets and it will
     * not restore writer's nextCheckpointId correctly as in case of
     * {@link io.delta.flink.sink.DeltaSink#createWriter}
     */
    private DeltaWriter<RowData> restoreWriter(
        Path basePath,
        List<DeltaWriterBucketState> states) throws IOException {

        DeltaWriter<RowData> writer = createNewWriter(basePath);
        writer.initializeState(states);
        return writer;
    }

    /**
     * Borrowed from {@code org.apache.flink.connector.file.sink.writer.FileWriterTest}
     */
    private static class ContextImpl implements SinkWriter.Context {
        private final long watermark;
        private final Long timestamp;

        ContextImpl() {
            this(0, 0L);
        }

        private ContextImpl(long watermark, Long timestamp) {
            this.watermark = watermark;
            this.timestamp = timestamp;
        }

        @Override
        public long currentWatermark() {
            return watermark;
        }

        @Override
        public Long timestamp() {
            return timestamp;
        }
    }

    /**
     * Borrowed from {@code org.apache.flink.connector.file.sink.writer.FileWriterTest}
     */
    private static class ManuallyTriggeredProcessingTimeService
        implements Sink.ProcessingTimeService {

        private long now;

        private final Queue<Tuple2<Long, ProcessingTimeCallback>> timers =
            new PriorityQueue<>(Comparator.comparingLong(o -> o.f0));

        @Override
        public long getCurrentProcessingTime() {
            return now;
        }

        @Override
        public void registerProcessingTimer(
            long time, ProcessingTimeCallback processingTimeCallback) {
            if (time <= now) {
                try {
                    processingTimeCallback.onProcessingTime(now);
                } catch (IOException | InterruptedException e) {
                    ExceptionUtils.rethrow(e);
                }
            } else {
                timers.add(new Tuple2<>(time, processingTimeCallback));
            }
        }

        public void advanceTo(long time) throws IOException, InterruptedException {
            if (time > now) {
                now = time;

                Tuple2<Long, ProcessingTimeCallback> timer;
                while ((timer = timers.peek()) != null && timer.f0 <= now) {
                    timer.f1.onProcessingTime(now);
                    timers.poll();
                }
            }
        }
    }
}
