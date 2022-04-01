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

package io.delta.flink.sink;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.stream.LongStream;

import io.delta.flink.sink.utils.DeltaSinkTestUtils;
import io.delta.flink.sink.utils.TestParquetReader;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.connector.file.sink.StreamingExecutionFileSinkITCase;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.Row;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import io.delta.standalone.DeltaLog;
import io.delta.standalone.actions.AddFile;
import io.delta.standalone.actions.CommitInfo;

/**
 * Tests the functionality of the {@link DeltaSink} in STREAMING mode.
 */
@RunWith(Parameterized.class)
public class DeltaSinkStreamingExecutionITCase extends StreamingExecutionFileSinkITCase {

    private static final Map<String, CountDownLatch> LATCH_MAP = new ConcurrentHashMap<>();

    @Parameterized.Parameters(
        name = "triggerFailover = {0}, isPartitioned = {1}"
    )
    public static Collection<Object[]> params() {
        return Arrays.asList(
            new Object[]{false, false},
            new Object[]{false, true},
            new Object[]{true, false},
            new Object[]{true, true}
        );
    }

    @Parameterized.Parameter(1)
    public Boolean isPartitioned;

    private String latchId;
    private String deltaTablePath;

    @Before
    public void setup() {
        this.latchId = UUID.randomUUID().toString();
        LATCH_MAP.put(latchId, new CountDownLatch(NUM_SOURCES * 2));
        try {
            deltaTablePath = TEMPORARY_FOLDER.newFolder().getAbsolutePath();
            if (isPartitioned) {
                DeltaSinkTestUtils.initTestForPartitionedTable(deltaTablePath);
            } else {
                DeltaSinkTestUtils.initTestForNonPartitionedTable(deltaTablePath);
            }
        } catch (IOException e) {
            throw new RuntimeException("Weren't able to setup the test dependencies", e);
        }
    }

    @After
    public void teardown() {
        LATCH_MAP.remove(latchId);
    }

    @Override
    @Test
    public void testFileSink() throws Exception {
        runDeltaSinkTest();
    }

    public void runDeltaSinkTest() throws Exception {
        // GIVEN
        DeltaLog deltaLog = DeltaLog.forTable(DeltaSinkTestUtils.getHadoopConf(), deltaTablePath);
        List<AddFile> initialDeltaFiles = deltaLog.snapshot().getAllFiles();
        long initialVersion = deltaLog.snapshot().getVersion();
        int initialTableRecordsCount = TestParquetReader.readAndValidateAllTableRecords(deltaLog);
        assertEquals(initialDeltaFiles.size(), 2);

        JobGraph jobGraph = createJobGraph(deltaTablePath);

        // WHEN
        try (MiniCluster miniCluster = DeltaSinkTestUtils.getMiniCluster()) {
            miniCluster.start();
            miniCluster.executeJobBlocking(jobGraph);
        }

        // THEN
        int writtenRecordsCount =
            DeltaSinkTestUtils.validateIfPathContainsParquetFilesWithData(deltaTablePath);
        assertEquals(NUM_RECORDS * NUM_SOURCES, writtenRecordsCount - initialTableRecordsCount);

        List<AddFile> finalDeltaFiles = deltaLog.update().getAllFiles();
        assertTrue(finalDeltaFiles.size() > initialDeltaFiles.size());
        Iterator<Long> it = LongStream.range(
            initialVersion + 1, deltaLog.snapshot().getVersion() + 1).iterator();
        long totalRowsAdded = 0;
        long totalAddedFiles = 0;
        while (it.hasNext()) {
            long currentVersion = it.next();
            CommitInfo currentCommitInfo = deltaLog.getCommitInfoAt(currentVersion);
            Optional<Map<String, String>> operationMetrics =
                currentCommitInfo.getOperationMetrics();
            assertTrue(operationMetrics.isPresent());
            totalRowsAdded += Long.parseLong(operationMetrics.get().get("numOutputRows"));
            totalAddedFiles += Long.parseLong(operationMetrics.get().get("numAddedFiles"));

            assertTrue(Integer.parseInt(operationMetrics.get().get("numOutputBytes")) > 0);

        }
        int finalTableRecordsCount = TestParquetReader.readAndValidateAllTableRecords(deltaLog);

        assertEquals(finalDeltaFiles.size() - initialDeltaFiles.size(), totalAddedFiles);
        assertEquals((NUM_RECORDS * NUM_SOURCES), totalRowsAdded);
        assertEquals(finalTableRecordsCount - initialTableRecordsCount, totalRowsAdded);
    }

    /**
     * Creating the testing job graph in streaming mode. The graph created is [Source] -> [Delta
     * Sink]. The source would trigger failover if required.
     */
    @Override
    protected JobGraph createJobGraph(String path) {
        StreamExecutionEnvironment env = getTestStreamEnv();

        env.addSource(new DeltaStreamingExecutionTestSource(latchId, NUM_RECORDS, triggerFailover))
            .setParallelism(NUM_SOURCES)
            .sinkTo(DeltaSinkTestUtils.createDeltaSink(deltaTablePath, isPartitioned))
            .setParallelism(NUM_SINKS);

        StreamGraph streamGraph = env.getStreamGraph();
        return streamGraph.getJobGraph();
    }

    private StreamExecutionEnvironment getTestStreamEnv() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Configuration config = new Configuration();
        config.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.STREAMING);
        env.configure(config, getClass().getClassLoader());
        env.enableCheckpointing(10, CheckpointingMode.EXACTLY_ONCE);

        if (triggerFailover) {
            env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, Time.milliseconds(100)));
        } else {
            env.setRestartStrategy(RestartStrategies.noRestart());
        }

        return env;
    }

    ///////////////////////////////////////////////////////////////////////////
    // Streaming mode user functions
    ///////////////////////////////////////////////////////////////////////////

    /**
     * Implementation idea and some functions is borrowed from 'StreamingExecutionTestSource' in
     * {@link StreamingExecutionFileSinkITCase}
     */
    private static class DeltaStreamingExecutionTestSource
        extends RichParallelSourceFunction<RowData>
        implements CheckpointListener, CheckpointedFunction {

        private final String latchId;

        private final int numberOfRecords;

        /**
         * Whether the test is executing in a scenario that induces a failover. This doesn't mean
         * that this source induces the failover.
         */
        private final boolean isFailoverScenario;

        private ListState<Integer> nextValueState;

        private int nextValue;

        private volatile boolean isCanceled;

        private volatile boolean snapshottedAfterAllRecordsOutput;

        private volatile boolean isWaitingCheckpointComplete;

        private volatile boolean hasCompletedCheckpoint;

        private volatile boolean isLastCheckpointInterval;

        DeltaStreamingExecutionTestSource(
            String latchId, int numberOfRecords, boolean isFailoverScenario) {
            this.latchId = latchId;
            this.numberOfRecords = numberOfRecords;
            this.isFailoverScenario = isFailoverScenario;
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            nextValueState =
                context.getOperatorStateStore()
                    .getListState(new ListStateDescriptor<>("nextValue", Integer.class));

            if (nextValueState.get() != null && nextValueState.get().iterator().hasNext()) {
                nextValue = nextValueState.get().iterator().next();
            }
        }

        @Override
        public void run(SourceContext<RowData> ctx) throws Exception {
            if (isFailoverScenario && getRuntimeContext().getAttemptNumber() == 0) {
                // In the first execution, we first send a part of record...
                sendRecordsUntil((int) (numberOfRecords * FAILOVER_RATIO * 0.5), ctx);

                // Wait till the first part of data is committed.
                while (!hasCompletedCheckpoint) {
                    Thread.sleep(50);
                }

                // Then we write the second part of data...
                sendRecordsUntil((int) (numberOfRecords * FAILOVER_RATIO), ctx);

                // And then trigger the failover.
                if (getRuntimeContext().getIndexOfThisSubtask() == 0) {
                    throw new RuntimeException("Designated Exception");
                } else {
                    while (true) {
                        Thread.sleep(50);
                    }
                }
            } else {
                // If we are not going to trigger failover or we have already triggered failover,
                // run until finished.
                sendRecordsUntil(numberOfRecords, ctx);

                isWaitingCheckpointComplete = true;
                CountDownLatch latch = LATCH_MAP.get(latchId);
                latch.await();
            }
        }

        private void sendRecordsUntil(int targetNumber, SourceContext<RowData> ctx) {
            while (!isCanceled && nextValue < targetNumber) {
                synchronized (ctx.getCheckpointLock()) {
                    RowData row = DeltaSinkTestUtils.CONVERTER.toInternal(
                        Row.of(
                            String.valueOf(nextValue),
                            String.valueOf((nextValue + nextValue)),
                            nextValue)
                    );
                    ctx.collect(row);
                    nextValue++;
                }
            }
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            nextValueState.update(Collections.singletonList(nextValue));
            if (isWaitingCheckpointComplete) {
                snapshottedAfterAllRecordsOutput = true;
            }
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) {
            if (isWaitingCheckpointComplete && snapshottedAfterAllRecordsOutput
                && isLastCheckpointInterval) {
                CountDownLatch latch = LATCH_MAP.get(latchId);
                latch.countDown();
            }

            if (isWaitingCheckpointComplete && snapshottedAfterAllRecordsOutput
                && !isLastCheckpointInterval) {
                // we set the job to run for one additional checkpoint interval to avoid any
                // premature job termination and race conditions
                isLastCheckpointInterval = true;
            }

            hasCompletedCheckpoint = true;
        }

        @Override
        public void cancel() {
            isCanceled = true;
        }
    }
}
