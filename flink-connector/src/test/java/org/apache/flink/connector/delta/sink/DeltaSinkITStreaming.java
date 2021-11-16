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

package org.apache.flink.connector.delta.sink;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.connector.file.sink.StreamingExecutionFileSinkITCase;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.table.data.RowData;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class DeltaSinkITStreaming extends StreamingExecutionFileSinkITCase {

    private static final Map<String, CountDownLatch> LATCH_MAP = new ConcurrentHashMap<>();

    @Parameterized.Parameters(
            name = "triggerFailover = {0}"
    )
    public static Collection<Object[]> params() {
        return Arrays.asList(
                new Object[]{false},
                new Object[]{true}
        );
    }

    private String latchId;
    String deltaTablePath;

    @Before
    public void setup() {
        this.latchId = UUID.randomUUID().toString();
        LATCH_MAP.put(latchId, new CountDownLatch(NUM_SOURCES * 2));
        try {
            deltaTablePath = TEMPORARY_FOLDER.newFolder().getAbsolutePath();
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
        JobGraph jobGraph = createJobGraph(deltaTablePath);

        // WHEN
        try (MiniCluster miniCluster = getMiniCluster()) {
            miniCluster.start();
            miniCluster.executeJobBlocking(jobGraph);
        }

        // THEN
    }

    @Override
    protected JobGraph createJobGraph(String path) {
        StreamExecutionEnvironment env = getTestStreamEnv();

        env.addSource(new DeltaStreamingExecutionTestSource(latchId, NUM_RECORDS, triggerFailover))
                .setParallelism(NUM_SOURCES)
                .sinkTo(createDeltaSink())
                .setParallelism(NUM_SINKS);

        StreamGraph streamGraph = env.getStreamGraph();
        return streamGraph.getJobGraph();
    }

    private DeltaSink<RowData> createDeltaSink() {
        return DeltaSink
                .forDeltaFormat(
                        new Path(deltaTablePath),
                        new org.apache.hadoop.conf.Configuration())
                .build();
    }

    private MiniCluster getMiniCluster() {
        final Configuration config = new Configuration();
        config.setString(RestOptions.BIND_PORT, "18081-19000");
        final MiniClusterConfiguration cfg =
                new MiniClusterConfiguration.Builder()
                        .setNumTaskManagers(1)
                        .setNumSlotsPerTaskManager(4)
                        .setConfiguration(config)
                        .build();
        return new MiniCluster(cfg);
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

                // Wait the last checkpoint to commit all the pending records.
                isWaitingCheckpointComplete = true;
                CountDownLatch latch = LATCH_MAP.get(latchId);
                latch.await();
            }
        }

        private void sendRecordsUntil(int targetNumber, SourceContext<RowData> ctx) {
            while (!isCanceled && nextValue < targetNumber) {
                synchronized (ctx.getCheckpointLock()) {
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
        public void notifyCheckpointComplete(long checkpointId) throws Exception {
            if (isWaitingCheckpointComplete && snapshottedAfterAllRecordsOutput) {
                CountDownLatch latch = LATCH_MAP.get(latchId);
                latch.countDown();
            }

            hasCompletedCheckpoint = true;
        }

        @Override
        public void cancel() {
            isCanceled = true;
        }
    }
}
