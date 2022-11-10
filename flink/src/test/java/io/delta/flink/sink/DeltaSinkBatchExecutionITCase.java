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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import io.delta.flink.sink.internal.DeltaSinkInternal;
import io.delta.flink.sink.internal.committables.DeltaCommittable;
import io.delta.flink.sink.internal.committables.DeltaGlobalCommittable;
import io.delta.flink.sink.internal.committer.DeltaGlobalCommitter;
import io.delta.flink.sink.internal.writer.DeltaWriterBucketState;
import io.delta.flink.sink.utils.DeltaSinkTestUtils;
import io.delta.flink.utils.DeltaTestUtils;
import io.delta.flink.utils.TestParquetReader;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.connector.sink.Committer;
import org.apache.flink.api.connector.sink.GlobalCommitter;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.table.data.RowData;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.rules.TemporaryFolder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.delta.standalone.DeltaLog;
import io.delta.standalone.actions.AddFile;
import io.delta.standalone.actions.CommitInfo;

/**
 * Tests the functionality of the {@link DeltaSink} in BATCH mode.
 */
public class DeltaSinkBatchExecutionITCase {

    private static final int NUM_SINKS = 3;

    private static final int NUM_RECORDS = 10000;

    public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

    private String deltaTablePath;

    @BeforeAll
    public static void beforeAll() throws IOException {
        TEMPORARY_FOLDER.create();
    }

    @AfterAll
    public static void afterAll() {
        TEMPORARY_FOLDER.delete();
    }

    @BeforeEach
    public void setup() {
        try {
            deltaTablePath = TEMPORARY_FOLDER.newFolder().getAbsolutePath();
        } catch (IOException e) {
            throw new RuntimeException("Weren't able to setup the test dependencies", e);
        }
    }

    /**
     * Arguments for parametrized Delta Sink test.
     * Parameters are:
     * <ul>
     *     <li>isPartitioned</li>
     *     <li>triggerFailover</li>
     * </ul>
     */
    private static Stream<Arguments> deltaSinkArguments() {
        return Stream.of(
            Arguments.of(false, false),
            Arguments.of(true, false),
            Arguments.of(false, true),
            Arguments.of(true, true)
        );
    }

    @ParameterizedTest(name = "isPartitioned = {0}, triggerFailover = {1}")
    @MethodSource("deltaSinkArguments")
    public void testFileSink(boolean isPartitioned, boolean triggerFailover) throws Exception {

        initSourceFolder(isPartitioned, deltaTablePath);

        // GIVEN
        DeltaLog deltaLog = DeltaLog.forTable(DeltaTestUtils.getHadoopConf(), deltaTablePath);
        List<AddFile> initialDeltaFiles = deltaLog.snapshot().getAllFiles();
        int initialTableRecordsCount = TestParquetReader.readAndValidateAllTableRecords(deltaLog);
        long initialVersion = deltaLog.snapshot().getVersion();

        if (isPartitioned) {
            assertEquals(1, initialDeltaFiles.size());
        } else {
            assertEquals(2, initialDeltaFiles.size());
        }

        JobGraph jobGraph = createJobGraph(deltaTablePath, isPartitioned, triggerFailover);

        // WHEN
        try (MiniCluster miniCluster = DeltaSinkTestUtils.getMiniCluster()) {
            miniCluster.start();
            miniCluster.executeJobBlocking(jobGraph);
        }

        // THEN
        int writtenRecordsCount =
            DeltaSinkTestUtils.validateIfPathContainsParquetFilesWithData(deltaTablePath);
        assertEquals(NUM_RECORDS, writtenRecordsCount - initialTableRecordsCount);

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

        assertEquals(finalDeltaFiles.size() - initialDeltaFiles.size(), totalAddedFiles);
        assertEquals(NUM_RECORDS, totalRowsAdded);
    }

    private String initSourceFolder(boolean isPartitioned, String deltaTablePath) {
        try {
            if (isPartitioned) {
                DeltaTestUtils.initTestForPartitionedTable(deltaTablePath);
            } else {
                DeltaTestUtils.initTestForNonPartitionedTable(deltaTablePath);
            }

            return deltaTablePath;
        } catch (IOException e) {
            throw new RuntimeException("Weren't able to setup the test dependencies", e);
        }
    }

    protected JobGraph createJobGraph(
            String deltaTablePath,
            boolean isPartitioned,
            boolean triggerFailover) {

        StreamExecutionEnvironment env = getTestStreamEnv(triggerFailover);

        Sink<RowData, DeltaCommittable, DeltaWriterBucketState, DeltaGlobalCommittable> deltaSink =
            DeltaSinkTestUtils.createDeltaSink(deltaTablePath, isPartitioned);

        if (triggerFailover) {
            deltaSink = new FailoverDeltaSink<>((DeltaSinkInternal<RowData>) deltaSink);
        }

        env.fromCollection(DeltaSinkTestUtils.getTestRowData(NUM_RECORDS))
            .setParallelism(1)
            .sinkTo(deltaSink)
            .setParallelism(NUM_SINKS);

        StreamGraph streamGraph = env.getStreamGraph();
        return streamGraph.getJobGraph();
    }

    private StreamExecutionEnvironment getTestStreamEnv(boolean triggerFailover) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Configuration config = new Configuration();
        config.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.BATCH);
        env.configure(config, getClass().getClassLoader());

        if (triggerFailover) {
            env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, Time.milliseconds(100)));
        } else {
            env.setRestartStrategy(RestartStrategies.noRestart());
        }

        return env;
    }

    private static class FailoverDeltaSink<IN>
        implements Sink<IN, DeltaCommittable, DeltaWriterBucketState, DeltaGlobalCommittable> {

        private final DeltaSinkInternal<IN> decoratedSink;

        private FailoverDeltaSink(DeltaSinkInternal<IN> decoratedSink) {
            this.decoratedSink = decoratedSink;
        }

        @Override
        public SinkWriter<IN, DeltaCommittable, DeltaWriterBucketState> createWriter(
            InitContext initContext, List<DeltaWriterBucketState> list) throws IOException {
            return this.decoratedSink.createWriter(initContext, list);
        }

        @Override
        public Optional<SimpleVersionedSerializer<DeltaWriterBucketState>>
            getWriterStateSerializer() {

            return this.decoratedSink.getWriterStateSerializer();
        }

        @Override
        public Optional<Committer<DeltaCommittable>> createCommitter() throws IOException {
            return this.decoratedSink.createCommitter();
        }

        @Override
        public Optional<GlobalCommitter<DeltaCommittable, DeltaGlobalCommittable>>
            createGlobalCommitter() throws IOException {

            return Optional.of(new FailoverDeltaGlobalCommitter(
                (DeltaGlobalCommitter) this.decoratedSink.createGlobalCommitter().get()));
        }

        @Override
        public Optional<SimpleVersionedSerializer<DeltaCommittable>> getCommittableSerializer() {
            return this.decoratedSink.getCommittableSerializer();
        }

        @Override
        public Optional<SimpleVersionedSerializer<DeltaGlobalCommittable>>
            getGlobalCommittableSerializer() {
            return this.decoratedSink.getGlobalCommittableSerializer();
        }
    }

    private static class FailoverDeltaGlobalCommitter
        implements GlobalCommitter<DeltaCommittable, DeltaGlobalCommittable> {

        private static boolean THROW_EXCEPTION = true;

        private final DeltaGlobalCommitter decoratedGlobalCommitter;

        private FailoverDeltaGlobalCommitter(DeltaGlobalCommitter decoratedGlobalCommitter) {
            this.decoratedGlobalCommitter = decoratedGlobalCommitter;
        }

        @Override
        public List<DeltaGlobalCommittable> filterRecoveredCommittables(
            List<DeltaGlobalCommittable> list) throws IOException {
            return this.decoratedGlobalCommitter.filterRecoveredCommittables(list);
        }

        @Override
        public DeltaGlobalCommittable combine(List<DeltaCommittable> list) throws IOException {
            return this.decoratedGlobalCommitter.combine(list);
        }

        @Override
        public List<DeltaGlobalCommittable> commit(List<DeltaGlobalCommittable> list)
            throws IOException, InterruptedException {
            try {
                if (THROW_EXCEPTION) {
                    throw new RuntimeException("Designed Global Committer Exception.");
                }
            } finally {
                THROW_EXCEPTION = false;
            }
            return this.decoratedGlobalCommitter.commit(list);
        }

        @Override
        public void endOfInput() throws IOException, InterruptedException {
            this.decoratedGlobalCommitter.endOfInput();
        }

        @Override
        public void close() throws Exception {
            this.decoratedGlobalCommitter.close();
        }
    }
}
