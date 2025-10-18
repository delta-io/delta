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
import org.apache.flink.api.connector.sink.GlobalCommitter;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.table.data.RowData;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.rules.TemporaryFolder;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.delta.standalone.DeltaLog;
import io.delta.standalone.actions.AddFile;
import io.delta.standalone.actions.CommitInfo;

/**
 * Tests the functionality of the {@link DeltaSink} in BATCH mode.
 */
public class DeltaSinkBatchExecutionITCase extends DeltaSinkExecutionITCaseBase {

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
     * This test executes simple source -> sink job with Flink cluster failures caused by
     * an Exception thrown from {@link GlobalCommitter}.
     * Depending on value of exceptionMode parameter, exception will be thrown before or after
     * committing data to the Delta log.
     * @param exceptionMode whether to throw an exception before or after Delta log commit.
     */
    @ResourceLock("BatchFailoverDeltaGlobalCommitter")
    @ParameterizedTest(name = "isPartitioned = {0}, exceptionMode = {1}")
    @CsvSource({
        "false, NONE",
        "true, NONE",
        "false, BEFORE_COMMIT",
        "false, AFTER_COMMIT",
        "true, BEFORE_COMMIT",
        "true, AFTER_COMMIT"
    })
    public void testFileSink(boolean isPartitioned, GlobalCommitterExceptionMode exceptionMode)
            throws Exception {

        FailoverDeltaGlobalCommitter.reset();
        assertThat(
            "Test setup issue. Static FailoverDeltaGlobalCommitter.throwException field"
                + " must be reset to true before test.",
            FailoverDeltaGlobalCommitter.throwException,
            equalTo(true)
        );

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

        JobGraph jobGraph = createJobGraph(deltaTablePath, isPartitioned, exceptionMode);

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

        if (!GlobalCommitterExceptionMode.NONE.equals(exceptionMode)) {
            assertThat(
                "It seems that Flink job did not throw an exception even though"
                    + " used exceptionMode indicates it should."
                    + " Used exception mode was " + exceptionMode,
                FailoverDeltaGlobalCommitter.throwException,
                equalTo(false)
            );
        } else {
            assertThat(
                "It seems that Flink job throw an exception even though"
                    + " used exceptionMode indicates it should not."
                    + " Used exception mode was " + exceptionMode,
                FailoverDeltaGlobalCommitter.throwException,
                equalTo(true)
            );
        }
    }

    protected JobGraph createJobGraph(
            String deltaTablePath,
            boolean isPartitioned,
            GlobalCommitterExceptionMode exceptionMode) {

        boolean triggerFailover = !GlobalCommitterExceptionMode.NONE.equals(exceptionMode);
        StreamExecutionEnvironment env = getTestStreamEnv(triggerFailover);

        Sink<RowData, DeltaCommittable, DeltaWriterBucketState, DeltaGlobalCommittable> deltaSink =
            DeltaSinkTestUtils.createDeltaSink(deltaTablePath, isPartitioned);

        deltaSink = new FailoverDeltaSink((DeltaSinkInternal<RowData>) deltaSink, exceptionMode);

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

    /**
     * Wrapper for original {@link DeltaSinkInternal} that can be used for IT testing batch jobs.
     * This implementation will use {@link FailoverDeltaGlobalCommitter} as GlobalCommitter.
     */
    private static class FailoverDeltaSink extends FailoverDeltaSinkBase<RowData> {

        private final GlobalCommitterExceptionMode exceptionMode;

        private FailoverDeltaSink(
                DeltaSinkInternal<RowData> deltaSink,
                GlobalCommitterExceptionMode exceptionMode) {

            super(deltaSink);
            this.exceptionMode = exceptionMode;
        }

        @Override
        public Optional<GlobalCommitter<DeltaCommittable, DeltaGlobalCommittable>>
            createGlobalCommitter() throws IOException {

            return Optional.of(
                new FailoverDeltaGlobalCommitter(
                    (DeltaGlobalCommitter) this.decoratedSink.createGlobalCommitter().get(),
                    this.exceptionMode)
            );
        }
    }

    /**
     * Wrapper for original {@link DeltaGlobalCommitter} that can be used for IT testing Batch jobs.
     * This implementation will throw an exception once per Batch job, before or after committing
     * data to the delta log.
     * <p>
     * This implementation uses a static field as a flag, so it cannot be used in multithreading
     * test setup where there will be multiple tests using this class running at the same time.
     * This would cause unpredictable results.
     */
    private static class FailoverDeltaGlobalCommitter extends FailoverDeltaGlobalCommitterBase {

        /**
         * JVM global static flag that indicates where exception should be thrown from
         * FailoverDeltaGlobalCommitter
         */
        public static boolean throwException = true;

        private final GlobalCommitterExceptionMode exceptionMode;

        private FailoverDeltaGlobalCommitter(
                DeltaGlobalCommitter decoratedGlobalCommitter,
                GlobalCommitterExceptionMode exceptionMode) {

            super(decoratedGlobalCommitter);
            this.exceptionMode = exceptionMode;
        }

        @Override
        public List<DeltaGlobalCommittable> commit(List<DeltaGlobalCommittable> list)
            throws IOException, InterruptedException {

            switch (exceptionMode) {
                case BEFORE_COMMIT:
                    if (throwException) {
                        throwException = false;
                        throw new RuntimeException("Designed Exception from Global Committer BEFORE"
                            + " Delta log commit.");
                    }
                    return this.decoratedGlobalCommitter.commit(list);
                case AFTER_COMMIT:
                    List<DeltaGlobalCommittable> commit =
                        this.decoratedGlobalCommitter.commit(list);
                    if (throwException) {
                        throwException = false;
                        throw new RuntimeException("Designed Exception from Global Committer AFTER"
                            + " Delta log commit.");
                    }
                    return commit;
                case NONE:
                    return this.decoratedGlobalCommitter.commit(list);
                default:
                    throw new RuntimeException("Unexpected Exception mode");
            }
        }

        /**
         * Reset static fields since those are initialized only once per entire JVM.
         */
        public static void reset() {
            throwException = true;
        }
    }
}
