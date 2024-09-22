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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import io.delta.flink.sink.internal.DeltaSinkInternal;
import io.delta.flink.sink.internal.committables.DeltaCommittable;
import io.delta.flink.sink.internal.committables.DeltaGlobalCommittable;
import io.delta.flink.sink.internal.committer.DeltaGlobalCommitter;
import io.delta.flink.sink.internal.writer.DeltaWriterBucketState;
import io.delta.flink.sink.utils.DeltaSinkTestUtils;
import io.delta.flink.utils.CheckpointCountingSource;
import io.delta.flink.utils.DeltaTableAsserts;
import io.delta.flink.utils.DeltaTestUtils;
import io.delta.flink.utils.TestParquetReader;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.connector.sink.GlobalCommitter;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.table.data.RowData;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static io.delta.flink.utils.DeltaTestUtils.buildCluster;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.delta.standalone.DeltaLog;
import io.delta.standalone.actions.AddFile;
import io.delta.standalone.actions.CommitInfo;

/**
 * Tests the functionality of the {@link DeltaSink} in STREAMING mode.
 */
public class DeltaSinkStreamingExecutionITCase extends DeltaSinkExecutionITCaseBase {

    private static final Logger LOG =
        LoggerFactory.getLogger(DeltaSinkStreamingExecutionITCase.class);

    private static final int NUM_SOURCES = 4;

    private static final int NUM_SINKS = 3;

    private static final int NUM_RECORDS = 10000;

    private static final double FAILOVER_RATIO = 0.4;

    public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();

    private static final Map<String, CountDownLatch> LATCH_MAP = new ConcurrentHashMap<>();

    private final MiniClusterWithClientResource miniClusterResource = buildCluster(10);

    private String latchId;

    private String deltaTablePath;

    private Path savepointPath;

    @BeforeAll
    public static void beforeAll() throws IOException {
        TMP_FOLDER.create();
    }

    @AfterAll
    public static void afterAll() {
        TMP_FOLDER.delete();
    }

    @BeforeEach
    public void setup() throws IOException {
        try {
            miniClusterResource.before();
        } catch (Exception e) {
            throw new RuntimeException("Weren't able to setup the test dependencies", e);
        }

        deltaTablePath = TMP_FOLDER.newFolder().getAbsolutePath();
        this.latchId = UUID.randomUUID().toString();
        LATCH_MAP.put(latchId, new CountDownLatch(NUM_SOURCES * 2));
        savepointPath = TMP_FOLDER.newFolder().toPath();
    }

    @AfterEach
    public void teardown() {
        miniClusterResource.after();
        LATCH_MAP.remove(latchId);
    }

    /**
     * Arguments for parametrized Delta Sink test.
     * Parameters are:
     * <ul>
     *     <li>isPartitioned</li>
     *     <li>triggerFailover</li>
     * </ul>
     */
    @ParameterizedTest(name = "isPartitioned = {0}, triggerFailover = {1}")
    @CsvSource({
        "false, false",
        "true, false",
        "false, true",
        "true, true"
    })
    public void testDeltaSink(boolean isPartitioned, boolean triggerFailover) throws Exception {

        initSourceFolder(isPartitioned, deltaTablePath);

        JobGraph jobGraph = createJobGraphWithFailoverSource(
            deltaTablePath,
            triggerFailover,
            isPartitioned
        );

        runDeltaSinkTest(deltaTablePath, jobGraph, NUM_RECORDS);
    }

    /**
     * This test executes simple source -> sink job with multiple Flink cluster failures caused by
     * an Exception thrown from {@link GlobalCommitter}. Depending on value of exceptionMode
     * parameter, exception will be thrown before or after committing data to the Delta log.
     *
     * @param exceptionMode whether to throw an exception before or after Delta log commit.
     */
    @Disabled(
        "This test is flaky, for some runs it fails with unexpected numbers of files. "
            + "Investigation of if this is a connector or test issue is ongoing")
    @ResourceLock("StreamingFailoverDeltaGlobalCommitter")
    @ParameterizedTest(name = "isPartitioned = {0}, exceptionMode = {1}")
    @CsvSource({
        "false, BEFORE_COMMIT",
        "false, AFTER_COMMIT",
        "true, BEFORE_COMMIT",
        "true, AFTER_COMMIT"
    })
    public void testFileSinkWithGlobalCommitterFailover(
            boolean isPartitioned,
            GlobalCommitterExceptionMode exceptionMode) throws Exception {

        // GIVEN
        FailoverDeltaGlobalCommitter.reset();

        assertThat(
            "Test setup issue. Static FailoverDeltaGlobalCommitter.checkpointCounter field"
                + " must be reset to 0 before test.",
            FailoverDeltaGlobalCommitter.checkpointCounter,
            equalTo(0)
        );

        assertThat(
            "Test setup issue. Static FailoverDeltaGlobalCommitter.checkpointCounter"
                + " designExceptionCounter must be reset to 0 before test.",
            FailoverDeltaGlobalCommitter.designExceptionCounter,
            equalTo(0)
        );

        initSourceFolder(isPartitioned, deltaTablePath);
        DeltaTestUtils.resetDeltaLogLastModifyTimestamp(deltaTablePath);

        Set<Integer> checkpointsToFailOn = new HashSet<>(Arrays.asList(5, 10, 11, 14));

        int recordsPerCheckpoint = 100;
        int totalNumberOfCheckpoints = 15;
        JobGraph jobGraph = createJobGraphWithFailoverGlobalCommitter(
            deltaTablePath,
            exceptionMode,
            recordsPerCheckpoint,
            totalNumberOfCheckpoints,
            checkpointsToFailOn,
            isPartitioned
        );

        // WHEN/THEN
        runDeltaSinkTest(deltaTablePath, jobGraph, recordsPerCheckpoint * totalNumberOfCheckpoints);

        assertThat(
            "Flink test job had fewer exceptions than expected. "
                + "Please verify test setup, for example Flink Restart Strategy limit.",
            checkpointsToFailOn.size(),
            equalTo(FailoverDeltaGlobalCommitter.designExceptionCounter)
        );
    }

    @Test
    public void canDisableDeltaCheckpointing() throws Exception {
        final org.apache.hadoop.conf.Configuration hadoopConf =
            new org.apache.hadoop.conf.Configuration();
        hadoopConf.set("io.delta.standalone.checkpointing.enabled", "false");

        DeltaTestUtils.initTestForNonPartitionedTable(deltaTablePath);

        StreamExecutionEnvironment env = getTestStreamEnv(false); // no failover
        env.addSource(new CheckpointCountingSource(1_000, 12))
            .setParallelism(1)
            .sinkTo(DeltaSinkTestUtils.createDeltaSink(deltaTablePath, false, hadoopConf))
            .setParallelism(3);

        StreamGraph streamGraph = env.getStreamGraph();
        try (MiniCluster miniCluster = DeltaSinkTestUtils.getMiniCluster()) {
            miniCluster.start();
            miniCluster.executeJobBlocking(streamGraph.getJobGraph());
        }

        List<String> deltaCheckpointFiles = getDeltaCheckpointFiles(deltaTablePath);

        assertThat("There should be no delta checkpoint written", deltaCheckpointFiles.isEmpty());
    }

    /**
     * This test verifies if Flink Delta Source created Delta checkpoint after 10 commits.
     * This tests produces records using {@link CheckpointCountingSource} until at most 12 Flink
     * checkpoints will be created.
     * For every Flink checkpoint the {@link CheckpointCountingSource} produces new batch of
     * records.
     * After approximately 10 Flink checkpoints there should be a Delta checkpoint created.
     */
    @Test
    public void testSinkDeltaCheckpoint() throws Exception {
        StreamExecutionEnvironment env = setUpEnvAndJob(savepointPath, 3);
        env.execute();

        // Now there should be a Delta Checkpoint under _delta_log folder.
        List<String> deltaCheckpointFiles = getDeltaCheckpointFiles(deltaTablePath);
        assertThat(
            "Missing Delta's checkpoint file",
            deltaCheckpointFiles.contains("00000000000000000010.checkpoint.parquet"),
            equalTo(true)
        );
    }

    @Test
    public void shouldThrow_resumeSink_savepointDrainState() throws Exception {
        StreamExecutionEnvironment env = setUpEnvAndJob(savepointPath, 3);
        MiniCluster miniCluster = miniClusterResource.getMiniCluster();

        // terminate = true, means stop with savepoint --drain.
        // which means that job stat should be flushed.
        JobGraph jobFromSavePoint = startAndStopJobWithSavepoint(env, miniCluster, true);

        // Job stopped with savepoint --drain cannot be resumed due to
        // https://issues.apache.org/jira/browse/FLINK-30238.
        JobExecutionException exception = assertThrows(JobExecutionException.class,
            () -> miniCluster.executeJobBlocking(jobFromSavePoint));

        assertThat(exception.getCause().getCause().getMessage(),
            equalTo(
                "Currently it is not supported to update the CommittableSummary for a "
                    + "checkpoint coming from the same subtask. Please check the status of "
                    + "FLINK-25920")
        );
    }

    @Disabled(
        "This test is flaky, for some runs it fails reporting duplicated piles committed into the"
            + " delta log. We should investigate if this is issue with test of Delta Connector.")
    @Test
    public void shouldResumeSink_savepointNoDrainState() throws Exception {
        StreamExecutionEnvironment env = setUpEnvAndJob(savepointPath, 3);
        MiniCluster miniCluster = miniClusterResource.getMiniCluster();
        // terminate = false, means stop with savepoint without flushing jobs state.
        JobGraph jobFromSavePoint = startAndStopJobWithSavepoint(env, miniCluster, false);
        miniCluster.executeJobBlocking(jobFromSavePoint);

        DeltaLog targetDeltaTable =
            DeltaLog.forTable(DeltaTestUtils.getHadoopConf(), deltaTablePath);
        DeltaTableAsserts.assertThat(targetDeltaTable)
            .hasNoDataLoss("name")
            .hasNoDuplicateAddFiles();
    }

    @Disabled(
        "This test is flaky, for some runs it fails with 'Seems there was a duplicated AddFile in"
            + " Delta log. Investigation of if this is a connector or test issue is ongoing")
    @ParameterizedTest(
        name = "init parallelism level = {0}, parallelism level after resuming job = {1}")
    @CsvSource({"3, 3", "3, 6", "6, 3"})
    public void testCheckpointLikeASavepointRecovery(
            int initSinkParallelismLevel,
            int resumeSinkParallelismLevel) throws Exception {

        checkArgument(
            initSinkParallelismLevel <= miniClusterResource.getNumberSlots(),
            "initSinkParallelismLevel is bigger than mini-cluster capacity, change parameter of "
                + "buildCluster(...)"
        );
        checkArgument(
            resumeSinkParallelismLevel <= miniClusterResource.getNumberSlots(),
            "resumeSinkParallelismLevel is bigger than mini-cluster capacity, change parameter of "
                + "buildCluster(...)"
        );

        StreamExecutionEnvironment env = setUpEnvAndJob(savepointPath, initSinkParallelismLevel);
        MiniCluster miniCluster = miniClusterResource.getMiniCluster();

        StreamGraph streamGraph = env.getStreamGraph();
        JobGraph jobGraph = streamGraph.getJobGraph();
        miniCluster.submitJob(jobGraph);

        // sleep for around 5 checkpoints
        Thread.sleep(5 * 1000);
        assertThat(miniCluster.getJobStatus(jobGraph.getJobID()).get(),
            equalTo(JobStatus.RUNNING));

        miniCluster.cancelJob(jobGraph.getJobID()).get(5, TimeUnit.SECONDS);

        Path checkpointDataFolder = findLastCheckpoint();
        LOG.info("Resuming from path - " + checkpointDataFolder.toUri());
        StreamExecutionEnvironment resumedEnv =
            setUpEnvAndJob(savepointPath, resumeSinkParallelismLevel);

        JobGraph jobFromSavePoint = resumedEnv.getStreamGraph().getJobGraph();
        jobFromSavePoint.setSavepointRestoreSettings(
            SavepointRestoreSettings.forPath(checkpointDataFolder.toUri().toString())
        );

        // execute job from last checkpoint is it was a savepoint.
        miniCluster.executeJobBlocking(jobFromSavePoint);

        // THEN
        DeltaLog targetDeltaTable =
            DeltaLog.forTable(DeltaTestUtils.getHadoopConf(), deltaTablePath);
        DeltaTableAsserts.assertThat(targetDeltaTable)
            .hasNoDataLoss("name")
            .hasNoDuplicateAddFiles();
    }

    private Path findLastCheckpoint() throws IOException {
        // we want to take last checkpoint created.
        Optional<Path> newestFolder = findNewestFolder(savepointPath);
        Path checkpointDir =
            newestFolder.orElseThrow(() -> new RuntimeException("Missing Checkpoint folder."));

        Optional<Path> checkpointData = Files.list(checkpointDir)
            .filter(Files::isDirectory)
            .filter(f -> f.getFileName().toString().startsWith("chk"))
            .findFirst();

        return checkpointData.orElseThrow(
            () -> new RuntimeException("Missing Checkpoint data folder."));
    }

    /**
     * Starts a Flink job registered on provided StreamExecutionEnvironment. In next step, after
     * around 5 seconds this method will stop the job triggering Flink's savepoint.
     *
     * @param env         the {@link StreamExecutionEnvironment} with registered Flink job.
     * @param miniCluster the {@link MiniCluster} instance for this test.
     * @param terminate   if true then savepoint --drain option will be used to stop create the
     *                    savepoint.
     * @return a {@link JobGraph} instance that represents a Flink job with restore from savepoint
     * path set. This will resume job from created savepoint.
     */
    private JobGraph startAndStopJobWithSavepoint(
            StreamExecutionEnvironment env,
            MiniCluster miniCluster,
            boolean terminate) throws Exception {
        if (!miniCluster.isRunning()) {
            miniCluster.start();
        }
        StreamGraph streamGraph = env.getStreamGraph();
        JobGraph jobGraph = streamGraph.getJobGraph();
        miniCluster.submitJob(jobGraph);

        // sleep for around 5 checkpoints
        Thread.sleep(5 * 1000);
        assertThat(miniCluster.getJobStatus(jobGraph.getJobID()).get(),
            equalTo(JobStatus.RUNNING));

        String savepoint = miniCluster.stopWithSavepoint(
            jobGraph.getJobID(),
            "src/test/resources/checkpoints/",
            terminate, // terminated = true, means use savepoint with --drain option.
            SavepointFormatType.CANONICAL).get();

        LOG.info("Savepoint path - " + savepoint);
        JobGraph jobFromSavePoint = streamGraph.getJobGraph();
        jobFromSavePoint.setSavepointRestoreSettings(
            SavepointRestoreSettings.forPath(savepoint)
        );
        return jobFromSavePoint;
    }

    private StreamExecutionEnvironment setUpEnvAndJob(Path savepointPath, int sinkParallelism) {
        StreamExecutionEnvironment env = getTestStreamEnv(false); // no failover
        CheckpointConfig config = env.getCheckpointConfig();
        config
            .setExternalizedCheckpointCleanup(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        config.setCheckpointStorage(savepointPath.toUri().normalize().toString());

        env.addSource(new CheckpointCountingSource(1_000, 24))
            .setParallelism(1)
            .sinkTo(DeltaSinkTestUtils.createDeltaSink(deltaTablePath, false)) // not partitioned
            .setParallelism(sinkParallelism);
        return env;
    }

    private List<String> getDeltaCheckpointFiles(String deltaTablePath) throws IOException {
        try (Stream<Path> stream = Files.list(Paths.get(deltaTablePath + "/_delta_log/"))) {
            return stream
                .filter(file -> !Files.isDirectory(file))
                .map(file -> file.getFileName().toString())
                .filter(fileName -> fileName.endsWith(".checkpoint.parquet"))
                .collect(Collectors.toList());
        }
    }

    public void runDeltaSinkTest(
            String deltaTablePath,
            JobGraph jobGraph,
            int numOfRecordsPerSource) throws Exception {

        // GIVEN
        DeltaLog deltaLog = DeltaLog.forTable(DeltaTestUtils.getHadoopConf(), deltaTablePath);
        List<AddFile> initialDeltaFiles = deltaLog.snapshot().getAllFiles();

        long initialVersion = deltaLog.snapshot().getVersion();
        int initialTableRecordsCount = TestParquetReader
            .readAndValidateAllTableRecords(deltaLog);
        assertEquals(2, initialTableRecordsCount);

        // WHEN
        MiniCluster miniCluster = miniClusterResource.getMiniCluster();
        miniCluster.executeJobBlocking(jobGraph);

        // THEN
        int writtenRecordsCount =
            DeltaSinkTestUtils.validateIfPathContainsParquetFilesWithData(deltaTablePath);
        assertEquals(
            numOfRecordsPerSource * NUM_SOURCES,
            writtenRecordsCount - initialTableRecordsCount
        );

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
        assertEquals(((long) numOfRecordsPerSource * NUM_SOURCES), totalRowsAdded);
        assertEquals(finalTableRecordsCount - initialTableRecordsCount, totalRowsAdded);
    }

    /**
     * Creating the testing job graph in streaming mode. The graph created is [Source] -> [Delta
     * Sink]. The source would trigger failover if required.
     */
    protected JobGraph createJobGraphWithFailoverSource(
            String deltaTablePath,
            boolean triggerFailover,
            boolean isPartitioned) {

        StreamExecutionEnvironment env = getTestStreamEnv(triggerFailover);

        env.addSource(new DeltaStreamingExecutionTestSource(latchId, NUM_RECORDS, triggerFailover))
            .setParallelism(NUM_SOURCES)
            .sinkTo(DeltaSinkTestUtils.createDeltaSink(deltaTablePath, isPartitioned))
            .setParallelism(NUM_SINKS);

        StreamGraph streamGraph = env.getStreamGraph();
        return streamGraph.getJobGraph();
    }

    /**
     * Creating the testing job graph in streaming mode. The graph created is [Source] -> [Delta
     * Sink]. The sink will contain global committer that will throw an exception before or after
     * committing to the delta log after certain number of Flink checkpoints.
     */
    protected JobGraph createJobGraphWithFailoverGlobalCommitter(
            String deltaTablePath,
            GlobalCommitterExceptionMode exceptionMode,
            int recordsPerCheckpoint,
            int numberOfCheckpoints,
            Set<Integer> checkpointsToFailOn,
            boolean isPartitioned) {

        checkArgument(
            numberOfCheckpoints > 1,
            "Number of checkpoints must be at least 2."
        );

        StreamExecutionEnvironment env = getTestStreamEnv(true);

        Sink<RowData, DeltaCommittable, DeltaWriterBucketState, DeltaGlobalCommittable> deltaSink =
            DeltaSinkTestUtils.createDeltaSink(deltaTablePath, isPartitioned);

        deltaSink = new FailoverDeltaSink(
            (DeltaSinkInternal<RowData>) deltaSink,
            exceptionMode,
            checkpointsToFailOn
            );

        env.addSource(new CheckpointCountingSource(recordsPerCheckpoint, numberOfCheckpoints))
            .setParallelism(NUM_SOURCES)
            .sinkTo(deltaSink)
            .setParallelism(NUM_SINKS);

        StreamGraph streamGraph = env.getStreamGraph();
        return streamGraph.getJobGraph();
    }

    private StreamExecutionEnvironment getTestStreamEnv(boolean triggerFailover) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Configuration config = new Configuration();
        config.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.STREAMING);
        env.configure(config, getClass().getClassLoader());
        env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE);

        if (triggerFailover) {
            env.setRestartStrategy(RestartStrategies.fixedDelayRestart(10, Time.milliseconds(100)));
        } else {
            env.setRestartStrategy(RestartStrategies.noRestart());
        }

        return env;
    }

    public static Optional<Path> findNewestFolder(Path dir) throws IOException {
        if (Files.isDirectory(dir)) {
            return Files.list(dir)
                .filter(Files::isDirectory)
                .min((p1, p2) -> Long.compare(p2.toFile().lastModified(),
                    p1.toFile().lastModified()));
        }

        return Optional.empty();
    }

    ///////////////////////////////////////////////////////////////////////////
    // Streaming mode user functions
    ///////////////////////////////////////////////////////////////////////////

    /**
     * Implementation idea and some functions is borrowed from 'StreamingExecutionTestSource' in
     * {@code org.apache.flink.connector.file.sink.StreamingExecutionFileSinkITCase}
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
                    RowData row = DeltaSinkTestUtils.TEST_ROW_TYPE_CONVERTER.toInternal(
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

    /**
     * Wrapper for original {@link DeltaSinkInternal} that can be used for IT testing batch jobs.
     * This implementation will use {@link FailoverDeltaGlobalCommitter} as GlobalCommitter.
     */
    private static class FailoverDeltaSink extends FailoverDeltaSinkBase<RowData> {

        private final GlobalCommitterExceptionMode exceptionMode;

        private final Set<Integer> checkpointsToFailOn;

        private FailoverDeltaSink(
                DeltaSinkInternal<RowData> deltaSink,
                GlobalCommitterExceptionMode exceptionMode,
                Set<Integer> checkpointsToFailOn) {

            super(deltaSink);
            this.exceptionMode = exceptionMode;
            this.checkpointsToFailOn = checkpointsToFailOn;
        }

        @Override
        public Optional<GlobalCommitter<DeltaCommittable, DeltaGlobalCommittable>>
            createGlobalCommitter() throws IOException {

            return Optional.of(new FailoverDeltaGlobalCommitter(
                (DeltaGlobalCommitter) this.decoratedSink.createGlobalCommitter().get(),
                this.exceptionMode,
                this.checkpointsToFailOn)
            );
        }
    }

    /**
     * Wrapper for original {@link DeltaGlobalCommitter} that can be used for IT testing Streaming
     * jobs. This implementation will throw an exception before or after committing data to the
     * delta log.
     * <p>
     * This implementation uses a static fields as a flag, so it cannot be used in multithreading
     * test setup where there will be multiple tests using this class running at the same time.
     * This would cause unpredictable results.
     */
    private static class FailoverDeltaGlobalCommitter extends FailoverDeltaGlobalCommitterBase {

        /**
         * Counter for checkpoints that this committer proceeded.
         */
        public static int checkpointCounter;

        /**
         * Counter for number of thrown exceptions.
         */
        public static int designExceptionCounter;

        private final GlobalCommitterExceptionMode exceptionMode;

        /**
         * Checkpoint counts when exception should be thrown.
         */
        private final Set<Integer> checkpointsToFailOn;

        private FailoverDeltaGlobalCommitter(
                DeltaGlobalCommitter decoratedGlobalCommitter,
                GlobalCommitterExceptionMode exceptionMode,
                Set<Integer> checkpointsToFailOn) {

            super(decoratedGlobalCommitter);
            this.exceptionMode = exceptionMode;
            this.checkpointsToFailOn = checkpointsToFailOn;
        }

        @Override
        public List<DeltaGlobalCommittable> commit(List<DeltaGlobalCommittable> list)
            throws IOException, InterruptedException {

            checkpointCounter++;

            switch (exceptionMode) {
                case BEFORE_COMMIT:
                    if (checkpointsToFailOn.contains(checkpointCounter)) {
                        designExceptionCounter++;
                        throw new RuntimeException("Designed Exception from Global Committer BEFORE"
                            + " Delta log commit.");
                    }
                    return this.decoratedGlobalCommitter.commit(list);
                case AFTER_COMMIT:
                    List<DeltaGlobalCommittable> commit =
                        this.decoratedGlobalCommitter.commit(list);
                    if (checkpointsToFailOn.contains(checkpointCounter)) {
                        designExceptionCounter++;
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
            checkpointCounter = 0;
            designExceptionCounter = 0;
        }
    }
}
