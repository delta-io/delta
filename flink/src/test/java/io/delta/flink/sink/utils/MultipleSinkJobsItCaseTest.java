package io.delta.flink.sink.utils;

import java.io.IOException;

import io.delta.flink.utils.CheckpointCountingSource;
import io.delta.flink.utils.DeltaTestUtils;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.rules.TemporaryFolder;
import static io.delta.flink.utils.DeltaTestUtils.buildCluster;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;

import io.delta.standalone.DeltaLog;
import io.delta.standalone.data.CloseableIterator;
import io.delta.standalone.data.RowRecord;

/**
 * This test executes multiple Flink Jobs that contains Delta Sink connector. Every job is executed
 * one after another. This test checks if Flink was able to unload all classes from executed job. If
 * some classes were not unloaded, for example connector was using static thread pool Flink will
 * throw an exception:
 * <pre>
 *     Caused by: java.lang.IllegalStateException:
 *     Trying to access closed classloader. Please check if you store classloaders directly
 *     or indirectly in static fields. If the stacktrace suggests that the leak occurs in
 *     a third party library and cannot be fixed immediately, you can disable this check with
 *     the configuration 'classloader.check-leaked-classloader'.
 * </pre>
 */
public class MultipleSinkJobsItCaseTest {

    private static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();

    private static final int PARALLELISM = 4;

    private static final int RECORDS_PER_CHECKPOINT = 100;

    private static final int NUMBER_OF_CHECKPOINTS = 5;

    public static final int SINK_PARALLELISM = 1;

    private static final int RECORDS_PER_JOB_EXECUTION =
        SINK_PARALLELISM * RECORDS_PER_CHECKPOINT * NUMBER_OF_CHECKPOINTS;

    private final MiniClusterWithClientResource miniClusterResource = buildCluster(PARALLELISM);

    private String deltaTablePath;

    @BeforeAll
    public static void beforeAll() throws IOException {
        TMP_FOLDER.create();
    }

    @AfterAll
    public static void afterAll() {
        TMP_FOLDER.delete();
    }

    @BeforeEach
    public void setup() {
        try {
            deltaTablePath = TMP_FOLDER.newFolder().getAbsolutePath();
            miniClusterResource.before();
        } catch (Exception e) {
            throw new RuntimeException("Weren't able to setup the test dependencies", e);
        }
    }

    @Test
    public void testMultipleSinkJobsOnOneCluster() throws Exception {
        // It is important to not initialize Delta's snapshot state from test's main thread.
        // With this the thread from common ForkJoinPool would not be returned to the pool between
        // job executions. This could cause false positive results for these tests.

        int numberOfJobs = 3;

        for (int i = 0; i < numberOfJobs; i++) {
            JobGraph jobGraph = createJobGraph(deltaTablePath);
            this.miniClusterResource.getMiniCluster().executeJobBlocking(jobGraph);
        }

        DeltaLog deltaLog = DeltaLog.forTable(DeltaTestUtils.getHadoopConf(), deltaTablePath);
        int actualNumberOfRows = 0;
        try (CloseableIterator<RowRecord> iterator = deltaLog.snapshot().open()) {
            while (iterator.hasNext()) {
                iterator.next();
                actualNumberOfRows++;
            }
        }

        assertThat(actualNumberOfRows, equalTo(numberOfJobs * RECORDS_PER_JOB_EXECUTION));
    }

    protected JobGraph createJobGraph(String deltaTablePath) {

        StreamExecutionEnvironment env = getTestStreamEnv();

        env.addSource(new CheckpointCountingSource(RECORDS_PER_CHECKPOINT, NUMBER_OF_CHECKPOINTS))
            .setParallelism(SINK_PARALLELISM)
            .sinkTo(DeltaSinkTestUtils.createDeltaSink(deltaTablePath, false))
            .setParallelism(SINK_PARALLELISM);

        StreamGraph streamGraph = env.getStreamGraph();
        return streamGraph.getJobGraph();
    }

    private StreamExecutionEnvironment getTestStreamEnv() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Configuration config = new Configuration();
        config.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.STREAMING);
        env.configure(config, getClass().getClassLoader());
        env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE);
        env.setRestartStrategy(RestartStrategies.noRestart());

        return env;
    }
}
