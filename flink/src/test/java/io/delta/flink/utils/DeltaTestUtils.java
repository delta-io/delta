package io.delta.flink.utils;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import io.delta.flink.utils.RecordCounterToFail.FailCheck;
import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.runtime.highavailability.nonha.embedded.HaLeadershipControl;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.RpcServiceSharing;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.operators.collect.ClientAndIterator;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.types.Row;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystemTestHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeltaTestUtils {

    private static final Logger LOG = LoggerFactory.getLogger(DeltaTestUtils.class);

    ///////////////////////////////////////////////////////////////////////////
    // hadoop conf test utils
    ///////////////////////////////////////////////////////////////////////////

    public static org.apache.hadoop.conf.Configuration getHadoopConf() {
        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
        conf.set("parquet.compression", "SNAPPY");
        conf.set("io.delta.standalone.PARQUET_DATA_TIME_ZONE_ID", "UTC");
        return conf;
    }

    /**
     * Set up a simple hdfs mock as default filesystem. This FS should not be used by reference
     * of DeltaLog. If used, and Delta log will use default filesystem (mockfs:///) path,
     * it would return a null. This allows to verify that full paths, including schema are used
     * and passed around.
     */
    public static org.apache.hadoop.conf.Configuration getConfigurationWithMockFs() {
        org.apache.hadoop.conf.Configuration hadoopConf = DeltaTestUtils.getHadoopConf();

        hadoopConf.set("fs.defaultFS", "mockfs:///");
        hadoopConf.setClass("fs.mockfs.impl",
            FileSystemTestHelper.MockFileSystem.class, FileSystem.class);

        return hadoopConf;
    }

    ///////////////////////////////////////////////////////////////////////////
    // test data utils
    ///////////////////////////////////////////////////////////////////////////

    public static final String TEST_DELTA_TABLE_INITIAL_STATE_NP_DIR =
        "/test-data/test-non-partitioned-delta-table-initial-state";

    public static final String TEST_DELTA_TABLE_INITIAL_STATE_P_DIR =
        "/test-data/test-partitioned-delta-table-initial-state";

    public static final String TEST_DELTA_LARGE_TABLE_INITIAL_STATE_DIR =
        "/test-data/test-non-partitioned-delta-table_1100_records";

    public static final String TEST_DELTA_TABLE_ALL_DATA_TYPES =
        "/test-data/test-non-partitioned-delta-table-alltypes";

    public static final String TEST_VERSIONED_DELTA_TABLE =
        "/test-data/test-non-partitioned-delta-table-4-versions";

    public static void initTestForAllDataTypes(String targetTablePath)
        throws IOException {
        initTestFor(TEST_DELTA_TABLE_ALL_DATA_TYPES, targetTablePath);
    }

    public static void initTestForNonPartitionedTable(String targetTablePath)
        throws IOException {
        initTestFor(TEST_DELTA_TABLE_INITIAL_STATE_NP_DIR, targetTablePath);
    }

    public static void initTestForPartitionedTable(String targetTablePath)
        throws IOException {
        initTestFor(TEST_DELTA_TABLE_INITIAL_STATE_P_DIR, targetTablePath);
    }

    public static void initTestForNonPartitionedLargeTable(String targetTablePath)
        throws IOException {
        initTestFor(TEST_DELTA_LARGE_TABLE_INITIAL_STATE_DIR, targetTablePath);
    }

    public static void initTestForVersionedTable(String targetTablePath)
        throws IOException {
        initTestFor(TEST_VERSIONED_DELTA_TABLE, targetTablePath);
    }

    public static void initTestFor(String testDeltaTableInitialStateNpDir, String targetTablePath)
        throws IOException {
        File resourcesDirectory = new File("src/test/resources");
        String initialTablePath =
            resourcesDirectory.getAbsolutePath() + testDeltaTableInitialStateNpDir;
        FileUtils.copyDirectory(
            new File(initialTablePath),
            new File(targetTablePath));
    }

    public static void triggerFailover(FailoverType type, JobID jobId, Runnable afterFailAction,
        MiniCluster miniCluster) throws Exception {
        switch (type) {
            case NONE:
                afterFailAction.run();
                break;
            case TASK_MANAGER:
                restartTaskManager(afterFailAction, miniCluster);
                break;
            case JOB_MANAGER:
                triggerJobManagerFailover(jobId, afterFailAction, miniCluster);
                break;
        }
    }

    public static void triggerJobManagerFailover(
        JobID jobId, Runnable afterFailAction, MiniCluster miniCluster) throws Exception {
        LOG.info("Triggering Job Manager failover.");
        HaLeadershipControl haLeadershipControl = miniCluster.getHaLeadershipControl().get();
        haLeadershipControl.revokeJobMasterLeadership(jobId).get();
        afterFailAction.run();
        haLeadershipControl.grantJobMasterLeadership(jobId).get();
    }

    public static void restartTaskManager(Runnable afterFailAction, MiniCluster miniCluster)
        throws Exception {
        LOG.info("Triggering Task Manager failover.");
        miniCluster.terminateTaskManager(0).get();
        afterFailAction.run();
        miniCluster.startTaskManager();
    }

    public static MiniClusterWithClientResource buildCluster(int parallelismLevel) {
        Configuration configuration = new Configuration();

        // By default, let's check for leaked classes in tests.
        configuration.set(CoreOptions.CHECK_LEAKED_CLASSLOADER, true);

        return new MiniClusterWithClientResource(
            new MiniClusterResourceConfiguration.Builder()
                .setNumberTaskManagers(1)
                .setNumberSlotsPerTaskManager(parallelismLevel)
                .setRpcServiceSharing(RpcServiceSharing.DEDICATED)
                .withHaLeadershipControl()
                .setConfiguration(configuration)
                .build());
    }

    public static <T> List<T> testBoundedStream(
            DataStream<T> stream,
            MiniClusterWithClientResource miniClusterResource)
        throws Exception {

        return testBoundedStream(
            FailoverType.NONE,
            (FailCheck) integer -> true,
            stream,
            miniClusterResource
        );
    }

    /**
     * A utility method to test bounded {@link DataStream} with failover scenarios.
     * <p>
     * The created environment can perform a failover after condition described by {@link FailCheck}
     * which is evaluated every record produced by {@code DeltaSource}
     *
     * @param failoverType The {@link FailoverType} type that should be performed for given test
     *                     setup.
     * @param failCheck    The {@link FailCheck} condition which is evaluated for every row produced
     *                     by source.
     * @param stream       The {@link DataStream} under test.
     * @param miniClusterResource          The {@link MiniClusterWithClientResource} where given
     *                                     stream under test is executed.
     * @return A {@link List} of produced records.
     * <p>
     * @implNote The {@code RecordCounterToFail::wrapWithFailureAfter} for every row checks the
     * "fail check" and if true and if this is a first fail check it completes the FAIL {@code
     * CompletableFuture} and waits on continueProcessing {@code CompletableFuture} next.
     * <p>
     * The flow is:
     * <ul>
     *      <li>
     *          The main test thread creates Flink's Streaming Environment.
     *      </li>
     *      <li>
     *          The main test thread creates Delta source.
     *      </li>
     *      <li>
     *          The main test thread wraps created source with {@code wrapWithFailureAfter} which
     *          has the {@code FailCheck} condition.
     *      </li>
     *      <li>
     *          The main test thread starts the "test Flink cluster" to produce records from
     *          Source via {@code DataStreamUtils.collectWithClient(...)}. As a result there is a
     *          Flink mini cluster created and data is consumed by source on a new thread.
     *      </li>
     *      <li>
     *          The main thread waits for "fail signal" that is issued by calling fail
     *          .complete. This is done on that new thread from point above. After calling {@code
     *          fail.complete} the source thread waits on {@code continueProcessing.get()};
     *       </li>
     *       <li>
     *           When the main thread sees that fail.complete was executed by the Source
     *          thread, it triggers the "generic" failover based on failoverType by calling
     *          {@code triggerFailover(
     *          ...)}.
     *      </li>
     *      <li>
     *          After failover is complied, the main thread calls
     *          {@code RecordCounterToFail::continueProcessing},
     *          which releases the Source thread and resumes record consumption.
     *       </li>
     * </ul>
     * For test where FailoverType == NONE, we trigger fail signal on a first record, Main thread
     * executes triggerFailover method which only sends a continueProcessing signal that resumes
     * the Source thread.
     */
    public static <T> List<T> testBoundedStream(
            FailoverType failoverType,
            FailCheck failCheck,
            DataStream<T> stream,
            MiniClusterWithClientResource miniClusterResource)
        throws Exception {

        DataStream<T> failingStreamDecorator =
            RecordCounterToFail.wrapWithFailureAfter(stream, failCheck);

        ClientAndIterator<T> client =
            DataStreamUtils.collectWithClient(
                failingStreamDecorator, "Bounded Delta Source Test");
        JobID jobId = client.client.getJobID();

        // Wait with main thread until FailCheck from RecordCounterToFail.wrapWithFailureAfter
        // triggers.
        RecordCounterToFail.waitToFail();

        // Trigger The Failover with desired failover failoverType and continue processing after
        // recovery.
        DeltaTestUtils.triggerFailover(
            failoverType,
            jobId,
            RecordCounterToFail::continueProcessing,
            miniClusterResource.getMiniCluster());

        final List<T> result = new ArrayList<>();
        while (client.iterator.hasNext()) {
            result.add(client.iterator.next());
        }

        return result;
    }

    /**
     * A utility method to test unbounded {@link DataStream} with failover scenarios.
     * <p>
     * The created environment can perform a failover after condition described by {@link FailCheck}
     * which is evaluated every record produced by {@code DeltaSource}
     *
     * @param failoverType        The {@link FailoverType} type that should be performed for given
     *                            test setup.
     * @param testDescriptor      The {@link TestDescriptor} used for test run.
     * @param failCheck           The {@link FailCheck} condition which is evaluated for every row
     *                            produced by source.
     * @param stream              The {@link DataStream} under test.
     * @param miniClusterResource The {@link MiniClusterWithClientResource} where given stream under
     *                            test is executed.
     * @return A {@link List} of produced records.
     * @implNote The {@code RecordCounterToFail::wrapWithFailureAfter} for every row checks the
     * "fail check" and if true and if this is a first fail check it completes the FAIL {@code
     * CompletableFuture} and waits on continueProcessing {@code CompletableFuture} next.
     * <p>
     * The flow is:
     * <ul>
     *      <li>
     *          The main test thread creates Flink's Streaming Environment.
     *      </li>
     *      <li>
     *          The main test thread creates Delta source.
     *      </li>
     *      <li>
     *          The main test thread wraps created source with {@code wrapWithFailureAfter} which
     *          has the {@code FailCheck} condition.
     *      </li>
     *      <li>
     *          The main test thread starts the "test Flink cluster" to produce records from
     *          Source via {@code DataStreamUtils.collectWithClient(...)}. As a result there is a
     *          Flink mini cluster created and data is consumed by source on a new thread.
     *      </li>
     *      <li>
     *          The main thread waits for "fail signal" that is issued by calling fail
     *          .complete. This is done on that new thread from point above. After calling {@code
     *          fail.complete} the source thread waits on {@code continueProcessing.get()};
     *       </li>
     *       <li>
     *           When the main thread sees that fail.complete was executed by the Source
     *          thread, it triggers the "generic" failover based on failoverType by calling
     *          {@code triggerFailover(...)}.
     *      </li>
     *      <li>
     *          After failover is complied, the main thread calls
     *          {@code RecordCounterToFail::continueProcessing},
     *          which releases the Source thread and resumes record consumption.
     *       </li>
     * </ul>
     * For test where FailoverType == NONE, we trigger fail signal on a first record, Main thread
     * executes triggerFailover method which only sends a continueProcessing signal that resumes
     * the Source thread.
     */
    public static <T> List<List<T>> testContinuousStream(
            FailoverType failoverType,
            TestDescriptor testDescriptor,
            FailCheck failCheck,
            DataStream<T> stream,
            MiniClusterWithClientResource miniClusterResource) throws Exception {

        DataStream<T> failingStreamDecorator =
            RecordCounterToFail.wrapWithFailureAfter(stream, failCheck);

        ClientAndIterator<T> client =
            DataStreamUtils.collectWithClient(failingStreamDecorator,
                "Continuous Delta Source Test");

        JobID jobId = client.client.getJobID();

        ExecutorService singleThreadExecutor = Executors.newSingleThreadExecutor();

        // Read data from initial snapshot
        Future<List<T>> initialDataFuture =
            startInitialResultsFetcherThread(testDescriptor, client, singleThreadExecutor);

        DeltaTableUpdater tableUpdater = new DeltaTableUpdater(testDescriptor.getTablePath());

        // Read data from table updates.
        Future<List<T>> tableUpdaterFuture =
            startTableUpdaterThread(testDescriptor, tableUpdater, client, singleThreadExecutor);

        RecordCounterToFail.waitToFail();
        DeltaTestUtils.triggerFailover(
            failoverType,
            jobId,
            RecordCounterToFail::continueProcessing,
            miniClusterResource.getMiniCluster());

        // Main thread waits up to 5 minutes for all threads to finish. Fails of timeout.
        List<List<T>> totalResults = new ArrayList<>();
        totalResults.add(initialDataFuture.get(5, TimeUnit.MINUTES));
        totalResults.add(tableUpdaterFuture.get(5, TimeUnit.MINUTES));
        client.client.cancel().get(5, TimeUnit.MINUTES);

        return totalResults;
    }

    public static <T> Future<List<T>> startInitialResultsFetcherThread(
            TestDescriptor testDescriptor,
            ClientAndIterator<T> client,
            ExecutorService threadExecutor) {

        return threadExecutor.submit(
            () -> (DataStreamUtils.collectRecordsFromUnboundedStream(client,
                testDescriptor.getInitialDataSize())));
    }

    public static <T> Future<List<T>> startTableUpdaterThread(
            TestDescriptor testDescriptor,
            DeltaTableUpdater tableUpdater,
            ClientAndIterator<T> client,
            ExecutorService threadExecutor) {

        return threadExecutor.submit(
            () ->
            {
                List<T> results = new LinkedList<>();
                testDescriptor.getUpdateDescriptors().forEach(descriptor -> {
                    tableUpdater.writeToTable(descriptor);
                    List<T> records = DataStreamUtils.collectRecordsFromUnboundedStream(client,
                        descriptor.getNumberOfNewRows());
                    LOG.info("Stream update result size: " + records.size());
                    results.addAll(records);
                });
                return results;
            });
    }

    /**
     * Creates a {@link TestDescriptor} for tests. The descriptor created by this method
     * describes a scenario where Delta table will be updated
     * {@link TableUpdateDescriptor#getNumberOfNewVersions()}
     * times, where every update/version will contain
     * {@link TableUpdateDescriptor#getNumberOfRecordsPerNewVersion()}
     * new unique rows.
     */
    public static TestDescriptor prepareTableUpdates(
            String tablePath,
            RowType rowType,
            int initialDataSize,
            TableUpdateDescriptor tableUpdateDescriptor) {

        TestDescriptor testDescriptor =
            new TestDescriptor(tablePath, initialDataSize);

        for (int i = 0; i < tableUpdateDescriptor.getNumberOfNewVersions(); i++) {
            List<Row> newRows = new ArrayList<>();
            for (int j = 0; j < tableUpdateDescriptor.getNumberOfRecordsPerNewVersion(); j++) {
                newRows.add(Row.of("John-" + i + "-" + j, "Wick-" + i + "-" + j, j * i));
            }
            testDescriptor.add(rowType, newRows);
        }
        return testDescriptor;
    }
}
