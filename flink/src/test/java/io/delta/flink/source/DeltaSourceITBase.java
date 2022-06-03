package io.delta.flink.source;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.delta.flink.sink.utils.DeltaSinkTestUtils;
import io.delta.flink.source.RecordCounterToFail.FailCheck;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.runtime.highavailability.nonha.embedded.HaLeadershipControl;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.RpcServiceSharing;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.collect.ClientAndIterator;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.TestLogger;
import org.junit.jupiter.api.Test;
import org.junit.rules.TemporaryFolder;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertThrows;

public abstract class DeltaSourceITBase extends TestLogger {

    public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();

    protected static final LogicalType[] DATA_COLUMN_TYPES =
        {new CharType(), new CharType(), new IntType()};

    protected static final List<String> NAME_COLUMN_VALUES =
        Stream.of("Jan", "Jan").collect(Collectors.toList());

    protected static final Set<String> SURNAME_COLUMN_VALUES =
        Stream.of("Kowalski", "Duda").collect(Collectors.toSet());

    protected static final Set<Integer> AGE_COLUMN_VALUES =
        Stream.of(1, 2).collect(Collectors.toSet());

    /**
     * Columns that are not used as a partition columns.
     */
    protected static final String[] DATA_COLUMN_NAMES = {"name", "surname", "age"};

    // Large table has no partitions.
    protected static final String[] LARGE_TABLE_ALL_COLUMN_NAMES = {"col1", "col2", "col3"};

    protected static final int SMALL_TABLE_COUNT = 2;

    protected static final int LARGE_TABLE_RECORD_COUNT = 1100;

    protected static final int PARALLELISM = 4;

    protected static final ExecutorService SINGLE_THREAD_EXECUTOR =
        Executors.newSingleThreadExecutor();

    public final MiniClusterWithClientResource miniClusterResource = buildCluster();

    /**
     * Schema for this table has only {@link #DATA_COLUMN_NAMES} of type {@link #DATA_COLUMN_TYPES}
     * columns.
     */
    protected String nonPartitionedTablePath;

    /**
     * Schema for this table contains data columns {@link #DATA_COLUMN_NAMES} and col1, col2
     * partition columns. Types of data columns are {@link #DATA_COLUMN_TYPES}
     */
    protected String partitionedTablePath;

    /**
     * Schema for this table has only {@link #LARGE_TABLE_ALL_COLUMN_NAMES} of type {@link
     * #LARGE_TABLE_ALL_COLUMN_NAMES} columns. Column types are long, long, String
     */
    protected String nonPartitionedLargeTablePath;

    private static void triggerFailover(FailoverType type, JobID jobId, Runnable afterFailAction,
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

    private static void triggerJobManagerFailover(
        JobID jobId, Runnable afterFailAction, MiniCluster miniCluster) throws Exception {
        System.out.println("Triggering Job Manager failover.");
        HaLeadershipControl haLeadershipControl = miniCluster.getHaLeadershipControl().get();
        haLeadershipControl.revokeJobMasterLeadership(jobId).get();
        afterFailAction.run();
        haLeadershipControl.grantJobMasterLeadership(jobId).get();
    }

    public static void restartTaskManager(Runnable afterFailAction, MiniCluster miniCluster)
        throws Exception {
        System.out.println("Triggering Task Manager failover.");
        miniCluster.terminateTaskManager(0).get();
        afterFailAction.run();
        miniCluster.startTaskManager();
    }

    public static void beforeAll() throws IOException {
        TMP_FOLDER.create();
    }

    public static void afterAll() {
        TMP_FOLDER.delete();
    }

    public void setup() {
        try {
            miniClusterResource.before();

            nonPartitionedTablePath = TMP_FOLDER.newFolder().getAbsolutePath();
            nonPartitionedLargeTablePath = TMP_FOLDER.newFolder().getAbsolutePath();
            partitionedTablePath = TMP_FOLDER.newFolder().getAbsolutePath();

            // TODO Move this from DeltaSinkTestUtils to DeltaTestUtils
            DeltaSinkTestUtils.initTestForSourcePartitionedTable(partitionedTablePath);
            DeltaSinkTestUtils.initTestForNonPartitionedTable(nonPartitionedTablePath);
            DeltaSinkTestUtils.initTestForNonPartitionedLargeTable(
                nonPartitionedLargeTablePath);
        } catch (Exception e) {
            throw new RuntimeException("Weren't able to setup the test dependencies", e);
        }
    }

    public void after() {
        miniClusterResource.after();
    }

    @Test
    public void testReadPartitionedTableSkippingPartitionColumns() throws Exception {

        // GIVEN, the full schema of used table is {name, surname, age} + col1, col2 as a partition
        // columns. The size of version 0 is two rows.
        DeltaSource<RowData> deltaSource = initSourceForColumns(
            partitionedTablePath,
            DATA_COLUMN_NAMES
        );

        // WHEN
        List<RowData> resultData = testWithPartitions(deltaSource);

        List<String> readNames =
            resultData.stream()
                .map(row -> row.getString(0).toString()).collect(Collectors.toList());

        Set<String> readSurnames =
            resultData.stream().map(row -> row.getString(1).toString()).collect(Collectors.toSet());

        Set<Integer> readAge =
            resultData.stream().map(row -> row.getInt(2)).collect(Collectors.toSet());

        // THEN
        assertThat("Source read different number of rows that Delta Table have.",
            resultData.size(),
            equalTo(SMALL_TABLE_COUNT));

        // check for column values
        assertThat("Source produced different values for [name] column",
            readNames,
            equalTo(NAME_COLUMN_VALUES));

        assertThat("Source produced different values for [surname] column",
            readSurnames,
            equalTo(SURNAME_COLUMN_VALUES));

        assertThat("Source produced different values for [age] column", readAge,
            equalTo(AGE_COLUMN_VALUES));

        // Checking that we don't have more columns.
        assertNoMoreColumns(resultData,3);
    }

    @Test
    public void testReadOnlyPartitionColumns() throws Exception {

        // GIVEN, the full schema of used table is {name, surname, age} + col1, col2 as a partition
        // columns. The size of version 0 is two rows.
        DeltaSource<RowData> deltaSource = initSourceForColumns(
            partitionedTablePath,
            new String[]{"col1", "col2"}
        );

        // WHEN
        List<RowData> resultData = testWithPartitions(deltaSource);

        // THEN
        assertThat("Source read different number of rows that Delta Table have.",
            resultData.size(),
            equalTo(SMALL_TABLE_COUNT));

        // check partition column values
        String col1_partitionValue = "val1";
        String col2_partitionValue = "val2";
        assertAll(() ->
            resultData.forEach(rowData -> {
                    assertPartitionValue(rowData, 0, col1_partitionValue);
                    assertPartitionValue(rowData, 1, col2_partitionValue);
                }
            )
        );

        // Checking that we don't have more columns.
        assertNoMoreColumns(resultData,2);
    }

    @Test
    public void testWithOnePartition() throws Exception {

        // GIVEN, the full schema of used table is {name, surname, age} + col1, col2 as a partition
        // columns. The size of version 0 is two rows.
        DeltaSource<RowData> deltaSource = initSourceForColumns(
            partitionedTablePath,
            new String[]{"surname", "age", "col2"} // sipping [name] column
        );

        // WHEN
        List<RowData> resultData = testWithPartitions(deltaSource);

        Set<String> readSurnames =
            resultData.stream().map(row -> row.getString(0).toString()).collect(Collectors.toSet());

        Set<Integer> readAge =
            resultData.stream().map(row -> row.getInt(1)).collect(Collectors.toSet());

        // THEN
        assertThat("Source read different number of rows that Delta Table have.",
            resultData.size(),
            equalTo(SMALL_TABLE_COUNT));

        // check for column values
        assertThat("Source produced different values for [surname] column",
            readSurnames,
            equalTo(SURNAME_COLUMN_VALUES));

        assertThat("Source produced different values for [age] column", readAge,
            equalTo(AGE_COLUMN_VALUES));

        // check partition column value
        String col2_partitionValue = "val2";
        resultData.forEach(rowData -> assertPartitionValue(rowData, 2, col2_partitionValue));

        // Checking that we don't have more columns.
        assertNoMoreColumns(resultData,3);
    }

    @Test
    public void testWithBothPartitions() throws Exception {

        // GIVEN, the full schema of used table is {name, surname, age} + col1, col2 as a partition
        // columns. The size of version 0 is two rows.
        DeltaSource<RowData> deltaSource = initSourceAllColumns(partitionedTablePath);

        // WHEN
        List<RowData> resultData = testWithPartitions(deltaSource);

        List<String> readNames =
            resultData.stream()
                .map(row -> row.getString(0).toString()).collect(Collectors.toList());

        Set<String> readSurnames =
            resultData.stream().map(row -> row.getString(1).toString()).collect(Collectors.toSet());

        Set<Integer> readAge =
            resultData.stream().map(row -> row.getInt(2)).collect(Collectors.toSet());

        // THEN
        assertThat("Source read different number of rows that Delta Table have.",
            resultData.size(),
            equalTo(SMALL_TABLE_COUNT));

        // check for column values
        assertThat("Source produced different values for [name] column",
            readNames,
            equalTo(NAME_COLUMN_VALUES));

        assertThat("Source produced different values for [surname] column",
            readSurnames,
            equalTo(SURNAME_COLUMN_VALUES));

        assertThat("Source produced different values for [age] column", readAge,
            equalTo(AGE_COLUMN_VALUES));

        // check for partition column values
        String col1_partitionValue = "val1";
        String col2_partitionValue = "val2";

        resultData.forEach(rowData -> {
            assertPartitionValue(rowData, 3, col1_partitionValue);
            assertPartitionValue(rowData, 4, col2_partitionValue);
        });

        // Checking that we don't have more columns.
        assertNoMoreColumns(resultData,5);
    }

    protected abstract DeltaSource<RowData> initSourceAllColumns(String tablePath);

    protected abstract DeltaSource<RowData> initSourceForColumns(
        String tablePath,
        String[] columnNames);

    protected abstract List<RowData> testWithPartitions(DeltaSource<RowData> deltaSource)
        throws Exception;

    /**
     * Base method used for testing {@link DeltaSource} in {@link Boundedness#BOUNDED} mode. This
     * method creates a {@link StreamExecutionEnvironment} and uses provided {@code
     * DeltaSource} instance without any failover.
     *
     * @param source The {@link DeltaSource} that should be used in this test.
     * @param <T>    Type of objects produced by source.
     * @return A {@link List} of produced records.
     */
    protected <T> List<T> testBoundedDeltaSource(DeltaSource<T> source)
        throws Exception {

        // Since we don't do any failover here (used FailoverType.NONE) we don't need any
        // actually FailCheck.
        // We do need to pass the check at least once, to call
        // RecordCounterToFail#continueProcessing.get() hence (FailCheck) integer -> true
        return testBoundedDeltaSource(FailoverType.NONE, source, (FailCheck) integer -> true);
    }

    /**
     * Base method used for testing {@link DeltaSource} in {@link Boundedness#BOUNDED} mode. This
     * method creates a {@link StreamExecutionEnvironment} and uses provided {@code
     * DeltaSource} instance.
     * <p>
     * <p>
     * The created environment can perform a failover after condition described by {@link FailCheck}
     * which is evaluated every record produced by {@code DeltaSource}
     *
     * @param failoverType The {@link FailoverType} type that should be performed for given test
     *                     setup.
     * @param source       The {@link DeltaSource} that should be used in this test.
     * @param failCheck    The {@link FailCheck} condition which is evaluated for every row produced
     *                     by source.
     * @param <T>          Type of objects produced by source.
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
    protected <T> List<T> testBoundedDeltaSource(FailoverType failoverType, DeltaSource<T> source,
        FailCheck failCheck) throws Exception {

        if (source.getBoundedness() != Boundedness.BOUNDED) {
            throw new RuntimeException(
                "Not using Bounded source in Bounded test setup. This will not work properly.");
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(PARALLELISM);
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5, 1000));

        DataStream<T> stream =
            env.fromSource(source, WatermarkStrategy.noWatermarks(), "delta-source");

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
        triggerFailover(
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
     * Base method used for testing {@link DeltaSource} in {@link Boundedness#CONTINUOUS_UNBOUNDED}
     * mode. This method creates a {@link StreamExecutionEnvironment} and uses provided {@code
     * DeltaSource} instance.
     * <p>
     * <p>
     * The created environment can perform a failover after condition described by {@link FailCheck}
     * which is evaluated every record produced by {@code DeltaSource}
     *
     * @param failoverType The {@link FailoverType} type that should be performed for given test
     *                     setup.
     * @param source       The {@link DeltaSource} that should be used in this test.
     * @param failCheck    The {@link FailCheck} condition which is evaluated for every row produced
     *                     by source.
     * @param <T>          Type of objects produced by source.
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
    protected <T> List<List<T>> testContinuousDeltaSource(
        FailoverType failoverType, DeltaSource<T> source, ContinuousTestDescriptor testDescriptor,
        FailCheck failCheck)
        throws Exception {

        StreamExecutionEnvironment env = prepareStreamingEnvironment(source);

        DeltaTableUpdater tableUpdater = new DeltaTableUpdater(source.getTablePath().toString());

        DataStream<T> stream =
            env.fromSource(source, WatermarkStrategy.noWatermarks(), "delta-source");

        DataStream<T> failingStreamDecorator =
            RecordCounterToFail.wrapWithFailureAfter(stream, failCheck);

        ClientAndIterator<T> client =
            DataStreamUtils.collectWithClient(failingStreamDecorator,
                "Continuous Delta Source Test");

        JobID jobId = client.client.getJobID();

        // Read data from initial snapshot
        Future<List<T>> initialDataFuture =
            startInitialResultsFetcherThread(testDescriptor, client);

        // Read data from table updates.
        Future<List<T>> tableUpdaterFuture =
            startTableUpdaterThread(testDescriptor, tableUpdater, client);

        RecordCounterToFail.waitToFail();
        triggerFailover(
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

    protected <T> Future<List<T>> startInitialResultsFetcherThread(
        ContinuousTestDescriptor testDescriptor,
        ClientAndIterator<T> client) {
        return SINGLE_THREAD_EXECUTOR.submit(
            () -> (DataStreamUtils.collectRecordsFromUnboundedStream(client,
                testDescriptor.getInitialDataSize())));
    }

    protected void assertPartitionValue(
            RowData rowData,
            int partitionColumnPosition,
            String partitionValue) {
        assertThat(
            "Partition column has a wrong value.",
            rowData.getString(partitionColumnPosition).toString(),
            equalTo(partitionValue)
        );
    }

    protected <T> StreamExecutionEnvironment prepareStreamingEnvironment(DeltaSource<T> source) {
        return prepareStreamingEnvironment(source, PARALLELISM);
    }

    protected <T> StreamExecutionEnvironment prepareStreamingEnvironment(
            DeltaSource<T> source,
            int parallelismLevel) {
        if (source.getBoundedness() != Boundedness.CONTINUOUS_UNBOUNDED) {
            throw new RuntimeException(
                "Not using using Continuous source in Continuous test setup. This will not work "
                    + "properly.");
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelismLevel);
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        env.enableCheckpointing(200L);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5, 1000));
        return env;
    }

    private MiniClusterWithClientResource buildCluster() {
        Configuration configuration = new Configuration();
        configuration.set(CoreOptions.CHECK_LEAKED_CLASSLOADER, false);

        return new MiniClusterWithClientResource(
            new MiniClusterResourceConfiguration.Builder()
                .setNumberTaskManagers(1)
                .setNumberSlotsPerTaskManager(PARALLELISM)
                .setRpcServiceSharing(RpcServiceSharing.DEDICATED)
                .withHaLeadershipControl()
                .setConfiguration(configuration)
                .build());
    }

    private <T> Future<List<T>> startTableUpdaterThread(ContinuousTestDescriptor testDescriptor,
        DeltaTableUpdater tableUpdater, ClientAndIterator<T> client) {
        return SINGLE_THREAD_EXECUTOR.submit(
            () ->
            {
                List<T> results = new LinkedList<>();
                testDescriptor.getUpdateDescriptors().forEach(descriptor -> {
                    tableUpdater.writeToTable(descriptor);
                    List<T> records = DataStreamUtils.collectRecordsFromUnboundedStream(client,
                        descriptor.getNumberOfNewRows());
                    System.out.println("Stream update result size: " + records.size());
                    results.addAll(records);
                });
                return results;
            });
    }

    private void assertNoMoreColumns(List<RowData> resultData, int columnIndex) {
        resultData.forEach(rowData ->
            assertThrows(
                ArrayIndexOutOfBoundsException.class,
                () -> rowData.getString(columnIndex),
                "Found row with extra column."
            )
        );
    }

    public enum FailoverType {

        /**
         * Indicates that no failover should take place.
         */
        NONE,

        /**
         * Indicates that failover was caused by Task Manager failure
         */
        TASK_MANAGER,

        /**
         * Indicates that failover was caused by Job Manager failure
         */
        JOB_MANAGER
    }

}
