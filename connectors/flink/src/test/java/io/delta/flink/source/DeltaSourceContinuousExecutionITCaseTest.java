package io.delta.flink.source;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.delta.flink.internal.options.DeltaConnectorConfiguration;
import io.delta.flink.source.internal.DeltaSourceOptions;
import io.delta.flink.utils.DeltaTableUpdater;
import io.delta.flink.utils.DeltaTestUtils;
import io.delta.flink.utils.FailoverType;
import io.delta.flink.utils.RecordCounterToFail.FailCheck;
import io.delta.flink.utils.TableUpdateDescriptor;
import io.delta.flink.utils.TestDescriptor;
import io.delta.flink.utils.TestDescriptor.Descriptor;
import io.github.artsok.ParameterizedRepeatedIfExceptionsTest;
import io.github.artsok.RepeatedIfExceptionsTest;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.collect.ClientAndIterator;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.types.Row;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static io.delta.flink.utils.ExecutionITCaseTestConstants.*;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.fail;

public class DeltaSourceContinuousExecutionITCaseTest extends DeltaSourceITBase {

    private static final Logger LOG =
        LoggerFactory.getLogger(DeltaSourceContinuousExecutionITCaseTest.class);

    /**
     * Number of rows in Delta table before inserting a new data into it.
     */
    private static final int INITIAL_DATA_SIZE = 2;

    @BeforeAll
    public static void beforeAll() throws IOException {
        DeltaSourceITBase.beforeAll();
    }

    @AfterAll
    public static void afterAll() {
        DeltaSourceITBase.afterAll();
    }

    @BeforeEach
    public void setup() {
        super.setup();
    }

    @AfterEach
    public void after() {
        super.after();
    }

    @ParameterizedRepeatedIfExceptionsTest(
        suspend = 2000L, repeats = 3, name = "{index}: FailoverType = [{0}]"
    )
    @EnumSource(FailoverType.class)
    public void shouldReadTableWithNoUpdates(FailoverType failoverType) throws Exception {

        // GIVEN
        DeltaSource<RowData> deltaSource = initSourceAllColumns(nonPartitionedTablePath);

        // WHEN
        // Fail TaskManager or JobManager after half of the records or do not fail anything if
        // FailoverType.NONE.
        List<List<RowData>> resultData = testContinuousDeltaSource(failoverType, deltaSource,
            new TestDescriptor(
                deltaSource.getTablePath().toUri().toString(),
                INITIAL_DATA_SIZE),
            (FailCheck) readRows -> readRows == SMALL_TABLE_COUNT / 2);

        // total number of read rows.
        int totalNumberOfRows = resultData.stream().mapToInt(List::size).sum();

        // Each row has a unique column across all Delta table data. We are converting List or
        // read rows to set of values for that unique column.
        // If there were any duplicates or missing values we will catch them here by comparing
        // size of that Set to expected number of rows.
        Set<String> uniqueValues =
            resultData.stream().flatMap(Collection::stream).map(row -> row.getString(1).toString())
                .collect(Collectors.toSet());

        // THEN
        assertThat("Source read different number of rows that Delta Table have.", totalNumberOfRows,
            equalTo(SMALL_TABLE_COUNT));
        assertThat("Source Produced Different Rows that were in Delta Table", uniqueValues,
            equalTo(SURNAME_COLUMN_VALUES));
    }

    @ParameterizedRepeatedIfExceptionsTest(
        suspend = 2000L, repeats = 3, name = "{index}: FailoverType = [{0}]"
    )
    @EnumSource(FailoverType.class)
    public void shouldReadLargeDeltaTableWithNoUpdates(FailoverType failoverType) throws Exception {

        // GIVEN
        DeltaSource<RowData> deltaSource = initSourceAllColumns(nonPartitionedLargeTablePath);

        // WHEN
        List<List<RowData>> resultData = testContinuousDeltaSource(failoverType, deltaSource,
            new TestDescriptor(
                deltaSource.getTablePath().toUri().toString(),
                LARGE_TABLE_RECORD_COUNT),
            (FailCheck) readRows -> readRows == LARGE_TABLE_RECORD_COUNT / 2);

        int totalNumberOfRows = resultData.stream().mapToInt(List::size).sum();

        // Each row has a unique column across all Delta table data. We are converting List or
        // read rows to set of values for that unique column.
        // If there were any duplicates or missing values we will catch them here by comparing
        // size of that Set to expected number of rows.
        Set<Long> uniqueValues =
            resultData.stream().flatMap(Collection::stream).map(row -> row.getLong(0))
                .collect(Collectors.toSet());

        // THEN
        assertThat("Source read different number of rows that Delta Table have.", totalNumberOfRows,
            equalTo(LARGE_TABLE_RECORD_COUNT));
        assertThat("Source Produced Different Rows that were in Delta Table", uniqueValues.size(),
            equalTo(LARGE_TABLE_RECORD_COUNT));
    }

    @ParameterizedRepeatedIfExceptionsTest(
        suspend = 2000L, repeats = 3, name = "{index}: FailoverType = [{0}]"
    )
    @EnumSource(FailoverType.class)
    // This test updates Delta Table 5 times, so it will take some time to finish.
    public void shouldReadDeltaTableFromSnapshotAndUpdatesUsingUserSchema(FailoverType failoverType)
        throws Exception {

        // GIVEN
        DeltaSource<RowData> deltaSource =
            initSourceForColumns(nonPartitionedTablePath, new String[]{"name", "surname"});

        shouldReadDeltaTableFromSnapshotAndUpdates(deltaSource, failoverType);
    }

    @ParameterizedRepeatedIfExceptionsTest(
        suspend = 2000L, repeats = 3, name = "{index}: FailoverType = [{0}]"
    )
    @EnumSource(FailoverType.class)
    // This test updates Delta Table 5 times, so it will take some time to finish. About 1 minute.
    public void shouldReadDeltaTableFromSnapshotAndUpdatesUsingDeltaLogSchema(
            FailoverType failoverType) throws Exception {

        // GIVEN
        DeltaSource<RowData> deltaSource = initSourceAllColumns(nonPartitionedTablePath);

        shouldReadDeltaTableFromSnapshotAndUpdates(deltaSource, failoverType);
    }

    /**
     * This test verifies that Delta source is reading the same snapshot that was used by Source
     * builder for schema discovery.
     * <p>
     * The Snapshot is created two times, first time in builder for schema discovery and second time
     * during source enumerator object initialization, which happens when job is deployed on a
     * Flink cluster. We need to make sure that the same snapshot will be used in both cases.
     * <p>
     * For Continuous mode we need to have a test that will read not content of the snapshot but
     * changes. Therefore, we are using source with option "startingVersion".
     * <p>
     * There is a known issue with processing "startingVersion" option if we want to read only
     * changes from initial Snapshot version, version 0. The reason for this is that in its
     * current form, Source will not process Metadata nor Protocol actions. Source will throw an
     * exception if those actions were a part of processed version.
     * <p>
     * Test scenario:
     * <ul>
     *     <li>
     *         Add new version to Delta table, to make head version == 1 which will be used as a
     *         startingVersion value. This is to mitigate that known issue described above.
     *     </li>
     *     <li>
     *         Create source object with option "startingVersion" set to 1.
     *         In this step, source will get Delta table snapshot for version == 1
     *         and build schema from it.
     *     </li>
     *     <li>
     *         Update Delta table by adding more extra rows. This will change head Snapshot to
     *         version 2.
     *     </li>
     *     <li>
     *         Start the pipeline, Delta source will start reading Delta table.
     *     </li>
     *     <li>
     *         Expectation is that Source should read changes from version 1 and 2.
     *     </li>
     * </ul>
     *
     */
    @RepeatedIfExceptionsTest(suspend = 2000L, repeats = 3)
    public void shouldReadLoadedSchemaVersion() throws Exception {

        // Add version 1 to delta Table.
        DeltaTableUpdater tableUpdater = new DeltaTableUpdater(nonPartitionedTablePath);

        Descriptor versionOneUpdate = new Descriptor(
            RowType.of(true, DATA_COLUMN_TYPES, DATA_COLUMN_NAMES),
            Arrays.asList(
                Row.of("John-K", "Wick-P", 1410),
                Row.of("John-K", "Wick-P", 1411),
                Row.of("John-K", "Wick-P", 1412)
            )
        );
        tableUpdater.writeToTable(versionOneUpdate);
        // Create a Source object with option "startingVersion" == 1;
        DeltaSource<RowData> source =  DeltaSource.forContinuousRowData(
                Path.fromLocalFile(new File(nonPartitionedTablePath)),
                DeltaTestUtils.getHadoopConf()
            )
            .startingVersion(1)
            .build();

        // Add another version with new rows.
        Descriptor versionTwoUpdate = new Descriptor(
            RowType.of(true, DATA_COLUMN_TYPES, DATA_COLUMN_NAMES),
            Arrays.asList(
                Row.of("John-K", "Wick-P", 1510),
                Row.of("John-K", "Wick-P", 1511),
                Row.of("John-K", "Wick-P", 1512),
                Row.of("John-K", "Wick-P", 1510),
                Row.of("John-K", "Wick-P", 1511),
                Row.of("John-K", "Wick-P", 1512)
            )
        );
        tableUpdater.writeToTable(versionTwoUpdate);

        // Deploy job on a cluster with parallelism level == 1. This will create a local cluster
        // with only one reader, so we will read versions in order 1 followed by 2.
        StreamExecutionEnvironment env = prepareStreamingEnvironment(source, 1);

        DataStream<RowData> stream =
            env.fromSource(source, WatermarkStrategy.noWatermarks(), "delta-source");

        ClientAndIterator<RowData> client =
            DataStreamUtils.collectWithClient(stream,"Continuous Delta Source Test");

        // The expected number of changes/rows is equal to the sum of version 1 and version 2 new
        // rows.
        int expectedNumberOfChanges =
            versionOneUpdate.getNumberOfNewRows() + versionTwoUpdate.getNumberOfNewRows();

        ExecutorService singleThreadExecutor = Executors.newSingleThreadExecutor();

        // Read data
        Future<List<RowData>> dataFuture =
            DeltaTestUtils.startInitialResultsFetcherThread(
                new TestDescriptor(
                    source.getTablePath().toUri().toString(),
                    versionOneUpdate.getNumberOfNewRows() + versionTwoUpdate.getNumberOfNewRows()),
                client,
                singleThreadExecutor
            );

        // Creating a new thread that will wait some time to check if there are any "extra"
        // unexpected records.
        Future<List<RowData>> unexpectedFuture = singleThreadExecutor.submit(
            () -> DataStreamUtils.collectRecordsFromUnboundedStream(client, 1));

        DeltaConnectorConfiguration sourceConfiguration = source.getSourceConfiguration();
        // Main thread waits some time. To stay on a safe side, we wait doubled time of update
        // check interval. So source will have time to check table twice for updates.
        long testTimeout =
            sourceConfiguration.getValue(DeltaSourceOptions.UPDATE_CHECK_INTERVAL) * 2;
        List<RowData> results = dataFuture.get(testTimeout, TimeUnit.MILLISECONDS);

        try {
            List<RowData> unexpectedData = unexpectedFuture.get(testTimeout, TimeUnit.MILLISECONDS);
            fail(
                String.format("Got unexpected [%d] extra rows.", unexpectedData.size()));
        } catch (TimeoutException e) {
            // expected because we should not have any additional records coming from the pipeline
            // since there should be no updates.
        }

        client.client.cancel().get(testTimeout, TimeUnit.MILLISECONDS);

        assertThat(results.size(), equalTo(expectedNumberOfChanges));
        assertThat(
            "The first processed element does not match the first row from Delta table version 1.",
            results.get(0).getInt(2),
            equalTo(versionOneUpdate.getRows().get(0).getField(2))
        );
        assertThat(
            "The last processed element does not match the last row from Delta table version 2.",
            results.get(results.size() - 1).getInt(2),
            equalTo(
                versionTwoUpdate.getRows()
                    .get(versionTwoUpdate.getNumberOfNewRows() - 1).getField(2)
            )
        );
    }

    /**
     * @return Stream of test {@link Arguments} elements. Arguments are in order:
     * <ul>
     *     <li>Version used as a value of "startingVersion" option.</li>
     *     <li>Expected number of record/changes read starting from version defined by
     *     startingVersion</li>
     *     <li>Lowest expected value of col1 column for version defined by startingVersion</li>
     * </ul>
     */
    private static Stream<Arguments> startingVersionArguments() {
        return Stream.of(
            // Skipping version 0 due to know issue of not supporting Metadata and Protocol actions
            // Waiting for Delta standalone enhancement.
            // Arguments.of(0, 75, 0),
            Arguments.of(1, 70, 5),
            Arguments.of(2, 60, 15),
            Arguments.of(3, 40, 35)
        );
    }

    @ParameterizedRepeatedIfExceptionsTest(
        suspend = 2000L,
        repeats = 3,
        name =
            "{index}: startingVersion = [{0}], "
                + "Expected Number of rows = [{1}], "
                + "Start Index = [{2}]"
    )
    @MethodSource("startingVersionArguments")
    public void shouldReadStartingVersion(
            long versionAsOf,
            int expectedNumberOfRow,
            int startIndex) throws Exception {

        // this test uses test-non-partitioned-delta-table-4-versions table. See README.md from
        // table's folder for detail information about this table.
        String sourceTablePath = TMP_FOLDER.newFolder().getAbsolutePath();
        DeltaTestUtils.initTestForVersionedTable(sourceTablePath);

        DeltaSource<RowData> deltaSource = DeltaSource
            .forContinuousRowData(
                new Path(sourceTablePath),
                DeltaTestUtils.getHadoopConf())
            .startingVersion(versionAsOf)
            .build();

        List<RowData> rowData = testContinuousDeltaSource(
            deltaSource,
            new TestDescriptor(sourceTablePath, expectedNumberOfRow)
        );

        assertRows("startingVersion " + versionAsOf, expectedNumberOfRow, startIndex, rowData);
    }

    private static final String[] startingTimestampValues = {
        "2022-06-15 13:23:33.613",
        "2022-06-15 13:24:33.630",
        "2022-06-15 13:25:33.633",
        "2022-06-15 13:26:33.634",
    };

    /**
     * @return Stream of test {@link Arguments} elements. Arguments are in order:
     * <ul>
     *     <li>Version used as a value of "startingTimestamp" option.</li>
     *     <li>Expected number of record/changes read starting from version defined by
     *     startingTimestamp</li>
     *     <li>Lowest expected value of col1 column for version defined by startingTimestamp</li>
     * </ul>
     */
    private static Stream<Arguments> startingTimestampArguments() {
        return Stream.of(
            // Skipping version 0 due to know issue of not supporting Metadata and Protocol actions
            // Waiting for Delta standalone enhancement.
            // Arguments.of(startingTimestampValues[0], 75, 0),
            Arguments.of(startingTimestampValues[1], 70, 5),
            Arguments.of(startingTimestampValues[2], 60, 15),
            Arguments.of(startingTimestampValues[3], 40, 35)
        );
    }

    @ParameterizedRepeatedIfExceptionsTest(
        suspend = 2000L,
        repeats = 3,
        name =
            "{index}: startingTimestamp = [{0}], "
                + "Expected Number of rows = [{1}], "
                + "Start Index = [{2}]"
    )
    @MethodSource("startingTimestampArguments")
    public void shouldReadStartingTimestamp(
            String startingTimestamp,
            int expectedNumberOfRow,
            int startIndex) throws Exception {

        LOG.info("Running shouldReadStartingTimestamp test for startingTimestamp - "
            + startingTimestamp);
        // this test uses test-non-partitioned-delta-table-4-versions table. See README.md from
        // table's folder for detail information about this table.
        String sourceTablePath = TMP_FOLDER.newFolder().getAbsolutePath();
        DeltaTestUtils.initTestForVersionedTable(sourceTablePath);

        // Delta standalone uses "last modification time" file attribute for providing commits
        // before/after or at timestamp. It Does not use an actually commits creation timestamp
        // from Delta's log.
        changeDeltaLogLastModifyTimestamp(sourceTablePath, startingTimestampValues);

        DeltaSource<RowData> deltaSource = DeltaSource
            .forContinuousRowData(
                new Path(sourceTablePath),
                DeltaTestUtils.getHadoopConf())
            .startingTimestamp(startingTimestamp)
            .build();

        List<RowData> rowData = testContinuousDeltaSource(
            deltaSource,
            new TestDescriptor(sourceTablePath, expectedNumberOfRow)
        );

        assertRows(
            "startingTimestamp " + startingTimestamp,
            expectedNumberOfRow,
            startIndex,
            rowData
        );
    }

    private void assertRows(
        String sizeMsg,
        int expectedNumberOfRow,
        int startIndex,
        List<RowData> rowData) {

        String rangeMessage =
            "Index value for col1 should be in range of <" + startIndex + " - 74>";

        assertAll(() -> {
                assertThat(
                    "Source read different number of rows that expected for " + sizeMsg,
                    rowData.size(), equalTo(expectedNumberOfRow)
                );
                rowData.forEach(row -> {
                    LOG.info("Row content " + row);
                    long col1Val = row.getLong(0);
                    assertThat(
                        rangeMessage + " but was " + col1Val,
                        col1Val >= startIndex,
                        equalTo(true)
                    );
                    assertThat(rangeMessage  + " but was " + col1Val, col1Val <= 74, equalTo(true));
                });
            }
        );
    }

    @Override
    protected List<RowData> testSource(
            DeltaSource<RowData> deltaSource,
            TestDescriptor testDescriptor) throws Exception {
        return testContinuousDeltaSource(
                FailoverType.NONE,
                deltaSource,
                testDescriptor,
                (FailCheck) integer -> true)
            .get(0);
    }

    /**
     * Initialize a Delta source in continuous mode that should take entire Delta table schema
     * from Delta's metadata.
     */
    protected DeltaSource<RowData> initSourceAllColumns(String tablePath) {

        // Making sure that we are using path with schema to file system "file://"
        Configuration hadoopConf = DeltaTestUtils.getConfigurationWithMockFs();

        Path path = Path.fromLocalFile(new File(tablePath));
        assertThat(path.toUri().getScheme(), equalTo("file"));

        return DeltaSource.forContinuousRowData(
                Path.fromLocalFile(new File(tablePath)),
                hadoopConf
            )
            .build();
    }

    /**
     * Initialize a Delta source in continuous mode that should take only user defined columns
     * from Delta's metadata.
     */
    protected DeltaSource<RowData> initSourceForColumns(
            String tablePath,
            String[] columnNames) {

        // Making sure that we are using path with schema to file system "file://"
        Configuration hadoopConf = DeltaTestUtils.getConfigurationWithMockFs();

        Path path = Path.fromLocalFile(new File(tablePath));
        assertThat(path.toUri().getScheme(), equalTo("file"));

        return DeltaSource.forContinuousRowData(
                Path.fromLocalFile(new File(tablePath)),
                hadoopConf
            )
            .columnNames(Arrays.asList(columnNames))
            .build();
    }

    private void shouldReadDeltaTableFromSnapshotAndUpdates(
            DeltaSource<RowData> deltaSource,
            FailoverType failoverType)
            throws Exception {

        int numberOfTableUpdateBulks = 5;
        int rowsPerTableUpdate = 5;

        TestDescriptor testDescriptor = DeltaTestUtils.prepareTableUpdates(
                deltaSource.getTablePath().toUri().toString(),
                RowType.of(DATA_COLUMN_TYPES, DATA_COLUMN_NAMES),
                INITIAL_DATA_SIZE,
            new TableUpdateDescriptor(numberOfTableUpdateBulks, rowsPerTableUpdate)
        );

        // WHEN
        List<List<RowData>> resultData =
            testContinuousDeltaSource(failoverType, deltaSource, testDescriptor,
                (FailCheck) readRows -> readRows
                    ==
                    (INITIAL_DATA_SIZE + numberOfTableUpdateBulks * rowsPerTableUpdate)
                        / 2);

        int totalNumberOfRows = resultData.stream().mapToInt(List::size).sum();

        // Each row has a unique column across all Delta table data. We are converting List or
        // read rows to set of values for that unique column.
        // If there were any duplicates or missing values we will catch them here by comparing
        // size of that Set to expected number of rows.
        Set<String> uniqueValues =
            resultData.stream().flatMap(Collection::stream)
                .map(row -> row.getString(1).toString())
                .collect(Collectors.toSet());

        // THEN
        assertThat("Source read different number of rows that Delta Table have.",
            totalNumberOfRows,
            equalTo(INITIAL_DATA_SIZE + numberOfTableUpdateBulks * rowsPerTableUpdate));
        assertThat("Source Produced Different Rows that were in Delta Table",
            uniqueValues.size(),
            equalTo(INITIAL_DATA_SIZE + numberOfTableUpdateBulks * rowsPerTableUpdate));
    }

    /**
     * Base method used for testing {@link DeltaSource} in {@link Boundedness#CONTINUOUS_UNBOUNDED}
     * mode. This method creates a {@link StreamExecutionEnvironment} and uses provided {@code
     * DeltaSource} instance without any failover.
     *
     * @param source         The {@link DeltaSource} that should be used in this test.
     * @param testDescriptor The {@link TestDescriptor} used for test run.
     * @param <T>            Type of objects produced by source.
     * @return A {@link List} of produced records.
     */
    private <T> List<T> testContinuousDeltaSource(
            DeltaSource<T> source,
            TestDescriptor testDescriptor)
        throws Exception {

        // Since we don't do any failover here (used FailoverType.NONE) we don't need any
        // actually FailCheck.
        // We do need to pass the check at least once, to call
        // RecordCounterToFail#continueProcessing.get() hence (FailCheck) integer -> true
        List<List<T>> tmpResult = testContinuousDeltaSource(
            FailoverType.NONE,
            source,
            testDescriptor,
            (FailCheck) integer -> true
        );

        ArrayList<T> result = new ArrayList<>();
        for (List<T> list : tmpResult) {
            result.addAll(list);
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
     * @param failoverType   The {@link FailoverType} type that should be performed for given test
     *                       setup.
     * @param source         The {@link DeltaSource} that should be used in this test.
     * @param testDescriptor The {@link TestDescriptor} used for test run.
     * @param failCheck      The {@link FailCheck} condition which is evaluated for every row
     *                       produced by source.
     * @param <T>            Type of objects produced by source.
     * @return A {@link List} of produced records.
     * @implNote For Implementation details please refer to
     * {@link DeltaTestUtils#testContinuousStream(FailoverType,
     * TestDescriptor, FailCheck, DataStream, MiniClusterWithClientResource)}
     */
    private <T> List<List<T>> testContinuousDeltaSource(
            FailoverType failoverType,
            DeltaSource<T> source,
            TestDescriptor testDescriptor,
            FailCheck failCheck)
            throws Exception {

        StreamExecutionEnvironment env = prepareStreamingEnvironment(source);

        DataStream<T> stream =
            env.fromSource(source, WatermarkStrategy.noWatermarks(), "delta-source");

        return DeltaTestUtils.testContinuousStream(
            failoverType,
            testDescriptor,
            failCheck,
            stream,
            miniClusterResource
        );
    }
}
