package io.delta.flink.source;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import io.delta.flink.DeltaTestUtils;
import io.delta.flink.sink.utils.DeltaSinkTestUtils;
import io.delta.flink.source.ContinuousTestDescriptor.Descriptor;
import io.delta.flink.source.RecordCounterToFail.FailCheck;
import io.delta.flink.source.internal.DeltaSourceConfiguration;
import io.delta.flink.source.internal.DeltaSourceOptions;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.collect.ClientAndIterator;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

public class DeltaSourceContinuousExecutionITCaseTest extends DeltaSourceITBase {

    /**
     * Number of updates done on Delta table, where each updated is bounded into one transaction
     */
    private static final int NUMBER_OF_TABLE_UPDATE_BULKS = 5;

    /**
     * Number of rows added per each update of Delta table
     */
    private static final int ROWS_PER_TABLE_UPDATE = 5;

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

    @ParameterizedTest(name = "{index}: FailoverType = [{0}]")
    @EnumSource(FailoverType.class)
    public void shouldReadTableWithNoUpdates(FailoverType failoverType) throws Exception {

        // GIVEN
        DeltaSource<RowData> deltaSource = initSourceAllColumns(nonPartitionedTablePath);

        // WHEN
        // Fail TaskManager or JobManager after half of the records or do not fail anything if
        // FailoverType.NONE.
        List<List<RowData>> resultData = testContinuousDeltaSource(failoverType, deltaSource,
            new ContinuousTestDescriptor(INITIAL_DATA_SIZE),
            (FailCheck) readRows -> readRows == SMALL_TABLE_COUNT / 2);

        // total number of read rows.
        int totalNumberOfRows = resultData.stream().mapToInt(List::size).sum();

        // Each row has a unique column across all Delta table data. We are converting List or
        // read rows to set of values for that unique column.
        // If there were eny duplicates or missing values we will catch them here by comparing
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

    @ParameterizedTest(name = "{index}: FailoverType = [{0}]")
    @EnumSource(FailoverType.class)
    public void shouldReadLargeDeltaTableWithNoUpdates(FailoverType failoverType) throws Exception {

        // GIVEN
        DeltaSource<RowData> deltaSource = initSourceAllColumns(nonPartitionedLargeTablePath);

        // WHEN
        List<List<RowData>> resultData = testContinuousDeltaSource(failoverType, deltaSource,
            new ContinuousTestDescriptor(LARGE_TABLE_RECORD_COUNT),
            (FailCheck) readRows -> readRows == LARGE_TABLE_RECORD_COUNT / 2);

        int totalNumberOfRows = resultData.stream().mapToInt(List::size).sum();

        // Each row has a unique column across all Delta table data. We are converting List or
        // read rows to set of values for that unique column.
        // If there were eny duplicates or missing values we will catch them here by comparing
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

    @ParameterizedTest(name = "{index}: FailoverType = [{0}]")
    @EnumSource(FailoverType.class)
    // This test updates Delta Table 5 times, so it will take some time to finish.
    public void shouldReadDeltaTableFromSnapshotAndUpdatesUsingUserSchema(FailoverType failoverType)
        throws Exception {

        // GIVEN
        DeltaSource<RowData> deltaSource =
            initSourceForColumns(nonPartitionedTablePath, new String[]{"name", "surname"});

        shouldReadDeltaTableFromSnapshotAndUpdates(deltaSource, failoverType);
    }

    @ParameterizedTest(name = "{index}: FailoverType = [{0}]")
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
    @Test
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
                DeltaSinkTestUtils.getHadoopConf()
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

        // Read data
        Future<List<RowData>> dataFuture =
            startInitialResultsFetcherThread(
                new ContinuousTestDescriptor(
                    versionOneUpdate.getNumberOfNewRows() + versionTwoUpdate.getNumberOfNewRows()),
                client
            );

        // Creating a new thread that will wait some time to check if there are any "extra"
        // unexpected records.
        Future<List<RowData>> unexpectedFuture = SINGLE_THREAD_EXECUTOR.submit(
            () -> DataStreamUtils.collectRecordsFromUnboundedStream(client, 1));

        DeltaSourceConfiguration sourceConfiguration = source.getSourceConfiguration();
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

    @Override
    protected List<RowData> testWithPartitions(DeltaSource<RowData> deltaSource) throws Exception {
        return testContinuousDeltaSource(FailoverType.NONE, deltaSource,
            new ContinuousTestDescriptor(2), (FailCheck) integer -> true).get(0);
    }

    /**
     * Initialize a Delta source in continuous mode that should take entire Delta table schema
     * from Delta's metadata.
     */
    protected DeltaSource<RowData> initSourceAllColumns(String tablePath) {

        Configuration hadoopConf = DeltaTestUtils.getHadoopConf();

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

        Configuration hadoopConf = DeltaTestUtils.getHadoopConf();

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
        ContinuousTestDescriptor testDescriptor = prepareTableUpdates();

        // WHEN
        List<List<RowData>> resultData =
            testContinuousDeltaSource(failoverType, deltaSource, testDescriptor,
                (FailCheck) readRows -> readRows
                    ==
                    (INITIAL_DATA_SIZE + NUMBER_OF_TABLE_UPDATE_BULKS * ROWS_PER_TABLE_UPDATE)
                        / 2);

        int totalNumberOfRows = resultData.stream().mapToInt(List::size).sum();

        // Each row has a unique column across all Delta table data. We are converting List or
        // read rows to set of values for that unique column.
        // If there were eny duplicates or missing values we will catch them here by comparing
        // size of that Set to expected number of rows.
        Set<String> uniqueValues =
            resultData.stream().flatMap(Collection::stream)
                .map(row -> row.getString(1).toString())
                .collect(Collectors.toSet());

        // THEN
        assertThat("Source read different number of rows that Delta Table have.",
            totalNumberOfRows,
            equalTo(INITIAL_DATA_SIZE + NUMBER_OF_TABLE_UPDATE_BULKS * ROWS_PER_TABLE_UPDATE));
        assertThat("Source Produced Different Rows that were in Delta Table",
            uniqueValues.size(),
            equalTo(INITIAL_DATA_SIZE + NUMBER_OF_TABLE_UPDATE_BULKS * ROWS_PER_TABLE_UPDATE));
    }

    /**
     * Creates a {@link ContinuousTestDescriptor} for tests. The descriptor created by this method
     * describes a scenario where Delta table will be updated {@link #NUMBER_OF_TABLE_UPDATE_BULKS}
     * times, where every update will contain {@link #ROWS_PER_TABLE_UPDATE} new unique rows.
     */
    private ContinuousTestDescriptor prepareTableUpdates() {
        ContinuousTestDescriptor testDescriptor = new ContinuousTestDescriptor(INITIAL_DATA_SIZE);
        for (int i = 0; i < NUMBER_OF_TABLE_UPDATE_BULKS; i++) {
            List<Row> newRows = new ArrayList<>();
            for (int j = 0; j < ROWS_PER_TABLE_UPDATE; j++) {
                newRows.add(Row.of("John-" + i + "-" + j, "Wick-" + i + "-" + j, j * i));
            }
            testDescriptor.add(
                RowType.of(DATA_COLUMN_TYPES, DATA_COLUMN_NAMES), newRows);
        }
        return testDescriptor;
    }
}
