package io.delta.flink.source;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.delta.flink.utils.DeltaTableUpdater;
import io.delta.flink.utils.DeltaTestUtils;
import io.delta.flink.utils.FailoverType;
import io.delta.flink.utils.RecordCounterToFail.FailCheck;
import io.delta.flink.utils.TestDescriptor;
import io.delta.flink.utils.TestDescriptor.Descriptor;
import io.github.artsok.ParameterizedRepeatedIfExceptionsTest;
import io.github.artsok.RepeatedIfExceptionsTest;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.jupiter.api.Assertions.assertAll;

public class DeltaSourceBoundedExecutionITCaseTest extends DeltaSourceITBase {

    private static final Logger LOG =
        LoggerFactory.getLogger(DeltaSourceBoundedExecutionITCaseTest.class);

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
    public void shouldReadDeltaTableUsingDeltaLogSchema(FailoverType failoverType)
            throws Exception {
        DeltaSource<RowData> deltaSource =
            initSourceAllColumns(nonPartitionedLargeTablePath);

        shouldReadDeltaTable(deltaSource, failoverType);
    }

    @ParameterizedRepeatedIfExceptionsTest(
        suspend = 2000L, repeats = 3, name = "{index}: FailoverType = [{0}]"
    )
    @EnumSource(FailoverType.class)
    // NOTE that this test can take some time to finish since we are restarting JM here.
    // It can be around 30 seconds or so.
    // Test if SplitEnumerator::addSplitsBack works well,
    // meaning if splits were added back to the Enumerator's state and reassigned to new TM.
    public void shouldReadDeltaTableUsingUserSchema(FailoverType failoverType) throws Exception {

        DeltaSource<RowData> deltaSource =
            initSourceForColumns(nonPartitionedLargeTablePath, new String[] {"col1", "col2"});

        shouldReadDeltaTable(deltaSource, failoverType);
    }

    /**
     * This test verifies that Delta source is reading the same snapshot that was used by Source
     * builder for schema discovery.
     * <p>
     * The Snapshot is created two times, first time in builder for schema discovery and second
     * time during source enumerator object initialization, which happens when job is deployed on a
     * Flink cluster. We need to make sure that the same snapshot will be used in both cases.
     * <p>
     * Test scenario:
     * <ul>
     *     <li>
     *         Create source object. In this step, source will get Delta table head snapshot
     *         (version 0) and build schema from its metadata.
     *     </li>
     *     <li>
     *         Update Delta table by adding one extra row. This will change head Snapshot to
     *         version 1.
     *     </li>
     *     <li>
     *         Start the pipeline, Delta source will start reading Delta table.
     *     </li>
     *     <li>
     *         Expectation is that Source should read the version 0, the one that was used for
     *         creating format schema. Version 0 has 2 records in it.
     *     </li>
     * </ul>
     *
     */
    @RepeatedIfExceptionsTest(suspend = 2000L, repeats = 3)
    public void shouldReadLoadedSchemaVersion() throws Exception {

        // Create a Delta source instance. In this step, builder discovered Delta table schema
        // and create Table format based on this schema acquired from snapshot.
        DeltaSource<RowData> source = initSourceAllColumns(nonPartitionedTablePath);

        // Updating table with new data, changing head  Snapshot version.
        Descriptor update = new Descriptor(
            RowType.of(true, DATA_COLUMN_TYPES, DATA_COLUMN_NAMES),
            Collections.singletonList(Row.of("John-K", "Wick-P", 1410))
        );

        DeltaTableUpdater tableUpdater = new DeltaTableUpdater(nonPartitionedTablePath);
        tableUpdater.writeToTable(update);

        // Starting pipeline and reading the data. Source should read Snapshot version used for
        // schema discovery in buildr, so before table update.
        List<RowData> rowData = testBoundedDeltaSource(source);

        // We are expecting to read version 0, before table update.
        assertThat(rowData.size(), equalTo(SMALL_TABLE_COUNT));
    }

    /**
     * @return Stream of test {@link Arguments} elements. Arguments are in order:
     * <ul>
     *     <li>Snapshot version used as a value of "versionAsOf" option.</li>
     *     <li>Expected number of record for version defined by versionAsOf</li>
     *     <li>Highest value of col1 column for version defined by versionAsOf</li>
     * </ul>
     */
    private static Stream<Arguments> versionAsOfArguments() {
        return Stream.of(
            Arguments.of(0, 5, 4),
            Arguments.of(1, 15, 14),
            Arguments.of(2, 35, 34),
            Arguments.of(3, 75, 74)
        );
    }

    @ParameterizedRepeatedIfExceptionsTest(
        suspend = 2000L,
        repeats = 3,
        name =
            "{index}: versionAsOf = [{0}], "
                + "Expected Number of rows = [{1}], "
                + "End Index = [{2}]"
    )
    @MethodSource("versionAsOfArguments")
    public void shouldReadVersionAsOf(
            long versionAsOf,
            int expectedNumberOfRow,
            int endIndex) throws Exception {

        // this test uses test-non-partitioned-delta-table-4-versions table. See README.md from
        // table's folder for detail information about this table.
        String sourceTablePath = TMP_FOLDER.newFolder().getAbsolutePath();
        DeltaTestUtils.initTestForVersionedTable(sourceTablePath);

        DeltaSource<RowData> deltaSource = DeltaSource
            .forBoundedRowData(
                new Path(sourceTablePath),
                DeltaTestUtils.getHadoopConf())
            .versionAsOf(versionAsOf)
            .build();

        List<RowData> rowData = testBoundedDeltaSource(deltaSource);

        assertRows("versionAsOf " + versionAsOf, expectedNumberOfRow, endIndex, rowData);
    }

    private static final String[] timestampAsOfValues = {
        "2022-06-15 13:24:33.613",
        "2022-06-15 13:25:33.632",
        "2022-06-15 13:26:33.633",

        // Local filesystem will truncate the logFile last modified timestamps to the nearest
        // second. So, for example, "2022-06-15 13:27:33.001" would be after last commit.
        "2022-06-15 13:27:33.000"
    };

    /**
     * @return Stream of test {@link Arguments} elements. Arguments are in order:
     * <ul>
     *     <li>Timestamp used as a value of "timestampAsOf" option.</li>
     *     <li>Expected number of record in version defined by timestampAsOf</li>
     *     <li>Highest value of col1 column for version defined by versionAsOf</li>
     * </ul>
     */
    private static Stream<Arguments> timestampAsOfArguments() {
        return Stream.of(
            Arguments.of(timestampAsOfValues[0], 5, 4),
            Arguments.of(timestampAsOfValues[1], 15, 14),
            Arguments.of(timestampAsOfValues[2], 35, 34),
            Arguments.of(timestampAsOfValues[3], 75, 74)
        );
    }

    @ParameterizedRepeatedIfExceptionsTest(
        suspend = 2000L,
        repeats = 3,
        name =
            "{index}: timestampAsOf = [{0}], "
                + "Expected Number of rows = [{1}], "
                + "End Index = [{2}]"
    )
    @MethodSource("timestampAsOfArguments")
    public void shouldReadTimestampAsOf(
            String timestampAsOf,
            int expectedNumberOfRow,
            int endIndex) throws Exception {

        // this test uses test-non-partitioned-delta-table-4-versions table. See README.md from
        // table's folder for detail information about this table.
        String sourceTablePath = TMP_FOLDER.newFolder().getAbsolutePath();
        DeltaTestUtils.initTestForVersionedTable(sourceTablePath);

        // Delta standalone uses "last modification time" file attribute for providing commits
        // before/after or at timestamp. It Does not use an actually commits creation timestamp
        // from Delta's log.
        changeDeltaLogLastModifyTimestamp(sourceTablePath, timestampAsOfValues);

        DeltaSource<RowData> deltaSource = DeltaSource
            .forBoundedRowData(
                new Path(sourceTablePath),
                DeltaTestUtils.getHadoopConf())
            .timestampAsOf(timestampAsOf)
            .build();

        List<RowData> rowData = testBoundedDeltaSource(deltaSource);

        assertRows("timestampAsOf " + timestampAsOf, expectedNumberOfRow, endIndex, rowData);
    }

    private void assertRows(
            String sizeMsg,
            int expectedNumberOfRow,
            int endIndex,
            List<RowData> rowData) {

        String rangeMessage = "Index value for col1 should be in range of <0 - " + endIndex + ">";

        assertAll(() -> {
                assertThat(
                    "Source read different number of rows that expected for " + sizeMsg,
                    rowData.size(), equalTo(expectedNumberOfRow)
                );
                rowData.forEach(row -> {
                    LOG.info("Row content " + row);
                    long col1Val = row.getLong(0);
                    assertThat(rangeMessage + " but was " + col1Val, col1Val >= 0, equalTo(true));
                    assertThat(
                        rangeMessage + " but was " + col1Val,
                        col1Val <= endIndex,
                        equalTo(true)
                    );
                });
            }
        );
    }

    @Override
    protected List<RowData> testSource(
            DeltaSource<RowData> deltaSource,
            TestDescriptor testDescriptor) throws Exception {
        return testBoundedDeltaSource(deltaSource);
    }

    /**
     * Initialize a Delta source in bounded mode that should take entire Delta table schema
     * from Delta's metadata.
     */
    protected DeltaSource<RowData> initSourceAllColumns(String tablePath) {

        // Making sure that we are using path with schema to file system "file://"
        Configuration hadoopConf = DeltaTestUtils.getConfigurationWithMockFs();

        Path path = Path.fromLocalFile(new File(tablePath));
        assertThat(path.toUri().getScheme(), equalTo("file"));

        return DeltaSource.forBoundedRowData(
                path,
                hadoopConf
            )
            .build();
    }

    /**
     * Initialize a Delta source in bounded mode that should take only user defined columns
     * from Delta's metadata.
     */
    protected DeltaSource<RowData> initSourceForColumns(
            String tablePath,
            String[] columnNames) {

        // Making sure that we are using path with schema to file system "file://"
        Configuration hadoopConf = DeltaTestUtils.getConfigurationWithMockFs();

        return DeltaSource.forBoundedRowData(
                Path.fromLocalFile(new File(tablePath)),
                hadoopConf
            )
            .columnNames(Arrays.asList(columnNames))
            .build();
    }

    private void shouldReadDeltaTable(
            DeltaSource<RowData> deltaSource,
            FailoverType failoverType) throws Exception {
        // WHEN
        // Fail TaskManager or JobManager after half of the records or do not fail anything if
        // FailoverType.NONE.
        List<RowData> resultData = testBoundedDeltaSource(failoverType, deltaSource,
            (FailCheck) readRows -> readRows == LARGE_TABLE_RECORD_COUNT / 2);

        Set<Long> actualValues =
            resultData.stream().map(row -> row.getLong(0)).collect(Collectors.toSet());

        // THEN
        assertThat("Source read different number of rows that Delta table have.",
            resultData.size(),
            equalTo(LARGE_TABLE_RECORD_COUNT));
        assertThat("Source Must Have produced some duplicates.", actualValues.size(),
            equalTo(LARGE_TABLE_RECORD_COUNT));
    }

    /**
     * Base method used for testing {@link DeltaSource} in {@link Boundedness#BOUNDED} mode. This
     * method creates a {@link StreamExecutionEnvironment} and uses provided {@code
     * DeltaSource} instance without any failover.
     *
     * @param source The {@link DeltaSource} that should be used in this test.
     * @param <T>    Type of objects produced by source.
     * @return A {@link List} of produced records.
     */
    private <T> List<T> testBoundedDeltaSource(DeltaSource<T> source)
        throws Exception {

        // Since we don't do any failover here (used FailoverType.NONE) we don't need any
        // actually FailCheck.
        // We do need to pass the check at least once, to call
        // RecordCounterToFail#continueProcessing.get() hence (FailCheck) integer -> true
        return testBoundedDeltaSource(FailoverType.NONE, source, (FailCheck) integer -> true);
    }

    /**
     * Base method used for testing {@link DeltaSource} in {@link Boundedness#BOUNDED} mode. This
     * method creates a {@link StreamExecutionEnvironment} and uses provided {@code DeltaSource}
     * instance.
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
     * @implNote For Implementation details please refer to
     * {@link DeltaTestUtils#testBoundedStream(FailoverType,
     * FailCheck, DataStream, MiniClusterWithClientResource)} method.
     */
    private <T> List<T> testBoundedDeltaSource(FailoverType failoverType, DeltaSource<T> source,
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

        return DeltaTestUtils
            .testBoundedStream(failoverType, failCheck, stream, miniClusterResource);
    }
}
