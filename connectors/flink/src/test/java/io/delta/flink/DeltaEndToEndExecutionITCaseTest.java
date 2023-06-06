package io.delta.flink;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.ZoneOffset;
import java.util.concurrent.atomic.AtomicInteger;

import io.delta.flink.sink.DeltaSink;
import io.delta.flink.sink.internal.DeltaSinkInternal;
import io.delta.flink.source.DeltaSource;
import io.delta.flink.utils.DeltaTestUtils;
import io.delta.flink.utils.FailoverType;
import io.delta.flink.utils.RecordCounterToFail.FailCheck;
import io.delta.flink.utils.TableUpdateDescriptor;
import io.delta.flink.utils.TestDescriptor;
import io.github.artsok.ParameterizedRepeatedIfExceptionsTest;
import io.github.artsok.RepeatedIfExceptionsTest;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static io.delta.flink.utils.DeltaTestUtils.buildCluster;
import static io.delta.flink.utils.DeltaTestUtils.verifyDeltaTable;
import static io.delta.flink.utils.ExecutionITCaseTestConstants.ALL_DATA_TABLE_COLUMN_NAMES;
import static io.delta.flink.utils.ExecutionITCaseTestConstants.ALL_DATA_TABLE_COLUMN_TYPES;
import static io.delta.flink.utils.ExecutionITCaseTestConstants.ALL_DATA_TABLE_RECORD_COUNT;
import static io.delta.flink.utils.ExecutionITCaseTestConstants.DATA_COLUMN_NAMES;
import static io.delta.flink.utils.ExecutionITCaseTestConstants.DATA_COLUMN_TYPES;
import static io.delta.flink.utils.ExecutionITCaseTestConstants.LARGE_TABLE_ALL_COLUMN_NAMES;
import static io.delta.flink.utils.ExecutionITCaseTestConstants.LARGE_TABLE_ALL_COLUMN_TYPES;
import static io.delta.flink.utils.ExecutionITCaseTestConstants.LARGE_TABLE_RECORD_COUNT;
import static io.delta.flink.utils.ExecutionITCaseTestConstants.SMALL_TABLE_COUNT;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.hamcrest.core.IsNot.not;
import static org.junit.jupiter.api.Assertions.assertAll;

import io.delta.standalone.Snapshot;
import io.delta.standalone.data.CloseableIterator;
import io.delta.standalone.data.RowRecord;

public class DeltaEndToEndExecutionITCaseTest {

    private static final Logger LOG =
        LoggerFactory.getLogger(DeltaEndToEndExecutionITCaseTest.class);

    private static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();

    private static final int PARALLELISM = 4;

    private final MiniClusterWithClientResource miniClusterResource = buildCluster(PARALLELISM);

    private String sourceTablePath;

    private String sinkTablePath;

    @BeforeAll
    public static void beforeAll() throws IOException {
        TMP_FOLDER.create();
    }

    @AfterAll
    public static void afterAll() {
        TMP_FOLDER.delete();
    }

    @BeforeEach
    public void setUp() {
        try {
            miniClusterResource.before();

            sourceTablePath = TMP_FOLDER.newFolder().getAbsolutePath();
            sinkTablePath = TMP_FOLDER.newFolder().getAbsolutePath();

        } catch (Exception e) {
            throw new RuntimeException("Weren't able to setup the test dependencies", e);
        }
    }

    @AfterEach
    public void afterEach() {
        miniClusterResource.after();
    }

    @ParameterizedRepeatedIfExceptionsTest(
        suspend = 2000L,
        repeats = 3,
        name = "{index}: FailoverType = [{0}]"
    )
    @EnumSource(FailoverType.class)
    public void testEndToEndBoundedStream(FailoverType failoverType) throws Exception {
        DeltaTestUtils.initTestForNonPartitionedLargeTable(sourceTablePath);

        // Making sure that we are using path with schema to file system "file://"
        Configuration hadoopConfiguration = DeltaTestUtils.getConfigurationWithMockFs();

        Path sourceTablePath = Path.fromLocalFile(new File(this.sourceTablePath));
        Path sinkTablePath = Path.fromLocalFile(new File(this.sinkTablePath));

        assertThat(sinkTablePath.toUri().getScheme(), equalTo("file"));
        assertThat(sinkTablePath.toUri().getScheme(), equalTo("file"));

        DeltaSource<RowData> deltaSource = DeltaSource.forBoundedRowData(
                sourceTablePath,
                hadoopConfiguration
            )
            .build();

        RowType rowType = RowType.of(LARGE_TABLE_ALL_COLUMN_TYPES, LARGE_TABLE_ALL_COLUMN_NAMES);
        DeltaSinkInternal<RowData> deltaSink = DeltaSink.forRowData(
                sinkTablePath,
                hadoopConfiguration,
                rowType)
            .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(PARALLELISM);
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5, 1000));

        DataStream<RowData> stream =
            env.fromSource(deltaSource, WatermarkStrategy.noWatermarks(), "delta-source");
        stream.sinkTo(deltaSink);

        DeltaTestUtils.testBoundedStream(
            failoverType,
            (FailCheck) readRows -> readRows == LARGE_TABLE_RECORD_COUNT / 2,
            stream,
            miniClusterResource
        );

        verifyDeltaTable(this.sinkTablePath, rowType, LARGE_TABLE_RECORD_COUNT);
    }

    @ParameterizedRepeatedIfExceptionsTest(
        suspend = 2000L,
        repeats = 3,
        name = "{index}: FailoverType = [{0}]"
    )
    @EnumSource(FailoverType.class)
    public void testEndToEndContinuousStream(FailoverType failoverType) throws Exception {
        DeltaTestUtils.initTestForNonPartitionedTable(sourceTablePath);

        // Making sure that we are using path with schema to file system "file://"
        Configuration hadoopConfiguration = DeltaTestUtils.getConfigurationWithMockFs();

        Path sourceTablePath = Path.fromLocalFile(new File(this.sourceTablePath));
        Path sinkTablePath = Path.fromLocalFile(new File(this.sinkTablePath));

        assertThat(sinkTablePath.toUri().getScheme(), equalTo("file"));
        assertThat(sinkTablePath.toUri().getScheme(), equalTo("file"));

        DeltaSource<RowData> deltaSource = DeltaSource.forContinuousRowData(
                sourceTablePath,
                hadoopConfiguration
            )
            .build();

        RowType rowType = RowType.of(DATA_COLUMN_TYPES, DATA_COLUMN_NAMES);
        DeltaSinkInternal<RowData> deltaSink = DeltaSink.forRowData(
                sinkTablePath,
                hadoopConfiguration,
                rowType)
            .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(PARALLELISM);
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5, 1000));
        env.enableCheckpointing(100);

        DataStream<RowData> stream =
            env.fromSource(deltaSource, WatermarkStrategy.noWatermarks(), "delta-source");
        stream.sinkTo(deltaSink);

        int numberOfTableUpdateBulks = 5;
        int rowsPerTableUpdate = 5;
        int expectedRowCount = SMALL_TABLE_COUNT + numberOfTableUpdateBulks * rowsPerTableUpdate;

        TestDescriptor testDescriptor = DeltaTestUtils.prepareTableUpdates(
            deltaSource.getTablePath().toUri().toString(),
            RowType.of(DATA_COLUMN_TYPES, DATA_COLUMN_NAMES),
            SMALL_TABLE_COUNT,
            new TableUpdateDescriptor(numberOfTableUpdateBulks, rowsPerTableUpdate)
        );

        DeltaTestUtils.testContinuousStream(
            failoverType,
            testDescriptor,
            (FailCheck) readRows -> readRows == expectedRowCount/ 2,
            stream,
            miniClusterResource
        );

        verifyDeltaTable(this.sinkTablePath, rowType, expectedRowCount);
    }

    @RepeatedIfExceptionsTest(suspend = 2000L, repeats = 3)
    public void testEndToEndReadAllDataTypes() throws Exception {

        // this test uses test-non-partitioned-delta-table-alltypes table. See README.md from
        // table's folder for detail information about this table.
        DeltaTestUtils.initTestForAllDataTypes(sourceTablePath);

        // Making sure that we are using path with schema to file system "file://"
        Configuration hadoopConfiguration = DeltaTestUtils.getConfigurationWithMockFs();

        Path sourceTablePath = Path.fromLocalFile(new File(this.sourceTablePath));
        Path sinkTablePath = Path.fromLocalFile(new File(this.sinkTablePath));

        assertThat(sinkTablePath.toUri().getScheme(), equalTo("file"));
        assertThat(sinkTablePath.toUri().getScheme(), equalTo("file"));

        DeltaSource<RowData> deltaSource = DeltaSource.forBoundedRowData(
                sourceTablePath,
                hadoopConfiguration
            )
            .build();

        RowType rowType = RowType.of(ALL_DATA_TABLE_COLUMN_TYPES, ALL_DATA_TABLE_COLUMN_NAMES);
        DeltaSinkInternal<RowData> deltaSink = DeltaSink.forRowData(
                sinkTablePath,
                hadoopConfiguration,
                rowType)
            .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(PARALLELISM);
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5, 1000));

        DataStream<RowData> stream =
            env.fromSource(deltaSource, WatermarkStrategy.noWatermarks(), "delta-source");
        stream.sinkTo(deltaSink);

        DeltaTestUtils.testBoundedStream(stream, miniClusterResource);

        Snapshot snapshot = verifyDeltaTable(
            this.sinkTablePath,
            rowType,
            ALL_DATA_TABLE_RECORD_COUNT
        );

        assertRowsFromSnapshot(snapshot);
    }

    /**
     * Read entire snapshot using delta standalone and check every column.
     * @param snapshot {@link Snapshot} to read data from.
     */
    private void assertRowsFromSnapshot(Snapshot snapshot) throws IOException {

        final AtomicInteger index = new AtomicInteger(0);
        try(CloseableIterator<RowRecord> iterator = snapshot.open()) {
            while (iterator.hasNext()) {
                final int i = index.getAndIncrement();

                BigDecimal expectedBigDecimal = BigDecimal.valueOf((double) i).setScale(18);

                RowRecord row = iterator.next();
                LOG.info("Row Content: " + row.toString());
                assertAll(() -> {
                        assertThat(
                            row.getByte(ALL_DATA_TABLE_COLUMN_NAMES[0]),
                            equalTo(new Integer(i).byteValue())
                        );
                        assertThat(
                            row.getShort(ALL_DATA_TABLE_COLUMN_NAMES[1]),
                            equalTo((short) i)
                        );
                        assertThat(row.getInt(ALL_DATA_TABLE_COLUMN_NAMES[2]), equalTo(i));
                        assertThat(
                            row.getDouble(ALL_DATA_TABLE_COLUMN_NAMES[3]),
                            equalTo(new Integer(i).doubleValue())
                        );
                        assertThat(
                            row.getFloat(ALL_DATA_TABLE_COLUMN_NAMES[4]),
                            equalTo(new Integer(i).floatValue())
                        );

                        // In Source Table this column was generated as: BigInt(x)
                        assertThat(
                            row.getBigDecimal(ALL_DATA_TABLE_COLUMN_NAMES[5]),
                            equalTo(expectedBigDecimal)
                        );

                        // In Source Table this column was generated as: BigDecimal(x),
                        // There is a problem with parquet library used by delta standalone when
                        // reading BigDecimal values. The issue should be resolved
                        // after https://github.com/delta-io/connectors/pull/303
                        if (i > 0) {
                            assertThat(
                                row.getBigDecimal(ALL_DATA_TABLE_COLUMN_NAMES[6]),
                                not(equalTo(expectedBigDecimal))
                            );
                        }

                        // same value for all columns
                        assertThat(
                            row.getTimestamp(ALL_DATA_TABLE_COLUMN_NAMES[7])
                                .toLocalDateTime().toInstant(ZoneOffset.UTC),
                            equalTo(Timestamp.valueOf("2022-06-14 18:54:24.547557")
                                .toLocalDateTime().toInstant(ZoneOffset.UTC))
                        );
                        assertThat(
                            row.getString(ALL_DATA_TABLE_COLUMN_NAMES[8]),
                            equalTo(String.valueOf(i))
                        );

                        // same value for all columns
                        assertThat(row.getBoolean(ALL_DATA_TABLE_COLUMN_NAMES[9]), equalTo(true));
                    }
                );
            }
        }
    }
}
