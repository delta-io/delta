package io.delta.flink.source;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.time.ZoneOffset;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import io.delta.flink.source.internal.enumerator.supplier.TimestampFormatConverter;
import io.delta.flink.utils.DeltaTestUtils;
import io.delta.flink.utils.ExecutionITCaseTestConstants;
import io.delta.flink.utils.TestDescriptor;
import io.github.artsok.RepeatedIfExceptionsTest;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.TestLogger;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static io.delta.flink.utils.DeltaTestUtils.buildCluster;
import static io.delta.flink.utils.ExecutionITCaseTestConstants.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertThrows;

public abstract class DeltaSourceITBase extends TestLogger {

    private static final Logger LOG = LoggerFactory.getLogger(DeltaSourceITBase.class);

    protected static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();

    protected static final int PARALLELISM = 4;

    protected final MiniClusterWithClientResource miniClusterResource = buildCluster(PARALLELISM);

    /**
     * Schema for this table has only {@link ExecutionITCaseTestConstants#DATA_COLUMN_NAMES}
     * of type {@link ExecutionITCaseTestConstants#DATA_COLUMN_TYPES} columns.
     */
    protected String nonPartitionedTablePath;

    /**
     * Schema for this table contains data columns
     * {@link ExecutionITCaseTestConstants#DATA_COLUMN_NAMES} and col1, col2
     * partition columns. Types of data columns are
     * {@link ExecutionITCaseTestConstants#DATA_COLUMN_TYPES}
     */
    protected String partitionedTablePath;

    /**
     * Schema for this table has only
     * {@link ExecutionITCaseTestConstants#LARGE_TABLE_ALL_COLUMN_NAMES} of type
     * {@link ExecutionITCaseTestConstants#LARGE_TABLE_ALL_COLUMN_TYPES} columns.
     * Column types are long, long, String
     */
    protected String nonPartitionedLargeTablePath;

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

            DeltaTestUtils.initTestForPartitionedTable(partitionedTablePath);
            DeltaTestUtils.initTestForNonPartitionedTable(nonPartitionedTablePath);
            DeltaTestUtils.initTestForNonPartitionedLargeTable(
                nonPartitionedLargeTablePath);
        } catch (Exception e) {
            throw new RuntimeException("Weren't able to setup the test dependencies", e);
        }
    }

    public void after() {
        miniClusterResource.after();
    }

    @RepeatedIfExceptionsTest(suspend = 2000L, repeats = 3)
    public void testReadPartitionedTableSkippingPartitionColumns() throws Exception {

        // GIVEN, the full schema of used table is {name, surname, age} + col1, col2 as a partition
        // columns. The size of version 0 is two rows.
        DeltaSource<RowData> deltaSource = initSourceForColumns(
            partitionedTablePath,
            DATA_COLUMN_NAMES
        );

        // WHEN
        List<RowData> resultData = this.testSource(
            deltaSource,
            new TestDescriptor(partitionedTablePath, 2)
        );

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

    @RepeatedIfExceptionsTest(suspend = 2000L, repeats = 3)
    public void testReadOnlyPartitionColumns() throws Exception {

        // GIVEN, the full schema of used table is {name, surname, age} + col1, col2 as a partition
        // columns. The size of version 0 is two rows.
        DeltaSource<RowData> deltaSource = initSourceForColumns(
            partitionedTablePath,
            new String[]{"col1", "col2"}
        );

        // WHEN
        List<RowData> resultData = this.testSource(
            deltaSource,
            new TestDescriptor(partitionedTablePath, 2)
        );

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

    @RepeatedIfExceptionsTest(suspend = 2000L, repeats = 3)
    public void testWithOnePartition() throws Exception {

        // GIVEN, the full schema of used table is {name, surname, age} + col1, col2 as a partition
        // columns. The size of version 0 is two rows.
        DeltaSource<RowData> deltaSource = initSourceForColumns(
            partitionedTablePath,
            new String[]{"surname", "age", "col2"} // sipping [name] column
        );

        // WHEN
        List<RowData> resultData = this.testSource(
            deltaSource,
            new TestDescriptor(partitionedTablePath, 2)
        );

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

    @RepeatedIfExceptionsTest(suspend = 2000L, repeats = 3)
    public void testWithBothPartitions() throws Exception {

        // GIVEN, the full schema of used table is {name, surname, age} + col1, col2 as a partition
        // columns. The size of version 0 is two rows.
        DeltaSource<RowData> deltaSource = initSourceAllColumns(partitionedTablePath);

        // WHEN
        List<RowData> resultData = this.testSource(
            deltaSource,
            new TestDescriptor(partitionedTablePath, 2)
        );

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

    @RepeatedIfExceptionsTest(suspend = 2000L, repeats = 3)
    public void shouldReadTableWithAllDataTypes() throws Exception {
        String sourceTablePath = TMP_FOLDER.newFolder().getAbsolutePath();
        DeltaTestUtils.initTestForAllDataTypes(sourceTablePath);

        DeltaSource<RowData> deltaSource = initSourceAllColumns(sourceTablePath);

        List<RowData> rowData = this.testSource(
            deltaSource,
            new TestDescriptor(sourceTablePath, ALL_DATA_TABLE_RECORD_COUNT)
        );

        assertThat(
            "Source read different number of records than expected.",
            rowData.size(),
            equalTo(5)
        );

        Iterator<RowData> rowDataIterator = rowData.iterator();
        AtomicInteger index = new AtomicInteger(0);
        while (rowDataIterator.hasNext()) {
            int i = index.getAndIncrement();
            RowData row = rowDataIterator.next();
            LOG.info("Row Content: " + row);
            assertRowValues(i, row);
        }
    }

    private void assertRowValues(int i, RowData row) {
        assertAll(() -> {
                assertThat(row.getByte(0), equalTo(new Integer(i).byteValue()));
                assertThat(row.getShort(1), equalTo((short) i));
                assertThat(row.getInt(2), equalTo(i));
                assertThat(row.getDouble(3), equalTo(new Integer(i).doubleValue()));
                assertThat(row.getFloat(4), equalTo(new Integer(i).floatValue()));
                assertThat(
                    row.getDecimal(5, 1, 1).toBigDecimal().setScale(18),
                    equalTo(BigDecimal.valueOf(i).setScale(18))
                );
                assertThat(
                    row.getDecimal(6, 1, 1).toBigDecimal().setScale(18),
                    equalTo(BigDecimal.valueOf(i).setScale(18))
                );

                // same value for all columns
                assertThat(
                    row.getTimestamp(7, 18).toLocalDateTime().toInstant(ZoneOffset.UTC),
                    equalTo(Timestamp.valueOf("2022-06-14 18:54:24.547557")
                        .toLocalDateTime().toInstant(ZoneOffset.UTC))
                );
                assertThat(row.getString(8).toString(), equalTo(String.valueOf(i)));

                // same value for all columns
                assertThat(row.getBoolean(9), equalTo(true));
            }
        );
    }

    protected abstract DeltaSource<RowData> initSourceAllColumns(String tablePath);

    protected abstract DeltaSource<RowData> initSourceForColumns(
        String tablePath,
        String[] columnNames);

    /**
     * Test a source without failover setup.
     * @param deltaSource delta source to test.
     * @param testDescriptor A {@link TestDescriptor} for this test run.
     * @return A {@link List} of produced records.
     */
    protected abstract List<RowData> testSource(
            DeltaSource<RowData> deltaSource,
            TestDescriptor testDescriptor) throws Exception;

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

    /**
     * Changes last modification time for delta log files.
     *
     * @param sourceTablePath  Path to delta log to change last modification time.
     * @param lastModifyValues An array of times to which last modification time should be change
     *                         to. The timestamps must be in format of `2022-02-24 04:55:00`
     */
    protected void changeDeltaLogLastModifyTimestamp(
            String sourceTablePath,
            String[] lastModifyValues) throws IOException {

        List<java.nio.file.Path> sortedLogFiles =
            Files.list(Paths.get(sourceTablePath + "/_delta_log"))
                .filter(file -> file.getFileName().toUri().toString().endsWith(".json"))
                .sorted()
                .collect(Collectors.toList());

        assertThat(
            "Delta log for table " + sourceTablePath + " size, does not match"
                + " test's last modify argument size " + lastModifyValues.length,
            sortedLogFiles.size(),
            equalTo(lastModifyValues.length)
        );

        int i = 0;
        for (java.nio.file.Path logFile : sortedLogFiles) {
            String timestampAsOfValue = lastModifyValues[i++];
            long toTimestamp = TimestampFormatConverter.convertToTimestamp(timestampAsOfValue);
            LOG.info(
                "Changing Last Modified timestamp on file " + logFile
                    + " to " + timestampAsOfValue + " -> " + timestampAsOfValue
            );
            assertThat(
                "Unable to modify " + logFile + " last modified timestamp.",
                logFile.toFile().setLastModified(toTimestamp), equalTo(true));
        }
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
}
