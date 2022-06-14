package io.delta.flink.source;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import io.delta.flink.source.internal.DeltaSourceOptions;
import io.delta.flink.source.internal.builder.DeltaConfigOption;
import io.delta.flink.source.internal.builder.DeltaSourceBuilderBase;
import io.delta.flink.source.internal.enumerator.supplier.TimestampFormatConverter;
import io.delta.flink.source.internal.exceptions.DeltaSourceValidationException;
import io.delta.flink.utils.DeltaTestUtils;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.data.RowData;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.junit.jupiter.MockitoExtension;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.delta.standalone.types.StringType;
import io.delta.standalone.types.StructField;

@ExtendWith(MockitoExtension.class)
class RowDataContinuousDeltaSourceBuilderTest extends RowDataDeltaSourceBuilderTestBase {

    @AfterEach
    public void afterEach() {
        closeDeltaLogStatic();
    }

    ////////////////////////////////
    // Continuous-only test cases //
    ////////////////////////////////

    @Test
    public void shouldCreateSource() {
        when(deltaLog.snapshot()).thenReturn(headSnapshot);

        StructField[] schema = {new StructField("col1", new StringType())};
        mockDeltaTableForSchema(schema);

        DeltaSource<RowData> boundedSource = DeltaSource.forContinuousRowData(
                new Path(TABLE_PATH),
                DeltaTestUtils.getHadoopConf())
            .build();

        assertThat(boundedSource, notNullValue());
        assertThat(boundedSource.getBoundedness(), equalTo(Boundedness.CONTINUOUS_UNBOUNDED));
    }

    @ParameterizedTest(name = "{index}: StartingVersion = {0}")
    @ValueSource(ints = {0, 10})
    public void shouldCreateSourceForStartingVersion(int startingVersion) {

        String stringStartingVersion = String.valueOf(startingVersion);

        when(deltaLog.getSnapshotForVersionAsOf(startingVersion)).thenReturn(headSnapshot);

        StructField[] schema = {new StructField("col1", new StringType())};
        mockDeltaTableForSchema(schema);

        String startingVersionKey = DeltaSourceOptions.STARTING_VERSION.key();
        List<RowDataContinuousDeltaSourceBuilder> builders = Arrays.asList(
            // set via dedicated method long
            getBuilderAllColumns().startingVersion(startingVersion),

            // set via dedicated method String
            getBuilderAllColumns().startingVersion(stringStartingVersion),

            // set via generic option(int) method
            getBuilderAllColumns().option(startingVersionKey, startingVersion),

            // set via generic option(long) method
            getBuilderAllColumns().option(startingVersionKey, (long) startingVersion),

            // set via generic option(int) String
            getBuilderAllColumns().option(startingVersionKey, stringStartingVersion)
        );

        assertAll(() -> {
            for (RowDataContinuousDeltaSourceBuilder builder : builders) {
                DeltaSource<RowData> source = builder.build();
                assertThat(source, notNullValue());
                assertThat(source.getBoundedness(), equalTo(Boundedness.CONTINUOUS_UNBOUNDED));
                assertThat(
                    source.getSourceConfiguration().getValue(DeltaSourceOptions.STARTING_VERSION),
                    equalTo(stringStartingVersion)
                );
            }
            // as many calls as we had builders
            verify(deltaLog, times(builders.size())).getSnapshotForVersionAsOf(startingVersion);
        });
    }

    @Test
    public void shouldThrowOnSourceWithInvalidStartingVersion() {

        String startingVersionKey = DeltaSourceOptions.STARTING_VERSION.key();
        List<Executable> builders = Arrays.asList(
            () -> getBuilderAllColumns().startingVersion("not_a_version"),
            () -> getBuilderAllColumns().option(startingVersionKey, "not_a_version"),
            () -> getBuilderAllColumns().option(startingVersionKey, ""),
            () -> getBuilderAllColumns().option(startingVersionKey, " "),
            () -> getBuilderAllColumns().option(startingVersionKey, true),
            () -> getBuilderAllColumns().option(startingVersionKey, -1),
            () -> getBuilderAllColumns().option(startingVersionKey, null)
        );

        // execute "option" on builder with invalid value.
        assertAll(() -> {
            for (Executable builderExecutable : builders) {
                DeltaSourceValidationException exception =
                    assertThrows(DeltaSourceValidationException.class, builderExecutable);
                LOG.info("Option Validation Exception: ", exception);
                assertThat(
                    exception
                        .getValidationMessages()
                        .stream().allMatch(
                            message -> message.contains(
                                "Illegal value used for [startingVersion] option. "
                                    + "Expected values are non-negative integers or \"latest\" "
                                    + "keyword (case insensitive). Used value was"
                            )
                        ),
                    equalTo(true)
                );
            }
        });
    }

    @Test
    public void shouldCreateSourceForStartingTimestamp() {
        String startingTimestamp = "2022-02-24T04:55:00.001";
        long long_startingTimestamp =
            TimestampFormatConverter.convertToTimestamp(startingTimestamp);

        long snapshotVersion = headSnapshot.getVersion();
        when(deltaLog.getVersionAtOrAfterTimestamp(long_startingTimestamp))
            .thenReturn(snapshotVersion);
        when(deltaLog.getSnapshotForVersionAsOf(snapshotVersion)).thenReturn(headSnapshot);

        StructField[] schema = {new StructField("col1", new StringType())};
        mockDeltaTableForSchema(schema);

        String startingTimestampKey = DeltaSourceOptions.STARTING_TIMESTAMP.key();
        List<RowDataContinuousDeltaSourceBuilder> builders = Arrays.asList(
            getBuilderAllColumns().startingTimestamp(startingTimestamp),
            getBuilderAllColumns().option(startingTimestampKey, startingTimestamp)
        );

        assertAll(() -> {
            for (RowDataContinuousDeltaSourceBuilder builder : builders) {
                DeltaSource<RowData> source = builder.build();
                assertThat(source, notNullValue());
                assertThat(source.getBoundedness(), equalTo(Boundedness.CONTINUOUS_UNBOUNDED));
                assertThat(source.getSourceConfiguration()
                        .getValue(DeltaSourceOptions.STARTING_TIMESTAMP),
                    equalTo(long_startingTimestamp));
            }
            // as many calls as we had builders
            verify(deltaLog, times(builders.size()))
                .getVersionAtOrAfterTimestamp(long_startingTimestamp);
            verify(deltaLog, times(builders.size())).getSnapshotForVersionAsOf(snapshotVersion);
        });
    }

    @Test
    public void shouldThrowOnSourceWithInvalidStartingTimestamp() {
        String timestamp = "not_a_date";

        List<Executable> builders = Arrays.asList(
            // set via dedicated method
            () -> getBuilderAllColumns().startingTimestamp(timestamp),

            // set via generic option(String)
            () -> getBuilderAllColumns()
                .option(DeltaSourceOptions.TIMESTAMP_AS_OF.key(), timestamp),

            () -> getBuilderAllColumns().option(DeltaSourceOptions.TIMESTAMP_AS_OF.key(), ""),

            () -> getBuilderAllColumns().option(DeltaSourceOptions.TIMESTAMP_AS_OF.key(), " "),

            () -> getBuilderAllColumns().option(DeltaSourceOptions.TIMESTAMP_AS_OF.key(), null)
        );

        // execute "set" or "option" on builder with invalid value.
        assertAll(() -> {
            for (Executable builderExecutable : builders) {
                DeltaSourceValidationException exception =
                    assertThrows(DeltaSourceValidationException.class, builderExecutable);
                LOG.info("Option Validation Exception: ", exception);
                assertThat(
                    exception
                        .getValidationMessages()
                        .stream()
                        .allMatch(
                            message -> message
                                .contains("class java.time.format.DateTimeParseException") ||
                                message.contains("class java.lang.IllegalArgumentException")
                        ),
                    equalTo(true)
                );
            }
        });
    }

    @Test
    public void shouldCreateSourceForUpdateCheckInterval() {

        long updateInterval = 10;
        String string_updateInterval = "10";

        when(deltaLog.snapshot()).thenReturn(headSnapshot);

        StructField[] schema = {new StructField("col1", new StringType())};
        mockDeltaTableForSchema(schema);

        String updateCheckIntervalKey = DeltaSourceOptions.UPDATE_CHECK_INTERVAL.key();
        List<RowDataContinuousDeltaSourceBuilder> builders = Arrays.asList(
            getBuilderAllColumns().updateCheckIntervalMillis(updateInterval),
            getBuilderAllColumns().option(updateCheckIntervalKey, updateInterval),
            getBuilderAllColumns().option(updateCheckIntervalKey, string_updateInterval)
        );

        assertAll(() -> {
            for (RowDataContinuousDeltaSourceBuilder builder : builders) {
                DeltaSource<RowData> source = builder.build();
                assertThat(source, notNullValue());
                assertThat(source.getBoundedness(), equalTo(Boundedness.CONTINUOUS_UNBOUNDED));
                assertThat(source.getSourceConfiguration()
                        .getValue(DeltaSourceOptions.UPDATE_CHECK_INTERVAL),
                    equalTo(updateInterval));
            }
        });
    }

    @Test
    public void shouldThrowOnSourceWithInvalidUpdateCheckInterval() {

        String updateCheckIntervalKey = DeltaSourceOptions.UPDATE_CHECK_INTERVAL.key();
        List<Executable> builders = Arrays.asList(
            () -> getBuilderAllColumns().option(updateCheckIntervalKey, "not_a_number"),
            () -> getBuilderAllColumns().option(updateCheckIntervalKey, ""),
            () -> getBuilderAllColumns().option(updateCheckIntervalKey, " "),
            () -> getBuilderAllColumns().option(updateCheckIntervalKey, null),
            () -> getBuilderAllColumns().option(updateCheckIntervalKey, true)
        );

        // execute "option" on builder with invalid value.
        assertAll(() -> {
            for (Executable builderExecutable : builders) {
                DeltaSourceValidationException exception =
                    assertThrows(DeltaSourceValidationException.class, builderExecutable);
                LOG.info("Option Validation Exception: ", exception);
                assertThat(
                    exception
                        .getValidationMessages()
                        .stream()
                        .allMatch(message ->
                            message.contains("class java.lang.NumberFormatException") ||
                                message.contains("class java.lang.IllegalArgumentException")
                        ),
                    equalTo(true)
                );
            }
        });
    }

    @Test
    public void shouldCreateSourceForIgnoreDeletes() {

        when(deltaLog.snapshot()).thenReturn(headSnapshot);

        StructField[] schema = {new StructField("col1", new StringType())};
        mockDeltaTableForSchema(schema);

        String ignoreDeletesKey = DeltaSourceOptions.IGNORE_DELETES.key();
        List<RowDataContinuousDeltaSourceBuilder> builders = Arrays.asList(
            getBuilderAllColumns().ignoreDeletes(true),
            getBuilderAllColumns().option(ignoreDeletesKey, true),
            getBuilderAllColumns().option(ignoreDeletesKey, "true")
        );

        assertAll(() -> {
            for (RowDataContinuousDeltaSourceBuilder builder : builders) {
                DeltaSource<RowData> source = builder.build();
                assertThat(source, notNullValue());
                assertThat(source.getBoundedness(), equalTo(Boundedness.CONTINUOUS_UNBOUNDED));
                assertThat(source.getSourceConfiguration()
                        .getValue(DeltaSourceOptions.IGNORE_DELETES),
                    equalTo(true));
            }
        });
    }

    @Test
    public void shouldThrowOnSourceWithInvalidIgnoreDeletes() {

        String ignoreDeletesKey = DeltaSourceOptions.IGNORE_DELETES.key();
        List<Executable> builders = Arrays.asList(
            () -> getBuilderAllColumns().option(ignoreDeletesKey, "not_a_boolean"),
            () -> getBuilderAllColumns().option(ignoreDeletesKey, " "),
            () -> getBuilderAllColumns().option(ignoreDeletesKey, ""),
            () -> getBuilderAllColumns().option(ignoreDeletesKey, null),
            () -> getBuilderAllColumns().option(ignoreDeletesKey, 1410)
        );

        // execute "option" on builder with invalid value.
        assertAll(() -> {
            for (Executable builderExecutable : builders) {
                DeltaSourceValidationException exception =
                    assertThrows(DeltaSourceValidationException.class, builderExecutable);
                LOG.info("Option Validation Exception: ", exception);
                assertThat(
                    exception
                        .getValidationMessages()
                        .stream()
                        .allMatch(message ->
                            message.contains(
                                "class java.lang.IllegalArgumentException - Illegal value used "
                                    + "for [ignoreDeletes] option. Expected values \"true\" or "
                                    + "\"false\" keywords (case insensitive) or boolean true, "
                                    + "false values. Used value was"
                            )
                        ),
                    equalTo(true)
                );
            }
        });
    }

    @Test
    public void shouldCreateSourceForIgnoreChanges() {

        when(deltaLog.snapshot()).thenReturn(headSnapshot);

        StructField[] schema = {new StructField("col1", new StringType())};
        mockDeltaTableForSchema(schema);

        String ignoreChangesKey = DeltaSourceOptions.IGNORE_CHANGES.key();
        List<RowDataContinuousDeltaSourceBuilder> builders = Arrays.asList(
            getBuilderAllColumns().ignoreChanges(true),
            getBuilderAllColumns().option(ignoreChangesKey, true),
            getBuilderAllColumns().option(ignoreChangesKey, "true")
        );

        assertAll(() -> {
            for (RowDataContinuousDeltaSourceBuilder builder : builders) {
                DeltaSource<RowData> source = builder.build();
                assertThat(source, notNullValue());
                assertThat(source.getBoundedness(), equalTo(Boundedness.CONTINUOUS_UNBOUNDED));
                assertThat(source.getSourceConfiguration()
                        .getValue(DeltaSourceOptions.IGNORE_CHANGES),
                    equalTo(true));
            }
        });
    }

    @Test
    public void shouldThrowOnSourceWithInvalidIgnoreChanges() {

        String ignoreChangesKey = DeltaSourceOptions.IGNORE_CHANGES.key();
        List<Executable> builders = Arrays.asList(
            () -> getBuilderAllColumns().option(ignoreChangesKey, "not_a_boolean"),
            () -> getBuilderAllColumns().option(ignoreChangesKey, ""),
            () -> getBuilderAllColumns().option(ignoreChangesKey, " "),
            () -> getBuilderAllColumns().option(ignoreChangesKey, null),
            () -> getBuilderAllColumns().option(ignoreChangesKey, 1410)
        );

        // execute "option" on builder with invalid value.
        assertAll(() -> {
            for (Executable builderExecutable : builders) {
                DeltaSourceValidationException exception =
                    assertThrows(DeltaSourceValidationException.class, builderExecutable);
                LOG.info("Option Validation Exception: ", exception);
                assertThat(
                    exception
                        .getValidationMessages()
                        .stream()
                        .allMatch(message ->
                            message.contains(
                                "class java.lang.IllegalArgumentException - Illegal value used "
                                    + "for [ignoreChanges] option. Expected values \"true\" or "
                                    + "\"false\" keywords (case insensitive) or boolean true, "
                                    + "false values. Used value was")
                        ),
                    equalTo(true)
                );
            }
        });
    }

    @Test
    public void shouldCreateSourceForUpdateCheckDelay() {

        long expectedUpdateCheckDelay = 10;

        when(deltaLog.snapshot()).thenReturn(headSnapshot);

        StructField[] schema = {new StructField("col1", new StringType())};
        mockDeltaTableForSchema(schema);

        String updateCheckDelayKey = DeltaSourceOptions.UPDATE_CHECK_INITIAL_DELAY.key();
        List<RowDataContinuousDeltaSourceBuilder> builders = Arrays.asList(
            // set via generic option(int) method.
            getBuilderAllColumns().option(updateCheckDelayKey, 10),

            // set via generic option(long) method.
            getBuilderAllColumns().option(updateCheckDelayKey, 10L),

            // set via generic option(String) method.
            getBuilderAllColumns().option(updateCheckDelayKey, "10")
        );

        assertAll(() -> {
            for (RowDataContinuousDeltaSourceBuilder builder : builders) {
                DeltaSource<RowData> source = builder.build();
                assertThat(source, notNullValue());
                assertThat(source.getBoundedness(), equalTo(Boundedness.CONTINUOUS_UNBOUNDED));
                assertThat(source.getSourceConfiguration()
                        .getValue(DeltaSourceOptions.UPDATE_CHECK_INITIAL_DELAY),
                    equalTo(expectedUpdateCheckDelay));
            }
        });
    }

    @Test
    public void shouldThrowOnSourceWithInvalidUpdateCheckDelay() {

        String updateCheckDelayKey = DeltaSourceOptions.UPDATE_CHECK_INITIAL_DELAY.key();
        List<Executable> builders = Arrays.asList(
            () -> getBuilderAllColumns().option(updateCheckDelayKey, "not_a_number"),
            () -> getBuilderAllColumns().option(updateCheckDelayKey, true)
        );

        // execute "option" on builder with invalid value.
        assertAll(() -> {
            for (Executable builderExecutable : builders) {
                DeltaSourceValidationException exception =
                    assertThrows(DeltaSourceValidationException.class, builderExecutable);
                LOG.info("Option Validation Exception: ", exception);
                assertThat(
                    exception
                        .getValidationMessages()
                        .stream()
                        .allMatch(message ->
                            message.contains("class java.lang.NumberFormatException - For input")
                        ),
                    equalTo(true)
                );
            }
        });
    }

    @Test
    public void shouldCreateSourceForParquetBatchSize() {

        int expectedParquetBatchSize = 100;

        when(deltaLog.snapshot()).thenReturn(headSnapshot);

        StructField[] schema = {new StructField("col1", new StringType())};
        mockDeltaTableForSchema(schema);

        String parquetBatchSize = DeltaSourceOptions.PARQUET_BATCH_SIZE.key();
        List<RowDataContinuousDeltaSourceBuilder> builders = Arrays.asList(
            // set via generic option(int) method.
            getBuilderAllColumns().option(parquetBatchSize, 100),

            // set via generic option(long) method.
            getBuilderAllColumns().option(parquetBatchSize, 100L),

            // set via generic option(string) method.
            getBuilderAllColumns().option(parquetBatchSize, "100")
        );

        assertAll(() -> {
            for (RowDataContinuousDeltaSourceBuilder builder : builders) {
                DeltaSource<RowData> source = builder.build();
                assertThat(source, notNullValue());
                assertThat(source.getBoundedness(), equalTo(Boundedness.CONTINUOUS_UNBOUNDED));
                assertThat(source.getSourceConfiguration()
                        .getValue(DeltaSourceOptions.PARQUET_BATCH_SIZE),
                    equalTo(expectedParquetBatchSize));
            }
        });
    }

    @Test
    public void shouldThrowOnSourceWithInvalidParquetBatchSize() {

        String parquetBatchSizeKey = DeltaSourceOptions.PARQUET_BATCH_SIZE.key();
        List<Executable> builders = Arrays.asList(
            () -> getBuilderAllColumns().option(parquetBatchSizeKey, "not_a_number"),
            () -> getBuilderAllColumns().option(parquetBatchSizeKey, ""),
            () -> getBuilderAllColumns().option(parquetBatchSizeKey, " "),
            () -> getBuilderAllColumns().option(parquetBatchSizeKey, null),
            () -> getBuilderAllColumns().option(parquetBatchSizeKey, true)
        );

        // execute "option" on builder with invalid value.
        assertAll(() -> {
            for (Executable builderExecutable : builders) {
                DeltaSourceValidationException exception =
                    assertThrows(DeltaSourceValidationException.class, builderExecutable);
                LOG.info("Option Validation Exception: ", exception);
                assertThat(
                    exception
                        .getValidationMessages()
                        .stream()
                        .allMatch(message ->
                            message.contains("class java.lang.NumberFormatException") ||
                                message.contains("class java.lang.IllegalArgumentException")
                        ),
                    equalTo(true)
                );
            }
        });
    }

    //////////////////////////////////////////////////////////////
    // Overridden parent methods for tests in base parent class //
    //////////////////////////////////////////////////////////////

    @Override
    public Collection<? extends DeltaSourceBuilderBase<?,?>> initBuildersWithInapplicableOptions() {
        return Arrays.asList(
            getBuilderWithOption(DeltaSourceOptions.VERSION_AS_OF, 10L),
            getBuilderWithOption(DeltaSourceOptions.TIMESTAMP_AS_OF, "2022-02-24T04:55:00.001")
        );
    }

    @Override
    protected RowDataContinuousDeltaSourceBuilder getBuilderWithOption(
            DeltaConfigOption<?> option,
            Object value) {
        RowDataContinuousDeltaSourceBuilder builder =
            DeltaSource.forContinuousRowData(
                new Path(TABLE_PATH),
                DeltaTestUtils.getHadoopConf()
            );

        return (RowDataContinuousDeltaSourceBuilder)
            setOptionOnBuilder(option.key(), value, builder);
    }

    @Override
    protected RowDataContinuousDeltaSourceBuilder getBuilderWithNulls() {
        return DeltaSource.forContinuousRowData(
            null,
            null
        );
    }

    @Override
    protected RowDataContinuousDeltaSourceBuilder getBuilderForColumns(String[] columnNames) {
        return DeltaSource.forContinuousRowData(
                new Path(TABLE_PATH),
                DeltaTestUtils.getHadoopConf()
            )
            .columnNames((columnNames != null) ? Arrays.asList(columnNames) : null);
    }

    @Override
    protected RowDataContinuousDeltaSourceBuilder getBuilderAllColumns() {
        return DeltaSource.forContinuousRowData(
            new Path(TABLE_PATH),
            DeltaTestUtils.getHadoopConf()
        );
    }

    @Override
    protected RowDataContinuousDeltaSourceBuilder getBuilderWithMutuallyExcludedOptions() {
        return DeltaSource.forContinuousRowData(
                new Path(TABLE_PATH),
                DeltaTestUtils.getHadoopConf()
            )
            .startingVersion(10)
            .startingTimestamp("2022-02-24T04:55:00.001");
    }

    @Override
    protected RowDataContinuousDeltaSourceBuilder getBuilderWithGenericMutuallyExcludedOptions() {
        return DeltaSource.forContinuousRowData(
                new Path(TABLE_PATH),
                DeltaTestUtils.getHadoopConf()
            )
            .option(DeltaSourceOptions.STARTING_VERSION.key(), 10)
            .option(
                DeltaSourceOptions.STARTING_TIMESTAMP.key(),"2022-02-24T04:55:00.001"
            );
    }

    @Override
    protected RowDataContinuousDeltaSourceBuilder
        getBuilderWithNullMandatoryFieldsAndExcludedOption() {
        return DeltaSource.forContinuousRowData(
                null,
                DeltaTestUtils.getHadoopConf()
            )
            .startingTimestamp("2022-02-24T04:55:00.001")
            .option(DeltaSourceOptions.STARTING_VERSION.key(), 10);
    }
}
