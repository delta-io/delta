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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
class RowDataBoundedDeltaSourceBuilderTest extends RowDataDeltaSourceBuilderTestBase {

    private static final Logger LOG =
        LoggerFactory.getLogger(RowDataBoundedDeltaSourceBuilderTest.class);

    @AfterEach
    public void afterEach() {
        closeDeltaLogStatic();
    }

    ///////////////////////////////
    //  Bounded-only test cases  //
    ///////////////////////////////

    @Test
    public void shouldCreateSource() {

        when(deltaLog.snapshot()).thenReturn(headSnapshot);

        StructField[] schema = {new StructField("col1", new StringType())};
        mockDeltaTableForSchema(schema);

        DeltaSource<RowData> source = DeltaSource.forBoundedRowData(
                new Path(TABLE_PATH),
                DeltaTestUtils.getHadoopConf())
            .build();

        assertThat(source, notNullValue());
        assertThat(source.getBoundedness(), equalTo(Boundedness.BOUNDED));
    }

    /**
     * Test for versionAsOf.
     * This tests also checks option's value type conversion.
     */
    @ParameterizedTest(name = "{index}: VersionAsOf = {0}")
    @ValueSource(ints = {0, 10})
    public void shouldCreateSourceForVersionAsOf(int versionAsOf) {
        when(deltaLog.getSnapshotForVersionAsOf(versionAsOf)).thenReturn(headSnapshot);

        StructField[] schema = {new StructField("col1", new StringType())};
        mockDeltaTableForSchema(schema);

        String versionAsOfKey = DeltaSourceOptions.VERSION_AS_OF.key();
        List<RowDataBoundedDeltaSourceBuilder> builders = Arrays.asList(
            // set via dedicated method
            getBuilderAllColumns().versionAsOf(versionAsOf),

            // set via generic option(int)
            getBuilderAllColumns().option(versionAsOfKey, versionAsOf),

            // set via generic option(long)
            getBuilderAllColumns().option(versionAsOfKey, (long) versionAsOf),

            // set via generic option(String)
            getBuilderAllColumns().option(versionAsOfKey, String.valueOf(versionAsOf))
        );

        assertAll(() -> {
            for (RowDataBoundedDeltaSourceBuilder builder : builders) {
                DeltaSource<RowData> source = builder.build();

                assertThat(source, notNullValue());
                assertThat(source.getBoundedness(), equalTo(Boundedness.BOUNDED));
                assertThat(source.getSourceConfiguration()
                    .getValue(DeltaSourceOptions.VERSION_AS_OF), equalTo((long) versionAsOf));
            }
            // as many calls as we had builders
            verify(deltaLog, times(builders.size())).getSnapshotForVersionAsOf(versionAsOf);
        });
    }

    /**
     * Test for versionAsOf.
     * This tests also checks option's value type conversion.
     */
    @Test
    public void shouldThrowOnSourceWithInvalidVersionAsOf() {

        String versionAsOfKey = DeltaSourceOptions.VERSION_AS_OF.key();
        List<Executable> builders = Arrays.asList(
            // set via dedicated builder method
            () -> getBuilderAllColumns().versionAsOf(-1),

            // set via generic option(String)
            () -> getBuilderAllColumns()
                .option(versionAsOfKey, "foo"),

            // set via generic option(int)
            () -> getBuilderAllColumns()
                .option(versionAsOfKey, -1),

            // set via generic option(object)
            () -> getBuilderAllColumns()
                .option(versionAsOfKey, null)
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
                        .allMatch(message ->
                            message.contains("class java.lang.NumberFormatException") ||
                            message.contains("class java.lang.IllegalArgumentException")
                        ),
                    equalTo(true)
                );
            }
        });
    }

    /**
     * Test for timestampAsOf
     * This tests also checks option's value type conversion.
     */
    @Test
    public void shouldCreateSourceForTimestampAsOf() {
        String timestamp = "2022-02-24T04:55:00.001";
        long timestampAsOf = TimestampFormatConverter.convertToTimestamp(timestamp);
        when(deltaLog.getSnapshotForTimestampAsOf(timestampAsOf)).thenReturn(headSnapshot);

        StructField[] schema = {new StructField("col1", new StringType())};
        mockDeltaTableForSchema(schema);

        List<RowDataBoundedDeltaSourceBuilder> builders = Arrays.asList(
            // set via dedicated method
            getBuilderAllColumns().timestampAsOf(timestamp),

            // set via generic option(String)
            getBuilderAllColumns().option(DeltaSourceOptions.TIMESTAMP_AS_OF.key(), timestamp)
        );

        assertAll(() -> {
            for (RowDataBoundedDeltaSourceBuilder builder : builders) {
                DeltaSource<RowData> source = builder.build();
                assertThat(source, notNullValue());
                assertThat(source.getBoundedness(), equalTo(Boundedness.BOUNDED));
                assertThat(source.getSourceConfiguration()
                    .getValue(DeltaSourceOptions.TIMESTAMP_AS_OF), equalTo(timestampAsOf));
            }
            // as many calls as we had builders
            verify(deltaLog, times(builders.size())).getSnapshotForTimestampAsOf(timestampAsOf);
        });
    }

    @Test
    public void shouldThrowOnSourceWithInvalidTimestampAsOf() {

        String timestampAsOfKey = DeltaSourceOptions.TIMESTAMP_AS_OF.key();
        List<Executable> builders = Arrays.asList(
            // set via dedicated method
            () -> getBuilderAllColumns().timestampAsOf("not_a_date"),

            // set via generic option(int)
            () -> getBuilderAllColumns()
                .option(timestampAsOfKey, 10),

            // set via generic option(long)
            () -> getBuilderAllColumns()
                .option(timestampAsOfKey, 10L),

            // set via generic option(boolean)
            () -> getBuilderAllColumns()
                .option(timestampAsOfKey, true),

            // set via generic option(String)
            () -> getBuilderAllColumns().option(timestampAsOfKey, "not_a_date")
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
                        .stream().allMatch(message -> message.contains(
                            "class java.time.format.DateTimeParseException"
                        )),
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
            getBuilderWithOption(DeltaSourceOptions.IGNORE_CHANGES, true),
            getBuilderWithOption(DeltaSourceOptions.IGNORE_DELETES, true),
            getBuilderWithOption(DeltaSourceOptions.UPDATE_CHECK_INTERVAL, 1000L),
            getBuilderWithOption(DeltaSourceOptions.UPDATE_CHECK_INITIAL_DELAY, 1000L),
            getBuilderWithOption(DeltaSourceOptions.STARTING_TIMESTAMP, "2022-02-24T04:55:00.001"),
            getBuilderWithOption(DeltaSourceOptions.STARTING_VERSION, "Latest")
        );
    }

    @Override
    protected RowDataBoundedDeltaSourceBuilder getBuilderWithOption(
            DeltaConfigOption<?> option,
            Object value) {
        RowDataBoundedDeltaSourceBuilder builder =
            DeltaSource.forBoundedRowData(
                new Path(TABLE_PATH),
                DeltaTestUtils.getHadoopConf()
            );

        return (RowDataBoundedDeltaSourceBuilder) setOptionOnBuilder(option.key(), value, builder);
    }

    @Override
    protected RowDataBoundedDeltaSourceBuilder getBuilderWithNulls() {
        return DeltaSource.forBoundedRowData(
            null,
            null
        );
    }

    @Override
    protected RowDataBoundedDeltaSourceBuilder getBuilderForColumns(String[] columnNames) {
        return DeltaSource.forBoundedRowData(
                new Path(TABLE_PATH),
                DeltaTestUtils.getHadoopConf()
            )
            .columnNames((columnNames != null) ? Arrays.asList(columnNames) : null);
    }

    @Override
    protected RowDataBoundedDeltaSourceBuilder getBuilderAllColumns() {
        return DeltaSource.forBoundedRowData(
                new Path(TABLE_PATH),
            DeltaTestUtils.getHadoopConf()
            );
    }

    @Override
    protected RowDataBoundedDeltaSourceBuilder getBuilderWithMutuallyExcludedOptions() {
        return DeltaSource.forBoundedRowData(
                new Path(TABLE_PATH),
                DeltaTestUtils.getHadoopConf()
            )
            .versionAsOf(10)
            .timestampAsOf("2022-02-24T04:55:00.001");
    }

    @Override
    protected RowDataBoundedDeltaSourceBuilder getBuilderWithGenericMutuallyExcludedOptions() {
        return DeltaSource.forBoundedRowData(
                new Path(TABLE_PATH),
                DeltaTestUtils.getHadoopConf()
            )
            .option(DeltaSourceOptions.VERSION_AS_OF.key(), 10)
            .option(
                DeltaSourceOptions.TIMESTAMP_AS_OF.key(),
                "2022-02-24T04:55:00.001"
            );
    }

    @Override
    protected RowDataBoundedDeltaSourceBuilder
        getBuilderWithNullMandatoryFieldsAndExcludedOption() {
        return DeltaSource.forBoundedRowData(
                null,
                DeltaTestUtils.getHadoopConf()
            )
            .timestampAsOf("2022-02-24T04:55:00.001")
            .option(DeltaSourceOptions.VERSION_AS_OF.key(), 10);
    }


}
