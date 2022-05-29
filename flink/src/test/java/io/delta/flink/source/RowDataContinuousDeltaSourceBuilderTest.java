package io.delta.flink.source;

import java.util.Arrays;

import io.delta.flink.sink.utils.DeltaSinkTestUtils;
import io.delta.flink.source.internal.DeltaSourceOptions;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.data.RowData;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
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
                DeltaSinkTestUtils.getHadoopConf())
            .build();

        assertThat(boundedSource, notNullValue());
        assertThat(boundedSource.getBoundedness(), equalTo(Boundedness.CONTINUOUS_UNBOUNDED));
    }

    @Test
    public void shouldCreateSourceWithOptions() {

        when(deltaLog.getSnapshotForVersionAsOf(10)).thenReturn(headSnapshot);

        StructField[] schema = {new StructField("col1", new StringType())};
        mockDeltaTableForSchema(schema);

        DeltaSource<RowData> boundedSource = DeltaSource.forContinuousRowData(
                new Path(TABLE_PATH),
                DeltaSinkTestUtils.getHadoopConf())
            .option(DeltaSourceOptions.STARTING_VERSION.key(), "10")
            .build();

        assertThat(boundedSource, notNullValue());
        assertThat(boundedSource.getBoundedness(), equalTo(Boundedness.CONTINUOUS_UNBOUNDED));
        assertThat(boundedSource.getSourceConfiguration()
            .getValue(DeltaSourceOptions.STARTING_VERSION), equalTo("10"));
    }

    //////////////////////////////////////////////////////////////
    // Overridden parent methods for tests in base parent class //
    //////////////////////////////////////////////////////////////

    @Override
    protected <T> RowDataContinuousDeltaSourceBuilder getBuilderWithOption(
        ConfigOption<T> option,
        T value) {
        RowDataContinuousDeltaSourceBuilder builder =
            DeltaSource.forContinuousRowData(
                new Path(TABLE_PATH),
                DeltaSinkTestUtils.getHadoopConf()
            );

        return (RowDataContinuousDeltaSourceBuilder) setOptionOnBuilder(option, value, builder);
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
                DeltaSinkTestUtils.getHadoopConf()
            )
            .columnNames((columnNames != null) ? Arrays.asList(columnNames) : null);
    }

    @Override
    protected RowDataContinuousDeltaSourceBuilder getBuilderWithMutuallyExcludedOptions() {
        return DeltaSource.forContinuousRowData(
                new Path(TABLE_PATH),
                DeltaSinkTestUtils.getHadoopConf()
            )
            .startingVersion(10)
            .startingTimestamp("2022-02-24T04:55:00.001");
    }

    @Override
    protected RowDataContinuousDeltaSourceBuilder getBuilderWithGenericMutuallyExcludedOptions() {
        return DeltaSource.forContinuousRowData(
                new Path(TABLE_PATH),
                DeltaSinkTestUtils.getHadoopConf()
            )
            .option(DeltaSourceOptions.STARTING_VERSION.key(), 10)
            .option(DeltaSourceOptions.STARTING_TIMESTAMP.key(), "2022-02-24T04:55:00.001");
    }

    @Override
    protected RowDataContinuousDeltaSourceBuilder
        getBuilderWithNullMandatoryFieldsAndExcludedOption() {
        return DeltaSource.forContinuousRowData(
                null,
                DeltaSinkTestUtils.getHadoopConf()
            )
            .startingTimestamp("2022-02-24T04:55:00.001")
            .option(DeltaSourceOptions.STARTING_VERSION.key(), 10);
    }
}
