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
class RowDataBoundedDeltaSourceBuilderTest extends RowDataDeltaSourceBuilderTestBase {

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

        DeltaSource<RowData> boundedSource = DeltaSource.forBoundedRowData(
                new Path(TABLE_PATH),
                DeltaSinkTestUtils.getHadoopConf())
            .build();

        assertThat(boundedSource, notNullValue());
        assertThat(boundedSource.getBoundedness(), equalTo(Boundedness.BOUNDED));
    }

    @Test
    public void shouldCreateSourceWithOptions() {
        when(deltaLog.getSnapshotForVersionAsOf(10L)).thenReturn(headSnapshot);

        StructField[] schema = {new StructField("col1", new StringType())};
        mockDeltaTableForSchema(schema);

        DeltaSource<RowData> boundedSource = DeltaSource.forBoundedRowData(
                new Path(TABLE_PATH),
                DeltaSinkTestUtils.getHadoopConf())
            .option(DeltaSourceOptions.VERSION_AS_OF.key(), 10L)
            .build();

        assertThat(boundedSource, notNullValue());
        assertThat(boundedSource.getBoundedness(), equalTo(Boundedness.BOUNDED));
        assertThat(boundedSource.getSourceConfiguration()
            .getValue(DeltaSourceOptions.VERSION_AS_OF), equalTo(10L));
    }

    //////////////////////////////////////////////////////////////
    // Overridden parent methods for tests in base parent class //
    //////////////////////////////////////////////////////////////

    @Override
    protected <T> RowDataBoundedDeltaSourceBuilder getBuilderWithOption(
            ConfigOption<T> option,
            T value) {
        RowDataBoundedDeltaSourceBuilder builder =
            DeltaSource.forBoundedRowData(
                new Path(TABLE_PATH),
                DeltaSinkTestUtils.getHadoopConf()
            );

        return (RowDataBoundedDeltaSourceBuilder) setOptionOnBuilder(option, value, builder);
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
                DeltaSinkTestUtils.getHadoopConf()
            )
            .columnNames((columnNames != null) ? Arrays.asList(columnNames) : null);
    }

    @Override
    protected RowDataBoundedDeltaSourceBuilder getBuilderWithMutuallyExcludedOptions() {
        return DeltaSource.forBoundedRowData(
                new Path(TABLE_PATH),
                DeltaSinkTestUtils.getHadoopConf()
            )
            .versionAsOf(10)
            .timestampAsOf("2022-02-24T04:55:00.001");
    }

    @Override
    protected RowDataBoundedDeltaSourceBuilder getBuilderWithGenericMutuallyExcludedOptions() {
        return DeltaSource.forBoundedRowData(
                new Path(TABLE_PATH),
                DeltaSinkTestUtils.getHadoopConf()
            )
            .option(DeltaSourceOptions.VERSION_AS_OF.key(), 10)
            .option(DeltaSourceOptions.TIMESTAMP_AS_OF.key(), "2022-02-24T04:55:00.001");
    }

    @Override
    protected RowDataBoundedDeltaSourceBuilder
        getBuilderWithNullMandatoryFieldsAndExcludedOption() {
        return DeltaSource.forBoundedRowData(
                null,
                DeltaSinkTestUtils.getHadoopConf()
            )
            .timestampAsOf("2022-02-24T04:55:00.001")
            .option(DeltaSourceOptions.VERSION_AS_OF.key(), 10);
    }
}
