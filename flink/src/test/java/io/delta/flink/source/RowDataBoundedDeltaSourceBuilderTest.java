package io.delta.flink.source;

import io.delta.flink.sink.utils.DeltaSinkTestUtils;
import io.delta.flink.source.internal.DeltaSourceOptions;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.junit.jupiter.api.Test;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

class RowDataBoundedDeltaSourceBuilderTest extends RowDataDeltaSourceBuilderTestBase {

    ////////////////////////////////
    // Continuous-only test cases //
    ////////////////////////////////

    @Test
    public void shouldCreateSource() {
        DeltaSource<RowData> boundedSource = DeltaSource.forBoundedRowData(
                new Path(TABLE_PATH),
                COLUMN_NAMES,
                COLUMN_TYPES,
                DeltaSinkTestUtils.getHadoopConf())
            .build();

        assertThat(boundedSource, notNullValue());
        assertThat(boundedSource.getBoundedness(), equalTo(Boundedness.BOUNDED));
    }

    @Test
    public void shouldCreateSourceWithOptions() {
        DeltaSource<RowData> boundedSource = DeltaSource.forBoundedRowData(
                new Path(TABLE_PATH),
                COLUMN_NAMES,
                COLUMN_TYPES,
                DeltaSinkTestUtils.getHadoopConf())
            .option(DeltaSourceOptions.VERSION_AS_OF.key(), 10)
            .build();

        assertThat(boundedSource, notNullValue());
        assertThat(boundedSource.getBoundedness(), equalTo(Boundedness.BOUNDED));
        assertThat(boundedSource.getSourceConfiguration()
            .getValue(DeltaSourceOptions.VERSION_AS_OF), equalTo(10));
    }

    //////////////////////////////////////////////////////////////
    // Overridden parent methods for tests in base parent class //
    //////////////////////////////////////////////////////////////

    @Override
    protected RowDataBoundedDeltaSourceBuilder getBuilderWithNulls() {
        return DeltaSource.forBoundedRowData(
            null,
            null,
            null,
            null
        );
    }

    @Override
    protected RowDataBoundedDeltaSourceBuilder getBuilderForColumns(
        String[] columnNames,
        LogicalType[] columnTypes) {
        return DeltaSource.forBoundedRowData(
            new Path(TABLE_PATH),
            columnNames,
            columnTypes,
            DeltaSinkTestUtils.getHadoopConf()
        );
    }

    @Override
    protected RowDataBoundedDeltaSourceBuilder getBuilderWithMutuallyExcludedOptions() {
        return DeltaSource.forBoundedRowData(
                new Path(TABLE_PATH),
                COLUMN_NAMES,
                COLUMN_TYPES,
                DeltaSinkTestUtils.getHadoopConf()
            )
            .versionAsOf(10)
            .timestampAsOf("2022-02-24T04:55:00.001");
    }

    @Override
    protected RowDataBoundedDeltaSourceBuilder getBuilderWithGenericMutuallyExcludedOptions() {
        return DeltaSource.forBoundedRowData(
                new Path(TABLE_PATH),
                COLUMN_NAMES,
                COLUMN_TYPES,
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
                null,
                null,
                DeltaSinkTestUtils.getHadoopConf()
            )
            .timestampAsOf("2022-02-24T04:55:00.001")
            .option(DeltaSourceOptions.VERSION_AS_OF.key(), 10);
    }
}
