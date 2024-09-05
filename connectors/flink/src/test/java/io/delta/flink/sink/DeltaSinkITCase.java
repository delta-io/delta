package io.delta.flink.sink;

import java.nio.file.Path;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.stream.IntStream;

import io.delta.flink.utils.DeltaTestUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.DataFormatConverters;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class DeltaSinkITCase {

    @Test
    public void testWritePartitionedByDate(@TempDir Path tempDir) throws Exception {
        final String deltaTablePath = tempDir.toString();

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        final RowType rowType = new RowType(Arrays.asList(
                new RowType.RowField("part1", new DateType()),
                new RowType.RowField("data1", new IntType())
        ));

        final DataFormatConverters.DataFormatConverter<RowData, Row> typeConverter =
                DataFormatConverters.getConverterForDataType(
                        TypeConversions.fromLogicalToDataType(rowType)
                );

        final DeltaSink<RowData> deltaSink = DeltaSink
                .forRowData(
                        new org.apache.flink.core.fs.Path(deltaTablePath),
                        DeltaTestUtils.getHadoopConf(),
                        rowType
                ).build();

        LocalDate startDate = LocalDate.of(2024, 8, 1);
        int numOfDays = 30;
        RowData[] elements = IntStream.range(1, numOfDays)
                .mapToObj(startDate::plusDays)
                .map(date -> Row.of(date, 0))
                .map(typeConverter::toInternal)
                .toArray(RowData[]::new);

        final DataStream<RowData> inputStream = env.fromElements(elements);

        inputStream.sinkTo(deltaSink);

        env.execute("Delta Sink Example");
    }
}
