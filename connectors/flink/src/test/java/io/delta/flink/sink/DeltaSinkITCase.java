package io.delta.flink.sink;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

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

import io.delta.standalone.DeltaLog;
import io.delta.standalone.data.CloseableIterator;
import io.delta.standalone.data.RowRecord;

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
                )
                .withPartitionColumns("part1")
                .build();

        LocalDate startDate = LocalDate.of(2024, 8, 1);
        int numOfDays = 30;
        List<LocalDate> inputDates = IntStream.range(1, numOfDays)
                .mapToObj(startDate::plusDays)
                .collect(Collectors.toList());

        RowData[] elements = inputDates.stream()
                .map(date -> Row.of(date, 0))
                .map(typeConverter::toInternal)
                .toArray(RowData[]::new);

        final DataStream<RowData> inputStream = env.fromElements(elements);

        inputStream.sinkTo(deltaSink);

        env.execute("Delta Sink Example");

        DeltaLog deltaLog =
                DeltaLog.forTable(DeltaTestUtils.getHadoopConf(), deltaTablePath);

        try (CloseableIterator<RowRecord> rowRecordIterator = deltaLog.snapshot().open()) {

            Iterable<RowRecord> iterable = () -> rowRecordIterator;
            Stream<RowRecord> rowRecordStream = StreamSupport.stream(iterable.spliterator(), false);

            List<LocalDate> resultDates = rowRecordStream
                    .map(rowRecord -> rowRecord.getDate("part1").toLocalDate())
                    .collect(Collectors.toList());

            org.assertj.core.api.Assertions.assertThat(resultDates)
                    .withFailMessage("Delta table failed to store date partitions.")
                    .containsAll(inputDates);

        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        List<String> deltaPartitionColumns = deltaLog
                .snapshot()
                .getMetadata()
                .getPartitionColumns();

        org.assertj.core.api.Assertions.assertThat(deltaPartitionColumns)
                .withFailMessage("Delta table failed to store date partitions to the delta log.")
                .containsAll(Collections.singletonList("part1"));
    }
}
