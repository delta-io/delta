package io.delta.flink.source.internal;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Stream;
import static java.util.Collections.singletonMap;

import io.delta.flink.source.internal.exceptions.DeltaSourceException;
import io.delta.flink.source.internal.state.DeltaSourceSplit;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;

class DeltaPartitionFieldExtractorTest {

    private static final Path ADD_FILE_PATH = new Path("some/path/0000.parquet");

    private DeltaPartitionFieldExtractor<DeltaSourceSplit> extractor;

    @BeforeEach
    public void setUp() {
        this.extractor = new DeltaPartitionFieldExtractor<>();
    }

    /**
     * @return Stream of test {@link Arguments} elements. Arguments are in order:
     * <ul>
     *     <li>Partition column name.</li>
     *     <li>Expected type for partition value.</li>
     *     <li>Map of Delta partitions.</li>
     *     <li>Expected value for partition column</li>
     * </ul>
     */
    private static Stream<Arguments> partitions() {
        return Stream.of(
            Arguments.of("col1", new CharType(), singletonMap("col1", "char"), "char"),
            Arguments.of("col2", new VarCharType(), singletonMap("col2", "varchar"), "varchar"),
            Arguments.of("col3", new BooleanType(), singletonMap("col3", "true"), Boolean.TRUE),
            Arguments.of(
                "col4",
                new TinyIntType(),
                singletonMap("col4", "1"),
                Integer.valueOf("1").byteValue()
            ),
            Arguments.of("col5", new SmallIntType(), singletonMap("col5", "2"), (short) 2),
            Arguments.of("col6", new IntType(), singletonMap("col6", "3"), 3),
            Arguments.of("col7", new BigIntType(), singletonMap("col7", "4"), (long) 4),
            Arguments.of("col8", new FloatType(), singletonMap("col8", "5.0"), (float) 5.0),
            Arguments.of("col9", new DoubleType(), singletonMap("col9", "6.0"), 6.0),
            Arguments.of(
                "col10",
                new DateType(),
                singletonMap("col10", "2022-02-24"),
                LocalDate.parse("2022-02-24")
            ),
            Arguments.of(
                "col11",
                new TimestampType(),
                singletonMap("col11", "2022-02-24T04:55:00"),
                LocalDateTime.parse("2022-02-24T04:55:00")
            ),
            Arguments.of(
                "col12",
                new DecimalType(),
                singletonMap("col12", "6"),
                new BigDecimal("6")
            )
        );
    }

    /**
     * Test for extracting Delta partition Value using {@link DeltaPartitionFieldExtractor}. This
     * test check extraction for every Flink's {@link LogicalType}.
     *
     * @param partitionColumn        Partition column to extract value for.
     * @param columnType             Type for partition column value.
     * @param splitPartitions        Map of Delta partitions from
     * {@link io.delta.standalone.actions.AddFile#getPartitionValues()}
     * @param expectedPartitionValue The expected value for extracted partition column after
     *                               converting it to {@link LogicalType}.
     */
    @ParameterizedTest(name = "{index}: column name = [{0}], type = [{1}], partition map = [{2}]")
    @MethodSource("partitions")
    public void extractValue(
            String partitionColumn,
            LogicalType columnType,
            Map<String, String> splitPartitions,
            Object expectedPartitionValue) {

        DeltaSourceSplit split = new DeltaSourceSplit(splitPartitions, "1", ADD_FILE_PATH, 0, 0);
        Object partitionValue = extractor.extract(split, partitionColumn, columnType);
        assertThat(partitionValue, equalTo(expectedPartitionValue));
    }

    @Test()
    public void shouldThrowOnNonPartitionColumn() {

        DeltaSourceSplit split =
            new DeltaSourceSplit(singletonMap("col1", "val1"), "1", ADD_FILE_PATH, 0, 0);
        DeltaSourceException exception = Assertions.assertThrows(DeltaSourceException.class,
            () -> extractor.extract(split, "notExistingPartitionColumn", new CharType()));

        Assertions.assertEquals(
            "Cannot find the partition value in Delta MetaData for column "
                + "notExistingPartitionColumn. Expected partition column names from MetaData are "
                + "[col1]",
            exception.getMessage());
    }

    @Test()
    public void shouldThrowOnNonePartitionedTable() {

        Map<String, String> noPartitions = Collections.emptyMap();
        DeltaSourceSplit split = new DeltaSourceSplit(noPartitions, "1", ADD_FILE_PATH, 0, 0);
        DeltaSourceException exception = Assertions.assertThrows(DeltaSourceException.class,
            () -> extractor.extract(split, "col1", new CharType()));

        Assertions.assertEquals(
            "Attempt to get a value for partition column from unpartitioned Delta Table. Column "
                + "name col1",
            exception.getMessage());
    }
}
