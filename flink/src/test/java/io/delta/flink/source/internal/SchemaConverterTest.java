package io.delta.flink.source.internal;

import java.util.Arrays;
import java.util.stream.Stream;

import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;

import io.delta.standalone.types.DataType;
import io.delta.standalone.types.StructField;
import io.delta.standalone.types.StructType;

public class SchemaConverterTest {

    /**
     * Stream of {@link Arguments} elements representing pairs of given Delta's {@link DataType} and
     * corresponding to it Flink's {@link LogicalType}
     *
     * @return Stream of test {@link Arguments} elements. Arguments.of(DataType, expectedLogicalType
     */
    private static Stream<Arguments> dataTypes() {
        return Stream.of(
            Arguments.of(new io.delta.standalone.types.FloatType(), new FloatType()),
            Arguments.of(new io.delta.standalone.types.IntegerType(), new IntType()),
            Arguments.of(new io.delta.standalone.types.StringType(), new VarCharType()),
            Arguments.of(new io.delta.standalone.types.DoubleType(), new DoubleType()),
            Arguments.of(
                new io.delta.standalone.types.MapType(
                    new io.delta.standalone.types.StringType(),
                    new io.delta.standalone.types.IntegerType(),
                    true // valueContainsNull
                ),
                new MapType(new VarCharType(), new IntType())),
            Arguments.of(
                new io.delta.standalone.types.ArrayType(
                    new io.delta.standalone.types.ByteType(),
                    true // containsNull
                ),
                new ArrayType(new TinyIntType())),
            Arguments.of(
                new io.delta.standalone.types.ArrayType(
                    new io.delta.standalone.types.StringType(),
                    true // containsNull
                ),
                new ArrayType(new VarCharType())),
            Arguments.of(new io.delta.standalone.types.StringType(), new VarCharType()),
            Arguments.of(new io.delta.standalone.types.BooleanType(), new BooleanType()),
            Arguments.of(new io.delta.standalone.types.ByteType(), new TinyIntType()),
            Arguments.of(new io.delta.standalone.types.ShortType(), new SmallIntType()),
            Arguments.of(new io.delta.standalone.types.LongType(), new BigIntType()),
            Arguments.of(new io.delta.standalone.types.BinaryType(), new BinaryType()),
            Arguments.of(new io.delta.standalone.types.TimestampType(), new TimestampType()),
            Arguments.of(new io.delta.standalone.types.DateType(), new DateType()),
            Arguments.of(new io.delta.standalone.types.StringType(), new VarCharType()),
            Arguments.of(new io.delta.standalone.types.DecimalType(10, 0), new DecimalType(10, 0)),
            Arguments.of(new io.delta.standalone.types.DecimalType(2, 0), new DecimalType(2)),
            Arguments.of(new io.delta.standalone.types.DecimalType(2, 2), new DecimalType(2, 2)),
            Arguments.of(new io.delta.standalone.types.DecimalType(38, 2), new DecimalType(38, 2)),
            Arguments.of(new io.delta.standalone.types.DecimalType(10, 1), new DecimalType(10, 1)),
            Arguments.of(
                new StructType(new StructField[]{
                    new StructField("f01", new io.delta.standalone.types.StringType()),
                    new StructField("f02", new io.delta.standalone.types.IntegerType()),
                }),
                new RowType(Arrays.asList(
                    new RowType.RowField("f01", new VarCharType()),
                    new RowType.RowField("f02", new IntType()))
                ))
        );
    }

    /**
     * Test to verify proper conversion of Delta's {@link DataType} type to Flink's {@link
     * LogicalType}
     */
    @ParameterizedTest(name = "{index}: Delta type = [{0}] -> Flink type = [{1}]")
    @MethodSource("dataTypes")
    public void shouldConvertToFlinkType(DataType deltaType, LogicalType expectedFlinkType) {
        LogicalType logicalType = SchemaConverter.toFlinkDataType(deltaType, true);

        assertThat(logicalType, equalTo(expectedFlinkType));
    }

    /**
     * Stream of {@link Arguments} elements representing pairs of given Delta's Map data type end
     * expected Flink's Map data type equivalent for different combination of key and value data
     * types.
     *
     * @return Stream of test {@link Arguments} elements. Arguments.of(deltaMapDataType,
     * expectedFlinkMapDataType)
     */
    private static Stream<Arguments> mapTypes() {
        return Stream.of(
            Arguments.of(
                new io.delta.standalone.types.MapType(
                    new io.delta.standalone.types.StringType(),
                    new io.delta.standalone.types.IntegerType(),
                    true
                ),
                new MapType(new VarCharType(), new IntType())),
            Arguments.of(
                new io.delta.standalone.types.MapType(
                    new io.delta.standalone.types.IntegerType(),
                    new io.delta.standalone.types.ArrayType(
                        new io.delta.standalone.types.ByteType(),
                        true // containsNull
                    ),
                    true
                ),
                new MapType(new IntType(), new ArrayType(new TinyIntType()))),
            Arguments.of(
                new io.delta.standalone.types.MapType(
                    new io.delta.standalone.types.LongType(),
                    new StructType(new StructField[]{
                        new StructField("f01", new io.delta.standalone.types.StringType()),
                        new StructField("f02", new io.delta.standalone.types.IntegerType()),
                    }),
                    true
                ),
                new MapType(new BigIntType(),
                    new RowType(Arrays.asList(
                        new RowType.RowField("f01", new VarCharType()),
                        new RowType.RowField("f02", new IntType())
                    )))),
            Arguments.of(
                new io.delta.standalone.types.MapType(
                    new io.delta.standalone.types.BinaryType(),
                    new io.delta.standalone.types.ShortType(),
                    true
                ),
                new MapType(new BinaryType(), new SmallIntType())),
            Arguments.of(
                new io.delta.standalone.types.MapType(
                    new io.delta.standalone.types.StringType(),
                    new io.delta.standalone.types.IntegerType(),
                    true
                ),
                new MapType(new VarCharType(), new IntType()))
        );
    }

    /**
     * Test to verify proper conversion of Delta's Map data type to Flink's Map data type for
     * different combination of key and value types.
     */
    @ParameterizedTest(name = "{index}: Delta type = [{0}] -> Flink type = [{1}]")
    @MethodSource("mapTypes")
    public void shouldConvertDeltaMapToFlinkMap(DataType deltaType, LogicalType expectedFlinkType) {

        LogicalType logicalType = SchemaConverter.toFlinkDataType(deltaType, true);

        assertThat(logicalType, equalTo(expectedFlinkType));
    }

}
