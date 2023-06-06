package io.delta.flink;

import java.util.stream.Stream;

import org.apache.flink.table.types.logical.LogicalType;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;

import io.delta.standalone.types.DataType;
import io.delta.standalone.types.StructField;
import io.delta.standalone.types.StructType;

/**
 * Test to verify Delta Type -> Flink Type -> Delta type conversion.
 */
public class CrossSchemaConversionTest {

    private static Stream<Arguments> dataTypes() {
        return Stream.of(
            Arguments.of(new io.delta.standalone.types.FloatType()),
            Arguments.of(new io.delta.standalone.types.IntegerType()),
            Arguments.of(new io.delta.standalone.types.StringType()),
            Arguments.of(new io.delta.standalone.types.DoubleType()),
            Arguments.of(
                new io.delta.standalone.types.MapType(
                    new io.delta.standalone.types.StringType(),
                    new io.delta.standalone.types.IntegerType(),
                    true // valueContainsNull
                )),
            Arguments.of(
                new io.delta.standalone.types.ArrayType(
                    new io.delta.standalone.types.ByteType(),
                    true // containsNull
                )),
            Arguments.of(
                new io.delta.standalone.types.ArrayType(
                    new io.delta.standalone.types.StringType(),
                    true // containsNull
                )),
            Arguments.of(new io.delta.standalone.types.StringType()),
            Arguments.of(new io.delta.standalone.types.BooleanType()),
            Arguments.of(new io.delta.standalone.types.ByteType()),
            Arguments.of(new io.delta.standalone.types.ShortType()),
            Arguments.of(new io.delta.standalone.types.LongType()),
            Arguments.of(new io.delta.standalone.types.BinaryType()),
            Arguments.of(new io.delta.standalone.types.TimestampType()),
            Arguments.of(new io.delta.standalone.types.DateType()),
            Arguments.of(new io.delta.standalone.types.StringType()),
            Arguments.of(new io.delta.standalone.types.DecimalType(10, 0)),
            Arguments.of(new io.delta.standalone.types.DecimalType(2, 0)),
            Arguments.of(new io.delta.standalone.types.DecimalType(2, 2)),
            Arguments.of(new io.delta.standalone.types.DecimalType(38, 2)),
            Arguments.of(new io.delta.standalone.types.DecimalType(10, 1)),
            Arguments.of(
                new StructType(new StructField[]{
                    new StructField("f01", new io.delta.standalone.types.StringType()),
                    new StructField("f02", new io.delta.standalone.types.IntegerType()),
                })),
            Arguments.of(
                new io.delta.standalone.types.MapType(
                    new io.delta.standalone.types.StringType(),
                    new io.delta.standalone.types.IntegerType(),
                    true
                )),
            Arguments.of(
                new io.delta.standalone.types.MapType(
                    new io.delta.standalone.types.IntegerType(),
                    new io.delta.standalone.types.ArrayType(
                        new io.delta.standalone.types.ByteType(),
                        true // containsNull
                    ),
                    true
                )),
            Arguments.of(
                new io.delta.standalone.types.MapType(
                    new io.delta.standalone.types.LongType(),
                    new StructType(new StructField[]{
                        new StructField("f01", new io.delta.standalone.types.StringType()),
                        new StructField("f02", new io.delta.standalone.types.IntegerType()),
                    }),
                    true
                )),
            Arguments.of(
                new io.delta.standalone.types.MapType(
                    new io.delta.standalone.types.BinaryType(),
                    new io.delta.standalone.types.ShortType(),
                    true
                )),
            Arguments.of(
                new io.delta.standalone.types.MapType(
                    new io.delta.standalone.types.StringType(),
                    new io.delta.standalone.types.IntegerType(),
                    true
                ))
        );
    }

    @ParameterizedTest
    @MethodSource("dataTypes")
    public void shouldConvertFromAndToDeltaType(DataType originalDeltaType) {
        LogicalType flinkType = io.delta.flink.source.internal.SchemaConverter
            .toFlinkDataType(originalDeltaType, true);

        DataType convertedDeltaType = io.delta.flink.sink.internal.SchemaConverter
            .toDeltaDataType(flinkType);
        assertThat("Converted Delta type is different that input type.",
            convertedDeltaType,
            equalTo(originalDeltaType));

    }

}
