package io.delta.standalone.internal.expressions;

import java.math.BigDecimal;
import java.util.Comparator;
import java.util.Date;

import io.delta.standalone.types.*;

public final class Util {

    public static Comparator<Object> createComparator(DataType dataType) {
        if (dataType instanceof IntegerType) {
            return new CastingComparator<Integer>();
        }

        if (dataType instanceof BooleanType) {
            return new CastingComparator<Boolean>();
        }

        if (dataType instanceof FloatType) {
            return new CastingComparator<Float>();
        }

        if (dataType instanceof LongType) {
            return new CastingComparator<Long>();
        }

        if (dataType instanceof ByteType) {
            return new CastingComparator<Byte>();
        }

        if (dataType instanceof ShortType) {
            return new CastingComparator<Short>();
        }

        if (dataType instanceof DoubleType) {
            return new CastingComparator<Double>();
        }

        if (dataType instanceof DecimalType) {
            return new CastingComparator<BigDecimal>();
        }

        if (dataType instanceof TimestampType) {
            return new CastingComparator<Date>();
        }

        if (dataType instanceof DateType) {
            return new CastingComparator<Date>();
        }

        if (dataType instanceof StringType) {
            return new CastingComparator<String>();
        }

        if (dataType instanceof BinaryType) {
            return (o1, o2) -> {
                byte[] one = (byte[]) o1;
                byte[] two = (byte[]) o2;
                int i = 0;
                while (i < one.length && i < two.length) {
                    if (one[i] != two[i]) {
                        return Byte.compare(one[i], two[i]);
                    }
                    i ++;
                }
                return Integer.compare(one.length, two.length);
            };
        }

        // unsupported comparison types: ArrayType, StructType, MapType
        throw new IllegalArgumentException(
            "Couldn't find matching comparator for DataType: " + dataType.getTypeName());
    }
}
