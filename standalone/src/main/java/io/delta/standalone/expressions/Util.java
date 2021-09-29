package io.delta.standalone.expressions;

import io.delta.standalone.types.BooleanType;
import io.delta.standalone.types.DataType;
import io.delta.standalone.types.IntegerType;
import io.delta.standalone.types.StringType;

public final class Util {

    public static CastingComparator<?> createCastingComparator(DataType dataType) {
        if (dataType instanceof IntegerType) {
            return new CastingComparator<Integer>();
        }

        if (dataType instanceof BooleanType) {
            return new CastingComparator<Boolean>();
        }

        if (dataType instanceof StringType) {
            return new CastingComparator<String>();
        }

        throw new RuntimeException("Couldn't find matching comparator for DataType: " + dataType.toString());
    }
}
