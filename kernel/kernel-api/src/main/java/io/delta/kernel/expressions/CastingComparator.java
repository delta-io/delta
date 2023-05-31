package io.delta.kernel.expressions;

import java.util.Comparator;

import io.delta.kernel.types.*;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.StringType;

public class CastingComparator<T extends Comparable<T>> implements Comparator<Object> {

    public static Comparator<Object> forDataType(DataType dataType) {
        if (dataType instanceof IntegerType) {
            return new CastingComparator<Integer>();
        }

        if (dataType instanceof BooleanType) {
            return new CastingComparator<Boolean>();
        }

        if (dataType instanceof LongType) {
            return new CastingComparator<Long>();
        }

        if (dataType instanceof StringType) {
            return new CastingComparator<String>();
        }

        throw new IllegalArgumentException(
            String.format("Unsupported DataType: %s", dataType.typeName())
        );
    }

    private final Comparator<T> comparator;

    public CastingComparator() {
        comparator = Comparator.naturalOrder();
    }

    @SuppressWarnings("unchecked")
    @Override
    public int compare(Object a, Object b) {
        return comparator.compare((T) a, (T) b);
    }
}
