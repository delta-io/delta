package io.delta.flinkv2.data.vector;

import io.delta.kernel.types.LongType;

import java.util.Arrays;

public class MutableLongColumnVector extends MutableAbstractColumnVector {

    private final long[] values;

    public MutableLongColumnVector(int size) {
        super(size, LongType.LONG);
        this.values = new long[size];
    }

    @Override
    public void setLong(int rowId, long value) {
        checkValidRowId(rowId);
        values[rowId] = value;
    }

    @Override
    public long getLong(int rowId) {
        checkValidRowId(rowId);
        return values[rowId];
    }

    @Override
    public String toString() {
        final String nullabilityStr = nullability.isPresent() ?
            String.format("Optional[%s]", Arrays.toString(nullability.get())) : "Optional.empty";

        return "MutableLongColumnVector{" +
            "values=" + Arrays.toString(values) +
            ", size=" + size +
            ", nullability=" + nullabilityStr +
            '}';
    }
}

