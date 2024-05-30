package io.delta.flinkv2.data.vector;

import io.delta.kernel.types.IntegerType;

import java.util.Arrays;

public class MutableIntColumnVector extends MutableAbstractColumnVector {

    private final int[] values;

    public MutableIntColumnVector(int size) {
        super(size, IntegerType.INTEGER);
        this.values = new int[size];
    }

    @Override
    public void setInt(int rowId, int value) {
        checkValidRowId(rowId);
        values[rowId] = value;
    }

    @Override
    public int getInt(int rowId) {
        checkValidRowId(rowId);
        return values[rowId];
    }

    @Override
    public String toString() {
        final String nullabilityStr = nullability.isPresent() ?
            String.format("Optional[%s]", Arrays.toString(nullability.get())) : "Optional.empty";

        return "MutableIntColumnVector{" +
            "values=" + Arrays.toString(values) +
            ", size=" + size +
            ", nullability=" + nullabilityStr +
            '}';
    }
}
