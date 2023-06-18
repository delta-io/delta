package io.delta.kernel.data;

import io.delta.kernel.types.StructType;

public class DefaultColumnarBatch
    implements ColumnarBatch
{
    private final StructType dataType;
    private final int size;
    private final ColumnVector[] columnVectors;

    public DefaultColumnarBatch(
        int size,
        StructType dataType,
        ColumnVector[] columnVectors
    )
    {
        this.dataType = dataType;
        this.size = size;
        this.columnVectors = new ColumnVector[columnVectors.length];
        // TODO: argument check.
        System.arraycopy(columnVectors, 0, this.columnVectors, 0, columnVectors.length);
    }

    @Override
    public StructType getSchema()
    {
        return dataType;
    }

    @Override
    public ColumnVector getColumnVector(int ordinal)
    {
        checkColumnOrdinal(ordinal);
        return columnVectors[ordinal];
    }

    @Override
    public int getSize()
    {
        return size;
    }

    private void checkColumnOrdinal(int ordinal)
    {
        if (ordinal < 0 || ordinal >= columnVectors.length) {
            throw new IllegalArgumentException("invalid column ordinal");
        }
    }
}
