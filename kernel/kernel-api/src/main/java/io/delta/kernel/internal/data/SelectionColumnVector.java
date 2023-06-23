package io.delta.kernel.internal.data;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.Row;
import io.delta.kernel.internal.deletionvectors.RoaringBitmapArray;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.DataType;

import java.util.List;
import java.util.Map;

/**
 * The selection vector for a columnar batch as a boolean {@link ColumnVector}.
 */
public class SelectionColumnVector
        implements ColumnVector {

    private final RoaringBitmapArray bitmap;
    private final ColumnVector rowIndices;

    public SelectionColumnVector(RoaringBitmapArray bitmap, ColumnVector rowIndices) {
        this.bitmap = bitmap;
        this.rowIndices = rowIndices;
    }

    @Override
    public DataType getDataType()
    {
        return BooleanType.INSTANCE;
    }

    @Override
    public int getSize()
    {
        return rowIndices.getSize();
    }

    @Override
    public void close() { }

    @Override
    public boolean isNullAt(int rowId)
    {
        return false;
    }

    @Override
    public boolean getBoolean(int rowId)
    {
        return !bitmap.contains(rowIndices.getLong(rowId));
    }

    @Override
    public byte getByte(int rowId)
    {
        throw new UnsupportedOperationException("Invalid type");
    }

    @Override
    public short getShort(int rowId)
    {
        throw new UnsupportedOperationException("Invalid type");
    }

    @Override
    public int getInt(int rowId)
    {
        throw new UnsupportedOperationException("Invalid type");
    }

    @Override
    public long getLong(int rowId)
    {
        throw new UnsupportedOperationException("Invalid type");
    }

    @Override
    public float getFloat(int rowId)
    {
        throw new UnsupportedOperationException("Invalid type");
    }

    @Override
    public double getDouble(int rowId)
    {
        throw new UnsupportedOperationException("Invalid type");
    }

    @Override
    public byte[] getBinary(int rowId)
    {
        throw new UnsupportedOperationException("Invalid type");
    }

    @Override
    public String getString(int rowId)
    {
        throw new UnsupportedOperationException("Invalid type");
    }

    @Override
    public <K, V> Map<K, V> getMap(int rowId)
    {
        throw new UnsupportedOperationException("Invalid type");
    }

    @Override
    public Row getStruct(int rowId)
    {
        throw new UnsupportedOperationException("Invalid type");
    }

    @Override
    public <T> List<T> getArray(int rowId)
    {
        throw new UnsupportedOperationException("Invalid type");
    }
}
