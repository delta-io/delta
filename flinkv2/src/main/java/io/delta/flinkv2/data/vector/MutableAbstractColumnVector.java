package io.delta.flinkv2.data.vector;

import io.delta.kernel.data.ArrayValue;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.MapValue;
import io.delta.kernel.types.DataType;

import java.math.BigDecimal;
import java.util.Optional;

/** Values should only be set by a single thread. */
public abstract class MutableAbstractColumnVector implements ColumnVector { // TODO: use some sort of builder instead?

    protected final int size;
    private final DataType dataType;
    protected Optional<boolean[]> nullability;

    public MutableAbstractColumnVector(int size, DataType dataType) {
        this.size = size;
        this.dataType = dataType;
        this.nullability = Optional.empty();
    }

    @Override
    public DataType getDataType() {
        return dataType;
    }

    @Override
    public int getSize() {
        return size;
    }

    @Override
    public void close() {
        // By default, nothing to close. If the implementation has any resources to release, it can
        // override it
    }

    @Override
    public boolean isNullAt(int rowId) {
        checkValidRowId(rowId);
        if (!nullability.isPresent()) {
            return false; // if there is no-nullability vector, every value is a non-null value
        }
        return nullability.get()[rowId];
    }

    public void setIsNullAt(int rowId) { // TODO: prevent setting after it is "finalized" ?
        checkValidRowId(rowId);

        if (!nullability.isPresent()) {
            nullability = Optional.of(new boolean[size]);
        }

        nullability.get()[rowId] = true;
    }

    ////////////////////////////////////////////////////
    // Public `set` API methods for child to override //
    ////////////////////////////////////////////////////

    public void setInt(int rowId, int value) {
        throw unsupportedDataSetException("int");
    }

    public void setLong(int rowId, long value) {
        throw unsupportedDataSetException("long");
    }

    ////////////////////////////////////////////////////
    // Public `get` API methods for child to override //
    ////////////////////////////////////////////////////

    @Override
    public boolean getBoolean(int rowId) {
        throw unsupportedDataAccessException("boolean");
    }

    @Override
    public byte getByte(int rowId) {
        throw unsupportedDataAccessException("byte");
    }

    @Override
    public short getShort(int rowId) {
        throw unsupportedDataAccessException("short");
    }

    @Override
    public int getInt(int rowId) {
        throw unsupportedDataAccessException("int");
    }

    @Override
    public long getLong(int rowId) {
        throw unsupportedDataAccessException("long");
    }

    @Override
    public float getFloat(int rowId) {
        throw unsupportedDataAccessException("float");
    }

    @Override
    public double getDouble(int rowId) {
        throw unsupportedDataAccessException("double");
    }

    @Override
    public byte[] getBinary(int rowId) {
        throw unsupportedDataAccessException("binary");
    }

    @Override
    public String getString(int rowId) {
        throw unsupportedDataAccessException("string");
    }

    @Override
    public BigDecimal getDecimal(int rowId) {
        throw unsupportedDataAccessException("decimal");
    }

    @Override
    public MapValue getMap(int rowId) {
        throw unsupportedDataAccessException("map");
    }

    @Override
    public ArrayValue getArray(int rowId) {
        throw unsupportedDataAccessException("array");
    }

    ///////////////////////
    // Protected methods //
    ///////////////////////

    protected void checkValidRowId(int rowId) {
        if (rowId < 0 || rowId >= size) {
            throw new IllegalArgumentException("invalid row access: " + rowId);
        }
    }

    ////////////////////
    // Helper methods //
    ////////////////////

    private UnsupportedOperationException unsupportedDataAccessException(String accessType) {
        String msg = String.format(
            "Trying to access a `%s` value from vector of type `%s`",
            accessType,
            getDataType());
        throw new UnsupportedOperationException(msg);
    }

    private UnsupportedOperationException unsupportedDataSetException(String accessType) {
        String msg = String.format(
            "Trying to write a `%s` value to vector of type `%s`",
            accessType,
            getDataType());
        throw new UnsupportedOperationException(msg);
    }
}
