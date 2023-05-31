package io.delta.kernel.data;

import io.delta.kernel.types.DataType;

import java.util.List;
import java.util.Map;

/**
 * Represents zero or more values of a single column.
 */
public interface ColumnVector extends AutoCloseable {
    /**
     * Returns the data type of this column vector.
     */
    DataType getDataType();

    /**
     * Number of eleements in the vector
     */
    int getSize();

    /**
     * Cleans up memory for this column vector. The column vector is not usable after this.
     * <p>
     * This overwrites {@link AutoCloseable#close} to remove the
     * {@code throws} clause, as column vector is in-memory and we don't expect any exception to
     * happen during closing.
     */
    @Override
    void close();

    /**
     * Returns whether the value at {@code rowId} is NULL.
     */
    boolean isNullAt(int rowId);

    /**
     * Returns the boolean type value for {@code rowId}. The return value is undefined and can be
     * anything, if the slot for {@code rowId} is null.
     */
    default boolean getBoolean(int rowId) {
        throw new UnsupportedOperationException("Invalid value request for data type");
    }
    /**
     * Returns the byte type value for {@code rowId}. The return value is undefined and can be
     * anything, if the slot for {@code rowId} is null.
     */
    default byte getByte(int rowId) {
        throw new UnsupportedOperationException("Invalid value request for data type");
    }

    /**
     * Returns the short type value for {@code rowId}. The return value is undefined and can be
     * anything, if the slot for {@code rowId} is null.
     */
    default short getShort(int rowId) {
        throw new UnsupportedOperationException("Invalid value request for data type");
    }

    /**
     * Returns the int type value for {@code rowId}. The return value is undefined and can be
     * anything, if the slot for {@code rowId} is null.
     */
    default int getInt(int rowId) {
        throw new UnsupportedOperationException("Invalid value request for data type");
    }

    /**
     * Returns the long type value for {@code rowId}. The return value is undefined and can be
     * anything, if the slot for {@code rowId} is null.
     */
    default long getLong(int rowId) {
        throw new UnsupportedOperationException("Invalid value request for data type");
    }

    /**
     * Returns the float type value for {@code rowId}. The return value is undefined and can be
     * anything, if the slot for {@code rowId} is null.
     */
    default float getFloat(int rowId) {
        throw new UnsupportedOperationException("Invalid value request for data type");
    }

    /**
     * Returns the double type value for {@code rowId}. The return value is undefined and can be
     * anything, if the slot for {@code rowId} is null.
     */
    default double getDouble(int rowId) {
        throw new UnsupportedOperationException("Invalid value request for data type");
    }

    /**
     * Returns the binary type value for {@code rowId}. If the slot for {@code rowId} is null, it
     * should return null.
     */
    default byte[] getBinary(int rowId) {
        throw new UnsupportedOperationException("Invalid value request for data type");
    }

    /**
     * Returns the string type value for {@code rowId}. If the slot for {@code rowId} is null, it
     * should return null.
     */
    default String getString(int rowId) {
        throw new UnsupportedOperationException("Invalid value request for data type");
    }

    default <K, V> Map<K, V> getMap(int rowId) {
        throw new UnsupportedOperationException("Invalid value request for data type");
    }

    default Row getStruct(int rowId) {
        throw new UnsupportedOperationException("Invalid value request for data type");
    }

    default <T> List<T> getArray(int rowId) {
        throw new UnsupportedOperationException("Invalid value request for data type");
    }
}
