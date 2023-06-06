/*
 * Copyright (2023) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.delta.kernel.data;

import java.util.List;
import java.util.Map;

import io.delta.kernel.types.DataType;

/**
 * Represents zero or more values of a single column.
 */
public interface ColumnVector extends AutoCloseable {
    /**
     * @return the data type of this column vector.
     */
    DataType getDataType();

    /**
     * @return number of elements in the vector
     */
    int getSize();

    /**
     * Cleans up memory for this column vector. The column vector is not usable after this.
     */
    @Override
    void close();

    /**
     * @return whether the value at {@code rowId} is NULL.
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
