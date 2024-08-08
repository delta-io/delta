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

import java.math.BigDecimal;

import io.delta.kernel.annotation.Evolving;
import io.delta.kernel.types.DataType;

/**
 * Represents zero or more values of a single column.
 *
 * @since 3.0.0
 */
@Evolving
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
     * @param rowId
     * @return whether the value at {@code rowId} is NULL.
     */
    boolean isNullAt(int rowId);

    /**
     * Returns the boolean type value for {@code rowId}. The return value is undefined and can be
     * anything, if the slot for {@code rowId} is null.
     *
     * @param rowId
     * @return Boolean value at the given row id
     */
    default boolean getBoolean(int rowId) {
        throw new UnsupportedOperationException("Invalid value request for data type");
    }

    /**
     * Returns the byte type value for {@code rowId}. The return value is undefined and can be
     * anything, if the slot for {@code rowId} is null.
     *
     * @param rowId
     * @return Byte value at the given row id
     */
    default byte getByte(int rowId) {
        throw new UnsupportedOperationException("Invalid value request for data type");
    }

    /**
     * Returns the short type value for {@code rowId}. The return value is undefined and can be
     * anything, if the slot for {@code rowId} is null.
     *
     * @param rowId
     * @return Short value at the given row id
     */
    default short getShort(int rowId) {
        throw new UnsupportedOperationException("Invalid value request for data type");
    }

    /**
     * Returns the int type value for {@code rowId}. The return value is undefined and can be
     * anything, if the slot for {@code rowId} is null.
     *
     * @param rowId
     * @return Integer value at the given row id
     */
    default int getInt(int rowId) {
        throw new UnsupportedOperationException("Invalid value request for data type");
    }

    /**
     * Returns the long type value for {@code rowId}. The return value is undefined and can be
     * anything, if the slot for {@code rowId} is null.
     *
     * @param rowId
     * @return Long value at the given row id
     */
    default long getLong(int rowId) {
        throw new UnsupportedOperationException("Invalid value request for data type");
    }

    /**
     * Returns the float type value for {@code rowId}. The return value is undefined and can be
     * anything, if the slot for {@code rowId} is null.
     *
     * @param rowId
     * @return Float value at the given row id
     */
    default float getFloat(int rowId) {
        throw new UnsupportedOperationException("Invalid value request for data type");
    }

    /**
     * Returns the double type value for {@code rowId}. The return value is undefined and can be
     * anything, if the slot for {@code rowId} is null.
     *
     * @param rowId
     * @return Double value at the given row id
     */
    default double getDouble(int rowId) {
        throw new UnsupportedOperationException("Invalid value request for data type");
    }

    /**
     * Returns the binary type value for {@code rowId}. The return value is undefined and can be
     * anything, if the slot for {@code rowId} is null.
     *
     * @param rowId
     * @return Binary value at the given row id
     */
    default byte[] getBinary(int rowId) {
        throw new UnsupportedOperationException("Invalid value request for data type");
    }

    /**
     * Returns the string type value for {@code rowId}. The return value is undefined and can be
     * anything, if the slot for {@code rowId} is null.
     *
     * @param rowId
     * @return String value at the given row id
     */
    default String getString(int rowId) {
        throw new UnsupportedOperationException("Invalid value request for data type");
    }

    /**
     * Returns the decimal type value for {@code rowId}. The return value is undefined and can be
     * anything, if the slot for {@code rowId} is null.
     * @param rowId
     * @return Decimal value at the given row id
     */
    default BigDecimal getDecimal(int rowId) {
        throw new UnsupportedOperationException("Invalid value request for data type");
    }

    /**
     * Return the map value located at {@code rowId}. Returns null if the slot for {@code rowId}
     * is null
     */
    default MapValue getMap(int rowId) {
        throw new UnsupportedOperationException("Invalid value request for data type");
    }

    /**
     * Return the array value located at {@code rowId}. Returns null if the slot for {@code rowId}
     * is null
     */
    default ArrayValue getArray(int rowId) {
        throw new UnsupportedOperationException("Invalid value request for data type");
    }

    /**
     * Return the variant value located at {@code rowId}. Returns null if the slot for {@code rowId}
     * is null
     */
    default VariantValue getVariant(int rowId) {
        throw new UnsupportedOperationException("Invalid value request for data type");
    }

    /**
     * Get the child vector associated with the given ordinal. This method is applicable only to the
     * {@code struct} type columns.
     *
     * @param ordinal Ordinal of the child vector to return.
     */
    default ColumnVector getChild(int ordinal) {
        throw new UnsupportedOperationException(
            "Child vectors are not available for vector of type " + getDataType());
    }
}
