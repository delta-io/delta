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
package io.delta.kernel.defaults.internal.data.vector;

import java.math.BigDecimal;
import java.util.Optional;
import static java.util.Objects.requireNonNull;

import io.delta.kernel.data.ArrayValue;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.MapValue;
import io.delta.kernel.data.VariantValue;
import io.delta.kernel.types.DataType;

import static io.delta.kernel.internal.util.Preconditions.checkArgument;

/**
 * Abstract implementation of {@link ColumnVector} that provides the default functionality
 * common to most of the specific data type {@link ColumnVector} implementations.
 */
public abstract class AbstractColumnVector
    implements ColumnVector {
    private final int size;
    private final DataType dataType;
    private final Optional<boolean[]> nullability;

    protected AbstractColumnVector(int size, DataType dataType, Optional<boolean[]> nullability) {
        checkArgument(size >= 0, "invalid size: %s", size);
        nullability.ifPresent(array ->
            checkArgument(array.length >= size,
                "invalid number of values (%s) for given size (%s)", array.length, size)
        );
        this.size = size;
        this.dataType = requireNonNull(dataType);
        this.nullability = requireNonNull(nullability);
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
        // By default, nothing to close, if the implementation has any resources to release
        // it can override it
    }

    /**
     * Is the value at given {@code rowId} index is null?
     *
     * @param rowId
     * @return
     */
    @Override
    public boolean isNullAt(int rowId) {
        checkValidRowId(rowId);
        if (!nullability.isPresent()) {
            return false; // if there is no-nullability vector, every value is a non-null value
        }
        return nullability.get()[rowId];
    }

    public Optional<boolean[]> getNullability() {
        return nullability;
    }

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

    @Override
    public VariantValue getVariant(int rowId) {
        throw unsupportedDataAccessException("variant");
    }

    // TODO no need to override these here; update default implementations in `ColumnVector`
    //   to have a more informative exception message
    protected UnsupportedOperationException unsupportedDataAccessException(String accessType) {
        String msg = String.format(
            "Trying to access a `%s` value from vector of type `%s`",
            accessType,
            getDataType());
        throw new UnsupportedOperationException(msg);
    }

    /**
     * Helper method that make sure the given {@code rowId} position is valid in this vector
     *
     * @param rowId
     */
    protected void checkValidRowId(int rowId) {
        if (rowId < 0 || rowId >= size) {
            throw new IllegalArgumentException("invalid row access: " + rowId);
        }
    }
}
