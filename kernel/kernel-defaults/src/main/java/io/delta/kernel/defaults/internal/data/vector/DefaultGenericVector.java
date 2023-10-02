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

import io.delta.kernel.data.ArrayValue;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.MapValue;
import io.delta.kernel.data.Row;
import io.delta.kernel.types.*;

/**
 * Generic column vector implementation to expose an array of objects as a column vector.
 */
public class DefaultGenericVector implements ColumnVector {

    private final DataType dataType;
    private final Object[] values;

    public DefaultGenericVector(DataType dataType, Object[] values) {
        this.dataType = dataType;
        this.values = values;
    }

    @Override
    public DataType getDataType() {
        return dataType;
    }

    @Override
    public int getSize() {
        return values.length;
    }

    @Override
    public void close() {

    }

    @Override
    public boolean isNullAt(int rowId) {
        return values[rowId] == null;
    }

    @Override
    public boolean getBoolean(int rowId) {
        throwIfUnsafeAccess(BooleanType.class, "boolean");
        return (boolean) values[rowId];
    }

    @Override
    public byte getByte(int rowId) {
        throwIfUnsafeAccess(ByteType.class, "byte");
        return (byte) values[rowId];
    }

    @Override
    public short getShort(int rowId) {
        throwIfUnsafeAccess(ShortType.class, "short");
        return (short) values[rowId];
    }

    @Override
    public int getInt(int rowId) {
        throwIfUnsafeAccess(IntegerType.class, "integer");
        return (int) values[rowId];
    }

    @Override
    public long getLong(int rowId) {
        throwIfUnsafeAccess(LongType.class, "long");
        return (long) values[rowId];
    }

    @Override
    public float getFloat(int rowId) {
        throwIfUnsafeAccess(FloatType.class, "float");
        return (float) values[rowId];
    }

    @Override
    public double getDouble(int rowId) {
        throwIfUnsafeAccess(DoubleType.class, "double");
        return (double) values[rowId];
    }

    @Override
    public String getString(int rowId) {
        throwIfUnsafeAccess(StringType.class, "string");
        return (String) values[rowId];
    }

    @Override
    public BigDecimal getDecimal(int rowId) {
        throwIfUnsafeAccess(DecimalType.class, "decimal");
        return (BigDecimal) values[rowId];
    }

    @Override
    public byte[] getBinary(int rowId) {
        throwIfUnsafeAccess(BinaryType.class, "binary");
        return (byte[]) values[rowId];
    }

    @Override
    public Row getStruct(int rowId) {
        throwIfUnsafeAccess(StructType.class, "struct");
        return (Row) values[rowId];
    }

    @Override
    public ArrayValue getArray(int rowId) {
        // TODO: not sufficient check, also need to check the element type
        throwIfUnsafeAccess(ArrayType.class, "array");
        return (ArrayValue) values[rowId];
    }

    @Override
    public MapValue getMap(int rowId) {
        // TODO: not sufficient check, also need to check the element types
        throwIfUnsafeAccess(MapType.class, "map");
        return (MapValue) values[rowId];
    }

    private void throwIfUnsafeAccess( Class<? extends DataType> expDataType, String accessType) {
        if (!expDataType.isAssignableFrom(dataType.getClass())) {
            String msg = String.format(
                    "Trying to access a `%s` value from vector of type `%s`",
                    accessType,
                    dataType);
            throw new UnsupportedOperationException(msg);
        }
    }}
