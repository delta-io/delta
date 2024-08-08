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
import java.util.List;
import java.util.function.Function;

import io.delta.kernel.data.*;
import io.delta.kernel.types.*;

import static io.delta.kernel.internal.util.Preconditions.checkArgument;

/**
 * Generic column vector implementation to expose an array of objects as a column vector.
 */
public class DefaultGenericVector implements ColumnVector {

    public static DefaultGenericVector fromArray(DataType dataType, Object[] elements) {
        return new DefaultGenericVector(elements.length, dataType, rowId -> elements[rowId]);
    }

    public static DefaultGenericVector fromList(DataType dataType, List<Object> elements) {
        return new DefaultGenericVector(elements.size(), dataType, rowId -> elements.get(rowId));
    }

    private final int size;
    private final DataType dataType;
    private final Function<Integer, Object> rowIdToValueAccessor;

    protected DefaultGenericVector(
            int size,
            DataType dataType,
            Function<Integer, Object> rowIdToValueAccessor) {
        this.size = size;
        this.dataType = dataType;
        this.rowIdToValueAccessor = rowIdToValueAccessor;
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

    }

    @Override
    public boolean isNullAt(int rowId) {
        assertValidRowId(rowId);
        return rowIdToValueAccessor.apply(rowId) == null;
    }

    @Override
    public boolean getBoolean(int rowId) {
        assertValidRowId(rowId);
        throwIfUnsafeAccess(BooleanType.class, "boolean");
        return (boolean) rowIdToValueAccessor.apply(rowId);
    }

    @Override
    public byte getByte(int rowId) {
        assertValidRowId(rowId);
        throwIfUnsafeAccess(ByteType.class, "byte");
        return (byte) rowIdToValueAccessor.apply(rowId);
    }

    @Override
    public short getShort(int rowId) {
        assertValidRowId(rowId);
        throwIfUnsafeAccess(ShortType.class, "short");
        return (short) rowIdToValueAccessor.apply(rowId);
    }

    @Override
    public int getInt(int rowId) {
        assertValidRowId(rowId);
        throwIfUnsafeAccess(IntegerType.class, DateType.class, dataType.toString());
        return (int) rowIdToValueAccessor.apply(rowId);
    }

    @Override
    public long getLong(int rowId) {
        assertValidRowId(rowId);
        throwIfUnsafeAccess(
                LongType.class,
                TimestampType.class,
                TimestampNTZType.class,
                dataType.toString());
        return (long) rowIdToValueAccessor.apply(rowId);
    }

    @Override
    public float getFloat(int rowId) {
        assertValidRowId(rowId);
        throwIfUnsafeAccess(FloatType.class, "float");
        return (float) rowIdToValueAccessor.apply(rowId);
    }

    @Override
    public double getDouble(int rowId) {
        assertValidRowId(rowId);
        throwIfUnsafeAccess(DoubleType.class, "double");
        return (double) rowIdToValueAccessor.apply(rowId);
    }

    @Override
    public String getString(int rowId) {
        assertValidRowId(rowId);
        throwIfUnsafeAccess(StringType.class, "string");
        return (String) rowIdToValueAccessor.apply(rowId);
    }

    @Override
    public BigDecimal getDecimal(int rowId) {
        assertValidRowId(rowId);
        throwIfUnsafeAccess(DecimalType.class, "decimal");
        return (BigDecimal) rowIdToValueAccessor.apply(rowId);
    }

    @Override
    public byte[] getBinary(int rowId) {
        assertValidRowId(rowId);
        throwIfUnsafeAccess(BinaryType.class, "binary");
        return (byte[]) rowIdToValueAccessor.apply(rowId);
    }

    @Override
    public ArrayValue getArray(int rowId) {
        assertValidRowId(rowId);
        // TODO: not sufficient check, also need to check the element type
        throwIfUnsafeAccess(ArrayType.class, "array");
        return (ArrayValue) rowIdToValueAccessor.apply(rowId);
    }

    @Override
    public MapValue getMap(int rowId) {
        assertValidRowId(rowId);
        // TODO: not sufficient check, also need to check the element types
        throwIfUnsafeAccess(MapType.class, "map");
        return (MapValue) rowIdToValueAccessor.apply(rowId);
    }

    @Override
    public ColumnVector getChild(int ordinal) {
        throwIfUnsafeAccess(StructType.class, "struct");
        StructType structType = (StructType) dataType;
        return new DefaultSubFieldVector(
                getSize(),
                structType.at(ordinal).getDataType(),
                ordinal,
                (rowId) -> (Row) rowIdToValueAccessor.apply(rowId));
    }

    @Override
    public VariantValue getVariant(int rowId) {
        assertValidRowId(rowId);
        throwIfUnsafeAccess(VariantType.class, "variant");
        return (VariantValue) rowIdToValueAccessor.apply(rowId);
    }

    private void throwIfUnsafeAccess( Class<? extends DataType> expDataType, String accessType) {
        if (!expDataType.isAssignableFrom(dataType.getClass())) {
            String msg = String.format(
                    "Trying to access a `%s` value from vector of type `%s`",
                    accessType,
                    dataType);
            throw new UnsupportedOperationException(msg);
        }
    }

    private void throwIfUnsafeAccess(
            Class<? extends DataType> expDataType1,
            Class<? extends DataType> expDataType2,
            String accessType) {
        if (!(expDataType1.isAssignableFrom(dataType.getClass()) ||
                expDataType2.isAssignableFrom(dataType.getClass()))) {
            String msg = String.format(
                    "Trying to access a `%s` value from vector of type `%s`",
                    accessType,
                    dataType);
            throw new UnsupportedOperationException(msg);
        }
    }

    private void throwIfUnsafeAccess(
            Class<? extends DataType> expDataType1,
            Class<? extends DataType> expDataType2,
            Class<? extends DataType> expDataType3,
            String accessType) {
        if (!(expDataType1.isAssignableFrom(dataType.getClass()) ||
                expDataType2.isAssignableFrom(dataType.getClass()) ||
                expDataType3.isAssignableFrom(dataType.getClass()))) {
            String msg = String.format(
                    "Trying to access a `%s` value from vector of type `%s`",
                    accessType,
                    dataType);
            throw new UnsupportedOperationException(msg);
        }
    }

    private void assertValidRowId(int rowId) {
        checkArgument(rowId < size,
                "Invalid rowId: " + rowId + ", max allowed rowId is: " + (size - 1));
    }
}
