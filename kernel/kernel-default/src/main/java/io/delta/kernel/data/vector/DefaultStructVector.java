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
package io.delta.kernel.data.vector;

import static io.delta.kernel.DefaultKernelUtils.checkArgument;
import java.util.List;
import java.util.Map;
import static java.util.Objects.requireNonNull;
import java.util.Optional;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.Row;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.StructType;

/**
 * {@link io.delta.kernel.data.ColumnVector} implementation for struct type data.
 */
public class DefaultStructVector
    extends AbstractColumnVector
{
    private final ColumnVector[] memberVectors;

    /**
     * Create an instance of {@link ColumnVector} for {@code struct} type.
     *
     * @param size number of elements in the vector.
     * @param dataType {@code struct} datatype definition.
     * @param nullability Optional array of nullability value for each element in the vector.
     * All values in the vector are considered non-null when parameter is empty.
     * @param memberVectors column vectors for each member of the struct.
     */
    public DefaultStructVector(
        int size,
        DataType dataType,
        Optional<boolean[]> nullability,
        ColumnVector[] memberVectors)
    {
        super(size, dataType, nullability);
        checkArgument(dataType instanceof StructType, "not a struct type");
        StructType structType = (StructType) dataType;
        checkArgument(
            structType.length() == memberVectors.length,
            "expected a one column vector for each member");
        this.memberVectors = memberVectors;
    }

    @Override
    public Row getStruct(int rowId)
    {
        checkValidRowId(rowId);
        if (isNullAt(rowId)) {
            return null;
        }
        return new StructRow(this, rowId);
    }

    /**
     * Wrapper class to expose one member as a {@link Row}
     */
    private static class StructRow
        implements Row
    {
        private final DefaultStructVector structVector;
        private final int rowId;

        StructRow(DefaultStructVector structVector, int rowId)
        {
            this.structVector = requireNonNull(structVector, "structVector is null");
            checkArgument(
                rowId >= 0 && rowId < structVector.getSize(),
                "invalid row id: %s", rowId);
            this.rowId = rowId;
        }

        @Override
        public StructType getSchema()
        {
            return (StructType) structVector.getDataType();
        }

        @Override
        public boolean isNullAt(int ordinal)
        {
            return structVector.memberVectors[ordinal].isNullAt(rowId);
        }

        @Override
        public boolean getBoolean(int ordinal)
        {
            return structVector.memberVectors[ordinal].getBoolean(rowId);
        }

        @Override
        public byte getByte(int ordinal)
        {
            return structVector.memberVectors[ordinal].getByte(rowId);
        }

        @Override
        public short getShort(int ordinal)
        {
            return structVector.memberVectors[ordinal].getShort(rowId);
        }

        @Override
        public int getInt(int ordinal)
        {
            return structVector.memberVectors[ordinal].getInt(rowId);
        }

        @Override
        public long getLong(int ordinal)
        {
            return structVector.memberVectors[ordinal].getLong(rowId);
        }

        @Override
        public float getFloat(int ordinal)
        {
            return structVector.memberVectors[ordinal].getFloat(rowId);
        }

        @Override
        public double getDouble(int ordinal)
        {
            return structVector.memberVectors[ordinal].getDouble(rowId);
        }

        @Override
        public String getString(int ordinal)
        {
            return structVector.memberVectors[ordinal].getString(rowId);
        }

        @Override
        public byte[] getBinary(int ordinal)
        {
            return structVector.memberVectors[ordinal].getBinary(rowId);
        }

        @Override
        public Row getStruct(int ordinal)
        {
            return structVector.memberVectors[ordinal].getStruct(rowId);
        }

        @Override
        public <T> List<T> getArray(int ordinal)
        {
            return structVector.memberVectors[ordinal].getArray(rowId);
        }

        @Override
        public <K, V> Map<K, V> getMap(int ordinal)
        {
            return structVector.memberVectors[ordinal].getMap(rowId);
        }
    }
}
