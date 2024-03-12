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
import java.util.function.Function;
import static java.util.Objects.requireNonNull;

import io.delta.kernel.data.ArrayValue;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.MapValue;
import io.delta.kernel.data.Row;
import io.delta.kernel.data.VariantValue;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;

import static io.delta.kernel.internal.util.Preconditions.checkArgument;

/**
 * {@link ColumnVector} wrapper on top of {@link Row} objects. This wrapper allows referencing
 * any nested level column vector from a set of rows.
 */
public class DefaultSubFieldVector implements ColumnVector {
    private final int size;
    private final DataType dataType;
    private final int columnOrdinal;
    private final Function<Integer, Row> rowIdToRowAccessor;

    /**
     * Create an instance of {@link DefaultSubFieldVector}
     *
     * @param size               Number of elements in the vector
     * @param dataType           Datatype of the vector
     * @param columnOrdinal      Ordinal of the column represented by this vector in the rows
     *                           returned by {@link #rowIdToRowAccessor}
     * @param rowIdToRowAccessor {@link Function} that returns a {@link Row} object for given
     *                           rowId
     */
    public DefaultSubFieldVector(
            int size,
            DataType dataType,
            int columnOrdinal,
            Function<Integer, Row> rowIdToRowAccessor) {
        checkArgument(size >= 0, "invalid size: %s", size);
        this.size = size;
        checkArgument(columnOrdinal >= 0, "invalid column ordinal: %s", columnOrdinal);
        this.columnOrdinal = columnOrdinal;
        this.rowIdToRowAccessor =
                requireNonNull(rowIdToRowAccessor, "rowIdToRowAccessor is null");
        this.dataType = requireNonNull(dataType, "dataType is null");
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
    public void close() { /* nothing to close */ }

    @Override
    public boolean isNullAt(int rowId) {
        assertValidRowId(rowId);
        Row row = rowIdToRowAccessor.apply(rowId);
        return row == null || row.isNullAt(columnOrdinal);
    }

    @Override
    public boolean getBoolean(int rowId) {
        assertValidRowId(rowId);
        return rowIdToRowAccessor.apply(rowId).getBoolean(columnOrdinal);
    }

    @Override
    public byte getByte(int rowId) {
        assertValidRowId(rowId);
        return rowIdToRowAccessor.apply(rowId).getByte(columnOrdinal);
    }

    @Override
    public short getShort(int rowId) {
        assertValidRowId(rowId);
        return rowIdToRowAccessor.apply(rowId).getShort(columnOrdinal);
    }

    @Override
    public int getInt(int rowId) {
        assertValidRowId(rowId);
        return rowIdToRowAccessor.apply(rowId).getInt(columnOrdinal);
    }

    @Override
    public long getLong(int rowId) {
        assertValidRowId(rowId);
        return rowIdToRowAccessor.apply(rowId).getLong(columnOrdinal);
    }

    @Override
    public float getFloat(int rowId) {
        assertValidRowId(rowId);
        return rowIdToRowAccessor.apply(rowId).getFloat(columnOrdinal);
    }

    @Override
    public double getDouble(int rowId) {
        assertValidRowId(rowId);
        return rowIdToRowAccessor.apply(rowId).getDouble(columnOrdinal);
    }

    @Override
    public byte[] getBinary(int rowId) {
        assertValidRowId(rowId);
        return rowIdToRowAccessor.apply(rowId).getBinary(columnOrdinal);
    }

    @Override
    public String getString(int rowId) {
        assertValidRowId(rowId);
        return rowIdToRowAccessor.apply(rowId).getString(columnOrdinal);
    }

    @Override
    public BigDecimal getDecimal(int rowId) {
        assertValidRowId(rowId);
        return rowIdToRowAccessor.apply(rowId).getDecimal(columnOrdinal);
    }

    @Override
    public MapValue getMap(int rowId) {
        assertValidRowId(rowId);
        return rowIdToRowAccessor.apply(rowId).getMap(columnOrdinal);
    }

    @Override
    public ArrayValue getArray(int rowId) {
        assertValidRowId(rowId);
        return rowIdToRowAccessor.apply(rowId).getArray(columnOrdinal);
    }

    @Override
    public VariantValue getVariant(int rowId) {
        assertValidRowId(rowId);
        return rowIdToRowAccessor.apply(rowId).getVariant(columnOrdinal);
    }

    @Override
    public ColumnVector getChild(int childOrdinal) {
        StructType structType = (StructType) dataType;
        StructField childField = structType.at(childOrdinal);
        return new DefaultSubFieldVector(
            size,
            childField.getDataType(),
            childOrdinal,
            (rowId) -> {
                if (isNullAt(rowId)) {
                    return null;
                } else {
                    return rowIdToRowAccessor.apply(rowId).getStruct(columnOrdinal);
                }
            });
    }

    private void assertValidRowId(int rowId) {
        checkArgument(rowId < size,
                "Invalid rowId: " + rowId + ", max allowed rowId is: " + (size - 1));
    }
}
