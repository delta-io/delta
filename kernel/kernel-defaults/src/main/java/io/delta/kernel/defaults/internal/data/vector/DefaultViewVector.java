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
import java.util.Map;

import io.delta.kernel.data.ArrayValue;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.Row;
import io.delta.kernel.types.DataType;
import static io.delta.kernel.defaults.internal.DefaultKernelUtils.checkArgument;

// TODO test!!
public class DefaultViewVector implements ColumnVector {

    private final ColumnVector underlyingVector;
    private final int offset;
    private final int size;

    // TODO docs
    public DefaultViewVector(ColumnVector underlyingVector, int start, int end) {
        this.underlyingVector = underlyingVector;
        this.offset = start;
        this.size = end - start;
    }

    @Override
    public DataType getDataType() {
        return underlyingVector.getDataType();
    }

    @Override
    public int getSize() {
        return size;
    }

    @Override
    public void close() {
        underlyingVector.close();
    }

    @Override
    public boolean isNullAt(int rowId) {
        checkValidRowId(rowId);
        return underlyingVector.isNullAt(offset + rowId);
    }

    @Override
    public boolean getBoolean(int rowId) {
        checkValidRowId(rowId);
        return underlyingVector.getBoolean(offset + rowId);
    }

    @Override
    public byte getByte(int rowId) {
        checkValidRowId(rowId);
        return underlyingVector.getByte(offset + rowId);
    }

    @Override
    public short getShort(int rowId) {
        checkValidRowId(rowId);
        return underlyingVector.getShort(offset + rowId);
    }

    @Override
    public int getInt(int rowId) {
        checkValidRowId(rowId);
        return underlyingVector.getInt(offset + rowId);
    }

    @Override
    public long getLong(int rowId) {
        checkValidRowId(rowId);
        return underlyingVector.getLong(offset + rowId);
    }

    @Override
    public float getFloat(int rowId) {
        checkValidRowId(rowId);
        return underlyingVector.getFloat(offset + rowId);
    }

    @Override
    public double getDouble(int rowId) {
        checkValidRowId(rowId);
        return underlyingVector.getDouble(offset + rowId);
    }

    @Override
    public byte[] getBinary(int rowId) {
        checkValidRowId(rowId);
        return underlyingVector.getBinary(offset + rowId);
    }

    @Override
    public String getString(int rowId) {
        checkValidRowId(rowId);
        return underlyingVector.getString(offset + rowId);
    }

    @Override
    public BigDecimal getDecimal(int rowId) {
        checkValidRowId(rowId);
        return underlyingVector.getDecimal(offset + rowId);
    }

    @Override
    public <K, V> Map<K, V> getMap(int rowId) {
        checkValidRowId(rowId);
        return underlyingVector.getMap(offset + rowId);
    }

    @Override
    public Row getStruct(int rowId) {
        checkValidRowId(rowId);
        return underlyingVector.getStruct(offset + rowId);
    }

    @Override
    public ArrayValue getArray(int rowId) {
        checkValidRowId(rowId);
        return underlyingVector.getArray(offset + rowId);
    }

    private void checkValidRowId(int rowId) {
        checkArgument(rowId >= 0 && rowId < size,
                String.format(
                        "Invalid rowId=%s for size=%s",
                        rowId,
                        size
                ));
    }
}
