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
package io.delta.kernel.internal.data;

import java.math.BigDecimal;

import io.delta.kernel.data.ArrayValue;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.MapValue;
import io.delta.kernel.data.Row;
import io.delta.kernel.data.VariantValue;
import io.delta.kernel.types.StructType;

/**
 * A {@link Row} implementation that wraps a set of child vectors for a specific {@code rowId}.
 */
public abstract class ChildVectorBasedRow implements Row {

    private final int rowId;
    private final StructType schema;

    public ChildVectorBasedRow(int rowId, StructType schema) {
        this.rowId = rowId;
        this.schema = schema;
    }

    @Override
    public StructType getSchema() {
        return schema;
    }

    @Override
    public boolean isNullAt(int ordinal) {
        return getChild(ordinal).isNullAt(rowId);
    }

    @Override
    public boolean getBoolean(int ordinal) {
        return getChild(ordinal).getBoolean(rowId);
    }

    @Override
    public byte getByte(int ordinal) {
        return getChild(ordinal).getByte(rowId);
    }

    @Override
    public short getShort(int ordinal) {
        return getChild(ordinal).getShort(rowId);
    }

    @Override
    public int getInt(int ordinal) {
        return getChild(ordinal).getInt(rowId);
    }

    @Override
    public long getLong(int ordinal) {
        return getChild(ordinal).getLong(rowId);
    }

    @Override
    public float getFloat(int ordinal) {
        return getChild(ordinal).getFloat(rowId);
    }

    @Override
    public double getDouble(int ordinal) {
        return getChild(ordinal).getDouble(rowId);
    }

    @Override
    public String getString(int ordinal) {
        return getChild(ordinal).getString(rowId);
    }

    @Override
    public BigDecimal getDecimal(int ordinal) {
        return getChild(ordinal).getDecimal(rowId);
    }

    @Override
    public byte[] getBinary(int ordinal) {
        return getChild(ordinal).getBinary(rowId);
    }

    @Override
    public Row getStruct(int ordinal) {
        return StructRow.fromStructVector(getChild(ordinal), rowId);
    }

    @Override
    public ArrayValue getArray(int ordinal) {
        return getChild(ordinal).getArray(rowId);
    }

    @Override
    public MapValue getMap(int ordinal) {
        return getChild(ordinal).getMap(rowId);
    }

    @Override
    public VariantValue getVariant(int ordinal) {
        return getChild(ordinal).getVariant(rowId);
    }

    protected abstract ColumnVector getChild(int ordinal);
}
