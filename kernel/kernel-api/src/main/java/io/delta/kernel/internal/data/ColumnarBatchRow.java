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

import java.util.List;
import java.util.Map;
import java.util.Objects;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.types.StructType;

/**
 * Row abstraction around a columnar batch and a particular row within the columnar batch.
 */
public class ColumnarBatchRow
    implements Row
{
    private final ColumnarBatch columnarBatch;
    private final int rowId;

    public ColumnarBatchRow(ColumnarBatch columnarBatch, int rowId)
    {
        this.columnarBatch = Objects.requireNonNull(columnarBatch, "columnarBatch is null");
        this.rowId = rowId;
    }

    @Override
    public StructType getSchema()
    {
        return columnarBatch.getSchema();
    }

    @Override
    public boolean isNullAt(int ordinal)
    {
        return columnVector(ordinal).isNullAt(rowId);
    }

    @Override
    public boolean getBoolean(int ordinal)
    {
        return columnVector(ordinal).getBoolean(rowId);
    }

    @Override
    public byte getByte(int ordinal)
    {
        return columnVector(ordinal).getByte(rowId);
    }

    @Override
    public short getShort(int ordinal)
    {
        return columnVector(ordinal).getShort(rowId);
    }

    @Override
    public int getInt(int ordinal)
    {
        return columnVector(ordinal).getInt(rowId);
    }

    @Override
    public long getLong(int ordinal)
    {
        return columnVector(ordinal).getLong(rowId);
    }

    @Override
    public float getFloat(int ordinal)
    {
        return columnVector(ordinal).getFloat(rowId);
    }

    @Override
    public double getDouble(int ordinal)
    {
        return columnVector(ordinal).getDouble(rowId);
    }

    @Override
    public String getString(int ordinal)
    {
        return columnVector(ordinal).getString(rowId);
    }

    @Override
    public byte[] getBinary(int ordinal)
    {
        return columnVector(ordinal).getBinary(rowId);
    }

    @Override
    public Row getStruct(int ordinal)
    {
        return columnVector(ordinal).getStruct(rowId);
    }

    @Override
    public <T> List<T> getArray(int ordinal)
    {
        return columnVector(ordinal).getArray(rowId);
    }

    @Override
    public <K, V> Map<K, V> getMap(int ordinal)
    {
        return columnVector(ordinal).getMap(rowId);
    }

    private ColumnVector columnVector(int ordinal)
    {
        return columnarBatch.getColumnVector(ordinal);
    }
}
