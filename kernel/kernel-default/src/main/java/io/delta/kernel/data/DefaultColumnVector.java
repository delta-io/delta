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

import org.apache.parquet.Preconditions;

import io.delta.kernel.types.DataType;

/**
 * Wrapper around list of {@link Row}s to expose the rows as a columnar vector
 */
public class DefaultColumnVector implements ColumnVector
{
    private final DataType dataType;
    private final List<Row> rows;
    private final int columnOrdinal;

    public DefaultColumnVector(DataType dataType, List<Row> rows, int columnOrdinal)
    {
        this.dataType = dataType;
        this.rows = rows;
        this.columnOrdinal = columnOrdinal;
    }

    @Override
    public DataType getDataType()
    {
        return dataType;
    }

    @Override
    public int getSize()
    {
        return rows.size();
    }

    @Override
    public void close()
    {
        // nothing to close
    }

    @Override
    public boolean isNullAt(int rowId)
    {
        assertValidRowId(rowId);
        return rows.get(rowId).isNullAt(columnOrdinal);
    }

    @Override
    public boolean getBoolean(int rowId)
    {
        assertValidRowId(rowId);
        return rows.get(rowId).getBoolean(columnOrdinal);
    }

    @Override
    public byte getByte(int rowId)
    {
        assertValidRowId(rowId);
        return 0; // TODO
    }

    @Override
    public short getShort(int rowId)
    {
        assertValidRowId(rowId);
        return 0; // TODO
    }

    @Override
    public int getInt(int rowId)
    {
        assertValidRowId(rowId);
        return rows.get(rowId).getInt(columnOrdinal);
    }

    @Override
    public long getLong(int rowId)
    {
        assertValidRowId(rowId);
        return rows.get(rowId).getLong(columnOrdinal);
    }

    @Override
    public float getFloat(int rowId)
    {
        assertValidRowId(rowId);
        return 0; // TODO
    }

    @Override
    public double getDouble(int rowId)
    {
        assertValidRowId(rowId);
        return 0; // TODO
    }

    @Override
    public byte[] getBinary(int rowId)
    {
        assertValidRowId(rowId);
        return null; // TODO
    }

    @Override
    public <K, V> Map<K, V> getMap(int rowId)
    {
        assertValidRowId(rowId);
        return rows.get(rowId).getMap(columnOrdinal);
    }

    @Override
    public Row getStruct(int rowId)
    {
        assertValidRowId(rowId);
        return rows.get(rowId).getRecord(columnOrdinal);
    }

    @Override
    public <T> List<T> getArray(int rowId)
    {
        assertValidRowId(rowId);
        return rows.get(rowId).getList(columnOrdinal);
    }

    @Override
    public String getString(int rowId)
    {
        assertValidRowId(rowId);
        return rows.get(rowId).getString(columnOrdinal);
    }

    private void assertValidRowId(int rowId) {
        Preconditions.checkArgument(
                rowId < rows.size(),
                "Invalid rowId: " + rowId + ", max allowed rowId is: " + (rows.size() - 1));
    }
}
