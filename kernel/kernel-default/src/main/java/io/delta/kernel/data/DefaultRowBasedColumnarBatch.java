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

import static io.delta.kernel.DefaultKernelUtils.checkArgument;
import java.util.List;
import java.util.Map;

import io.delta.kernel.types.DataType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;

/**
 * {@link ColumnarBatch} wrapper around list of {@link Row} objects.
 */
public class DefaultRowBasedColumnarBatch
    implements ColumnarBatch
{
    private final StructType schema;
    private final List<Row> rows;

    public DefaultRowBasedColumnarBatch(StructType schema, List<Row> rows)
    {
        this.schema = schema;
        this.rows = rows;
    }

    @Override
    public StructType getSchema()
    {
        return schema;
    }

    @Override
    public int getSize()
    {
        return rows.size();
    }

    @Override
    public ColumnVector getColumnVector(int ordinal)
    {
        StructField field = schema.at(ordinal);
        return new DefaultColumnVector(field.getDataType(), rows, ordinal);
    }

    /**
     * Wrapper around list of {@link Row}s to expose the rows as a columnar vector
     */
    private static class DefaultColumnVector implements ColumnVector
    {
        private final DataType dataType;
        private final List<Row> rows;
        private final int columnOrdinal;

        DefaultColumnVector(DataType dataType, List<Row> rows, int columnOrdinal)
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
        public void close() { /* nothing to close */ }

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
            return rows.get(rowId).getByte(columnOrdinal);
        }

        @Override
        public short getShort(int rowId)
        {
            assertValidRowId(rowId);
            return rows.get(rowId).getShort(columnOrdinal);
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
            return rows.get(rowId).getFloat(columnOrdinal);
        }

        @Override
        public double getDouble(int rowId)
        {
            assertValidRowId(rowId);
            return rows.get(rowId).getDouble(columnOrdinal);
        }

        @Override
        public String getString(int rowId)
        {
            assertValidRowId(rowId);
            return rows.get(rowId).getString(columnOrdinal);
        }

        @Override
        public byte[] getBinary(int rowId)
        {
            assertValidRowId(rowId);
            return rows.get(rowId).getBinary(columnOrdinal);
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
            return rows.get(rowId).getStruct(columnOrdinal);
        }

        @Override
        public <T> List<T> getArray(int rowId)
        {
            assertValidRowId(rowId);
            return rows.get(rowId).getArray(columnOrdinal);
        }

        private void assertValidRowId(int rowId)
        {
            checkArgument(rowId < rows.size(),
                "Invalid rowId: " + rowId + ", max allowed rowId is: " + (rows.size() - 1));
        }
    }
}
