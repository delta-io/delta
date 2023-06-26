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
package io.delta.kernel.internal.util;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.FileDataReadResult;
import io.delta.kernel.data.Row;
import io.delta.kernel.fs.FileStatus;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;

import io.delta.kernel.internal.actions.AddFile;
import io.delta.kernel.internal.data.AddFileColumnarBatch;

public class InternalUtils
{
    private InternalUtils() {}

    public static Row getScanFileRow(FileStatus fileStatus)
    {
        AddFile addFile = new AddFile(
            fileStatus.getPath(),
            Collections.emptyMap(),
            fileStatus.getSize(),
            fileStatus.getModificationTime(),
            false /* dataChange */
        );

        return new AddFileColumnarBatch(Collections.singletonList(addFile))
            .getRows()
            .next();
    }

    /**
     * Utility method to read at most one row from the given data {@link FileDataReadResult} iterator.
     * If there is more than one row, an exception will be thrown.
     *
     * @param dataIter
     * @return
     */
    public static Optional<Row> getSingularRow(CloseableIterator<FileDataReadResult> dataIter)
        throws IOException
    {
        Row row = null;
        while (dataIter.hasNext()) {
            try (CloseableIterator<Row> rows = dataIter.next().getData().getRows()) {
                while (rows.hasNext()) {
                    if (row != null) {
                        throw new IllegalArgumentException(
                            "Given data batch contains more than one row");
                    }
                    row = rows.next();
                }
            }
        }
        return Optional.ofNullable(row);
    }

    // TODO: this is a bloated code to accomplish this. One thing we could do is add a new API
    // on ColumnarBatch that will replace the schema or not have the schema at all on the
    // ColumnarBatch.
    /**
     * Create a new {@link ColumnarBatch} with the given logical schema.
     *
     * @param columnarBatch
     * @param logicalSchema
     * @return
     */
    public static ColumnarBatch columnarBatchWithLogicalSchema(
        ColumnarBatch columnarBatch,
        StructType logicalSchema)
    {
        ensureTypeMatch(logicalSchema, columnarBatch.getSchema());
        return new ColumnarBatch()
        {
            @Override
            public StructType getSchema()
            {
                return logicalSchema;
            }

            @Override
            public ColumnVector getColumnVector(int ordinal)
            {
                return new ColumnVector()
                {
                    @Override
                    public DataType getDataType()
                    {
                        return logicalSchema.at(ordinal).getDataType();
                    }

                    @Override
                    public int getSize()
                    {
                        return columnarBatch.getColumnVector(ordinal).getSize();
                    }

                    @Override
                    public void close()
                    {
                        columnarBatch.getColumnVector(ordinal).close();
                    }

                    @Override
                    public boolean isNullAt(int rowId)
                    {
                        return columnarBatch.getColumnVector(ordinal).isNullAt(rowId);
                    }

                    @Override
                    public boolean getBoolean(int rowId)
                    {
                        return columnarBatch.getColumnVector(ordinal).getBoolean(rowId);
                    }

                    @Override
                    public byte getByte(int rowId)
                    {
                        return columnarBatch.getColumnVector(ordinal).getByte(rowId);
                    }

                    @Override
                    public short getShort(int rowId)
                    {
                        return columnarBatch.getColumnVector(ordinal).getShort(rowId);
                    }

                    @Override
                    public int getInt(int rowId)
                    {
                        return columnarBatch.getColumnVector(ordinal).getInt(rowId);
                    }

                    @Override
                    public long getLong(int rowId)
                    {
                        return columnarBatch.getColumnVector(ordinal).getLong(rowId);
                    }

                    @Override
                    public float getFloat(int rowId)
                    {
                        return columnarBatch.getColumnVector(ordinal).getFloat(rowId);
                    }

                    @Override
                    public double getDouble(int rowId)
                    {
                        return columnarBatch.getColumnVector(ordinal).getDouble(rowId);
                    }

                    @Override
                    public byte[] getBinary(int rowId)
                    {
                        return columnarBatch.getColumnVector(ordinal).getBinary(rowId);
                    }

                    @Override
                    public String getString(int rowId)
                    {
                        return columnarBatch.getColumnVector(ordinal).getString(rowId);
                    }

                    @Override
                    public <K, V> Map<K, V> getMap(int rowId)
                    {
                        return columnarBatch.getColumnVector(ordinal).getMap(rowId);
                    }

                    @Override
                    public Row getStruct(int rowId)
                    {
                        return columnarBatch.getColumnVector(ordinal).getStruct(rowId);
                    }

                    @Override
                    public <T> List<T> getArray(int rowId)
                    {
                        return columnarBatch.getColumnVector(ordinal).getArray(rowId);
                    }
                };
            }

            @Override
            public int getSize()
            {
                return columnarBatch.getSize();
            }
        };
    }

    /**
     * Utility method that checks the data types of given schemas are same except the field
     * names
     */
    private static void ensureTypeMatch(StructType schema1, StructType schema2)
    {
        if (schema1.length() != schema2.length()) {
            throw new IllegalArgumentException("Different number of fields");
        }

        for (int fieldId = 0; fieldId < schema1.length(); fieldId++) {
            StructField field1 = schema1.at(fieldId);
            StructField field2 = schema2.at(fieldId);

            if (!field1.getDataType().equals(field2.getDataType())) {
                throw new IllegalArgumentException(
                    String.format("Different schemas: \n%s,\n%s", schema1, schema2));
            }
        }
    }
}
