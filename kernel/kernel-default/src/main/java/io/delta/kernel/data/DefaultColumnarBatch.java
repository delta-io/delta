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

import io.delta.kernel.types.StructType;

public class DefaultColumnarBatch
    implements ColumnarBatch
{
    private final StructType dataType;
    private final int size;
    private final ColumnVector[] columnVectors;

    public DefaultColumnarBatch(
        int size,
        StructType dataType,
        ColumnVector[] columnVectors
    )
    {
        this.dataType = dataType;
        this.size = size;
        this.columnVectors = new ColumnVector[columnVectors.length];
        // TODO: argument check.
        System.arraycopy(columnVectors, 0, this.columnVectors, 0, columnVectors.length);
    }

    @Override
    public StructType getSchema()
    {
        return dataType;
    }

    @Override
    public ColumnVector getColumnVector(int ordinal)
    {
        checkColumnOrdinal(ordinal);
        return columnVectors[ordinal];
    }

    @Override
    public int getSize()
    {
        return size;
    }

    private void checkColumnOrdinal(int ordinal)
    {
        if (ordinal < 0 || ordinal >= columnVectors.length) {
            throw new IllegalArgumentException("invalid column ordinal");
        }
    }
}
