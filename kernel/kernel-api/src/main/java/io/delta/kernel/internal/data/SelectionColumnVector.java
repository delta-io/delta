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

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.Row;
import io.delta.kernel.internal.deletionvectors.RoaringBitmapArray;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.DataType;

import java.util.List;
import java.util.Map;

/**
 * The selection vector for a columnar batch as a boolean {@link ColumnVector}.
 */
public class SelectionColumnVector
        implements ColumnVector {

    private final RoaringBitmapArray bitmap;
    private final ColumnVector rowIndices;

    public SelectionColumnVector(RoaringBitmapArray bitmap, ColumnVector rowIndices) {
        this.bitmap = bitmap;
        this.rowIndices = rowIndices;
    }

    @Override
    public DataType getDataType()
    {
        return BooleanType.INSTANCE;
    }

    @Override
    public int getSize()
    {
        return rowIndices.getSize();
    }

    @Override
    public void close() {
        rowIndices.close();
    }

    @Override
    public boolean isNullAt(int rowId)
    {
        return false;
    }

    @Override
    public boolean getBoolean(int rowId)
    {
        return !bitmap.contains(rowIndices.getLong(rowId));
    }
}
