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

import java.util.Objects;

import io.delta.kernel.data.*;

/**
 * Row abstraction around a columnar batch and a particular row within the columnar batch.
 */
public class ColumnarBatchRow
    extends ChildVectorBasedRow {

    private final ColumnarBatch columnarBatch;

    public ColumnarBatchRow(ColumnarBatch columnarBatch, int rowId) {
        super(rowId, Objects.requireNonNull(columnarBatch, "columnarBatch is null").getSchema());
        this.columnarBatch = columnarBatch;
    }

    @Override
    protected ColumnVector getChild(int ordinal) {
        return columnarBatch.getColumnVector(ordinal);
    }
}
