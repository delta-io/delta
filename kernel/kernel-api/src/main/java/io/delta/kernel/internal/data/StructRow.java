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
import io.delta.kernel.types.StructType;

import static io.delta.kernel.internal.util.InternalUtils.checkArgument;

/**
 * A {@link Row} abstraction for a struct type column vector and a specific {@code rowId}.
 */
public class StructRow extends ChildVectorBasedRow {

    public static StructRow fromStructVector(ColumnVector columnVector, int rowId) {
        checkArgument(columnVector.getDataType() instanceof StructType);
        if (columnVector.isNullAt(rowId)) {
            return null;
        } else {
            return new StructRow(columnVector, rowId, (StructType) columnVector.getDataType());
        }
    }

    private final ColumnVector structVector;

    private StructRow(ColumnVector structVector, int rowId, StructType schema) {
        super(rowId, schema);
        this.structVector = structVector;
    }

    @Override
    protected ColumnVector getChild(int ordinal) {
        return structVector.getChild(ordinal);
    }
}
