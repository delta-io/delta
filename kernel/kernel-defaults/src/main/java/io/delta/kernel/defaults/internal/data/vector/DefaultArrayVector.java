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

import java.util.Optional;
import static java.util.Objects.requireNonNull;

import io.delta.kernel.data.ArrayValue;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.types.DataType;

import static io.delta.kernel.internal.util.Preconditions.checkArgument;

/**
 * {@link io.delta.kernel.data.ColumnVector} implementation for array type data.
 */
public class DefaultArrayVector
    extends AbstractColumnVector {
    private final int[] offsets;
    private final ColumnVector elementVector;

    /**
     * Create an instance of {@link io.delta.kernel.data.ColumnVector} for array type.
     *
     * @param size          number of elements in the vector.
     * @param nullability   Optional array of nullability value for each element in the vector.
     *                      All values in the vector are considered non-null when parameter is
     *                      empty.
     * @param offsets       Offsets into element vector on where the index of particular row
     *                      values start and end.
     * @param elementVector Vector containing the array elements.
     */
    public DefaultArrayVector(
        int size,
        DataType type,
        Optional<boolean[]> nullability,
        int[] offsets,
        ColumnVector elementVector) {
        super(size, type, nullability);
        checkArgument(offsets.length >= size + 1, "invalid offset array size");
        this.offsets = requireNonNull(offsets, "offsets is null");
        this.elementVector = requireNonNull(elementVector, "elementVector is null");
    }

    /**
     * Get the value at given {@code rowId}. The return value is undefined and can be
     * anything, if the slot for {@code rowId} is null.
     *
     * @param rowId
     * @return
     */
    @Override
    public ArrayValue getArray(int rowId) {
        checkValidRowId(rowId);
        if (isNullAt(rowId)) {
            return null;
        }
        // use the offsets array to find the starting and ending index in the underlying vector
        // for this rowId
        int start = offsets[rowId];
        int end = offsets[rowId + 1];
        return new ArrayValue() {

            // create a view over the elements for this rowId
            private final ColumnVector elements = new DefaultViewVector(elementVector, start, end);

            @Override
            public int getSize() {
                return elements.getSize();
            }

            @Override
            public ColumnVector getElements() {
                return elements;
            }
        };
    }

    public ColumnVector getElementVector() {
        return elementVector;
    }

    public int[] getOffsets() {
        return offsets;
    }
}
