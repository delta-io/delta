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

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.VariantValue;
import io.delta.kernel.types.DataType;

import static io.delta.kernel.internal.util.Preconditions.checkArgument;

/**
 * {@link io.delta.kernel.data.ColumnVector} implementation for variant type data.
 */
public class DefaultVariantVector
    extends AbstractColumnVector {
    private final ColumnVector valueVector;
    private final ColumnVector metadataVector;


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
    public DefaultVariantVector(
        int size,
        DataType type,
        Optional<boolean[]> nullability,
        ColumnVector value,
        ColumnVector metadata) {
        super(size, type, nullability);
        this.valueVector = requireNonNull(value, "value is null");
        this.metadataVector = requireNonNull(metadata, "metadata is null");
    }

    /**
     * Get the value at given {@code rowId}. The return value is undefined and can be
     * anything, if the slot for {@code rowId} is null.
     *
     * @param rowId
     * @return
     */
    @Override
    public VariantValue getVariant(int rowId) {
        checkValidRowId(rowId);
        if (isNullAt(rowId)) {
            return null;
        }
        return new VariantValue() {
            private final byte[] value = valueVector.getBinary(rowId);
            private final byte[] metadata = metadataVector.getBinary(rowId);

            @Override
            public byte[] getValue() {
                return value;
            }

            @Override
            public byte[] getMetadata() {
                return metadata;
            }
        };
    }

    @Override
    public ColumnVector getChild(int ordinal) {
        checkArgument(ordinal >= 0 && ordinal < 2, "Invalid ordinal " + ordinal);
        if (ordinal == 0) {
            return valueVector;
        } else {
            return metadataVector;
        }
    }
}
