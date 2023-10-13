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

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.StructType;

import static io.delta.kernel.defaults.internal.DefaultKernelUtils.checkArgument;

/**
 * {@link io.delta.kernel.data.ColumnVector} implementation for struct type data.
 */
public class DefaultStructVector
    extends AbstractColumnVector {
    private final ColumnVector[] memberVectors;

    /**
     * Create an instance of {@link ColumnVector} for {@code struct} type.
     *
     * @param size          number of elements in the vector.
     * @param dataType      {@code struct} datatype definition.
     * @param nullability   Optional array of nullability value for each element in the vector.
     *                      All values in the vector are considered non-null when parameter is
     *                      empty.
     * @param memberVectors column vectors for each member of the struct.
     */
    public DefaultStructVector(
        int size,
        DataType dataType,
        Optional<boolean[]> nullability,
        ColumnVector[] memberVectors) {
        super(size, dataType, nullability);
        checkArgument(dataType instanceof StructType, "not a struct type");
        StructType structType = (StructType) dataType;
        checkArgument(
            structType.length() == memberVectors.length,
            "expected a one column vector for each member");
        this.memberVectors = memberVectors;
    }

    @Override
    public ColumnVector getChild(int ordinal) {
        checkArgument(
            ordinal >= 0 && ordinal < memberVectors.length, "Invalid ordinal " + ordinal);
        return memberVectors[ordinal];
    }
}
