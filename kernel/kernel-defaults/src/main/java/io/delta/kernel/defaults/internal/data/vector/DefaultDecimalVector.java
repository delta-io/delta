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

import java.math.BigDecimal;
import java.util.Optional;
import static java.util.Objects.requireNonNull;

import io.delta.kernel.types.DataType;
import io.delta.kernel.types.DecimalType;

import static io.delta.kernel.defaults.internal.Preconditions.checkArgument;

/**
 * {@link io.delta.kernel.data.ColumnVector} implementation for decimal type data.
 */
public class DefaultDecimalVector extends AbstractColumnVector {

    private final BigDecimal[] values;

    /**
     * Create an instance of {@link io.delta.kernel.data.ColumnVector} for decimal type.
     *
     * @param size number of elements in the vector.
     * @param values column vector values.
     */
    public DefaultDecimalVector(
            DataType dataType,
            int size,
            BigDecimal[] values) {

        super(size, dataType, Optional.empty());
        checkArgument(dataType instanceof DecimalType,
                "invalid type for decimal vector: " + dataType);
        this.values = requireNonNull(values, "values is null");
        checkArgument(values.length >= size,
                "invalid number of values (%s) for given size (%s)", values.length, size);
    }

    @Override
    public boolean isNullAt(int rowId) {
        checkValidRowId(rowId);
        return values[rowId] == null;
    }

    @Override
    public BigDecimal getDecimal(int rowId) {
        checkValidRowId(rowId);
        return values[rowId];
    }
}
