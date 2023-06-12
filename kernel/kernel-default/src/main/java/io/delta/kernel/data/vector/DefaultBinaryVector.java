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
package io.delta.kernel.data.vector;

import io.delta.kernel.types.DataType;
import io.delta.kernel.types.StringType;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

import static io.delta.kernel.DefaultKernelUtils.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * {@link io.delta.kernel.data.ColumnVector} implementation for binary type data.
 */
public class DefaultBinaryVector
        extends AbstractColumnVector
{
    private final byte[][] values;

    /**
     * Create an instance of {@link io.delta.kernel.data.ColumnVector} for binary type.
     *
     * @param size number of elements in the vector.
     * @param values column vector values.
     */
    public DefaultBinaryVector(DataType dataType, int size, byte[][] values)
    {
        super(size, dataType, Optional.empty());
        // TODO: add check for BINARY when support for BINARY is added.
        checkArgument(dataType == StringType.INSTANCE, "invalid type");
        this.values = requireNonNull(values, "values is null");
        checkArgument(values.length >= size,
                "invalid number of values (%s) for given size (%s)", values.length, size);
        checkArgument(values.length >= 0, "invalid vector size: %s", values.length);
    }

    @Override
    public boolean isNullAt(int rowId)
    {
        checkValidRowId(rowId);
        return values[rowId] == null;
    }

    /**
     * Get the value at given {@code rowId}. The return value is undefined and can be
     * anything, if the slot for {@code rowId} is null.
     * The error check on {@code rowId} explicitly skipped for performance reasons.
     *
     * @param rowId
     * @return
     */
    @Override
    public String getString(int rowId)
    {
        if (!(getDataType() instanceof StringType)) {
            throw unsupportedDataAccessException("string");
        }
        checkValidRowId(rowId);
        byte[] value = values[rowId];
        if (value == null) {
            return null;
        }
        return StandardCharsets.UTF_8.decode(ByteBuffer.wrap(value)).toString();
    }

    /**
     * Get the value at given {@code rowId}. The return value is undefined and can be
     * anything, if the slot for {@code rowId} is null.
     * The error check on {@code rowId} explicitly skipped for performance reasons.
     *
     * @param rowId
     * @return
     */
    @Override
    public byte[] getBinary(int rowId)
    {
        checkValidRowId(rowId);
        return values[rowId];
    }
}
