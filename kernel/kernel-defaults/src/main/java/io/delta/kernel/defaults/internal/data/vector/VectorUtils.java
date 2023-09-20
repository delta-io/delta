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

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.types.*;

/**
 * Utility methods for {@link io.delta.kernel.data.ColumnVector} implementations.
 */
public class VectorUtils {
    private VectorUtils() {}

    /**
     * Get the value at given {@code rowId} from the column vector. The type of the value object
     * depends on the data type of the {@code vector}.
     *
     * @param vector
     * @param rowId
     * @return
     */
    public static Object getValueAsObject(ColumnVector vector, int rowId) {
        // TODO: may be it is better to just provide a `getObject` on the `ColumnVector` to
        // avoid the nested if-else statements.
        final DataType dataType = vector.getDataType();

        if (vector.isNullAt(rowId)) {
            return null;
        }

        if (dataType instanceof BooleanType) {
            return vector.getBoolean(rowId);
        } else if (dataType instanceof ByteType) {
            return vector.getByte(rowId);
        } else if (dataType instanceof ShortType) {
            return vector.getShort(rowId);
        } else if (dataType instanceof IntegerType || dataType instanceof DateType) {
            return vector.getInt(rowId);
        } else if (dataType instanceof LongType || dataType instanceof TimestampType) {
            return vector.getLong(rowId);
        } else if (dataType instanceof FloatType) {
            return vector.getFloat(rowId);
        } else if (dataType instanceof DoubleType) {
            return vector.getDouble(rowId);
        } else if (dataType instanceof StringType) {
            return vector.getString(rowId);
        } else if (dataType instanceof BinaryType) {
            return vector.getBinary(rowId);
        } else if (dataType instanceof StructType) {
            return vector.getStruct(rowId);
        } else if (dataType instanceof MapType) {
            return vector.getMap(rowId);
        } else if (dataType instanceof ArrayType) {
            return vector.getArray(rowId);
        } else if (dataType instanceof DecimalType) {
            return vector.getDecimal(rowId);
        }

        throw new UnsupportedOperationException(dataType + " is not supported yet");
    }
}
