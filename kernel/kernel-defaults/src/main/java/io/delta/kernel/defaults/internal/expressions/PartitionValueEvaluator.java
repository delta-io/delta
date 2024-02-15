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
package io.delta.kernel.defaults.internal.expressions;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.types.*;

import io.delta.kernel.internal.util.InternalUtils;

/**
 * Utility methods to evaluate {@code partition_value} expression
 */
class PartitionValueEvaluator {
    /**
     * Evaluate the {@code partition_value} expression for given input column vector and generate
     * a column vector with decoded values according to the given partition type.
     */
    static ColumnVector eval(ColumnVector input, DataType partitionType) {
        return new ColumnVector() {
            @Override
            public DataType getDataType() {
                return partitionType;
            }

            @Override
            public int getSize() {
                return input.getSize();
            }

            @Override
            public void close() {
                input.close();
            }

            @Override
            public boolean isNullAt(int rowId) {
                return input.isNullAt(rowId);
            }

            @Override
            public boolean getBoolean(int rowId) {
                return Boolean.parseBoolean(input.getString(rowId));
            }

            @Override
            public byte getByte(int rowId) {
                return Byte.parseByte(input.getString(rowId));
            }

            @Override
            public short getShort(int rowId) {
                return Short.parseShort(input.getString(rowId));
            }

            @Override
            public int getInt(int rowId) {
                if (partitionType.equivalent(IntegerType.INTEGER)) {
                    return Integer.parseInt(input.getString(rowId));
                } else if (partitionType.equivalent(DateType.DATE)) {
                    return InternalUtils.daysSinceEpoch(Date.valueOf(input.getString(rowId)));
                }
                throw new UnsupportedOperationException("Invalid value request for data type");
            }

            @Override
            public long getLong(int rowId) {
                if (partitionType.equivalent(LongType.LONG)) {
                    return Long.parseLong(input.getString(rowId));
                } else if (partitionType.equivalent(TimestampType.TIMESTAMP)) {
                    return InternalUtils.microsSinceEpoch(
                        Timestamp.valueOf(input.getString(rowId)));
                }
                throw new UnsupportedOperationException("Invalid value request for data type");
            }

            @Override
            public float getFloat(int rowId) {
                return Float.parseFloat(input.getString(rowId));
            }

            @Override
            public double getDouble(int rowId) {
                return Double.parseDouble(input.getString(rowId));
            }

            @Override
            public byte[] getBinary(int rowId) {
                return input.isNullAt(rowId) ? null : input.getString(rowId).getBytes();
            }

            @Override
            public String getString(int rowId) {
                return input.getString(rowId);
            }

            @Override
            public BigDecimal getDecimal(int rowId) {
                return input.isNullAt(rowId) ? null : new BigDecimal(input.getString(rowId));
            }
        };
    }
}
