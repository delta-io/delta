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

package io.delta.kernel.internal.expressions;

import java.util.Comparator;

import io.delta.kernel.types.BinaryType;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.ByteType;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.DateType;
import io.delta.kernel.types.DoubleType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.FloatType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.ShortType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.TimestampType;

public class CastingComparator<T extends Comparable<T>> implements Comparator<Object> {

    public static Comparator<Object> forDataType(DataType dataType) {
        if (dataType instanceof BooleanType) {
            return new CastingComparator<Boolean>();
        } else if (dataType instanceof ByteType) {
            return new CastingComparator<Byte>();
        } else if (dataType instanceof ShortType) {
            return new CastingComparator<Short>();
        } else if (dataType instanceof IntegerType) {
            return new CastingComparator<Integer>();
        } else if (dataType instanceof LongType) {
            return new CastingComparator<Long>();
        } else if (dataType instanceof FloatType) {
            return new CastingComparator<Float>();
        } else if (dataType instanceof DoubleType) {
            return new CastingComparator<Double>();
        } else if (dataType instanceof StringType) {
            return new CastingComparator<String>();
        } else if (dataType instanceof DateType) {
            // Date value is accessed as integer (number of days since epoch).
            // This may change in the future.
            return new CastingComparator<Integer>();
        } else if (dataType instanceof TimestampType) {
            // Timestamp value is accessed as long (epoch seconds). This may change in the future.
            return new CastingComparator<Long>();
        }

        throw new IllegalArgumentException(
            String.format("Unsupported DataType: %s", dataType));
    }

    private final Comparator<T> comparator;

    public CastingComparator() {
        comparator = Comparator.naturalOrder();
    }

    @SuppressWarnings("unchecked")
    @Override
    public int compare(Object a, Object b) {
        return comparator.compare((T) a, (T) b);
    }
}
