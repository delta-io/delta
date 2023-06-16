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

package io.delta.kernel.expressions;

import java.util.Comparator;

import io.delta.kernel.types.DataType;
import io.delta.kernel.types.PrimitiveType;

// TODO: exclude from public interfaces (move to "internal" somewhere?)
public class CastingComparator<T extends Comparable<T>> implements Comparator<Object> {

    public static Comparator<Object> forDataType(DataType dataType) {
        if (dataType.equals(PrimitiveType.INTEGER)) {
            return new CastingComparator<Integer>();
        }

        if (dataType.equals(PrimitiveType.BOOLEAN)) {
            return new CastingComparator<Boolean>();
        }

        if (dataType.equals(PrimitiveType.LONG)) {
            return new CastingComparator<Long>();
        }

        if (dataType.equals(PrimitiveType.STRING)) {
            return new CastingComparator<String>();
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
