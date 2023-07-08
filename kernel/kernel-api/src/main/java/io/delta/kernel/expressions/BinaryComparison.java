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

import io.delta.kernel.internal.expressions.CastingComparator;

/**
 * A {@link BinaryOperator} that compares the left and right {@link Expression}s and evaluates to a
 * boolean value.
 */
public abstract class BinaryComparison extends BinaryOperator implements Predicate {
    private final Comparator<Object> comparator;

    protected BinaryComparison(Expression left, Expression right, String symbol) {
        super(left, right, symbol);

        // super asserted that left and right DataTypes were the same

        comparator = CastingComparator.forDataType(left.dataType());
    }

    protected int compare(Object leftResult, Object rightResult) {
        return comparator.compare(leftResult, rightResult);
    }
}
