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

/**
 * A {@link BinaryExpression} that is an operator, meaning the string representation is
 * {@code x symbol y}, rather than {@code funcName(x, y)}.
 * <p>
 * Requires both inputs to be of the same data type.
 */
public abstract class BinaryOperator extends BinaryExpression {
    protected final String symbol;

    protected BinaryOperator(Expression left, Expression right, String symbol) {
        super(left, right);
        this.symbol = symbol;

        if (!left.dataType().equals(right.dataType())) {
            throw new IllegalArgumentException(
                String.format(
                    "BinaryOperator left and right DataTypes must be the same. Found %s and %s.",
                    left.dataType(),
                    right.dataType()
            ));
        }
    }

    @Override
    public String toString() {
        return String.format("(%s %s %s)", left.toString(), symbol, right.toString());
    }
}
