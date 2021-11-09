/*
 * Copyright (2020-present) The Delta Lake Project Authors.
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

package io.delta.standalone.expressions;

/**
 * Usage: {@code new GreaterThan(expr1, expr2)} - Returns true if {@code expr1} is greater than
 * {@code expr2}, else false.
 */
public final class GreaterThan extends BinaryComparison implements Predicate {
    public GreaterThan(Expression left, Expression right) {
        super(left, right, ">");
    }

    @Override
    public Object nullSafeEval(Object leftResult, Object rightResult) {
        return compare(leftResult, rightResult) > 0;
    }
}
