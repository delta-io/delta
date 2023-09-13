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

import java.util.Arrays;

import io.delta.kernel.annotation.Evolving;

/**
 * {@code AND} expression
 * <p>
 * Definition:
 * <p>
 * <ul>
 *     <li>Logical {@code expr1} AND {@code expr2} on two inputs.</li>
 *     <li>Requires both left and right input expressions of type {@link Predicate}.</li>
 *     <li>Result is null at least one of the inputs is null.</li>
 * </ul>
 *
 * @since 3.0.0
 */
@Evolving
public final class And extends Predicate {
    public And(Predicate left, Predicate right) {
        super("AND", Arrays.asList(left, right));
    }

    /**
     * @return Left side operand.
     */
    public Predicate getLeft() {
        return (Predicate) getChildren().get(0);
    }

    /**
     * @return Right side operand.
     */
    public Predicate getRight() {
        return (Predicate) getChildren().get(1);
    }

    @Override
    public String toString() {
        return "(" + getLeft() + " AND " + getRight() + ")";
    }
}
