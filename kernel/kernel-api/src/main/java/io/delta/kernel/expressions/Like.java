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

import io.delta.kernel.annotation.Evolving;

/**
 * {@code LIKE} expression
 * <p>
 * Definition:
 * <p><ul>
 *     <li>{@code expr1} LIKE {@code expr2} on two inputs.</li>
 *     <li>Requires both left and right input expressions of type {@link Expression}
 *     and only STRING data types is supported.</li>
 *     <li>Result is null when either of the inputs is null,
 *     true if left {@code LIKE} right, false otherwise.
 *     Note that matches are case sensitive.</li>
 * </ul>
 *
 * @since 3.3.0
 */
@Evolving
public final class Like extends Predicate{

    char escape = '\\';

    /**
     * Constructor for a binary Predicate expression
     *
     * @param left expression to evaluate predicate for
     * @param right expression left operand is evaluated against
     */
    public Like(Expression left, Expression right) {
        super("LIKE", left, right);
    }

    public Like(Expression left, Expression right, char escape) {
        this(left, right);
        this.escape = escape;
    }

    /**

     public Predicate getRight() {
     return (Predicate) getChildren().get(1);
     }

     /**
     * @return the escape character
     */
    public char getEscape() {
        return escape;
    }

}
