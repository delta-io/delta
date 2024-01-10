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
package io.delta.kernel.internal.skipping;

import java.util.Set;

import io.delta.kernel.expressions.Column;
import io.delta.kernel.expressions.Predicate;

/**
 * Used to hold a pair of {@link Predicate} expression and {@link Set<Column>} of columns referenced
 * by the expression.
 */
public class DataSkippingPredicate {

    private final Predicate expression;
    private final Set<Column> referencedCols;

    DataSkippingPredicate(Predicate expression, Set<Column> referencedCols) {
        this.expression = expression;
        this.referencedCols = referencedCols;
    }

    public Predicate getExpression() {
        return expression;
    }

    public Set<Column> getReferencedCols() {
        return referencedCols;
    }
}
