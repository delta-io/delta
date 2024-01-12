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

import java.util.*;

import io.delta.kernel.expressions.Column;
import io.delta.kernel.expressions.Expression;
import io.delta.kernel.expressions.Predicate;

/**
 * A {@link Predicate} with a set of columns referenced by the expression.
 */
public class DataSkippingPredicate extends Predicate {

    /** Set of {@link Column}s referenced by the predicate or any of its child expressions */
    private final Set<Column> referencedCols;

    /**
     * @param name the predicate name
     * @param children list of expressions that are input to this predicate.
     * @param referencedCols set of columns referenced by this predicate or any of its child
     *                       expressions
     */
    DataSkippingPredicate(String name, List<Expression> children, Set<Column> referencedCols) {
        super(name, children);
        this.referencedCols = Collections.unmodifiableSet(referencedCols);
    }

    /**
     * Constructor for a binary {@link DataSkippingPredicate} where both children are instances of
     * {@link DataSkippingPredicate}.
     * @param name the predicate name
     * @param left left input to this predicate
     * @param right right input to this predicate
     */
    DataSkippingPredicate(String name, DataSkippingPredicate left, DataSkippingPredicate right) {
        this(
            name,
            Arrays.asList(left, right),
            new HashSet<Column>(){
                {
                    addAll(left.getReferencedCols());
                    addAll(right.getReferencedCols());
                }
            });
    }

    public Set<Column> getReferencedCols() {
        return referencedCols;
    }
}
