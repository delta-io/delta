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

import java.util.Optional;

import io.delta.kernel.annotation.Evolving;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;

/**
 * Special interface for evaluating {@link Predicate} on input batch and return a selection
 * vector containing one value for each row in input batch indicating whether the row has passed
 * the predicate or not.
 *
 * Optionally it takes an existing selection vector along with the input batch for evaluation.
 * Result selection vector is combined with the given existing selection vector and a new selection
 * vector is returned. This mechanism allows running an input batch through several predicate
 * evaluations without rewriting the input batch to remove rows that do not pass the predicate
 * after each predicate evaluation. The new selection should be same or more selective as the
 * existing selection vector. For example if a row is marked as unselected in existing selection
 * vector, then it should remain unselected in the returned selection vector even when the given
 * predicate returns true for the row.
 *
 * @since 3.0.0
 */
@Evolving
public interface PredicateEvaluator {
    /**
     * Evaluate the predicate on given inputData. Combine the existing selection vector with the
     * output of the predicate result and return a new selection vector.
     *
     * @param inputData               {@link ColumnarBatch} of data to which the predicate
     *                                expression refers for input.
     * @param existingSelectionVector Optional existing selection vector. If not empty, it is
     *                                combined with the predicate result. The caller is also
     *                                releasing the ownership of `existingSelectionVector` to this
     *                                callee, and the callee is responsible for closing it.
     * @return A {@link ColumnVector} of boolean type that captures the predicate result for each
     * row together with the existing selection vector.
     */
    ColumnVector eval(ColumnarBatch inputData, Optional<ColumnVector> existingSelectionVector);
}
