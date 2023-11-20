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
package io.delta.kernel.defaults.internal.expressions;

import java.util.Arrays;
import java.util.Optional;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.expressions.*;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;

import io.delta.kernel.internal.util.Utils;

import io.delta.kernel.defaults.internal.data.vector.DefaultConstantVector;

/**
 * Default implementation of {@link PredicateEvaluator}. It makes use of the
 * {@link DefaultExpressionEvaluator} with some modification of the given predicate. Refer to
 * {@link #DefaultPredicateEvaluator(StructType, Predicate)} and
 * {@link #eval(ColumnarBatch, Optional)} for details.
 */
public class DefaultPredicateEvaluator implements PredicateEvaluator {
    private static final String EXISTING_SEL_VECTOR_COL_NAME =
        "____existing_selection_vector_value____";
    private static final StructField EXISTING_SEL_VECTOR_FIELD =
        new StructField(EXISTING_SEL_VECTOR_COL_NAME, BooleanType.BOOLEAN, false);

    private final ExpressionEvaluator expressionEvaluator;

    public DefaultPredicateEvaluator(StructType inputSchema, Predicate predicate) {
        // Create a predicate that takes into account of the selection value in existing selection
        // vector in addition to the given predicate. This is needed to make a row remain
        // unselected in the final vector when it is unselected in existing selection vector.
        Predicate rewrittenPredicate = new And(
            new Predicate(
                "=",
                Arrays.asList(new Column(EXISTING_SEL_VECTOR_COL_NAME), Literal.ofBoolean(true))),
            predicate);
        StructType rewrittenInputSchema = inputSchema.add(EXISTING_SEL_VECTOR_FIELD);
        this.expressionEvaluator = new DefaultExpressionEvaluator(
            rewrittenInputSchema, rewrittenPredicate, BooleanType.BOOLEAN);
    }

    @Override
    public ColumnVector eval(
        ColumnarBatch inputData,
        Optional<ColumnVector> existingSelectionVector) {
        try {
            ColumnVector newVector = existingSelectionVector.orElse(
                new DefaultConstantVector(BooleanType.BOOLEAN, inputData.getSize(), true));
            ColumnarBatch withExistingSelVector =
                inputData.withNewColumn(
                    inputData.getSchema().length(), EXISTING_SEL_VECTOR_FIELD, newVector);

            return expressionEvaluator.eval(withExistingSelVector);
        } finally {
            // release the existing selection vector.
            Utils.closeCloseables(existingSelectionVector.orElse(null));
        }
    }
}
