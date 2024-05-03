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
package io.delta.kernel.engine;

import io.delta.kernel.annotation.Evolving;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.expressions.Expression;
import io.delta.kernel.expressions.ExpressionEvaluator;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.expressions.PredicateEvaluator;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.StructType;

/**
 * Provides expression evaluation capability to Delta Kernel. Delta Kernel can use this client
 * to evaluate predicate on partition filters, fill up partition column values and any computation
 * on data using {@link Expression}s.
 *
 * @since 3.0.0
 */
@Evolving
public interface ExpressionHandler {

    /**
     * Create an {@link ExpressionEvaluator} that can evaluate the given <i>expression</i> on
     * {@link ColumnarBatch}s with the given <i>batchSchema</i>. The <i>expression</i> is
     * expected to be a scalar expression where for each one input row there
     * is a one output value.
     *
     * @param inputSchema Input data schema
     * @param expression  Expression to evaluate.
     * @param outputType  Expected result data type.
     */
    ExpressionEvaluator getEvaluator(
        StructType inputSchema,
        Expression expression,
        DataType outputType);

    /**
     * Create a {@link PredicateEvaluator} that can evaluate the given <i>predicate</i> expression
     * and return a selection vector ({@link ColumnVector} of {@code boolean} type).
     *
     * @param inputSchema Schema of the data referred by the given predicate expression.
     * @param predicate Predicate expression to evaluate.
     * @return
     */
    PredicateEvaluator getPredicateEvaluator(StructType inputSchema, Predicate predicate);

    /**
     * Create a selection vector, a boolean type {@link ColumnVector}, on top of the range of values
     * given in <i>values</i> array.
     *
     * @param values Array of initial boolean values for the selection vector. The ownership of
     *               this array is with the caller and this method shouldn't depend on it after the
     *               call is complete.
     * @param from   start index of the range, inclusive.
     * @param to     end index of the range, exclusive.
     * @return A {@link ColumnVector} of {@code boolean} type values.
     */
    ColumnVector createSelectionVector(boolean[] values, int from, int to);
}
