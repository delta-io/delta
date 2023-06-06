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

package io.delta.kernel.client;

import io.delta.kernel.expressions.Expression;
import io.delta.kernel.expressions.ExpressionEvaluator;
import io.delta.kernel.types.StructType;

/**
 * Provides expression evaluation capability to Delta Kernel. Delta Kernel can use this client
 * to evaluate predicate on partition filters, fill up partition column values and any computation
 * on data using {@link Expression}s.
 */
public interface ExpressionHandler
{
    /**
     * Create an {@link ExpressionEvaluator} that can evaluate the given <i>expression</i> on
     * {@link io.delta.kernel.data.ColumnarBatch}s with the given <i>batchSchema</i>.
     *
     * @param batchSchema Schema of the input data.
     * @param expression Expression to evaluate.
     * @return An {@link ExpressionEvaluator} instance bound to the given expression and
     *         batchSchema.
     */
    ExpressionEvaluator getEvaluator(StructType batchSchema, Expression expression);
}
