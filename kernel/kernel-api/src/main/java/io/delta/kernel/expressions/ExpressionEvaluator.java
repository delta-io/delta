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

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;

/**
 * Interface for implementing an {@link Expression} evaluator.
 * It contains one {@link Expression} which can be evaluated on multiple {@link ColumnarBatch}es
 * Connectors can implement this interface to optimize the evaluation using the
 * connector specific capabilities.
 */
public interface ExpressionEvaluator extends AutoCloseable
{
    /**
     * Evaluate the expression on given {@link ColumnarBatch} data.
     *
     * @param input input data in columnar format.
     * @return Result of the expression as a {@link ColumnVector}. Contains one value for each
     *         row of the input. The data type of the output is same as the type output of the
     *         expression this evaluator is using.
     */
    ColumnVector eval(ColumnarBatch input);
}
