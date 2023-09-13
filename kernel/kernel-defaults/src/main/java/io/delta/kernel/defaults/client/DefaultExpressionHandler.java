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
package io.delta.kernel.defaults.client;

import io.delta.kernel.client.ExpressionHandler;
import io.delta.kernel.expressions.Expression;
import io.delta.kernel.expressions.ExpressionEvaluator;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.StructType;

import io.delta.kernel.defaults.internal.expressions.DefaultExpressionEvaluator;

/**
 * Default implementation of {@link ExpressionHandler}
 */
public class DefaultExpressionHandler implements ExpressionHandler {
    @Override
    public ExpressionEvaluator getEvaluator(
        StructType inputSchema,
        Expression expression,
        DataType outputType) {
        return new DefaultExpressionEvaluator(inputSchema, expression, outputType);
    }
}
