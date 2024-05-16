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
package io.delta.kernel.internal.client;

import java.util.Optional;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.engine.ExpressionHandler;
import io.delta.kernel.expressions.Expression;
import io.delta.kernel.expressions.ExpressionEvaluator;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.expressions.PredicateEvaluator;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.StructType;

public class WrappedExpressionHandler implements ExpressionHandler, ClientExceptionWrapping {

    public static ExpressionHandler wrapExpressionHandler(Engine engine, String operation) {
        return new WrappedExpressionHandler(engine.getExpressionHandler(), operation);
    }

    private final ExpressionHandler baseHandler;
    private final String operationMessage;

    // TODO make operation string construction lazy
    private WrappedExpressionHandler(ExpressionHandler baseHandler, String operationMessage) {
        this.baseHandler = baseHandler;
        this.operationMessage = operationMessage;
    }

    @Override
    public ExpressionEvaluator getEvaluator(StructType inputSchema, Expression expression, DataType outputType) {
        ExpressionEvaluator baseEvaluator = wrapWithEngineException(
            () -> baseHandler.getEvaluator(inputSchema, expression, outputType),
            operationMessage);
        return new ExpressionEvaluator() {
            @Override
            public void close() throws Exception {
                baseEvaluator.close();
            }

            @Override
            public ColumnVector eval(ColumnarBatch input) {
                return wrapWithEngineException(
                    () -> baseEvaluator.eval(input),
                    operationMessage);
            }
        };
    }

    @Override
    public PredicateEvaluator getPredicateEvaluator(StructType inputSchema, Predicate predicate) {
        PredicateEvaluator baseEvaluator = wrapWithEngineException(
            () -> baseHandler.getPredicateEvaluator(inputSchema, predicate),
            operationMessage);
        return new PredicateEvaluator() {
            @Override
            public ColumnVector eval(ColumnarBatch inputData, Optional<ColumnVector> existingSelectionVector) {
                return wrapWithEngineException(
                    () -> baseEvaluator.eval(inputData, existingSelectionVector),
                    operationMessage);
            }
        };
    }

    @Override
    public ColumnVector createSelectionVector(boolean[] values, int from, int to) {
        return wrapWithEngineException(
            () -> baseHandler.createSelectionVector(values, from, to),
            operationMessage
        );
    }
}
