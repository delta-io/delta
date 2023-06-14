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

import java.util.Optional;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.data.vector.DefaultBooleanVector;
import io.delta.kernel.expressions.Expression;
import io.delta.kernel.expressions.ExpressionEvaluator;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;

public class DefaultExpressionHandler
        implements ExpressionHandler
{
    @Override
    public ExpressionEvaluator getEvaluator(StructType batchSchema, Expression expression)
    {
        return new DefaultExpressionEvaluator(expression);
    }

    private static class DefaultExpressionEvaluator
            implements ExpressionEvaluator
    {
        private final Expression expression;

        private DefaultExpressionEvaluator(Expression expression)
        {
            this.expression = expression;
        }

        @Override
        public ColumnVector eval(ColumnarBatch input)
        {
            if (!expression.dataType().equals(BooleanType.INSTANCE)) {
                throw new UnsupportedOperationException("not yet supported");
            }

            final int batchSize = input.getSize();
            boolean[] result = new boolean[batchSize];
            boolean[] nullResult = new boolean[batchSize];

            CloseableIterator<Row> rows = input.getRows();
            for (int currentIndex = 0; currentIndex < batchSize; currentIndex++) {
                Object evalResult = expression.eval(rows.next());
                if (evalResult == null) {
                    nullResult[currentIndex] = true;
                }
                else {
                    result[currentIndex] = ((Boolean) evalResult).booleanValue();
                }
            }
            return new DefaultBooleanVector(batchSize, Optional.of(nullResult), result);
        }

        @Override
        public void close()
                throws Exception
        {
            // nothing to close
        }
    }
}
