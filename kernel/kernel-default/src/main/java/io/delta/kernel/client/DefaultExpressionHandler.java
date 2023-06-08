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

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.DefaultBooleanColumnVector;
import io.delta.kernel.expressions.Expression;
import io.delta.kernel.expressions.ExpressionEvaluator;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.StructType;

import java.util.ArrayList;
import java.util.List;

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

            List<Boolean> result = new ArrayList<>();
            input.getRows().forEachRemaining(row -> result.add((Boolean) expression.eval(row)));
            return new DefaultBooleanColumnVector(result);
        }

        @Override
        public void close()
                throws Exception
        {
            // nothing to close
        }
    }
}
