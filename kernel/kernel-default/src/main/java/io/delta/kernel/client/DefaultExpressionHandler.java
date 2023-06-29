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

import static io.delta.kernel.DefaultKernelUtils.checkArgument;
import static io.delta.kernel.DefaultKernelUtils.daysSinceEpoch;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Optional;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.data.vector.DefaultBooleanVector;
import io.delta.kernel.data.vector.DefaultConstantVector;
import io.delta.kernel.expressions.Expression;
import io.delta.kernel.expressions.ExpressionEvaluator;
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.types.*;
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
            if (expression instanceof Literal) {
                return evalLiteralExpression(input, (Literal) expression);
            }

            if (expression.dataType().equals(BooleanType.INSTANCE)) {
                return evalBooleanOutputExpression(input, expression);
            }
            // TODO: Boolean output type expressions are good enough for first preview release
            // which enables partition pruning and file skipping using file stats.

            throw new UnsupportedOperationException("not yet implemented");
        }

        @Override
        public void close() { /* nothing to close */ }
    }

    private static ColumnVector evalBooleanOutputExpression(
        ColumnarBatch input, Expression expression)
    {
        checkArgument(expression.dataType().equals(BooleanType.INSTANCE),
            "expression should return a boolean");

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

    private static ColumnVector evalLiteralExpression(ColumnarBatch input, Literal literal)
    {
        Object result = literal.value();
        DataType dataType = literal.dataType();
        int size = input.getSize();

        if (result == null) {
            return new DefaultConstantVector(dataType, size, null);
        }

        if (dataType instanceof BooleanType ||
            dataType instanceof ByteType ||
            dataType instanceof ShortType ||
            dataType instanceof IntegerType ||
            dataType instanceof LongType ||
            dataType instanceof FloatType ||
            dataType instanceof DoubleType ||
            dataType instanceof StringType ||
            dataType instanceof BinaryType) {
            return new DefaultConstantVector(dataType, size, result);
        }
        else if (dataType instanceof DateType) {
            int numOfDaysSinceEpoch = daysSinceEpoch((Date) result);
            return new DefaultConstantVector(dataType, size, numOfDaysSinceEpoch);
        }
        else if (dataType instanceof TimestampType) {
            Timestamp timestamp = (Timestamp) result;
            long micros = timestamp.getTime() * 1000;
            return new DefaultConstantVector(dataType, size, micros);
        }

        throw new UnsupportedOperationException(
            "unsupported expression encountered: " + literal);
    }
}
