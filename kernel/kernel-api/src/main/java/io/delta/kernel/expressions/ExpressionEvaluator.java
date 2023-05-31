package io.delta.kernel.expressions;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.expressions.Expression;

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
