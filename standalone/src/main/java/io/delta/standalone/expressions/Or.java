package io.delta.standalone.expressions;

import io.delta.standalone.internal.exception.DeltaErrors;

/**
 * Usage: {@code new Or(expr1, expr2)} - Logical OR
 */
public final class Or extends BinaryOperator implements Predicate {

    public Or(Expression left, Expression right) {
        super(left, right, "||");
    }

    @Override
    public Object nullSafeEval(Object leftResult, Object rightResult) {
        if (!(leftResult instanceof Boolean) || !(rightResult instanceof Boolean)) {
            throw DeltaErrors.illegalExpressionValueType(
                    "OR",
                    "Boolean",
                    leftResult.getClass().getName(),
                    rightResult.getClass().getName());
        }

        return (boolean) leftResult || (boolean) rightResult;
    }
}
