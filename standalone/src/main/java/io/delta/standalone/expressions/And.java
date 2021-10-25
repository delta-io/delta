package io.delta.standalone.expressions;

import io.delta.standalone.internal.exception.DeltaErrors;

/**
 * Usage: {@code new And(expr1, expr2)} - Logical AND
 */
public final class And extends BinaryOperator implements Predicate {

    public And(Expression left, Expression right) {
        super(left, right, "&&");
    }

    @Override
    public Object nullSafeEval(Object leftResult, Object rightResult) {
        if (!(leftResult instanceof Boolean) || !(rightResult instanceof Boolean)) {
            throw DeltaErrors.illegalExpressionValueType(
                    "AND",
                    "Boolean",
                    leftResult.getClass().getName(),
                    rightResult.getClass().getName());
        }

        return (boolean) leftResult && (boolean) rightResult;
    }
}
