package io.delta.standalone.expressions;

/**
 * Usage: {@code new LessThanOrEqual(expr1, expr2)} - Returns true if {@code expr1} is less than or
 * equal to {@code expr2}, else false.
 */
public final class LessThanOrEqual extends BinaryComparison implements Predicate {
    public LessThanOrEqual(Expression left, Expression right) {
        super(left, right, "<=");
    }

    @Override
    public Object nullSafeEval(Object leftResult, Object rightResult) {
        return compare(leftResult, rightResult) <= 0;
    }
}
