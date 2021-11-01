package io.delta.standalone.expressions;

/**
 * Usage: {@code new GreaterThanOrEqual(expr1, expr2)} - Returns true if {@code expr1} is greater
 * than or equal to {@code expr2}, else false.
 */
public final class GreaterThanOrEqual extends BinaryComparison implements Predicate {
    public GreaterThanOrEqual(Expression left, Expression right) {
        super(left, right, ">=");
    }

    @Override
    public Object nullSafeEval(Object leftResult, Object rightResult) {
        return compare(leftResult, rightResult) >= 0;
    }
}
