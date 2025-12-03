package io.delta.standalone.expressions;

/**
 * Evaluates {@code expr1} &gt;= {@code expr2} for {@code new GreaterThanOrEqual(expr1, expr2)}.
 */
public final class GreaterThanOrEqual extends BinaryComparison implements Predicate {
    public GreaterThanOrEqual(Expression left, Expression right) {
        super(left, right, ">=");
    }

    @Override
    protected Object nullSafeEval(Object leftResult, Object rightResult) {
        return compare(leftResult, rightResult) >= 0;
    }
}
