package io.delta.standalone.expressions;

/**
 * Evaluates {@code expr1} &lt; {@code expr2} for {@code new LessThan(expr1, expr2)}.
 */
public final class LessThan extends BinaryComparison implements Predicate {
    public LessThan(Expression left, Expression right) {
        super(left, right, "<");
    }

    @Override
    protected Object nullSafeEval(Object leftResult, Object rightResult) {
        return compare(leftResult, rightResult) < 0;
    }
}
