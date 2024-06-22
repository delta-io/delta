package io.delta.standalone.expressions;

/**
 * Evaluates {@code expr1} = {@code expr2} for {@code new EqualTo(expr1, expr2)}.
 */
public final class EqualTo extends BinaryComparison implements Predicate {

    public EqualTo(Expression left, Expression right) {
        super(left, right, "=");
    }

    @Override
    protected Object nullSafeEval(Object leftResult, Object rightResult) {
        return compare(leftResult, rightResult) == 0;
    }
}
