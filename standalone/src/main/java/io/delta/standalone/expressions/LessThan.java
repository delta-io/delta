package io.delta.standalone.expressions;

/**
 * Usage: {@code new LessThan(expr1, expr2)} - Returns true if `expr1` is less than `expr2`, else
 * false.
 */
public final class LessThan extends BinaryComparison implements Predicate {
    public LessThan(Expression left, Expression right) {
        super(left, right, "<");
    }

    @Override
    public Object nullSafeEval(Object leftResult, Object rightResult) {
        return compare(leftResult, rightResult) < 0;
    }
}
