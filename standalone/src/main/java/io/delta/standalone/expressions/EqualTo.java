package io.delta.standalone.expressions;

/**
 * Usage: {@code new EqualTo(expr1, expr2)} - Returns true if `expr1` equals `expr2`, else false.
 */
public final class EqualTo extends BinaryComparison implements Predicate {

    public EqualTo(Expression left, Expression right) {
        super(left, right, "=");
    }

    @Override
    public Object nullSafeEval(Object leftResult, Object rightResult) {
        return compare(leftResult, rightResult) == 0;
    }
}
