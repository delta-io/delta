// todo: copyright

package io.delta.standalone.expressions;

/**
 * Usage: {@code new GreaterThan(expr1, expr2)} - Returns true if `expr1` is greater than `expr2`,
 * else false.
 */
public final class GreaterThan extends BinaryComparison implements Predicate {
    public GreaterThan(Expression left, Expression right) {
        super(left, right, ">");
    }

    @Override
    public Object nullSafeEval(Object leftResult, Object rightResult) {
        return compare(leftResult, rightResult) > 0;
    }
}
