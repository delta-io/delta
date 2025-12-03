package io.delta.standalone.expressions;

import java.util.Comparator;

import io.delta.standalone.internal.expressions.Util;

/**
 * A {@link BinaryOperator} that compares the left and right {@link Expression}s and evaluates to a
 * boolean value.
 */
public abstract class BinaryComparison extends BinaryOperator implements Predicate {
    private final Comparator<Object> comparator;

    protected BinaryComparison(Expression left, Expression right, String symbol) {
        super(left, right, symbol);

        // super asserted that left and right DataTypes were the same

        comparator = Util.createComparator(left.dataType());
    }

    protected int compare(Object leftResult, Object rightResult) {
        return comparator.compare(leftResult, rightResult);
    }
}
