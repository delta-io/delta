package io.delta.standalone.expressions;

/**
 * A [[BinaryOperator]] that compares the left and right [[Expression]]s and returns a boolean value.
 */
public abstract class BinaryComparison extends BinaryOperator implements Predicate {
    private final CastingComparator<?> comparator;

    public BinaryComparison(Expression left, Expression right, String symbol) {
        super(left, right, symbol);

        // super asserted that left and right DataTypes were the same

        comparator = Util.createCastingComparator(left.dataType());
    }

    protected int compare(Object leftResult, Object rightResult) {
        return comparator.compare(leftResult, rightResult);
    }
}
