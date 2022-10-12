package io.delta.standalone.expressions;

/**
 * A {@link BinaryExpression} that is an operator, meaning the string representation is
 * {@code x symbol y}, rather than {@code funcName(x, y)}.
 * <p>
 * Requires both inputs to be of the same data type.
 */
public abstract class BinaryOperator extends BinaryExpression {
    protected final String symbol;

    protected BinaryOperator(Expression left, Expression right, String symbol) {
        super(left, right);
        this.symbol = symbol;

        if (!left.dataType().equivalent(right.dataType())) {
            throw new IllegalArgumentException("BinaryOperator left and right DataTypes must be the"
                    + " same, found: " + left.dataType().getTypeName() + " " + symbol + " " +
                    right.dataType().getTypeName());
        }
    }

    @Override
    public String toString() {
        return "(" + left.toString() + " " + symbol + " " + right.toString() + ")";
    }
}
