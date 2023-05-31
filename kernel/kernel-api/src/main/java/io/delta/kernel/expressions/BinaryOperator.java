package io.delta.kernel.expressions;

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
            throw new IllegalArgumentException(
                String.format(
                    "BinaryOperator left and right DataTypes must be the same. Found %s and %s.",
                    left.dataType().typeName(),
                    right.dataType().typeName())
            );
        }
    }

    @Override
    public String toString() {
        return String.format("(%s %s %s)", left.toString(), symbol, right.toString());
    }
}