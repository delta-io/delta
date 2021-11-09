package io.delta.standalone.expressions;

/**
 * A {@link BinaryExpression} that is an operator, with two properties:
 * <ol>
 *   <li>The string representation is {@code x symbol y}, rather than {@code funcName(x, y)}.</li>
 *   <li>Two inputs are expected to be of the same type. If the two inputs have different types, an
 *       {@link IllegalArgumentException} will be thrown.</li>
 * </ol>
 */
public abstract class BinaryOperator extends BinaryExpression {
    protected final String symbol;

    public BinaryOperator(Expression left, Expression right, String symbol) {
        super(left, right);
        this.symbol = symbol;

        if (!left.dataType().equals(right.dataType())) {
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
