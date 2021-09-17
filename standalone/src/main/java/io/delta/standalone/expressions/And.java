package io.delta.standalone.expressions;

/**
 * Usage: new And(expr1, expr2) - Logical AND
 */
public final class And extends BinaryOperator implements Predicate {

    public And(Expression left, Expression right) {
        super(left, right, "&&");
    }

    @Override
    public Object nullSafeEval(Object leftResult, Object rightResult) {
        if (!(leftResult instanceof Boolean) || !(rightResult instanceof Boolean)) {
            throw new RuntimeException("'And' expression children.eval results must be Booleans");
        }

        return (boolean) leftResult && (boolean) rightResult;
    }
}
