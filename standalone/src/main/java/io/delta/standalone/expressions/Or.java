package io.delta.standalone.expressions;

/**
 * Usage: new Or(expr1, expr2) - Logical OR
 */
public final class Or extends BinaryOperator implements Predicate {

    public Or(Expression left, Expression right) {
        super(left, right, "||");
    }

    @Override
    public Object nullSafeEval(Object leftResult, Object rightResult) {
        if (!(leftResult instanceof Boolean) || !(rightResult instanceof Boolean)) {
            throw new RuntimeException("'Or' expression left.eval and right.eval results must be Booleans");
        }

        return (boolean) leftResult || (boolean) rightResult;
    }
}
