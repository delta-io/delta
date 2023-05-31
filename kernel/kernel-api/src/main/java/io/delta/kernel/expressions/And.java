package io.delta.kernel.expressions;

import java.util.Collection;

import io.delta.kernel.types.BooleanType;

/**
 * Evaluates logical {@code expr1} AND {@code expr2} for {@code new And(expr1, expr2)}.
 * <p>
 * Requires both left and right input expressions evaluate to booleans.
 */
public final class And extends BinaryOperator implements Predicate {

    public static And apply(Collection<Expression> conjunctions) {
        if (conjunctions.size() == 0) {
            throw new IllegalArgumentException("And.apply must be called with at least 1 element");
        }

        return (And) conjunctions
            .stream()
            // we start off with And(true, true)
            // then we get the 1st expression: And(And(true, true), expr1)
            // then we get the 2nd expression: And(And(true, true), expr1), expr2) etc.
            .reduce(new And(Literal.TRUE, Literal.TRUE), And::new);
    }

    public And(Expression left, Expression right) {
        super(left, right, "&&");
        if (!(left.dataType() instanceof BooleanType) ||
            !(right.dataType() instanceof BooleanType)) {

            throw new IllegalArgumentException(
                String.format(
                    "'And' requires expressions of type boolean. Got %s and %s.",
                    left.dataType().typeName(),
                    right.dataType().typeName()
                )
            );
        }
    }

    @Override
    public Object nullSafeEval(Object leftResult, Object rightResult) {
        return (boolean) leftResult && (boolean) rightResult;
    }
}
