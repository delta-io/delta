package io.delta.standalone.expressions;

import io.delta.standalone.data.RowRecord;

/**
 * Evaluates if {@code expr} is not null for {@code new IsNotNull(expr)}.
 */
public final class IsNotNull extends UnaryExpression implements Predicate {
    public IsNotNull(Expression child) {
        super(child);
    }

    @Override
    public Object eval(RowRecord record) {
        return child.eval(record) != null;
    }

    @Override
    public String toString() {
        return "(" + child.toString() + ") IS NOT NULL";
    }
}
