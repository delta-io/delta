package io.delta.standalone.expressions;

import io.delta.standalone.data.RowRecord;

/**
 * Usage: {@code new IsNull(expr)} - Returns true if {@code expr} is null, else false.
 */
public class IsNull extends UnaryExpression implements Predicate {
    public IsNull(Expression child) {
        super(child);
    }

    @Override
    public Object eval(RowRecord record) { return child.eval(record) == null; }

    @Override
    public String toString() {
        return "(" + child.toString() + ") IS NULL";
    }
}
