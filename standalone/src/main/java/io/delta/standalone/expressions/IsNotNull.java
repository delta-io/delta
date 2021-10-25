package io.delta.standalone.expressions;

import io.delta.standalone.data.RowRecord;
import io.delta.standalone.types.BooleanType;
import io.delta.standalone.types.DataType;

/**
 * Usage: {@code new IsNotNull(expr)} - Returns true if `expr` is not null, else false.
 */
public class IsNotNull extends UnaryExpression implements Predicate {
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
