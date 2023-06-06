package io.delta.standalone.expressions;

import io.delta.standalone.types.BooleanType;
import io.delta.standalone.internal.exception.DeltaErrors;

/**
 * Evaluates logical NOT {@code expr} for {@code new Not(expr)}.
 * <p>
 * Requires the child expression evaluates to a boolean.
 */
public final class Not extends UnaryExpression implements Predicate {
    public Not(Expression child) {
        super(child);
        if (!(child.dataType() instanceof BooleanType)) {
            throw DeltaErrors.illegalExpressionValueType(
                    "NOT",
                    "bool",
                    child.dataType().getTypeName());
        }
    }

    @Override
    public Object nullSafeEval(Object childResult) {
        return !((boolean) childResult);
    }

    @Override
    public String toString() {
        return "(NOT " + child.toString() + ")";
    }
}
