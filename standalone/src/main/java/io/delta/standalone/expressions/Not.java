package io.delta.standalone.expressions;

import io.delta.standalone.internal.exception.DeltaErrors;

/**
 * Usage: {@code new Not(expr)} - Logical not.
 */
public class Not extends UnaryExpression implements Predicate {
    public Not(Expression child) {
        super(child);
    }

    @Override
    public Object nullSafeEval(Object childResult) {
        if (!(childResult instanceof Boolean)) {
            throw DeltaErrors.illegalExpressionValueType(
                    "NOT",
                    "Boolean",
                    childResult.getClass().getName());
        }

        return !((boolean) childResult);
    }

    @Override
    public String toString() {
        return "(NOT " + child.toString() + ")";
    }
}
