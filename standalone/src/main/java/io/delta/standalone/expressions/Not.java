package io.delta.standalone.expressions;

/**
 * Usage: new Not(expr) - Logical not.
 */
public class Not extends UnaryExpression implements Predicate {
    public Not(Expression child) {
        super(child);
    }

    @Override
    public Object nullSafeEval(Object childResult) {
        if (!(childResult instanceof Boolean)) {
            throw new RuntimeException("'Not' expression child.eval result must be a Boolean");
        }

        return !((boolean) childResult);
    }

    @Override
    public String toString() {
        return "(NOT " + child.toString() + ")";
    }
}
