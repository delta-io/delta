package io.delta.standalone.expressions;

import io.delta.standalone.data.RowRecord;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * An expression with one input and one output. The output is by default evaluated to null
 * if the input is evaluated to null.
 */
public abstract class UnaryExpression implements Expression {
    protected final Expression child;

    public UnaryExpression(Expression child) {
        this.child = child;
    }

    public Expression getChild() {
        return child;
    }

    @Override
    public Object eval(RowRecord record) {
        Object childResult = child.eval(record);

        if (null == childResult) return null;

        return nullSafeEval(childResult);
    }

    protected Object nullSafeEval(Object childResult) {
        throw new IllegalArgumentException("UnaryExpressions must override either eval or nullSafeEval");
    }

    @Override
    public List<Expression> children() {
        return Collections.singletonList(child);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        UnaryExpression that = (UnaryExpression) o;
        return Objects.equals(child, that.child);
    }

    @Override
    public int hashCode() {
        return Objects.hash(child);
    }
}
