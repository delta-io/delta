package io.delta.standalone.expressions;

import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * An {@link Expression} with no children.
 */
public abstract class LeafExpression implements Expression {

    protected LeafExpression() {}

    @Override
    public List<Expression> children() {
        return Collections.emptyList();
    }

    @Override
    public Set<String> references() {
        return Collections.emptySet();
    }

    public abstract boolean equals(Object o);

    public abstract int hashCode();
}
