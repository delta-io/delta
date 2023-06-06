package io.delta.standalone.expressions;

import io.delta.standalone.types.BooleanType;
import io.delta.standalone.types.DataType;

/**
 * An {@link Expression} that defines a relation on inputs. Evaluates to true, false, or null.
 */
public interface Predicate extends Expression {
    @Override
    default DataType dataType() {
        return new BooleanType();
    }
}
