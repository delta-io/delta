package io.delta.kernel.expressions;

import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.DataType;

/**
 * An {@link Expression} that defines a relation on inputs. Evaluates to true, false, or null.
 */
public interface Predicate extends Expression {
    @Override
    default DataType dataType() {
        return BooleanType.INSTANCE;
    }
}
