package io.delta.standalone.expressions;

import io.delta.standalone.types.BooleanType;
import io.delta.standalone.types.DataType;

public interface Predicate extends Expression {
    @Override
    default DataType dataType() {
        return new BooleanType();
    }
}
