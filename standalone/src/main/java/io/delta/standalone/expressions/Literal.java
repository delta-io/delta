package io.delta.standalone.expressions;

import io.delta.standalone.data.RowRecord;
import io.delta.standalone.types.BooleanType;
import io.delta.standalone.types.DataType;
import io.delta.standalone.types.IntegerType;
import io.delta.standalone.types.StringType;

import java.util.Objects;

public final class Literal extends LeafExpression {
    public static final Literal True = Literal.of(Boolean.TRUE);
    public static final Literal False = Literal.of(false);

    private final Object value;
    private final DataType dataType;

    public Literal(Object value, DataType dataType) {
        Literal.validateLiteralValue(value, dataType);

        this.value = value;
        this.dataType = dataType;
    }

    public Object value() {
        return value;
    }

    @Override
    public Object eval(RowRecord record) {
        return value;
    }

    @Override
    public DataType dataType() {
        return dataType;
    }

    @Override
    public String toString() {
        return value.toString();
    }

    private static void validateLiteralValue(Object value, DataType dataType) {
        // TODO
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Literal literal = (Literal) o;
        return Objects.equals(value, literal.value) &&
            Objects.equals(dataType, literal.dataType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value, dataType);
    }

    public static Literal of(int value) {
        return new Literal(value, new IntegerType());
    }

    public static Literal of(boolean value) {
        return new Literal(value, new BooleanType());
    }

    public static Literal of(String value) {
        return new Literal(value, new StringType());
    }
}
