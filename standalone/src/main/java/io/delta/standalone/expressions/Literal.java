package io.delta.standalone.expressions;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Objects;

import io.delta.standalone.data.RowRecord;
import io.delta.standalone.types.*;

public final class Literal extends LeafExpression {
    public static final Literal True = Literal.of(true);
    public static final Literal False = Literal.of(false);

    private final Object value;
    private final DataType dataType;

    private Literal(Object value, DataType dataType) {
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
        return String.valueOf(value);
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

    public static Literal of(byte[] value) {
        return new Literal(value, new BinaryType());
    }

    public static Literal of(Date value) {
        return new Literal(value, new DateType());
    }

    public static Literal of(BigDecimal value) {
        //TODO: get the precision and scale from the value
        return new Literal(value, DecimalType.USER_DEFAULT);
    }

    public static Literal of(double value) {
        return new Literal(value, new DoubleType());
    }

    public static Literal of(float value) {
        return new Literal(value, new FloatType());
    }

    public static Literal of(long value) {
        return new Literal(value, new LongType());
    }

    public static Literal of(short value) {
        return new Literal(value, new ShortType());
    }

    public static Literal of(String value) {
        return new Literal(value, new StringType());
    }

    public static Literal of(Timestamp value) {
        return new Literal(value, new TimestampType());
    }

    public static Literal of(byte value) {
        return new Literal(value, new ByteType());
    }

    public static Literal ofNull(DataType dataType) {
        if (dataType instanceof NullType
                || dataType instanceof ArrayType
                || dataType instanceof MapType
                || dataType instanceof StructType) {
            throw new IllegalArgumentException(
                    dataType.getTypeName() + " is an invalid data type for Literal.");
        }
        return new Literal(null, dataType); }
}
