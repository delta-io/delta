package io.delta.standalone.expressions;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Objects;

import io.delta.standalone.data.RowRecord;
import io.delta.standalone.types.*;

/**
 * A literal value.
 * <p>
 * Only supports primitive data types, see
 * <a href="https://github.com/delta-io/delta/blob/master/PROTOCOL.md#primitive-types">Delta Transaction Log Protocol: Primitive Types</a>.
 */
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

    /**
     * @return a {@link Literal} with data type {@link IntegerType}
     */
    public static Literal of(int value) {
        return new Literal(value, new IntegerType());
    }

    /**
     * @return a {@link Literal} with data type {@link BooleanType}
     */
    public static Literal of(boolean value) {
        return new Literal(value, new BooleanType());
    }

    /**
     * @return a {@link Literal} with data type {@link BinaryType}
     */
    public static Literal of(byte[] value) {
        return new Literal(value, new BinaryType());
    }

    /**
     * @return a {@link Literal} with data type {@link DateType}
     */
    public static Literal of(Date value) {
        return new Literal(value, new DateType());
    }

    /**
     * @return a {@link Literal} with data type {@link DecimalType} with precision and scale
     *         inferred from {@code value}
     */
    public static Literal of(BigDecimal value) {
        return new Literal(value, new DecimalType(value.precision(), value.scale()));
    }

    /**
     * @return a {@link Literal} with data type {@link DoubleType}
     */
    public static Literal of(double value) {
        return new Literal(value, new DoubleType());
    }

    /**
     * @return a {@link Literal} with data type {@link FloatType}
     */
    public static Literal of(float value) {
        return new Literal(value, new FloatType());
    }

    /**
     * @return a {@link Literal} with data type {@link LongType}
     */
    public static Literal of(long value) {
        return new Literal(value, new LongType());
    }

    /**
     * @return a {@link Literal} with data type {@link ShortType}
     */
    public static Literal of(short value) {
        return new Literal(value, new ShortType());
    }

    /**
     * @return a {@link Literal} with data type {@link StringType}
     */
    public static Literal of(String value) {
        return new Literal(value, new StringType());
    }

    /**
     * @return a {@link Literal} with data type {@link TimestampType}
     */
    public static Literal of(Timestamp value) {
        return new Literal(value, new TimestampType());
    }

    /**
     * @return a {@link Literal} with data type {@link ByteType}
     */
    public static Literal of(byte value) {
        return new Literal(value, new ByteType());
    }

    /**
     * @return a null {@link Literal} with the given data type
     */
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
