/*
 * Copyright (2023) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.delta.kernel.expressions;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.Objects;

import io.delta.kernel.data.Row;
import io.delta.kernel.types.*;

/**
 * A literal value.
 * <p>
 * Only supports primitive data types, see
 * <a href="https://github.com/delta-io/delta/blob/master/PROTOCOL.md#primitive-types">Delta Transaction Log Protocol: Primitive Types</a>.
 */
public final class Literal extends LeafExpression
{
    public static final Literal TRUE = Literal.of(true);
    public static final Literal FALSE = Literal.of(false);

    /**
     * Create a boolean {@link Literal} object
     *
     * @param value boolean value
     * @return a {@link Literal} with data type {@link BooleanType}
     */
    public static Literal of(boolean value)
    {
        return new Literal(value, BooleanType.INSTANCE);
    }

    /**
     * @return a {@link Literal} with data type {@link ByteType}
     */
    public static Literal of(byte value)
    {
        return new Literal(value, ByteType.INSTANCE);
    }

    /**
     * @return a {@link Literal} with data type {@link ShortType}
     */
    public static Literal of(short value)
    {
        return new Literal(value, ShortType.INSTANCE);
    }

    /**
     * Create an integer {@link Literal} object
     *
     * @param value integer value
     * @return a {@link Literal} with data type {@link IntegerType}
     */
    public static Literal of(int value)
    {
        return new Literal(value, IntegerType.INSTANCE);
    }

    /**
     * Create a long {@link Literal} object
     *
     * @param value long value
     * @return a {@link Literal} with data type {@link LongType}
     */
    public static Literal of(long value)
    {
        return new Literal(value, LongType.INSTANCE);
    }

    /**
     * @return a {@link Literal} with data type {@link FloatType}
     */
    public static Literal of(float value)
    {
        return new Literal(value, FloatType.INSTANCE);
    }

    /**
     * @return a {@link Literal} with data type {@link DoubleType}
     */
    public static Literal of(double value)
    {
        return new Literal(value, DoubleType.INSTANCE);
    }

    /**
     * Create a string {@link Literal} object
     *
     * @param value string value
     * @return a {@link Literal} with data type {@link StringType}
     */
    public static Literal of(String value)
    {
        return new Literal(value, StringType.INSTANCE);
    }

    /**
     * @return a {@link Literal} with data type {@link BinaryType}
     */
    public static Literal of(byte[] value)
    {
        return new Literal(value, BinaryType.INSTANCE);
    }

    /**
     * @return a {@link Literal} with data type {@link DateType}
     */
    public static Literal of(Date value)
    {
        return new Literal(value, DateType.INSTANCE);
    }

    /**
     * @return a {@link Literal} with data type {@link TimestampType}
     */
    public static Literal of(Timestamp value)
    {
        return new Literal(value, TimestampType.INSTANCE);
    }

    /**
     * @return a null {@link Literal} with the given data type
     */
    public static Literal ofNull(DataType dataType)
    {
        if (dataType instanceof ArrayType
            || dataType instanceof MapType
            || dataType instanceof StructType) {
            throw new IllegalArgumentException(
                dataType + " is an invalid data type for Literal.");
        }
        return new Literal(null, dataType);
    }

    private final Object value;
    private final DataType dataType;

    private Literal(Object value, DataType dataType)
    {
        this.value = value;
        this.dataType = dataType;
    }

    public Object value()
    {
        return value;
    }

    @Override
    public Object eval(Row record)
    {
        return value;
    }

    @Override
    public DataType dataType()
    {
        return dataType;
    }

    @Override
    public String toString()
    {
        return String.valueOf(value);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Literal literal = (Literal) o;
        return Objects.equals(value, literal.value) &&
            Objects.equals(dataType, literal.dataType);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(value, dataType);
    }
}
