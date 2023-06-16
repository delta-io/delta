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

import java.util.Objects;

import io.delta.kernel.data.Row;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.PrimitiveType;

/**
 * A literal value.
 * <p>
 * Only supports primitive data types, see
 * <a href="https://github.com/delta-io/delta/blob/master/PROTOCOL.md#primitive-types">Delta Transaction Log Protocol: Primitive Types</a>.
 */
public final class Literal extends LeafExpression {

    ////////////////////////////////////////////////////////////////////////////////
    // Static Fields / Methods
    ////////////////////////////////////////////////////////////////////////////////

    public static final Literal TRUE = Literal.of(true);
    public static final Literal FALSE = Literal.of(false);

    /**
     * Create an integer {@link Literal} object
     * @param value integer value
     * @return a {@link Literal} with data type {@link PrimitiveType#INTEGER}
     */
    public static Literal of(int value) {
        return new Literal(value, PrimitiveType.INTEGER);
    }

    /**
     * Create a boolean {@link Literal} object
     * @param value boolean value
     * @return a {@link Literal} with data type {@link PrimitiveType#BOOLEAN}
     */
    public static Literal of(boolean value) {
        return new Literal(value, PrimitiveType.BOOLEAN);
    }

    /**
     * Create a long {@link Literal} object
     * @param value long value
     * @return a {@link Literal} with data type {@link PrimitiveType#LONG}
     */
    public static Literal of(long value) {
        return new Literal(value, PrimitiveType.LONG);
    }

    /**
     * Create a string {@link Literal} object
     * @param value string value
     * @return a {@link Literal} with data type {@link PrimitiveType#STRING}
     */
    public static Literal of(String value) {
        return new Literal(value, PrimitiveType.STRING);
    }

    ////////////////////////////////////////////////////////////////////////////////
    // Instance Fields / Methods
    ////////////////////////////////////////////////////////////////////////////////

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
    public Object eval(Row record) {
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
}
