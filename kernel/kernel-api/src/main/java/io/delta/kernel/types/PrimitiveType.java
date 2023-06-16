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
package io.delta.kernel.types;

import java.util.HashMap;
import java.util.Objects;
import java.util.Optional;

/**
 * Base class for all primitive types {@link DataType}.
 */
public class PrimitiveType extends DataType
{

    /**
     * Data representing {@code boolean} type values.
     */
    public static final DataType BOOLEAN = new PrimitiveType("boolean");

    /**
     * The data type representing {@code byte[]} values.
     */
    public static final DataType BINARY = new PrimitiveType("binary");

    /**
     * The data type representing {@code string} type values.
     */
    public static final DataType STRING = new PrimitiveType("string");

    /**
     * The data type representing {@code byte} type values.
     */
    public static final DataType BYTE = new PrimitiveType("byte");

    /**
     * The data type representing {@code short} type values.
     */
    public static final DataType SHORT = new PrimitiveType("short");

    /**
     * The data type representing {@code integer} type values.
     */
    public static final DataType INTEGER = new PrimitiveType("integer");

    /**
     * The data type representing {@code long} type values.
     */
    public static final DataType LONG = new PrimitiveType("long");

    /**
     * The data type representing {@code float} type values.
     */
    public static final DataType FLOAT = new PrimitiveType("float");

    /**
     * The data type representing {@code double} type values.
     */
    public static final DataType DOUBLE = new PrimitiveType("double");

    /**
     * A date type, supporting "0001-01-01" through "9999-12-31".
     * Internally, this is represented as the number of days from 1970-01-01.
     */
    public static final DataType DATE = new PrimitiveType("date");

    /**
     * The data type representing {@code java.sql.Timestamp} values.
     */
    public static final DataType TIMESTAMP = new PrimitiveType("timestamp");

    /**
     * Create a primitive type {@link DataType}
     *
     * @param primitiveTypeName Primitive type name.
     * @return
     */
    protected static DataType createPrimitive(String primitiveTypeName)
    {
        return Optional.ofNullable(nameToPrimitiveTypeMap.get(primitiveTypeName))
            .orElseThrow(
                () -> new IllegalArgumentException("Unknown primitive type " + primitiveTypeName));
    }

    /**
     * Is the given type name a primitive type?
     */
    protected static boolean isPrimitiveType(String typeName)
    {
        return nameToPrimitiveTypeMap.containsKey(typeName);
    }

    private static HashMap<String, DataType> nameToPrimitiveTypeMap = new HashMap<>();

    static {
        nameToPrimitiveTypeMap.put("boolean", PrimitiveType.BOOLEAN);
        nameToPrimitiveTypeMap.put("byte", PrimitiveType.BYTE);
        nameToPrimitiveTypeMap.put("short", PrimitiveType.SHORT);
        nameToPrimitiveTypeMap.put("integer", PrimitiveType.INTEGER);
        nameToPrimitiveTypeMap.put("long", PrimitiveType.LONG);
        nameToPrimitiveTypeMap.put("float", PrimitiveType.FLOAT);
        nameToPrimitiveTypeMap.put("double", PrimitiveType.DOUBLE);
        nameToPrimitiveTypeMap.put("date", PrimitiveType.DATE);
        nameToPrimitiveTypeMap.put("timestamp", PrimitiveType.TIMESTAMP);
        nameToPrimitiveTypeMap.put("binary", PrimitiveType.BINARY);
        nameToPrimitiveTypeMap.put("string", PrimitiveType.STRING);
    }

    private final String primitiveTypeName;

    private PrimitiveType(String primitiveTypeName)
    {
        this.primitiveTypeName = primitiveTypeName;
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
        PrimitiveType that = (PrimitiveType) o;
        return primitiveTypeName.equals(that.primitiveTypeName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(primitiveTypeName);
    }

    @Override
    public String toString()
    {
        return primitiveTypeName;
    }

    @Override
    public String toJson()
    {
        return String.format("\"%s\"", primitiveTypeName);
    }
}
