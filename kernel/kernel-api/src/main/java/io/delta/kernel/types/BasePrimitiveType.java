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

import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Base class for all primitive types {@link DataType}.
 */
public abstract class BasePrimitiveType extends DataType {
    /**
     * Create a primitive type {@link DataType}
     *
     * @param primitiveTypeName Primitive type name.
     * @return {@link DataType} for given primitive type name
     */
    public static DataType createPrimitive(String primitiveTypeName) {
        return Optional.ofNullable(nameToPrimitiveTypeMap.get().get(primitiveTypeName))
            .orElseThrow(
                () -> new IllegalArgumentException("Unknown primitive type " + primitiveTypeName));
    }

    /**
     * Is the given type name a primitive type?
     */
    public static boolean isPrimitiveType(String typeName) {
        return nameToPrimitiveTypeMap.get().containsKey(typeName);
    }

    /**
     * For testing only
     */
    public static List<DataType> getAllPrimitiveTypes() {
        return nameToPrimitiveTypeMap.get().values().stream().collect(Collectors.toList());
    }

    private static final Supplier<Map<String, DataType>> nameToPrimitiveTypeMap = () ->
        Collections.unmodifiableMap(new HashMap<String, DataType>() {
            {
                put("boolean", BooleanType.BOOLEAN);
                put("byte", ByteType.BYTE);
                put("short", ShortType.SHORT);
                put("integer", IntegerType.INTEGER);
                put("long", LongType.LONG);
                put("float", FloatType.FLOAT);
                put("double", DoubleType.DOUBLE);
                put("date", DateType.DATE);
                put("timestamp", TimestampType.TIMESTAMP);
                put("timestamp_ntz", TimestampNTZType.TIMESTAMP_NTZ);
                put("binary", BinaryType.BINARY);
                put("string", StringType.STRING);
                put("variant", VariantType.VARIANT);
            }
        });

    private final String primitiveTypeName;

    protected BasePrimitiveType(String primitiveTypeName) {
        this.primitiveTypeName = primitiveTypeName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BasePrimitiveType that = (BasePrimitiveType) o;
        return primitiveTypeName.equals(that.primitiveTypeName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(primitiveTypeName);
    }

    @Override
    public String toString() {
        return primitiveTypeName;
    }

    @Override
    public String toJson() {
        return String.format("\"%s\"", primitiveTypeName);
    }
}
