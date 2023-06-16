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
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Base class for all primitive types {@link DataType}.
 */
class BasePrimitiveType extends DataType
{
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

    /** For testing only */
    protected static List<DataType> getAllPrimitiveTypes() {
        return nameToPrimitiveTypeMap.values().stream().collect(Collectors.toList());
    }

    private static HashMap<String, DataType> nameToPrimitiveTypeMap = new HashMap<>();

    static {
        nameToPrimitiveTypeMap.put("boolean", BooleanType.INSTANCE);
        nameToPrimitiveTypeMap.put("byte", ByteType.INSTANCE);
        nameToPrimitiveTypeMap.put("short", ShortType.INSTANCE);
        nameToPrimitiveTypeMap.put("integer", IntegerType.INSTANCE);
        nameToPrimitiveTypeMap.put("long", LongType.INSTANCE);
        nameToPrimitiveTypeMap.put("float", FloatType.INSTANCE);
        nameToPrimitiveTypeMap.put("double", DoubleType.INSTANCE);
        nameToPrimitiveTypeMap.put("date", DateType.INSTANCE);
        nameToPrimitiveTypeMap.put("timestamp", TimestampType.INSTANCE);
        nameToPrimitiveTypeMap.put("binary", BinaryType.INSTANCE);
        nameToPrimitiveTypeMap.put("string", StringType.INSTANCE);
    }

    private final String primitiveTypeName;

    protected BasePrimitiveType(String primitiveTypeName)
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
        BasePrimitiveType that = (BasePrimitiveType) o;
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
