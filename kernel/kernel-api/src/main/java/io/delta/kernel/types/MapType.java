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

import java.util.Objects;

/**
 * Data type representing a map type.
 */
public class MapType extends DataType
{

    private final DataType keyType;
    private final DataType valueType;
    private final boolean valueContainsNull;

    public MapType(DataType keyType, DataType valueType, boolean valueContainsNull)
    {
        this.keyType = keyType;
        this.valueType = valueType;
        this.valueContainsNull = valueContainsNull;
    }

    public DataType getKeyType()
    {
        return keyType;
    }

    public DataType getValueType()
    {
        return valueType;
    }

    public boolean isValueContainsNull()
    {
        return valueContainsNull;
    }

    @Override
    public boolean equivalent(DataType dataType)
    {
        return dataType instanceof MapType &&
            ((MapType) dataType).getKeyType().equivalent(keyType) &&
            ((MapType) dataType).getValueType().equivalent(valueType) &&
            ((MapType) dataType).valueContainsNull == valueContainsNull;
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
        MapType mapType = (MapType) o;
        return valueContainsNull == mapType.valueContainsNull && keyType.equals(mapType.keyType) &&
            valueType.equals(mapType.valueType);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(keyType, valueType, valueContainsNull);
    }

    @Override
    public String toJson()
    {
        return String.format("{" +
            "\"type\": \"map\"," +
            "\"keyType\": %s," +
            "\"valueType\": %s," +
            "\"valueContainsNull\": %s" +
            "}", keyType.toJson(), valueType.toJson(), valueContainsNull);
    }

    @Override
    public String toString()
    {
        return String.format("Map[%s, %s]", keyType, valueType);
    }
}
