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

import io.delta.kernel.annotation.Evolving;

/**
 * Data type representing a {@code map} type.
 *
 * @since 3.0.0
 */
@Evolving
public class MapType extends DataType {

    private final StructField keyField;
    private final StructField valueField;

    public MapType(DataType keyType, DataType valueType, boolean valueContainsNull) {
        this.keyField = new StructField("key", keyType, false);
        this.valueField = new StructField("value", valueType, valueContainsNull);
    }

    public MapType(StructField keyField, StructField valueField) {
        this.keyField = keyField;
        this.valueField = valueField;
    }

    public StructField getKeyField() {
        return keyField;
    }

    public StructField getValueField() {
        return valueField;
    }

    public DataType getKeyType() {
        return getKeyField().getDataType();
    }

    public DataType getValueType() {
        return getValueField().getDataType();
    }

    public boolean isValueContainsNull() {
        return valueField.isNullable();
    }

    @Override
    public boolean equivalent(DataType dataType) {
        return dataType instanceof MapType &&
            ((MapType) dataType).getKeyType().equivalent(getKeyType()) &&
            ((MapType) dataType).getValueType().equivalent(getValueType()) &&
            ((MapType) dataType).isValueContainsNull() == isValueContainsNull();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MapType mapType = (MapType) o;
        return Objects.equals(keyField, mapType.keyField) &&
                Objects.equals(valueField, mapType.valueField);
    }

    @Override
    public int hashCode() {
        return Objects.hash(keyField, valueField);
    }

    @Override
    public String toJson() {
        return String.format("{" +
            "\"type\": \"map\"," +
            "\"keyType\": %s," +
            "\"valueType\": %s," +
            "\"valueContainsNull\": %s" +
            "}", getKeyType().toJson(), getValueType().toJson(), isValueContainsNull());
    }

    @Override
    public String toString() {
        return String.format("map[%s, %s]", getKeyType(), getValueType());
    }
}
