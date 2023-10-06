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
package io.delta.kernel.defaults.internal.data;

import java.math.BigDecimal;
import java.util.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.delta.kernel.data.ArrayValue;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.MapValue;
import io.delta.kernel.data.Row;
import io.delta.kernel.types.*;
import io.delta.kernel.internal.types.MixedDataType;

import io.delta.kernel.defaults.internal.data.vector.DefaultGenericVector;

public class DefaultJsonRow implements Row {
    private final Object[] parsedValues;
    private final StructType readSchema;

    public DefaultJsonRow(ObjectNode rootNode, StructType readSchema) {
        this.readSchema = readSchema;
        this.parsedValues = new Object[readSchema.length()];

        for (int i = 0; i < readSchema.length(); i++) {
            final StructField field = readSchema.at(i);
            final Object parsedValue = decodeField(rootNode, field);
            parsedValues[i] = parsedValue;
        }
    }

    @Override
    public StructType getSchema() {
        return readSchema;
    }

    @Override
    public boolean isNullAt(int ordinal) {
        return parsedValues[ordinal] == null;
    }

    @Override
    public boolean getBoolean(int ordinal) {
        return (boolean) parsedValues[ordinal];
    }

    @Override
    public byte getByte(int ordinal) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public short getShort(int ordinal) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public int getInt(int ordinal) {
        return (int) parsedValues[ordinal];
    }

    @Override
    public long getLong(int ordinal) {
        return (long) parsedValues[ordinal];
    }

    @Override
    public float getFloat(int ordinal) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public double getDouble(int ordinal) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public String getString(int ordinal) {
        return (String) parsedValues[ordinal];
    }

    @Override
    public BigDecimal getDecimal(int ordinal) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public byte[] getBinary(int ordinal) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Row getStruct(int ordinal) {
        return (DefaultJsonRow) parsedValues[ordinal];
    }

    @Override
    public ArrayValue getArray(int ordinal) {
        return (ArrayValue) parsedValues[ordinal];
    }

    @Override
    public MapValue getMap(int ordinal) {
        return (MapValue) parsedValues[ordinal];
    }

    private static void throwIfTypeMismatch(String expType, boolean hasExpType, JsonNode jsonNode) {
        if (!hasExpType) {
            throw new RuntimeException(
                String.format("Couldn't decode %s, expected a %s", jsonNode, expType));
        }
    }

    private static Object decodeElement(JsonNode jsonValue, DataType dataType) {
        if (jsonValue.isNull()) {
            return null;
        }

        if (dataType.equals(MixedDataType.INSTANCE)) {
            if (jsonValue.isTextual()) {
                return jsonValue.textValue();
            } else if (jsonValue instanceof ObjectNode) {
                return jsonValue.toString();
            }
            throwIfTypeMismatch("object or string", false, jsonValue);
        }

        if (dataType instanceof BooleanType) {
            throwIfTypeMismatch("boolean", jsonValue.isBoolean(), jsonValue);
            return jsonValue.booleanValue();
        }

        if (dataType instanceof IntegerType) {
            throwIfTypeMismatch(
                "integer", jsonValue.isIntegralNumber() && !jsonValue.isLong(), jsonValue);
            return jsonValue.intValue();
        }

        if (dataType instanceof LongType) {
            throwIfTypeMismatch("long", jsonValue.isIntegralNumber(), jsonValue);
            return jsonValue.numberValue().longValue();
        }

        if (dataType instanceof StringType) {
            // TODO: sometimes the Delta Log contains config as String -> String or String -> Int
            throwIfTypeMismatch(
                "string",
                jsonValue.isTextual() | jsonValue.isIntegralNumber(),
                jsonValue);
            return jsonValue.asText();
        }

        if (dataType instanceof StructType) {
            throwIfTypeMismatch("object", jsonValue.isObject(), jsonValue);
            return new DefaultJsonRow((ObjectNode) jsonValue, (StructType) dataType);
        }

        if (dataType instanceof ArrayType) {
            throwIfTypeMismatch("array", jsonValue.isArray(), jsonValue);
            final ArrayType arrayType = ((ArrayType) dataType);
            final ArrayNode jsonArray = (ArrayNode) jsonValue;
            final Object[] elements = new Object[jsonArray.size()];
            for (int i = 0; i < jsonArray.size(); i++) {
                final JsonNode element = jsonArray.get(i);
                final Object parsedElement = decodeElement(element, arrayType.getElementType());
                if (parsedElement == null && !arrayType.containsNull()) {
                    throw new RuntimeException("Array type expects no nulls as elements, but " +
                            "received `null` as array element");
                }
                elements[i] = parsedElement;
            }
            return new ArrayValue() {
                @Override
                public int getSize() {
                    return elements.length;
                }

                @Override
                public ColumnVector getElements() {
                    return new DefaultGenericVector(arrayType.getElementType(), elements);
                }
            };
        }

        if (dataType instanceof MapType) {
            throwIfTypeMismatch("map", jsonValue.isObject(), jsonValue);
            final MapType mapType = (MapType) dataType;
            if (!(mapType.getKeyType() instanceof StringType)) {
                throw new RuntimeException("MapType with a key type of `String` is supported, " +
                        "received a key type: " + mapType.getKeyType());
            }
            List<String> keys = new ArrayList<>(jsonValue.size());
            List<Object> values = new ArrayList<>(jsonValue.size());
            final Iterator<Map.Entry<String, JsonNode>> iter = jsonValue.fields();

            while (iter.hasNext()) {
                Map.Entry<String, JsonNode> entry = iter.next();
                String keyParsed = entry.getKey();
                Object valueParsed = decodeElement(entry.getValue(), mapType.getValueType());
                if (valueParsed == null && !mapType.isValueContainsNull()) {
                    throw new RuntimeException("Map type expects no nulls in values, but " +
                            "received `null` as value");
                }
                keys.add(keyParsed);
                values.add(valueParsed);
            }
            return new MapValue() {
                @Override
                public int getSize() {
                    return jsonValue.size();
                }

                @Override
                public ColumnVector getKeys() {
                    return new DefaultGenericVector(mapType.getKeyType(), keys.toArray());
                }

                @Override
                public ColumnVector getValues() {
                    return new DefaultGenericVector(mapType.getValueType(), values.toArray());
                }
            };
        }

        throw new UnsupportedOperationException(
            String.format("Unsupported DataType %s for RootNode %s", dataType, jsonValue)
        );
    }

    private static Object decodeField(ObjectNode rootNode, StructField field) {
        if (rootNode.get(field.getName()) == null || rootNode.get(field.getName()).isNull()) {
            if (field.isNullable()) {
                return null;
            }

            throw new RuntimeException(String.format(
                "Root node at key %s is null but field isn't nullable. Root node: %s",
                field.getName(),
                rootNode));
        }

        return decodeElement(rootNode.get(field.getName()), field.getDataType());
    }
}
