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
package io.delta.kernel.data;

import java.util.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.delta.kernel.types.ArrayType;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.MapType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.delta.kernel.types.UnresolvedDataType;

public class JsonRow implements Row
{
    ////////////////////////////////////////////////////////////////////////////////
    // Static Methods
    ////////////////////////////////////////////////////////////////////////////////

    private static Object decodeElement(JsonNode jsonValue, DataType dataType) {
        if (jsonValue.isNull()) {
            return null;
        }

        if (dataType instanceof UnresolvedDataType) {
            if (jsonValue.isTextual()) {
                return jsonValue.textValue();
            } else if (jsonValue instanceof ObjectNode) {
                throw new RuntimeException("TODO handle UnresolvedDataType of type object");
            }
        }

        if (dataType instanceof BooleanType) {
            if (!jsonValue.isBoolean()) {
                throw new RuntimeException(
                    String.format("Couldn't decode %s, expected a boolean", jsonValue)
                );
            }
            return jsonValue.booleanValue();
        }

        if (dataType instanceof IntegerType) {
            // TODO: handle other number cases (e.g. short) and throw on invalid cases (e.g. long)
            if (!jsonValue.isInt()) {
                throw new RuntimeException(
                    String.format("Couldn't decode %s, expected an int", jsonValue)
                );
            }
            return jsonValue.intValue();
        }

        if (dataType instanceof LongType) {
            if (!jsonValue.isNumber()) {
                throw new RuntimeException(
                    String.format("Couldn't decode %s, expected a long", jsonValue)
                );
            }
            return jsonValue.numberValue().longValue();
        }

        if (dataType instanceof StringType) {
            // TODO: some of the metadata values for the schema can have a mix of string and
            // non-strings in metadata map.
            // if (!jsonValue.isTextual()) {
            //    throw new RuntimeException(
            //        String.format("Couldn't decode %s, expected a string", jsonValue)
            //    );
            // }
            return jsonValue.asText();
        }

        if (dataType instanceof StructType) {
            if (!jsonValue.isObject()) {
                throw new RuntimeException(
                    String.format("Couldn't decode %s, expected a struct", jsonValue)
                );
            }
            return new JsonRow((ObjectNode) jsonValue, (StructType) dataType);
        }

        if (dataType instanceof ArrayType) {
            if (!jsonValue.isArray()) {
                throw new RuntimeException(
                    String.format("Couldn't decode %s, expected an array", jsonValue)
                );
            }
            final ArrayType arrayType = ((ArrayType) dataType);
            final ArrayNode jsonArray = (ArrayNode) jsonValue;
            final List<Object> output = new ArrayList<>();


            for (Iterator<JsonNode> it = jsonArray.elements(); it.hasNext();) {
                final JsonNode element = it.next();
                final Object parsedElement =
                    decodeElement(element, arrayType.getElementType());
                output.add(parsedElement);
            }
            return output;
        }

        if (dataType instanceof MapType) {
            if (!jsonValue.isObject()) {
                throw new RuntimeException(
                    String.format("Couldn't decode %s, expected a map", jsonValue)
                );
            }
            final MapType mapType = (MapType) dataType;
            final Iterator<Map.Entry<String, JsonNode>> iter = ((ObjectNode) jsonValue).fields();
            final Map<Object, Object> output = new HashMap<>();

            while (iter.hasNext()) {
                Map.Entry<String, JsonNode> entry = iter.next();
                String keyParsed = entry.getKey(); // TODO: handle non-String keys
                Object valueParsed = decodeElement(entry.getValue(), mapType.getValueType());
                output.put(keyParsed, valueParsed);
            }

            return output;
        }

        throw new UnsupportedOperationException(
            String.format("Unsupported DataType %s for RootNode %s", dataType.typeName(), jsonValue)
        );
    }

    private static Object decodeField(ObjectNode rootNode, StructField field) {
        if (rootNode.get(field.getName()) == null) {
            if (field.isNullable()) {
                return null;
            }

            throw new RuntimeException(
                String.format(
                    "Root node at key %s is null but field isn't nullable. Root node: %s",
                    field.getName(),
                    rootNode
                )
            );
        }

        return decodeElement(rootNode.get(field.getName()), field.getDataType());
    }

    ////////////////////////////////////////////////////////////////////////////////
    // Instance Fields / Methods
    ////////////////////////////////////////////////////////////////////////////////

    private final ObjectNode rootNode;
    private final Object[] parsedValues;
    private final StructType readSchema;

    public JsonRow(ObjectNode rootNode, StructType readSchema) {
        this.rootNode = rootNode;
        this.readSchema = readSchema;
        this.parsedValues = new Object[readSchema.length()];

        for (int i = 0; i < readSchema.length(); i++) {
            final StructField field = readSchema.at(i);
            final Object parsedValue = decodeField(rootNode, field);
            parsedValues[i] = parsedValue;
        }
    }

    ////////////////////////////////////////
    // Public APIs
    ////////////////////////////////////////

    @Override
    public StructType getSchema()
    {
        return readSchema;
    }

    @Override
    public boolean isNullAt(int ordinal) {
        return parsedValues[ordinal] == null;
    }

    @Override
    public boolean getBoolean(int ordinal) {
        assertType(ordinal, BooleanType.INSTANCE);
        return (boolean) parsedValues[ordinal];
    }

    @Override
    public int getInt(int ordinal) {
        assertType(ordinal, IntegerType.INSTANCE);
        return (int) parsedValues[ordinal];
    }

    @Override
    public long getLong(int ordinal) {
        assertType(ordinal, LongType.INSTANCE);
        return (long) parsedValues[ordinal];
    }

    @Override
    public String getString(int ordinal) {
        assertType(ordinal, StringType.INSTANCE);
        return (String) parsedValues[ordinal];
    }

    @Override
    public Row getRecord(int ordinal) {
        assertType(ordinal, StructType.EMPTY_INSTANCE);
        return (JsonRow) parsedValues[ordinal];
    }

    @Override
    public <T> List<T> getList(int ordinal) {
        assertType(ordinal, ArrayType.EMPTY_INSTANCE);
        return (List<T>) parsedValues[ordinal];
    }

    @Override
    public <K, V> Map<K, V> getMap(int ordinal) {
        assertType(ordinal, MapType.EMPTY_INSTANCE);
        return (Map<K, V>) parsedValues[ordinal];
    }

    @Override
    public String toString() {
        return "JsonRow{" +
            "rootNode=" + rootNode +
            ", parsedValues=" + parsedValues +
            ", readSchema=" + readSchema +
            '}';
    }

    ////////////////////////////////////////
    // Private Helper Methods
    ////////////////////////////////////////

    private void assertType(int ordinal, DataType expectedType) {
        final String actualTypeName = readSchema.at(ordinal).getDataType().typeName();
        if (!actualTypeName.equals(expectedType.typeName()) &&
            !actualTypeName.equals(UnresolvedDataType.INSTANCE.typeName())) {
            throw new RuntimeException(
                String.format(
                    "Tried to read %s at ordinal %s but actual data type is %s",
                    expectedType.typeName(),
                    ordinal,
                    actualTypeName
                )
            );
        }
    }
}
