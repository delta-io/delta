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
package io.delta.kernel.defaults.internal;

import java.util.*;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.fasterxml.jackson.databind.JsonNode;

import io.delta.kernel.types.*;
import static io.delta.kernel.internal.util.Preconditions.checkArgument;

/**
 * Parses Delta data types based on the
 * <a href="https://github.com/delta-io/delta/blob/master/PROTOCOL.md#primitive-types">
 *     serialization rules </a> outlined in the Delta Protocol.
 */
public class DataTypeParser {

    private DataTypeParser() {}

    public static StructType parseSchema(JsonNode json) {
        DataType parsedType = parseDataType(json);
        if (parsedType instanceof StructType) {
            return (StructType) parsedType;
        } else {
            throw new IllegalArgumentException(
                String.format("Could not parse %s as a valid StructType", json));
        }
    }

    /**
     * Parses a Delta data type from JSON. Data types can either be serialized as strings (for
     * primitive types) or as objects (for complex types).
     *
     * For example:
     * <pre>
     * // Map type field is serialized as:
     * {
     *   "name" : "f",
     *   "type" : {
     *     "type" : "map",
     *     "keyType" : "string",
     *     "valueType" : "string",
     *     "valueContainsNull" : true
     *   },
     *   "nullable" : true,
     *   "metadata" : { }
     * }
     *
     * // Integer type field serialized as:
     * {
     *   "name" : "a",
     *   "type" : "integer",
     *   "nullable" : false,
     *   "metadata" : { }
     * }
     * </pre>
     */
    public static DataType parseDataType(JsonNode json) {
        switch (json.getNodeType()) {
            case STRING:
                // simple types are stored as just a string
                return nameToType(json.textValue());
            case OBJECT:
                // complex types (array, map, or struct are stored as JSON objects)
                String type = getStringField(json, "type");
                switch (type) {
                    case "struct":
                        return parseStructType(json);
                    case "array":
                        return parseArrayType(json);
                    case "map":
                        return parseMapType(json);
                }
            default:
                throw new IllegalArgumentException(
                    String.format("Could not parse %s as a valid Delta data type", json));
        }
    }

    /**
     * Parses an <a href="https://github.com/delta-io/delta/blob/master/PROTOCOL.md#array-type">
     *     array type </a>
     */
    private static ArrayType parseArrayType(JsonNode json) {
        checkArgument(json.isObject() && json.size() == 3,
            String.format("Expected JSON object with 3 fields for array data type %s", json));
        boolean containsNull = getBooleanField(json, "containsNull");
        DataType dataType = parseDataType(getNonNullField(json, "elementType"));
        return new ArrayType(dataType, containsNull);
    }

    /**
     * Parses an <a href="https://github.com/delta-io/delta/blob/master/PROTOCOL.md#map-type">
     *     map type </a>
     */
    private static MapType parseMapType(JsonNode json) {
        checkArgument(json.isObject() && json.size() == 4,
            String.format("Expected JSON object with 4 fields for map data type %s", json));
        boolean valueContainsNull = getBooleanField(json, "valueContainsNull");
        DataType keyType = parseDataType(getNonNullField(json, "keyType"));
        DataType valueType = parseDataType(getNonNullField(json, "valueType"));
        return new MapType(keyType, valueType, valueContainsNull);
    }

    /**
     * Parses an <a href="https://github.com/delta-io/delta/blob/master/PROTOCOL.md#struct-type">
     *     struct type </a>
     */
    private static StructType parseStructType(JsonNode json) {
        checkArgument(json.isObject() && json.size() == 2,
            String.format("Expected JSON object with 2 fields for struct data type %s", json));
        JsonNode fieldsNode = getNonNullField(json, "fields");
        checkArgument(fieldsNode.isArray(),
            String.format("Expected array for fieldName=%s in %s", "fields", json));
        Iterator<JsonNode> fields = fieldsNode.elements();
        List<StructField> parsedFields = new ArrayList<>();
        while (fields.hasNext()) {
            parsedFields.add(parseStructField(fields.next()));
        }
        return new StructType(parsedFields);
    }

    /**
     * Parses an <a href="https://github.com/delta-io/delta/blob/master/PROTOCOL.md#struct-field">
     *     struct field </a>
     */
    private static StructField parseStructField(JsonNode json) {
        checkArgument(json.isObject(), "Expected JSON object for struct field");
        String name = getStringField(json, "name");
        DataType type = parseDataType(getNonNullField(json, "type"));
        boolean nullable = getBooleanField(json, "nullable");
        FieldMetadata metadata = parseFieldMetadata(json.get("metadata"));
        return new StructField(
            name,
            type,
            nullable,
            metadata
        );
    }

    /**
     * Parses an {@link FieldMetadata}.
     */
    private static FieldMetadata parseFieldMetadata(JsonNode json) {
        if (json == null || json.isNull()) {
            return FieldMetadata.builder().build();
        }

        checkArgument(json.isObject(), "Expected JSON object for struct field metadata");
        final Iterator<Map.Entry<String,JsonNode>> iterator = json.fields();
        final FieldMetadata.Builder builder = FieldMetadata.builder();
        while (iterator.hasNext()) {
            Map.Entry<String, JsonNode> entry = iterator.next();
            JsonNode value = entry.getValue();
            String key = entry.getKey();

            if (value.isNull()) {
                builder.putNull(key);
            } else if (value.isInt()) {
                builder.putLong(key, value.intValue());
            } else if (value.isDouble()) {
                builder.putDouble(key, value.doubleValue());
            } else if (value.isBoolean()) {
                builder.putBoolean(key, value.booleanValue());
            } else if (value.isTextual()) {
                builder.putString(key, value.textValue());
            } else if (value.isObject()) {
                builder.putMetadata(key, parseFieldMetadata(value));
            } else if (value.isArray()) {
                final Iterator<JsonNode> fields = value.elements();
                if (!fields.hasNext()) {
                    // If it is an empty array, we cannot infer its element type.
                    // We put an empty Array[Long].
                    builder.putLongArray(key, new Long[0]);
                } else {
                    final JsonNode head = fields.next();
                    // TODO could these have null elements?
                    if (head.isInt()) {
                        builder.putLongArray(
                            key,
                            buildList(value, node -> (long) node.intValue()).toArray(new Long[0])
                        );
                    } else if (head.isDouble()) {
                        builder.putDoubleArray(
                            key,
                            buildList(value, JsonNode::doubleValue).toArray(new Double[0])
                        );
                    } else if (head.isBoolean()) {
                        builder.putBooleanArray(
                            key,
                            buildList(value, JsonNode::booleanValue).toArray(new Boolean[0])
                        );
                    } else if (head.isTextual()) {
                        builder.putStringArray(
                            key,
                            buildList(value, JsonNode::textValue).toArray(new String[0])
                        );
                    } else if (head.isObject()) {
                        builder.putMetadataArray(
                            key,
                            buildList(value, DataTypeParser::parseFieldMetadata)
                                .toArray(new FieldMetadata[0])
                        );
                    } else {
                        throw new IllegalArgumentException(String.format(
                            "Unsupported type for Array as field metadata value: %s", value));
                    }
                }
            } else {
                throw new IllegalArgumentException(
                    String.format("Unsupported type for field metadata value: %s", value));
            }
        }
        return builder.build();
    }

    /**
     * For an array JSON node builds a {@link List} using the provided {@code accessor} for each
     * element.
     */
    private static <T> List<T> buildList(JsonNode json, Function<JsonNode, T> accessor) {
        List<T> result = new ArrayList<>();
        Iterator<JsonNode> elements = json.elements();
        while (elements.hasNext()) {
            result.add(accessor.apply(elements.next()));
        }
        return result;
    }

    private static String FIXED_DECIMAL = "decimal\\(\\s*(\\d+)\\s*,\\s*(\\-?\\d+)\\s*\\)";

    /**
     * Parses primitive string type names to a {@link DataType}
     */
    private static DataType nameToType(String name) {
        if (BasePrimitiveType.isPrimitiveType(name)) {
            return BasePrimitiveType.createPrimitive(name);
        } else if (name.equals("decimal")) {
            return DecimalType.USER_DEFAULT;
        } else if (name.matches(FIXED_DECIMAL)) {
            Matcher matcher = Pattern.compile(FIXED_DECIMAL).matcher(name);
            if (!matcher.matches()) {
                throw new IllegalStateException(
                    String.format("%s did not match decimal pattern", name));
            }
            int precision = Integer.parseInt(matcher.group(1));
            int scale = Integer.parseInt(matcher.group(2));
            return new DecimalType(precision, scale);
        } else {
            throw new IllegalArgumentException(
                String.format("%s is not a supported delta data type", name));
        }
    }

    private static JsonNode getNonNullField(JsonNode rootNode, String fieldName) {
        JsonNode node = rootNode.get(fieldName);
        if (node == null || node.isNull()) {
            throw new IllegalArgumentException(
                String.format("Expected non-null for fieldName=%s in %s", fieldName, rootNode));
        }
        return node;
    }

    private static String getStringField(JsonNode rootNode, String fieldName) {
        JsonNode node = getNonNullField(rootNode, fieldName);
        checkArgument(node.isTextual(),
            String.format("Expected string for fieldName=%s in %s", fieldName, rootNode));
        return node.textValue(); // double check this only works for string values! and isTextual()!
    }

    private static boolean getBooleanField(JsonNode rootNode, String fieldName) {
        JsonNode node = getNonNullField(rootNode, fieldName);
        checkArgument(node.isBoolean(),
            String.format("Expected boolean for fieldName=%s in %s", fieldName, rootNode));
        return node.booleanValue();
    }

}
