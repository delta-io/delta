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
package io.delta.kernel.internal.types;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import io.delta.kernel.client.JsonHandler;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.types.ArrayType;
import io.delta.kernel.types.BasePrimitiveType;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.DecimalType;
import io.delta.kernel.types.MapType;
import io.delta.kernel.types.MixedDataType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.Utils;

/**
 * Utility class to serialize and deserialize the table schema which is of type {@link StructType}.
 */
public class TableSchemaSerDe
{
    private TableSchemaSerDe()
    {
    }

    /**
     * Serialize the given table schema {@code structType} as JSON. This should produce Delta
     * Protocol complaint format.
     *
     * @param structType Data type to serialize
     * @return Table schema serialized as JSON string.
     */
    public static String toJson(StructType structType)
    {
        return structType.toJson();
    }

    /**
     * Deserialize the table schema from {@code serializedStructType} using the given
     * {@code tableClient}
     *
     * @param jsonHandler An instance of {@link JsonHandler} to use for parsing
     * JSON operations.
     * @param serializedStructType Table schema in JSON format compliant with the Delta Protocol.
     * @return Table schema
     */
    public static StructType fromJson(JsonHandler jsonHandler, String serializedStructType)
    {
        return parseStructType(jsonHandler, serializedStructType);
    }

    /**
     * Utility method to parse a given struct as struct type.
     */
    private static StructType parseStructType(JsonHandler jsonHandler, String serializedStructType)
    {
        Function<Row, StructType> evalMethod = (row) -> {
            final List<Row> fields = row.getArray(0);
            return new StructType(
                fields.stream()
                    .map(field -> parseStructField(jsonHandler, field))
                    .collect(Collectors.toList()));
        };
        return parseAndEvalSingleRow(
            jsonHandler, serializedStructType, STRUCT_TYPE_SCHEMA, evalMethod);
    }

    /**
     * Utility method to parse a {@link StructField} from the {@link Row}
     */
    private static StructField parseStructField(JsonHandler jsonHandler, Row row)
    {
        String name = row.getString(0);
        String serializedDataType = row.getString(1);
        DataType type = parseDataType(jsonHandler, serializedDataType);
        boolean nullable = row.getBoolean(2);
        Map<String, String> metadata = row.getMap(3);

        return new StructField(name, type, nullable, metadata);
    }

    /**
     * Utility method to parse the data type from the {@link Row}.
     */
    private static DataType parseDataType(JsonHandler jsonHandler, String serializedDataType)
    {
        if (BasePrimitiveType.isPrimitiveType(serializedDataType)) {
            return BasePrimitiveType.createPrimitive(serializedDataType);
        }

        // Check if it is decimal type
        if (serializedDataType.startsWith("decimal")) {
            if (serializedDataType.equalsIgnoreCase("decimal")) {
                return DecimalType.USER_DEFAULT;
            }

            // parse the precision and scale
            Matcher matcher = DECIMAL_TYPE_PATTERN.matcher(serializedDataType);
            if (!matcher.matches()) {
                throw new IllegalArgumentException(
                    "Invalid decimal type format: " + serializedDataType);
            }
            return new DecimalType(
                Integer.valueOf(matcher.group("precision")),
                Integer.valueOf(matcher.group("scale")));
        }
        // This must be a complex type which is described as an JSON object.

        Optional<ArrayType> arrayType = parseAsArrayType(jsonHandler, serializedDataType);
        if (arrayType.isPresent()) {
            return arrayType.get();
        }

        Optional<MapType> mapType = parseAsMapType(jsonHandler, serializedDataType);
        if (mapType.isPresent()) {
            return mapType.get();
        }

        return parseStructType(jsonHandler, serializedDataType);
    }

    private static Optional<ArrayType> parseAsArrayType(JsonHandler jsonHandler, String json)
    {
        Function<Row, Optional<ArrayType>> evalMethod = (row) -> {
            if (!"array".equalsIgnoreCase(row.getString(0))) {
                return Optional.empty();
            }

            if (row.isNullAt(1) || row.isNullAt(2)) {
                throw new IllegalArgumentException("invalid array serialized format: " + json);
            }

            // Now parse the element type and create an array data type object
            DataType elementType = parseDataType(jsonHandler, row.getString(1));
            boolean containsNull = row.getBoolean(2);

            return Optional.of(new ArrayType(elementType, containsNull));
        };

        return parseAndEvalSingleRow(jsonHandler, json, ARRAY_TYPE_SCHEMA, evalMethod);
    }

    private static Optional<MapType> parseAsMapType(JsonHandler jsonHandler, String json)
    {
        Function<Row, Optional<MapType>> evalMethod = (row -> {
            if (!"map".equalsIgnoreCase(row.getString(0))) {
                return Optional.empty();
            }

            if (row.isNullAt(1) || row.isNullAt(2) || row.isNullAt(3)) {
                throw new IllegalArgumentException("invalid map serialized format: " + json);
            }

            // Now parse the key and value types and create a map data type object
            DataType keyType = parseDataType(jsonHandler, row.getString(1));
            DataType valueType = parseDataType(jsonHandler, row.getString(2));
            boolean valueContainsNull = row.getBoolean(3);

            return Optional.of(new MapType(keyType, valueType, valueContainsNull));
        });

        return parseAndEvalSingleRow(jsonHandler, json, MAP_TYPE_SCHEMA, evalMethod);
    }

    /**
     * Helper method to parse a single json string
     */
    private static <R> R parseAndEvalSingleRow(
        JsonHandler jsonHandler,
        String jsonString,
        StructType outputSchema,
        Function<Row, R> evalFunction)
    {
        ColumnVector columnVector = Utils.singletonColumnVector(jsonString);
        ColumnarBatch result = jsonHandler.parseJson(columnVector, outputSchema);

        assert result.getSize() == 1;

        CloseableIterator<Row> rows = result.getRows();
        try {
            return evalFunction.apply(rows.next());
        }
        finally {
            Utils.closeCloseables(rows);
        }
    }

    /**
     * Schema of the one member ({@link StructField}) in {@link StructType}.
     */
    private static final StructType STRUCT_FIELD_SCHEMA = new StructType()
        .add("name", StringType.INSTANCE)
        .add("type", MixedDataType.INSTANCE) // Data type can be a string or a object.
        .add("nullable", BooleanType.INSTANCE)
        .add("metadata",
            new MapType(StringType.INSTANCE, StringType.INSTANCE, false /* valueContainsNull */));

    /**
     * Schema of the serialized {@link StructType}.
     */
    private static StructType STRUCT_TYPE_SCHEMA =
        new StructType()
            .add("fields", new ArrayType(STRUCT_FIELD_SCHEMA, false /* containsNull */));

    /**
     * Example Array Type in serialized format
     * {
     * "type" : "array",
     * "elementType" : {
     * "type" : "struct",
     * "fields" : [ {
     * "name" : "d",
     * "type" : "integer",
     * "nullable" : false,
     * "metadata" : { }
     * } ]
     * },
     * "containsNull" : true
     * }
     */
    private static StructType ARRAY_TYPE_SCHEMA =
        new StructType()
            .add("type", StringType.INSTANCE)
            .add("elementType", MixedDataType.INSTANCE)
            .add("containsNull", BooleanType.INSTANCE);

    /**
     * Example Map Type in serialized format
     * {
     * "type" : "map",
     * "keyType" : "string",
     * "valueType" : "string",
     * "valueContainsNull" : true
     * }
     */
    private static StructType MAP_TYPE_SCHEMA =
        new StructType()
            .add("type", StringType.INSTANCE)
            .add("keyType", MixedDataType.INSTANCE)
            .add("valueType", MixedDataType.INSTANCE)
            .add("valueContainsNull", BooleanType.INSTANCE);

    private static Pattern DECIMAL_TYPE_PATTERN =
        Pattern.compile("decimal\\(\\s*(?<precision>[0-9]+),\\s*(?<scale>[0-9]+)\\s*\\)");
}
