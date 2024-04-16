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

package io.delta.kernel.defaults.internal.json;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

import io.delta.kernel.data.*;
import io.delta.kernel.types.*;

import static io.delta.kernel.internal.util.Preconditions.checkArgument;

import io.delta.kernel.defaults.internal.data.DefaultJsonRow;

/**
 * Utilities method to serialize and deserialize {@link Row} objects.
 */
public class JsonUtils {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    // by default BigDecimals are truncated and read as floats
    private static final ObjectReader OBJECT_READER_READ_BIG_DECIMALS = OBJECT_MAPPER
            .reader(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);

    private static final DateTimeFormatter DATE_TIME_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");

    static {
        OBJECT_MAPPER.registerModule(
                new SimpleModule().addSerializer(Row.class, new RowSerializer()));
    }

    private JsonUtils() {
    }

    /**
     * Converts a {@link Row} to a single line JSON string.
     *
     * @param row the row to convert
     * @return JSON string
     */
    public static String rowToJson(Row row) {
        try {
            return OBJECT_MAPPER.writeValueAsString(row);
        } catch (JsonProcessingException ex) {
            throw new RuntimeException("Could not serialize row object to JSON", ex);
        }
    }

    /**
     * Converts a JSON string to a {@link Row}.
     *
     * @param json   JSON string
     * @param schema to read the JSON according the schema
     * @return {@link Row} instance with given schema.
     */
    public static Row rowFromJson(String json, StructType schema) {
        try {
            final JsonNode jsonNode = OBJECT_READER_READ_BIG_DECIMALS.readTree(json);
            return new DefaultJsonRow((ObjectNode) jsonNode, schema);
        } catch (JsonProcessingException ex) {
            throw new RuntimeException(String.format("Could not parse JSON: %s", json), ex);
        }
    }

    public static class RowSerializer extends StdSerializer<Row> {
        public RowSerializer() {
            super(Row.class);
        }

        @Override
        public void serialize(Row row, JsonGenerator gen, SerializerProvider provider)
                throws IOException {
            writeRow(gen, row, row.getSchema());
        }

        private void writeRow(JsonGenerator gen, Row row, StructType schema) throws IOException {
            gen.writeStartObject();
            for (int columnOrdinal = 0; columnOrdinal < schema.length(); columnOrdinal++) {
                StructField field = schema.at(columnOrdinal);
                if (!row.isNullAt(columnOrdinal)) {
                    gen.writeFieldName(field.getName());
                    writeValue(gen, row, columnOrdinal, field.getDataType());
                }
            }
            gen.writeEndObject();
        }

        private void writeStruct(JsonGenerator gen, ColumnVector vector, StructType type, int rowId)
                throws IOException {
            gen.writeStartObject();
            for (int columnOrdinal = 0; columnOrdinal < type.length(); columnOrdinal++) {
                StructField field = type.at(columnOrdinal);
                ColumnVector childVector = vector.getChild(columnOrdinal);
                if (!childVector.isNullAt(rowId)) {
                    gen.writeFieldName(field.getName());
                    writeValue(gen, childVector, rowId, field.getDataType());
                }
            }
            gen.writeEndObject();
        }

        private void writeArrayValue(JsonGenerator gen, ArrayValue arrayValue, ArrayType arrayType)
                throws IOException {
            gen.writeStartArray();
            ColumnVector arrayElems = arrayValue.getElements();
            for (int i = 0; i < arrayValue.getSize(); i++) {
                if (!arrayElems.isNullAt(i)) {
                    writeValue(gen, arrayValue.getElements(), i, arrayType.getElementType());
                }
            }
            gen.writeEndArray();
        }

        private void writeMapValue(JsonGenerator gen, MapValue mapValue, MapType mapType)
                throws IOException {
            assertSupportedMapType(mapType);
            gen.writeStartObject();
            ColumnVector keys = mapValue.getKeys();
            ColumnVector values = mapValue.getValues();
            for (int i = 0; i < mapValue.getSize(); i++) {
                if (!values.isNullAt(i)) {
                    gen.writeFieldName(keys.getString(i));
                    writeValue(gen, values, i, mapType.getValueType());
                }
            }
            gen.writeEndObject();
        }

        private void writeValue(JsonGenerator gen, Row row, int columnOrdinal, DataType type)
                throws IOException {
            assert !row.isNullAt(columnOrdinal) : "value should not be null";
            if (type instanceof BooleanType) {
                gen.writeBoolean(row.getBoolean(columnOrdinal));
            } else if (type instanceof ByteType) {
                gen.writeNumber(row.getByte(columnOrdinal));
            } else if (type instanceof ShortType) {
                gen.writeNumber(row.getShort(columnOrdinal));
            } else if (type instanceof IntegerType) {
                gen.writeNumber(row.getInt(columnOrdinal));
            } else if (type instanceof DateType) {
                int daysSinceEpochUTC = row.getInt(columnOrdinal);
                gen.writeString(LocalDate.ofEpochDay(daysSinceEpochUTC).toString());
            } else if (type instanceof LongType) {
                gen.writeNumber(row.getLong(columnOrdinal));
            } else if (type instanceof TimestampType || type instanceof TimestampNTZType) {
                long microsSinceEpochUTC = row.getLong(columnOrdinal);
                gen.writeString(microsToString(microsSinceEpochUTC));
            } else if (type instanceof FloatType) {
                gen.writeNumber(row.getFloat(columnOrdinal));
            } else if (type instanceof DoubleType) {
                gen.writeNumber(row.getDouble(columnOrdinal));
            } else if (type instanceof DecimalType) {
                gen.writeNumber(row.getDecimal(columnOrdinal));
            } else if (type instanceof StringType) {
                gen.writeString(row.getString(columnOrdinal));
            } else if (type instanceof StructType) {
                writeRow(gen, row.getStruct(columnOrdinal), (StructType) type);
            } else if (type instanceof ArrayType) {
                writeArrayValue(gen, row.getArray(columnOrdinal), (ArrayType) type);
            } else if (type instanceof MapType) {
                writeMapValue(gen, row.getMap(columnOrdinal), (MapType) type);
            } else {
                // `binary` type is not supported according the Delta Protocol
                throw new UnsupportedOperationException("unsupported data type: " + type);
            }
        }

        private void writeValue(JsonGenerator gen, ColumnVector vector, int rowId, DataType type)
                throws IOException {
            assert !vector.isNullAt(rowId) : "value should not be null";
            if (type instanceof BooleanType) {
                gen.writeBoolean(vector.getBoolean(rowId));
            } else if (type instanceof ByteType) {
                gen.writeNumber(vector.getByte(rowId));
            } else if (type instanceof ShortType) {
                gen.writeNumber(vector.getShort(rowId));
            } else if (type instanceof IntegerType) {
                gen.writeNumber(vector.getInt(rowId));
            } else if (type instanceof DateType) {
                int daysSinceEpochUTC = vector.getInt(rowId);
                gen.writeString(LocalDate.ofEpochDay(daysSinceEpochUTC).toString());
            } else if (type instanceof LongType) {
                gen.writeNumber(vector.getLong(rowId));
            } else if (type instanceof TimestampType || type instanceof TimestampNTZType) {
                long microsSinceEpochUTC = vector.getLong(rowId);
                gen.writeString(microsToString(microsSinceEpochUTC));
            } else if (type instanceof FloatType) {
                gen.writeNumber(vector.getFloat(rowId));
            } else if (type instanceof DoubleType) {
                gen.writeNumber(vector.getDouble(rowId));
            } else if (type instanceof DecimalType) {
                gen.writeNumber(vector.getDecimal(rowId));
            } else if (type instanceof StringType) {
                gen.writeString(vector.getString(rowId));
            } else if (type instanceof StructType) {
                writeStruct(gen, vector, (StructType) type, rowId);
            } else if (type instanceof ArrayType) {
                writeArrayValue(gen, vector.getArray(rowId), (ArrayType) type);
            } else if (type instanceof MapType) {
                writeMapValue(gen, vector.getMap(rowId), (MapType) type);
            } else {
                // `binary` type is not supported according the Delta Protocol
                throw new UnsupportedOperationException("unsupported data type: " + type);
            }
        }
    }

    private static String microsToString(long microsSinceEpochUTC) {
        long seconds = microsSinceEpochUTC / 1_000_000;
        int nanosOfSecond = (int) ((microsSinceEpochUTC % 1_000_000) * 1_000);
        Instant instant = Instant.ofEpochSecond(seconds, nanosOfSecond);
        return instant.toString();
    }

    private static void assertSupportedMapType(MapType keyType) {
        checkArgument(
                keyType.getKeyType() instanceof StringType,
                "Only STRING type keys are supported in MAP type in JSON serialization");
    }
}
