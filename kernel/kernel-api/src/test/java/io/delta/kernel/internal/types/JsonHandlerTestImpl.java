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

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.delta.kernel.client.FileReadContext;
import io.delta.kernel.client.JsonHandler;
import io.delta.kernel.data.*;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.types.ArrayType;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.MapType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.Utils;

/**
 * Implementation of {@link JsonHandler} for testing Delta Kernel APIs
 */
public class JsonHandlerTestImpl
    implements JsonHandler {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public CloseableIterator<FileReadContext> contextualizeFileReads(
        CloseableIterator<Row> fileIter, Predicate predicate) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public ColumnarBatch parseJson(ColumnVector jsonStringVector, StructType outputSchema) {
        List<Row> rows = new ArrayList<>();
        for (int i = 0; i < jsonStringVector.getSize(); i++) {
            final String json = jsonStringVector.getString(i);
            try {
                final JsonNode jsonNode = objectMapper.readTree(json);
                rows.add(new TestJsonRow((ObjectNode) jsonNode, outputSchema));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        return new ColumnarBatch() {
            @Override
            public StructType getSchema() {
                return outputSchema;
            }

            @Override
            public ColumnVector getColumnVector(int ordinal) {
                throw new UnsupportedOperationException("not yet implemented");
            }

            @Override
            public int getSize() {
                return rows.size();
            }

            @Override
            public CloseableIterator<Row> getRows() {
                return Utils.toCloseableIterator(rows.iterator());
            }
        };
    }

    @Override
    public CloseableIterator<FileDataReadResult> readJsonFiles(
        CloseableIterator<FileReadContext> fileIter, StructType physicalSchema) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    private static class TestJsonRow implements Row {
        static void throwIfTypeMismatch(String expType, boolean hasExpType, JsonNode jsonNode) {
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
                throwIfTypeMismatch("integer", jsonValue.isInt(), jsonValue);
                return jsonValue.intValue();
            }

            if (dataType instanceof LongType) {
                throwIfTypeMismatch("long", jsonValue.isLong(), jsonValue);
                return jsonValue.numberValue().longValue();
            }

            if (dataType instanceof StringType) {
                throwIfTypeMismatch("string", jsonValue.isTextual(), jsonValue);
                return jsonValue.asText();
            }

            if (dataType instanceof StructType) {
                throwIfTypeMismatch("object", jsonValue.isObject(), jsonValue);
                return new TestJsonRow((ObjectNode) jsonValue, (StructType) dataType);
            }

            if (dataType instanceof ArrayType) {
                throwIfTypeMismatch("array", jsonValue.isArray(), jsonValue);
                final ArrayType arrayType = ((ArrayType) dataType);
                final ArrayNode jsonArray = (ArrayNode) jsonValue;
                final List<Object> output = new ArrayList<>();

                for (Iterator<JsonNode> it = jsonArray.elements(); it.hasNext(); ) {
                    final JsonNode element = it.next();
                    final Object parsedElement = decodeElement(element, arrayType.getElementType());
                    output.add(parsedElement);
                }
                return new ArrayValue() {
                    @Override
                    public int getSize() {
                        return output.size();
                    }

                    @Override
                    public ColumnVector getElements() {
                        return new TestColumnVector(arrayType.getElementType(), output);
                    }
                };
            }

            if (dataType instanceof MapType) {
                throwIfTypeMismatch("map", jsonValue.isObject(), jsonValue);
                final MapType mapType = (MapType) dataType;
                final List<Object> keys = new ArrayList<>();
                final List<Object> values = new ArrayList<>();
                final Iterator<Map.Entry<String, JsonNode>> iter = jsonValue.fields();

                while (iter.hasNext()) {
                    Map.Entry<String, JsonNode> entry = iter.next();
                    String keyParsed = entry.getKey();
                    Object valueParsed = decodeElement(entry.getValue(), mapType.getValueType());
                    keys.add(keyParsed);
                    values.add(valueParsed);
                }

                return new MapValue() {

                    @Override
                    public int getSize() {
                        return keys.size();
                    }

                    @Override
                    public ColumnVector getKeys() {
                        return new TestColumnVector(mapType.getKeyType(), keys);
                    }

                    @Override
                    public ColumnVector getValues() {
                        return new TestColumnVector(mapType.getValueType(), values);
                    }
                };
            }

            throw new UnsupportedOperationException(
                String.format("Unsupported DataType %s for RootNode %s", dataType, jsonValue)
            );
        }

        private static Object decodeField(ObjectNode rootNode, StructField field) {
            if (rootNode.get(field.getName()) == null) {
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

        private final Object[] parsedValues;
        private final StructType readSchema;

        TestJsonRow(ObjectNode rootNode, StructType readSchema) {
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
            throw new UnsupportedOperationException("not yet implemented - test only");
        }

        @Override
        public short getShort(int ordinal) {
            throw new UnsupportedOperationException("not yet implemented - test only");
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
            throw new UnsupportedOperationException("not yet implemented - test only");
        }

        @Override
        public double getDouble(int ordinal) {
            throw new UnsupportedOperationException("not yet implemented - test only");
        }

        @Override
        public String getString(int ordinal) {
            return (String) parsedValues[ordinal];
        }

        @Override
        public BigDecimal getDecimal(int ordinal) {
            throw new UnsupportedOperationException("not yet implemented - test only");
        }

        @Override
        public byte[] getBinary(int ordinal) {
            throw new UnsupportedOperationException("not yet implemented - test only");
        }

        @Override
        public Row getStruct(int ordinal) {
            return (TestJsonRow) parsedValues[ordinal];
        }

        @Override
        public ArrayValue getArray(int ordinal) {
            return (ArrayValue) parsedValues[ordinal];
        }

        @Override
        public MapValue getMap(int ordinal) {
            return (MapValue) parsedValues[ordinal];
        }
    }

    private static class TestColumnVector implements ColumnVector {

        private final DataType dataType;
        private final List<Object> values;

        TestColumnVector(DataType dataType, List<Object> values) {
            this.dataType = dataType;
            this.values = values;
        }

        @Override
        public DataType getDataType() {
            return dataType;
        }

        @Override
        public int getSize() {
            return values.size();
        }

        @Override
        public void close() {

        }

        @Override
        public boolean isNullAt(int rowId) {
            return values.get(rowId) == null;
        }

        @Override
        public boolean getBoolean(int rowId) {
            return (boolean) values.get(rowId);
        }

        @Override
        public byte getByte(int rowId) {
            throw new UnsupportedOperationException("not yet implemented - test only");
        }

        @Override
        public short getShort(int rowId) {
            throw new UnsupportedOperationException("not yet implemented - test only");
        }

        @Override
        public int getInt(int rowId) {
            return (int) values.get(rowId);
        }

        @Override
        public long getLong(int rowId) {
            return (long) values.get(rowId);
        }

        @Override
        public float getFloat(int rowId) {
            throw new UnsupportedOperationException("not yet implemented - test only");
        }

        @Override
        public double getDouble(int rowId) {
            throw new UnsupportedOperationException("not yet implemented - test only");
        }

        @Override
        public String getString(int rowId) {
            return (String) values.get(rowId);
        }

        @Override
        public BigDecimal getDecimal(int rowId) {
            throw new UnsupportedOperationException("not yet implemented - test only");
        }

        @Override
        public byte[] getBinary(int rowId) {
            throw new UnsupportedOperationException("not yet implemented - test only");
        }

        @Override
        public Row getStruct(int rowId) {
            return (Row) values.get(rowId);
        }

        @Override
        public ArrayValue getArray(int rowId) {
            return (ArrayValue) values.get(rowId);
        }

        @Override
        public MapValue getMap(int rowId) {
            return (MapValue) values.get(rowId);
        }
    }
}
