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

import static io.delta.kernel.internal.util.Preconditions.checkArgument;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import io.delta.kernel.data.*;
import io.delta.kernel.defaults.internal.data.DefaultJsonRow;
import io.delta.kernel.defaults.internal.data.DefaultRowBasedColumnarBatch;
import io.delta.kernel.defaults.internal.data.vector.DefaultBooleanVector;
import io.delta.kernel.types.*;
import io.delta.kernel.utils.CloseableIterator;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Utilities method to serialize and deserialize {@link Row} objects with a limited set of data type
 * values.
 *
 * <p>Following are the supported data types: {@code boolean}, {@code byte}, {@code short}, {@code
 * int}, {@code long}, {@code float}, {@code double}, {@code string}, {@code StructType} (containing
 * any of the supported subtypes), {@code ArrayType}, {@code MapType} (only a map with string keys
 * is supported).
 *
 * <p>At a high-level, the JSON serialization is similar to that of Jackson's {@link ObjectMapper}.
 */
public class JsonUtils {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  static {
    OBJECT_MAPPER.registerModule(new SimpleModule().addSerializer(Row.class, new RowSerializer()));
  }

  private JsonUtils() {}

  /**
   * Converts a {@link Row} to a single line JSON string. This is currently used just in tests. Wll
   * be used as part of the refactoring planned in <a
   * href="https://github.com/delta-io/delta/issues/2929">#2929</a>
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
   * @param json JSON string
   * @param schema to read the JSON according the schema
   * @return {@link Row} instance with given schema.
   */
  public static Row rowFromJson(String json, StructType schema) {
    try {
      final JsonNode jsonNode = OBJECT_MAPPER.readTree(json);
      return new DefaultJsonRow((ObjectNode) jsonNode, schema);
    } catch (JsonProcessingException ex) {
      throw new RuntimeException(String.format("Could not parse JSON: %s", json), ex);
    }
  }

  public static String columnarBatchToJSON(ColumnarBatch batch) {
    List<String> jsonRows = new ArrayList<>();
    try (CloseableIterator<Row> allRows = batch.getRows()) {
      while (allRows.hasNext()) {
        Row row = allRows.next();
        jsonRows.add(rowToJson(row));
      }
      return "[" + String.join(",", jsonRows) + "]";
    } catch (Exception ex) {
      throw new RuntimeException("Could not serialize columnar batch to JSON", ex);
    }
  }

  public static ColumnarBatch columnarBatchFromJson(String json, StructType schema) {
    try {
      JsonNode rootNode = OBJECT_MAPPER.readTree(json);
      if (!rootNode.isArray()) {
        throw new IllegalArgumentException("Input JSON must be an array of objects.");
      }

      List<Row> rows = new ArrayList<>();
      for (JsonNode element : rootNode) {
        if (!element.isObject()) {
          throw new IllegalArgumentException("Each element in JSON array must be a JSON object.");
        }
        rows.add(new DefaultJsonRow((ObjectNode) element, schema));
      }

      return new DefaultRowBasedColumnarBatch(schema, rows);
    } catch (JsonProcessingException ex) {
      throw new RuntimeException("Failed to parse input JSON", ex);
    }
  }

  public static String filteredColumnarBatchToJson(FilteredColumnarBatch filteredBatch) {
    String batchJson = columnarBatchToJSON(filteredBatch.getData());
    String selectionJson =
        filteredBatch.getSelectionVector().map(JsonUtils::serializeSelectionVector).orElse("null");

    return String.format("{\"columnarBatch\":%s,\"selectionVector\":%s}", batchJson, selectionJson);
  }

  public static FilteredColumnarBatch filteredColumnarBatchFromJson(
      String json, StructType schema) {
    try {
      JsonNode root = OBJECT_MAPPER.readTree(json);

      // Parse the columnarBatch (rows)
      JsonNode batchNode = root.get("columnarBatch");
      if (batchNode == null || !batchNode.isArray()) {
        throw new IllegalArgumentException("Missing or invalid 'columnarBatch' field");
      }
      // Reuse your existing columnarBatchFromJson method by serializing the array back to string
      String batchJson = OBJECT_MAPPER.writeValueAsString(batchNode);
      ColumnarBatch batch = columnarBatchFromJson(batchJson, schema);

      // Parse the selection vector
      JsonNode selNode = root.get("selectionVector");
      Optional<ColumnVector> selectionVector = Optional.empty();
      if (selNode != null && !selNode.isNull()) {
        if (!selNode.isArray()) {
          throw new IllegalArgumentException("'selectionVector' must be an array");
        }
        int size = selNode.size();
        boolean[] values = new boolean[size];
        for (int i = 0; i < size; i++) {
          values[i] = selNode.get(i).asBoolean();
        }
        // Wrap boolean[] in BooleanColumnVector implementation
        selectionVector =
            Optional.of(
                new DefaultBooleanVector(size, Optional.empty() /* nullability */, values) {});
      }

      return new FilteredColumnarBatch(batch, selectionVector);
    } catch (Exception e) {
      throw new RuntimeException("Failed to deserialize FilteredColumnarBatch from JSON", e);
    }
  }

  private static String serializeSelectionVector(ColumnVector vector) {
    if (!vector.getDataType().equals(BooleanType.BOOLEAN)) {
      throw new IllegalArgumentException("Selection vector must be of boolean type");
    }

    int size = vector.getSize();
    StringBuilder sb = new StringBuilder("[");
    for (int i = 0; i < size; i++) {
      sb.append(vector.getBoolean(i));
      if (i < size - 1) {
        sb.append(",");
      }
    }
    sb.append("]");
    return sb.toString();
  }

  // todo: this is better; performance; columnar batch pruning reduce overhead
  // todo: maybe just a list of rows
  public static String filteredColumnarBatchToJson2(FilteredColumnarBatch filteredBatch) {
    List<String> includedJsonRows = new ArrayList<>();
    try (CloseableIterator<Row> rowIterator = filteredBatch.getRows()) {
      while (rowIterator.hasNext()) {
        Row row = rowIterator.next();
        includedJsonRows.add(rowToJson(row));
      }
      String selectionJson = "null"; // all rows should be selected

      return String.format(
          "{\"columnarBatch\":%s,\"selectionVector\":%s}", includedJsonRows, selectionJson);
    } catch (Exception e) {
      throw new RuntimeException("Could not serialize FilteredColumnarBatch to JSON", e);
    }
  }

  // todo: hard code method 3: only select one row

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
        if (arrayElems.isNullAt(i)) {
          // Jackson serializes the null values in the array, but not in the map
          gen.writeNull();
        } else {
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
        gen.writeFieldName(keys.getString(i));
        if (!values.isNullAt(i)) {
          writeValue(gen, values, i, mapType.getValueType());
        } else {
          gen.writeNull();
        }
      }
      gen.writeEndObject();
    }

    private void writeValue(JsonGenerator gen, Row row, int columnOrdinal, DataType type)
        throws IOException {
      checkArgument(!row.isNullAt(columnOrdinal), "value should not be null");
      if (type instanceof BooleanType) {
        gen.writeBoolean(row.getBoolean(columnOrdinal));
      } else if (type instanceof ByteType) {
        gen.writeNumber(row.getByte(columnOrdinal));
      } else if (type instanceof ShortType) {
        gen.writeNumber(row.getShort(columnOrdinal));
      } else if (type instanceof IntegerType) {
        gen.writeNumber(row.getInt(columnOrdinal));
      } else if (type instanceof LongType) {
        gen.writeNumber(row.getLong(columnOrdinal));
      } else if (type instanceof FloatType) {
        gen.writeNumber(row.getFloat(columnOrdinal));
      } else if (type instanceof DoubleType) {
        gen.writeNumber(row.getDouble(columnOrdinal));
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
      checkArgument(!vector.isNullAt(rowId), "value should not be null");
      if (type instanceof BooleanType) {
        gen.writeBoolean(vector.getBoolean(rowId));
      } else if (type instanceof ByteType) {
        gen.writeNumber(vector.getByte(rowId));
      } else if (type instanceof ShortType) {
        gen.writeNumber(vector.getShort(rowId));
      } else if (type instanceof IntegerType) {
        gen.writeNumber(vector.getInt(rowId));
      } else if (type instanceof LongType) {
        gen.writeNumber(vector.getLong(rowId));
      } else if (type instanceof FloatType) {
        gen.writeNumber(vector.getFloat(rowId));
      } else if (type instanceof DoubleType) {
        gen.writeNumber(vector.getDouble(rowId));
      } else if (type instanceof StringType) {
        gen.writeString(vector.getString(rowId));
      } else if (type instanceof StructType) {
        writeStruct(gen, vector, (StructType) type, rowId);
      } else if (type instanceof ArrayType) {
        writeArrayValue(gen, vector.getArray(rowId), (ArrayType) type);
      } else if (type instanceof MapType) {
        writeMapValue(gen, vector.getMap(rowId), (MapType) type);
      } else {
        throw new UnsupportedOperationException("unsupported data type: " + type);
      }
    }
  }

  private static void assertSupportedMapType(MapType keyType) {
    checkArgument(
        keyType.getKeyType() instanceof StringType,
        "Only STRING type keys are supported in MAP type in JSON serialization");
  }
}
