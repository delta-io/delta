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
package io.delta.kernel.statistics;

import static io.delta.kernel.internal.DeltaErrors.unsupportedStatsDataType;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import io.delta.kernel.exceptions.KernelException;
import io.delta.kernel.expressions.Column;
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.internal.DeltaErrors;
import io.delta.kernel.internal.skipping.StatsSchemaHelper;
import io.delta.kernel.internal.util.JsonUtils;
import io.delta.kernel.types.*;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Encapsulates statistics for a data file in a Delta Lake table and provides methods to serialize
 * those stats to JSON with basic physical-type validation. Note that connectors (e.g. Spark, Flink)
 * are responsible for ensuring the correctness of collected stats, including any necessary string
 * truncation, prior to constructing this class.
 */
public class DataFileStatistics {
  public static final DateTimeFormatter TIMESTAMP_FORMATTER =
      DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSX");
  public static final OffsetDateTime EPOCH = Instant.ofEpochSecond(0).atOffset(ZoneOffset.UTC);

  private final long numRecords;
  private final Map<Column, Literal> minValues;
  private final Map<Column, Literal> maxValues;
  private final Map<Column, Long> nullCount;
  private final Map<Column, Boolean> tightBounds;

  /**
   * Create a new instance of {@link DataFileStatistics}. The minValues, maxValues, nullCount and
   * tightBounds are all required fields. This class is primarily used to serialize stats to JSON
   * with type checking when constructing file actions and NOT used during data skipping. As such
   * the column names in minValues, maxValues and nullCount should be that of the physical data
   * schema that's reflected in the parquet files and NOT logical schema.
   *
   * @param numRecords Number of records in the data file.
   * @param minValues Map of column to minimum value of it in the data file. If the data file has
   *     all nulls for the column, the value will be null or not present in the map.
   * @param maxValues Map of column to maximum value of it in the data file. If the data file has
   *     all nulls for the column, the value will be null or not present in the map.
   * @param nullCount Map of column to number of nulls in the data file.
   * @param tightBounds Map of column to boolean indicating if min/max bounds are tight (accurate).
   */
  public DataFileStatistics(
      long numRecords,
      Map<Column, Literal> minValues,
      Map<Column, Literal> maxValues,
      Map<Column, Long> nullCount,
      Map<Column, Boolean> tightBounds) {
    Objects.requireNonNull(minValues, "minValues must not be null to serialize stats.");
    Objects.requireNonNull(maxValues, "maxValues must not be null to serialize stats.");
    Objects.requireNonNull(nullCount, "nullCount must not be null to serialize stats.");
    Objects.requireNonNull(tightBounds, "tightBounds must not be null to serialize stats.");

    this.numRecords = numRecords;
    this.minValues = Collections.unmodifiableMap(minValues);
    this.maxValues = Collections.unmodifiableMap(maxValues);
    this.nullCount = Collections.unmodifiableMap(nullCount);
    this.tightBounds = Collections.unmodifiableMap(tightBounds);
    ;
  }

  /**
   * Create a new instance of {@link DataFileStatistics} without tight bounds. This constructor is
   * for backward compatibility. Tight bounds will be empty.
   *
   * @param numRecords Number of records in the data file.
   * @param minValues Map of column to minimum value of it in the data file.
   * @param maxValues Map of column to maximum value of it in the data file.
   * @param nullCount Map of column to number of nulls in the data file.
   */
  public DataFileStatistics(
      long numRecords,
      Map<Column, Literal> minValues,
      Map<Column, Literal> maxValues,
      Map<Column, Long> nullCount) {
    this(numRecords, minValues, maxValues, nullCount, Collections.emptyMap());
  }

  /**
   * Utility method to deserialize statistics from a JSON string. NOTE: Currently, this method only
   * deserializes the numRecords field and ignores other statistics (minValues, maxValues,
   * nullCount). Full deserialization support for all statistics will be added in a future update.
   *
   * @param json Data statistics JSON string to deserialize.
   * @return An {@link Optional} containing the deserialized {@link DataFileStatistics} if present.
   * @throws KernelException if JSON parsing fails
   */
  public static Optional<DataFileStatistics> deserializeFromJson(String json) {
    JsonNode root;
    try {
      root = JsonUtils.mapper().readTree(json);
    } catch (IOException e) {
      throw new KernelException(String.format("Failed to parse JSON string: %s", json), e);
    }

    JsonNode numRecordsNode = root.get(StatsSchemaHelper.NUM_RECORDS);
    if (numRecordsNode == null || !numRecordsNode.isNumber()) {
      return Optional.empty();
    }

    long numRecords = numRecordsNode.asLong();
    return Optional.of(
        new DataFileStatistics(
            numRecords,
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap()));
  }

  /**
   * Utility method to deserialize statistics from a JSON string with full type information. This
   * overloaded version uses the provided schema to correctly parse min/max values and null counts
   * with their appropriate data types.
   *
   * @param json Data statistics JSON string to deserialize.
   * @param physicalSchema The physical schema providing type information for columns. Must match
   *     the schema used during serialization.
   * @return An {@link Optional} containing the deserialized {@link DataFileStatistics} if present.
   * @throws KernelException if JSON parsing fails or if values don't match expected types
   */
  public static Optional<DataFileStatistics> deserializeFromJson(
      String json, StructType physicalSchema) {
    JsonNode root;
    try {
      root = JsonUtils.mapper().readTree(json);
    } catch (IOException e) {
      throw new KernelException(String.format("Failed to parse JSON string: %s", json), e);
    }

    // Parse numRecords
    JsonNode numRecordsNode = root.get(StatsSchemaHelper.NUM_RECORDS);
    if (numRecordsNode == null || !numRecordsNode.isNumber()) {
      return Optional.empty();
    }
    long numRecords = numRecordsNode.asLong();

    // Parse minValues
    Map<Column, Literal> minValues = new HashMap<>();
    JsonNode minNode = root.get(StatsSchemaHelper.MIN);
    if (minNode != null && minNode.isObject() && physicalSchema != null) {
      parseMinMaxValues(minNode, minValues, new Column(new String[0]), physicalSchema);
    }

    // Parse maxValues
    Map<Column, Literal> maxValues = new HashMap<>();
    JsonNode maxNode = root.get(StatsSchemaHelper.MAX);
    if (maxNode != null && maxNode.isObject() && physicalSchema != null) {
      parseMinMaxValues(maxNode, maxValues, new Column(new String[0]), physicalSchema);
    }

    // Parse nullCount
    Map<Column, Long> nullCount = new HashMap<>();
    JsonNode nullCountNode = root.get(StatsSchemaHelper.NULL_COUNT);
    if (nullCountNode != null && nullCountNode.isObject() && physicalSchema != null) {
      parseNullCounts(nullCountNode, nullCount, new Column(new String[0]), physicalSchema);
    }

    // Parse tightBounds
    Map<Column, Boolean> tightBounds = new HashMap<>();
    JsonNode tightBoundsNode = root.get(StatsSchemaHelper.TIGHT_BOUNDS);
    if (tightBoundsNode != null && tightBoundsNode.isObject() && physicalSchema != null) {
      parseTightBounds(tightBoundsNode, tightBounds, new Column(new String[0]), physicalSchema);
    }

    return Optional.of(
        new DataFileStatistics(numRecords, minValues, maxValues, nullCount, tightBounds));
  }

  /**
   * Get the number of records in the data file.
   *
   * @return Number of records in the data file.
   */
  public long getNumRecords() {
    return numRecords;
  }

  /**
   * Get the minimum values of the columns in the data file. The map may contain statistics for only
   * a subset of columns in the data file.
   *
   * @return Map of column to minimum value of it in the data file.
   */
  public Map<Column, Literal> getMinValues() {
    return minValues;
  }

  /**
   * Get the maximum values of the columns in the data file. The map may contain statistics for only
   * a subset of columns in the data file.
   *
   * @return Map of column to minimum value of it in the data file.
   */
  public Map<Column, Literal> getMaxValues() {
    return maxValues;
  }

  /**
   * Get the number of nulls of columns in the data file. The map may contain statistics for only a
   * subset of columns in the data file.
   *
   * @return Map of column to number of nulls in the data file.
   */
  public Map<Column, Long> getNullCount() {
    return nullCount;
  }

  /**
   * Get the tight bounds information for columns in the data file. Tight bounds indicate whether
   * the min/max values for each column are guaranteed to be accurate bounds for the data.
   *
   * @return An unmodifiable map of column to tight bounds boolean value. Returns an empty map if no
   *     tight bounds information is available.
   */
  public Map<Column, Boolean> getTightBounds() {
    return tightBounds;
  }
  /**
   * Serializes the statistics as a JSON string.
   *
   * <p>Example: For nested column structures:
   *
   * <pre>
   * Input:
   *   minValues = {
   *     new Column(new String[]{"a", "b", "c"}) mapped to Literal.ofInt(10),
   *     new Column("d") mapped to Literal.ofString("value")
   *   }
   *
   * Output JSON:
   *   {
   *     "minValues": {
   *       "a": {
   *         "b": {
   *           "c": 10
   *         }
   *       },
   *       "d": "value"
   *     }
   *   }
   * </pre>
   *
   * @param physicalSchema the optional physical schema. If provided, all min/max values and null
   *     counts will be included and validated against their physical types. If null, only
   *     numRecords will be serialized without validation.
   * @return a JSON representation of the statistics.
   * @throws KernelException if dataSchema is provided and there's a type mismatch between the
   *     Literal values and the expected types in the schema, or if an unsupported data type is
   *     found.
   */
  public String serializeAsJson(StructType physicalSchema) {
    return JsonUtils.generate(
        gen -> {
          gen.writeStartObject();
          gen.writeNumberField(StatsSchemaHelper.NUM_RECORDS, numRecords);

          if (physicalSchema != null) {
            gen.writeObjectFieldStart(StatsSchemaHelper.MIN);
            writeJsonValues(
                gen,
                physicalSchema,
                minValues,
                new Column(new String[0]),
                (g, v) -> writeJsonValue(g, v));
            gen.writeEndObject();

            gen.writeObjectFieldStart(StatsSchemaHelper.MAX);
            writeJsonValues(
                gen,
                physicalSchema,
                maxValues,
                new Column(new String[0]),
                (g, v) -> writeJsonValue(g, v));
            gen.writeEndObject();

            gen.writeObjectFieldStart(StatsSchemaHelper.NULL_COUNT);
            writeJsonValues(
                gen,
                physicalSchema,
                nullCount,
                new Column(new String[0]),
                (g, v) -> g.writeNumber(v));
            gen.writeEndObject();

            gen.writeObjectFieldStart(StatsSchemaHelper.TIGHT_BOUNDS);
            writeJsonValues(
                gen,
                physicalSchema,
                tightBounds,
                new Column(new String[0]),
                (g, v) -> g.writeBoolean(v));
            gen.writeEndObject();
          }

          gen.writeEndObject();
        });
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof DataFileStatistics)) {
      return false;
    }
    DataFileStatistics that = (DataFileStatistics) o;
    return numRecords == that.numRecords
        && Objects.equals(minValues, that.minValues)
        && Objects.equals(maxValues, that.maxValues)
        && Objects.equals(nullCount, that.nullCount)
        && Objects.equals(tightBounds, that.tightBounds);
  }

  @Override
  public int hashCode() {
    int result = Long.hashCode(numRecords);
    result = 31 * result + Objects.hash(minValues.keySet());
    result = 31 * result + Objects.hash(maxValues.keySet());
    result = 31 * result + Objects.hash(nullCount.keySet());
    result = 31 * result + Objects.hash(tightBounds.keySet());
    return result;
  }

  @Override
  public String toString() {
    return String.format(
        "DataFileStatistics(numRecords=%s, minValues=%s, maxValues=%s," +
                "nullCount=%s, tightBounds=%s)",
        numRecords, minValues, maxValues, nullCount, tightBounds);
  }

  /////////////////////////////////////////////////////////////////////////////////
  /// Private methods                                                           ///
  /////////////////////////////////////////////////////////////////////////////////

  private <T> void writeJsonValues(
      JsonGenerator generator,
      StructType schema,
      Map<Column, T> values,
      Column parentCol,
      JsonUtils.JsonValueWriter<T> writer)
      throws IOException {
    if (schema == null) {
      return;
    }
    for (StructField field : schema.fields()) {
      Column colPath = parentCol.appendNestedField(field.getName());
      if (field.getDataType() instanceof StructType) {
        generator.writeObjectFieldStart(field.getName());
        writeJsonValues(generator, (StructType) field.getDataType(), values, colPath, writer);
        generator.writeEndObject();
      } else {
        T value = values.get(colPath);
        if (value != null) {
          if (value instanceof Literal) {
            validateLiteralType(field, (Literal) value);
          }
          generator.writeFieldName(field.getName());
          writer.write(generator, value);
        }
      }
    }
  }

  /**
   * Validates that the literal's data type matches the expected field type.
   *
   * @param field The schema field with the expected data type
   * @param literal The literal to validate
   * @throws KernelException if the data types don't match
   */
  private void validateLiteralType(StructField field, Literal literal) {
    if (literal.getDataType() == null || !literal.getDataType().equals(field.getDataType())) {
      throw DeltaErrors.statsTypeMismatch(
          field.getName(), field.getDataType(), literal.getDataType());
    }
  }

  private void writeJsonValue(JsonGenerator generator, Literal literal) throws IOException {
    if (literal == null || literal.getValue() == null) {
      generator.writeNull();
      return;
    }
    DataType type = literal.getDataType();
    Object value = literal.getValue();
    if (type instanceof BooleanType) {
      generator.writeBoolean((Boolean) value);
    } else if (type instanceof ByteType) {
      generator.writeNumber(((Number) value).byteValue());
    } else if (type instanceof ShortType) {
      generator.writeNumber(((Number) value).shortValue());
    } else if (type instanceof IntegerType) {
      generator.writeNumber(((Number) value).intValue());
    } else if (type instanceof LongType) {
      generator.writeNumber(((Number) value).longValue());
    } else if (type instanceof FloatType) {
      float f = ((Number) value).floatValue();
      if (Float.isNaN(f) || Float.isInfinite(f)) {
        generator.writeString(String.valueOf(f));
      } else {
        generator.writeNumber(f);
      }
    } else if (type instanceof DoubleType) {
      double d = ((Number) value).doubleValue();
      if (Double.isNaN(d) || Double.isInfinite(d)) {
        generator.writeString(String.valueOf(d));
      } else {
        generator.writeNumber(d);
      }
    } else if (type instanceof StringType) {
      generator.writeString((String) value);
    } else if (type instanceof BinaryType) {
      generator.writeString(new String((byte[]) value, StandardCharsets.UTF_8));
    } else if (type instanceof DecimalType) {
      generator.writeNumber((BigDecimal) value);
    } else if (type instanceof DateType) {
      generator.writeString(
          LocalDate.ofEpochDay(((Number) value).longValue()).format(ISO_LOCAL_DATE));
    } else if (type instanceof TimestampType) {
      long epochMicros = (long) value;
      LocalDateTime localDateTime = ChronoUnit.MICROS.addTo(EPOCH, epochMicros).toLocalDateTime();
      LocalDateTime truncated = localDateTime.truncatedTo(ChronoUnit.MILLIS);
      generator.writeString(TIMESTAMP_FORMATTER.format(truncated.atOffset(ZoneOffset.UTC)));
    } else if (type instanceof TimestampNTZType) {
      long epochMicros = (long) value;
      LocalDateTime localDateTime = ChronoUnit.MICROS.addTo(EPOCH, epochMicros).toLocalDateTime();
      LocalDateTime truncated = localDateTime.truncatedTo(ChronoUnit.MILLIS);
      generator.writeString(truncated.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
    } else {
      throw unsupportedStatsDataType(type);
    }
  }
  /**
   * Helper method to recursively parse nested JSON values back into Column->Literal maps for
   * min/max values using the schema for type information. This is the inverse of the
   * writeJsonValues method in DataFileStatistics.serializeAsJson.
   *
   * <p>Example JSON structure being parsed:
   *
   * <pre>
   * {
   *   "simpleColumn": 42,
   *   "nestedColumn": {
   *     "field1": "value1",
   *     "field2": {
   *       "subfield": 10.5
   *     }
   *   }
   * }
   * </pre>
   *
   * <p>This would create Column entries:
   *
   * <ul>
   *   <li>Column(["simpleColumn"]) → Literal.ofInt(42)
   *   <li>Column(["nestedColumn", "field1"]) → Literal.ofString("value1")
   *   <li>Column(["nestedColumn", "field2", "subfield"]) → Literal.ofDouble(10.5)
   * </ul>
   *
   * @param node The JSON node to parse
   * @param resultMap The map to populate with Column->Literal mappings
   * @param currentColumn The current column path being built
   * @param schema The schema for the current level
   */
  private static void parseMinMaxValues(
      JsonNode node, Map<Column, Literal> resultMap, Column currentColumn, StructType schema) {
    if (node == null || !node.isObject() || schema == null) {
      return;
    }

    for (StructField field : schema.fields()) {
      String fieldName = field.getName();
      JsonNode valueNode = node.get(fieldName);

      if (valueNode == null) {
        // Field not present in JSON, skip
        continue;
      }

      Column newColumn = currentColumn.appendNestedField(fieldName);
      DataType fieldType = field.getDataType();

      if (fieldType instanceof StructType) {
        // This is a nested structure, recurse deeper
        if (valueNode.isObject()) {
          parseMinMaxValues(valueNode, resultMap, newColumn, (StructType) fieldType);
        }
      } else {
        // This is a leaf value
        Literal literal = parseJsonValueToLiteral(valueNode, fieldType);
        if (literal != null) {
          resultMap.put(newColumn, literal);
        }
      }
    }
  }

  /**
   * Helper method to recursively parse nested JSON null count values back into Column->Long maps
   * using the schema for type information.
   *
   * <p>Example JSON structure being parsed:
   *
   * <pre>
   * {
   *   "simpleColumn": 5,
   *   "nestedColumn": {
   *     "field1": 0,
   *     "field2": {
   *       "subfield": 10
   *     }
   *   }
   * }
   * </pre>
   *
   * <p>This would create Column entries:
   *
   * <ul>
   *   <li>Column(["simpleColumn"]) → 5L
   *   <li>Column(["nestedColumn", "field1"]) → 0L
   *   <li>Column(["nestedColumn", "field2", "subfield"]) → 10L
   * </ul>
   *
   * @param node The JSON node to parse
   * @param resultMap The map to populate with Column->Long mappings
   * @param currentColumn The current column path being built
   * @param schema The schema for the current level
   */
  private static void parseNullCounts(
      JsonNode node, Map<Column, Long> resultMap, Column currentColumn, StructType schema) {
    if (node == null || !node.isObject() || schema == null) {
      return;
    }

    for (StructField field : schema.fields()) {
      String fieldName = field.getName();
      JsonNode valueNode = node.get(fieldName);

      if (valueNode == null) {
        // Field not present in JSON, skip
        continue;
      }

      Column newColumn = currentColumn.appendNestedField(fieldName);
      DataType fieldType = field.getDataType();

      if (fieldType instanceof StructType) {
        // This is a nested structure, recurse deeper
        if (valueNode.isObject()) {
          parseNullCounts(valueNode, resultMap, newColumn, (StructType) fieldType);
        }
      } else {
        // This is a leaf value - parse as long for null count
        if (valueNode.isNumber()) {
          resultMap.put(newColumn, valueNode.asLong());
        }
      }
    }
  }

  /**
   * Helper method to convert JSON node value to Literal based on the expected data type from
   * schema. Uses the schema type information to eliminate ambiguity when parsing JSON values.
   *
   * @param valueNode The JSON node containing the value
   * @param dataType The expected data type from the schema
   * @return The corresponding Literal, or null if the value is null
   * @throws KernelException if the JSON value cannot be parsed as the expected type
   */
  private static Literal parseJsonValueToLiteral(JsonNode valueNode, DataType dataType) {
    if (valueNode == null || valueNode.isNull()) {
      return null;
    }

    try {
      if (dataType instanceof BooleanType) {
        if (!valueNode.isBoolean()) {
          throw new KernelException(
              String.format("Expected boolean value but got: %s", valueNode.toString()));
        }
        return Literal.ofBoolean(valueNode.asBoolean());

      } else if (dataType instanceof ByteType) {
        if (!valueNode.isNumber()) {
          throw new KernelException(
              String.format("Expected byte value but got: %s", valueNode.toString()));
        }
        return Literal.ofByte((byte) valueNode.asInt());

      } else if (dataType instanceof ShortType) {
        if (!valueNode.isNumber()) {
          throw new KernelException(
              String.format("Expected short value but got: %s", valueNode.toString()));
        }
        return Literal.ofShort(valueNode.shortValue());

      } else if (dataType instanceof IntegerType) {
        if (!valueNode.isNumber()) {
          throw new KernelException(
              String.format("Expected integer value but got: %s", valueNode.toString()));
        }
        return Literal.ofInt(valueNode.asInt());

      } else if (dataType instanceof LongType) {
        if (!valueNode.isNumber()) {
          throw new KernelException(
              String.format("Expected long value but got: %s", valueNode.toString()));
        }
        return Literal.ofLong(valueNode.asLong());

      } else if (dataType instanceof FloatType) {
        if (valueNode.isTextual()) {
          // Special float values are stored as strings during serialization
          String textValue = valueNode.asText();
          switch (textValue) {
            case "NaN":
              return Literal.ofFloat(Float.NaN);
            case "Infinity":
              return Literal.ofFloat(Float.POSITIVE_INFINITY);
            case "-Infinity":
              return Literal.ofFloat(Float.NEGATIVE_INFINITY);
            default:
              throw new KernelException(
                  String.format("Expected float value but got unexpected string: %s", textValue));
          }
        }
        if (!valueNode.isNumber()) {
          throw new KernelException(
              String.format("Expected float value but got: %s", valueNode.toString()));
        }
        return Literal.ofFloat(valueNode.floatValue());

      } else if (dataType instanceof DoubleType) {
        if (valueNode.isTextual()) {
          // Special double values are stored as strings during serialization
          String textValue = valueNode.asText();
          switch (textValue) {
            case "NaN":
              return Literal.ofDouble(Double.NaN);
            case "Infinity":
              return Literal.ofDouble(Double.POSITIVE_INFINITY);
            case "-Infinity":
              return Literal.ofDouble(Double.NEGATIVE_INFINITY);
            default:
              throw new KernelException(
                  String.format("Expected double value but got unexpected string: %s", textValue));
          }
        }
        if (!valueNode.isNumber()) {
          throw new KernelException(
              String.format("Expected double value but got: %s", valueNode.toString()));
        }
        return Literal.ofDouble(valueNode.asDouble());

      } else if (dataType instanceof StringType) {
        if (!valueNode.isTextual()) {
          throw new KernelException(
              String.format("Expected string value but got: %s", valueNode.toString()));
        }
        return Literal.ofString(valueNode.asText());

      } else if (dataType instanceof BinaryType) {
        if (!valueNode.isTextual()) {
          throw new KernelException(
              String.format("Expected binary (as string) value but got: %s", valueNode.toString()));
        }
        // Binary data was stored as UTF-8 string during serialization
        return Literal.ofBinary(valueNode.asText().getBytes(StandardCharsets.UTF_8));

      } else if (dataType instanceof DecimalType) {
        if (!valueNode.isNumber()) {
          throw new KernelException(
              String.format("Expected decimal value but got: %s", valueNode.toString()));
        }
        DecimalType decimalType = (DecimalType) dataType;
        BigDecimal decimal = valueNode.decimalValue();
        return Literal.ofDecimal(decimal, decimalType.getPrecision(), decimalType.getScale());

      } else if (dataType instanceof DateType) {
        if (!valueNode.isTextual()) {
          throw new KernelException(
              String.format("Expected date (as string) value but got: %s", valueNode.toString()));
        }
        String textValue = valueNode.asText();
        LocalDate date = LocalDate.parse(textValue, ISO_LOCAL_DATE);
        return Literal.ofDate((int) date.toEpochDay());

      } else if (dataType instanceof TimestampType) {
        if (!valueNode.isTextual()) {
          throw new KernelException(
              String.format(
                  "Expected timestamp (as string) value but got: %s", valueNode.toString()));
        }
        String textValue = valueNode.asText();
        OffsetDateTime offsetDateTime =
            OffsetDateTime.parse(textValue, DataFileStatistics.TIMESTAMP_FORMATTER);
        long epochMicros = ChronoUnit.MICROS.between(DataFileStatistics.EPOCH, offsetDateTime);
        return Literal.ofTimestamp(epochMicros);

      } else if (dataType instanceof TimestampNTZType) {
        if (!valueNode.isTextual()) {
          throw new KernelException(
              String.format(
                  "Expected timestamp NTZ (as string) value but got: %s", valueNode.toString()));
        }
        String textValue = valueNode.asText();
        LocalDateTime localDateTime =
            LocalDateTime.parse(textValue, DateTimeFormatter.ISO_LOCAL_DATE_TIME);
        long epochMicros =
            ChronoUnit.MICROS.between(DataFileStatistics.EPOCH.toLocalDateTime(), localDateTime);
        return Literal.ofTimestampNtz(epochMicros);

      } else {
        throw unsupportedStatsDataType(dataType);
      }
    } catch (Exception e) {
      if (e instanceof KernelException) {
        throw (KernelException) e;
      }
      throw new KernelException(
          String.format(
              "Failed to parse value '%s' as %s", valueNode.toString(), dataType.toString()),
          e);
    }
  }

  /**
   * Helper method to recursively parse nested JSON tight bounds values back into Column->Boolean
   * maps using the schema for type information. Tight bounds indicate whether the min/max values
   * are guaranteed to be accurate bounds. A value of true means the bounds are tight (accurate),
   * false means they may not be accurate (e.g., due to deletions that haven't been compacted yet).
   *
   * <p>Example JSON structure being parsed:
   *
   * <pre>
   * {
   *   "simpleColumn": true,
   *   "nestedColumn": {
   *     "field1": false,
   *     "field2": {
   *       "subfield": true
   *     }
   *   }
   * }
   * </pre>
   *
   * <p>This would create Column entries:
   *
   * <ul>
   *   <li>Column(["simpleColumn"]) → true
   *   <li>Column(["nestedColumn", "field1"]) → false
   *   <li>Column(["nestedColumn", "field2", "subfield"]) → true
   * </ul>
   *
   * @param node The JSON node to parse
   * @param resultMap The map to populate with Column->Boolean mappings
   * @param currentColumn The current column path being built
   * @param schema The schema for the current level
   */
  private static void parseTightBounds(
      JsonNode node, Map<Column, Boolean> resultMap, Column currentColumn, StructType schema) {
    if (node == null || !node.isObject() || schema == null) {
      return;
    }

    for (StructField field : schema.fields()) {
      String fieldName = field.getName();
      JsonNode valueNode = node.get(fieldName);

      if (valueNode == null) {
        // Field not present in JSON, skip
        continue;
      }

      Column newColumn = currentColumn.appendNestedField(fieldName);
      DataType fieldType = field.getDataType();

      if (fieldType instanceof StructType) {
        // This is a nested structure, recurse deeper
        if (valueNode.isObject()) {
          parseTightBounds(valueNode, resultMap, newColumn, (StructType) fieldType);
        }
      } else {
        // This is a leaf value - parse as boolean for tight bounds
        if (valueNode.isBoolean()) {
          resultMap.put(newColumn, valueNode.asBoolean());
        }
      }
    }
  }
}
