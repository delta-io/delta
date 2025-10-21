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
import java.util.*;

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
  private final Optional<Boolean> tightBounds;

  /**
   * Create a new instance of {@link DataFileStatistics}. The minValues, maxValues, and nullCount
   * are required fields. The tightBounds field is optional - pass Optional.empty() if not
   * specified, Optional.of(true) or Optional.of(false) for explicit values.
   *
   * @param numRecords Number of records in the data file.
   * @param minValues Map of column to minimum value of it in the data file. If the data file has
   *     all nulls for the column, the value will be null or not present in the map.
   * @param maxValues Map of column to maximum value of it in the data file. If the data file has
   *     all nulls for the column, the value will be null or not present in the map.
   * @param nullCount Map of column to number of nulls in the data file.
   * @param tightBounds Optional boolean indicating if bounds are tight (accurate). Pass
   *     Optional.empty() if not specified.
   */
  public DataFileStatistics(
      long numRecords,
      Map<Column, Literal> minValues,
      Map<Column, Literal> maxValues,
      Map<Column, Long> nullCount,
      Optional<Boolean> tightBounds) {
    Objects.requireNonNull(minValues, "minValues must not be null to serialize stats.");
    Objects.requireNonNull(maxValues, "maxValues must not be null to serialize stats.");
    Objects.requireNonNull(nullCount, "nullCount must not be null to serialize stats.");

    this.numRecords = numRecords;
    this.minValues = Collections.unmodifiableMap(minValues);
    this.maxValues = Collections.unmodifiableMap(maxValues);
    this.nullCount = Collections.unmodifiableMap(nullCount);
    this.tightBounds = tightBounds;
  }

  /**
   * Utility method to extract only the numRecords field from a statistics JSON string.
   *
   * @param json Data statistics JSON string to deserialize.
   * @return An {@link Optional} containing the numRecords value if present.
   * @throws KernelException if JSON parsing fails
   */
  public static Optional<Long> getNumRecords(String json) {
    // Delegate to the full deserialization method with null schema to only parse numRecords
    Optional<DataFileStatistics> stats = deserializeFromJson(json, null);
    return stats.map(DataFileStatistics::getNumRecords);
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
    // Check if schema is null or empty
    if (physicalSchema == null || physicalSchema.fields().isEmpty()) {
      // Return statistics with only numRecords
      return Optional.of(
          new DataFileStatistics(
              numRecords, new HashMap<>(), new HashMap<>(), new HashMap<>(), Optional.empty()));
    }
    // Parse minValues
    Map<Column, Literal> minValues = new HashMap<>();
    JsonNode minNode = root.get(StatsSchemaHelper.MIN);
    if (minNode != null && minNode.isObject()) {
      parseMinMaxValues(minNode, minValues, new Column(new String[0]), physicalSchema);
    }

    // Parse maxValues
    Map<Column, Literal> maxValues = new HashMap<>();
    JsonNode maxNode = root.get(StatsSchemaHelper.MAX);
    if (maxNode != null && maxNode.isObject()) {
      parseMinMaxValues(maxNode, maxValues, new Column(new String[0]), physicalSchema);
    }

    // Parse nullCount
    Map<Column, Long> nullCount = new HashMap<>();
    JsonNode nullCountNode = root.get(StatsSchemaHelper.NULL_COUNT);
    if (nullCountNode != null && nullCountNode.isObject()) {
      parseNullCounts(nullCountNode, nullCount, new Column(new String[0]), physicalSchema);
    }

    // Parse tightBounds
    Optional<Boolean> tightBounds = Optional.empty();
    JsonNode tightBoundsNode = root.get(StatsSchemaHelper.TIGHT_BOUNDS);
    if (tightBoundsNode != null && tightBoundsNode.isBoolean()) {
      tightBounds = Optional.of(tightBoundsNode.asBoolean());
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
   * Get the tight bounds information for the data file. Tight bounds indicate whether the values
   * are guaranteed to be accurate bounds for the data.
   *
   * @return The tight bounds boolean value.
   */
  public Optional<Boolean> getTightBounds() {
    return tightBounds;
  }

  /**
   * Returns a new DataFileStatistics instance with tightBounds set to false. This is useful when
   * the statistics bounds are no longer guaranteed to be tight, such as after applying deletion
   * vectors.
   *
   * @return A new DataFileStatistics with tightBounds set to false
   */
  public DataFileStatistics withoutTightBounds() {
    return new DataFileStatistics(
        this.numRecords, this.minValues, this.maxValues, this.nullCount, Optional.of(false));
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

            if (tightBounds.isPresent()) {
              gen.writeBooleanField(StatsSchemaHelper.TIGHT_BOUNDS, tightBounds.get());
            }
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
    result = 31 * result + Objects.hash(tightBounds);
    return result;
  }

  @Override
  public String toString() {
    return String.format(
        "DataFileStatistics(numRecords=%s, minValues=%s, maxValues=%s,"
            + "nullCount=%s, tightBounds=%s)",
        numRecords,
        minValues,
        maxValues,
        nullCount,
        tightBounds.map(Object::toString).orElse("empty"));
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
    // Variant stats in JSON are Z85 encoded strings, all other stats should match the field type
    DataType expectedLiteralType =
        field.getDataType() instanceof VariantType ? StringType.STRING : field.getDataType();
    if (literal.getDataType() == null || !literal.getDataType().equals(expectedLiteralType)) {
      throw DeltaErrors.statsTypeMismatch(
          field.getName(), expectedLiteralType, literal.getDataType());
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
        Literal literal = JsonUtils.parseJsonValueToLiteral(valueNode, fieldType);
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
}
