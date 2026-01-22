/*
 * Copyright (2024) The Delta Lake Project Authors.
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

package io.delta.kernel.internal.util;

import static io.delta.kernel.internal.DeltaErrors.unsupportedStatsDataType;
import static io.delta.kernel.statistics.DataFileStatistics.EPOCH;
import static io.delta.kernel.statistics.DataFileStatistics.TIMESTAMP_FORMATTER;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.delta.kernel.exceptions.KernelException;
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.types.*;
import java.io.IOException;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Map;

public class JsonUtils {
  private JsonUtils() {}

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final JsonFactory FACTORY = new JsonFactory();

  public static JsonFactory factory() {
    return FACTORY;
  }

  public static ObjectMapper mapper() {
    return MAPPER;
  }

  @FunctionalInterface
  public interface ToJson {
    void generate(JsonGenerator generator) throws IOException;
  }

  @FunctionalInterface
  public interface JsonValueWriter<T> {
    void write(JsonGenerator generator, T value) throws IOException;
  }

  /**
   * Utility class for writing JSON with a Jackson {@link JsonGenerator}.
   *
   * @param toJson function that produces JSON using a {@link JsonGenerator}
   * @return a JSON string produced from the generator
   */
  public static String generate(ToJson toJson) {
    try (StringWriter writer = new StringWriter();
        JsonGenerator generator = factory().createGenerator(writer)) {
      toJson.generate(generator);
      generator.flush();
      return writer.toString();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Parses the given JSON string into a map of key-value pairs.
   *
   * <p>The JSON string should be in the format:
   *
   * <pre>{@code {"key1": "value1", "key2": "value2", ...}}</pre>
   *
   * where both keys and values are strings.
   *
   * @param jsonString The JSON string to parse
   * @return A map containing the key-value pairs extracted from the JSON string
   */
  public static Map<String, String> parseJSONKeyValueMap(String jsonString) {
    if (jsonString == null || jsonString.trim().isEmpty()) {
      return Collections.emptyMap();
    }
    try {
      return MAPPER.readValue(jsonString, new TypeReference<Map<String, String>>() {});
    } catch (Exception e) {
      throw new KernelException(String.format("Failed to parse JSON string: %s", jsonString), e);
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
  public static Literal parseJsonValueToLiteral(JsonNode valueNode, DataType dataType) {
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
        OffsetDateTime offsetDateTime = OffsetDateTime.parse(textValue, TIMESTAMP_FORMATTER);
        long epochMicros = ChronoUnit.MICROS.between(EPOCH, offsetDateTime);
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
        long epochMicros = ChronoUnit.MICROS.between(EPOCH.toLocalDateTime(), localDateTime);
        return Literal.ofTimestampNtz(epochMicros);

      } else if (dataType instanceof VariantType) {
        if (!valueNode.isTextual()) {
          throw new KernelException(
              String.format("Expected variant as string value but got: %s", valueNode));
        }
        String textValue = valueNode.asText();
        return Literal.ofString(textValue);
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
   * Get the timestamp formatter used for parsing/formatting timestamps. Package-private for use by
   * DataFileStatistics.
   */
  static DateTimeFormatter getTimestampFormatter() {
    return TIMESTAMP_FORMATTER;
  }

  /** Get the epoch offset date time constant. Package-private for use by DataFileStatistics. */
  static OffsetDateTime getEpoch() {
    return EPOCH;
  }
}
