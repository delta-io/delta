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
package io.delta.kernel.internal.util;

import static io.delta.kernel.statistics.DataFileStatistics.EPOCH;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;

import com.fasterxml.jackson.databind.JsonNode;
import io.delta.kernel.exceptions.KernelException;
import io.delta.kernel.expressions.Column;
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.internal.skipping.StatsSchemaHelper;
import io.delta.kernel.statistics.DataFileStatistics;
import java.io.IOException;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.*;

/** Encapsulates various utility methods for statistics related operations. */
public class StatsUtils {

  private StatsUtils() {}

  /**
   * Utility method to deserialize statistics from a JSON string.
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

    // Parse numRecords
    JsonNode numRecordsNode = root.get(StatsSchemaHelper.NUM_RECORDS);
    if (numRecordsNode == null || !numRecordsNode.isNumber()) {
      return Optional.empty();
    }
    long numRecords = numRecordsNode.asLong();

    // Parse minValues
    Map<Column, Literal> minValues = new HashMap<>();
    JsonNode minNode = root.get(StatsSchemaHelper.MIN);
    if (minNode != null && minNode.isObject()) {
      parseNestedJsonValues(minNode, minValues, new String[0], true);
    }

    // Parse maxValues
    Map<Column, Literal> maxValues = new HashMap<>();
    JsonNode maxNode = root.get(StatsSchemaHelper.MAX);
    if (maxNode != null && maxNode.isObject()) {
      parseNestedJsonValues(maxNode, maxValues, new String[0], true);
    }

    // Parse nullCount
    Map<Column, Long> nullCount = new HashMap<>();
    JsonNode nullCountNode = root.get(StatsSchemaHelper.NULL_COUNT);
    if (nullCountNode != null && nullCountNode.isObject()) {
      parseNestedJsonValues(nullCountNode, nullCount, new String[0], false);
    }

    return Optional.of(new DataFileStatistics(numRecords, minValues, maxValues, nullCount));
  }

  /**
   * Helper method to recursively parse nested JSON values back into Column->Value maps. This is the
   * inverse of the writeJsonValues method in DataFileStatistics.serializeAsJson.
   *
   * @param node The JSON node to parse
   * @param resultMap The map to populate (either Map<Column, Literal> or Map<Column, Long>)
   * @param currentPath The current column path being built
   * @param isLiteralMap True if parsing into Literal values, false if parsing into Long values
   */
  private static void parseNestedJsonValues(
      JsonNode node, Map<Column, ?> resultMap, String[] currentPath, boolean isLiteralMap) {
    if (node == null || !node.isObject()) {
      return;
    }

    Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
    while (fields.hasNext()) {
      Map.Entry<String, JsonNode> entry = fields.next();
      String fieldName = entry.getKey();
      JsonNode valueNode = entry.getValue();

      // Build the new path by appending this field name
      String[] newPath = new String[currentPath.length + 1];
      System.arraycopy(currentPath, 0, newPath, 0, currentPath.length);
      newPath[currentPath.length] = fieldName;

      if (valueNode.isObject()) {
        // This is a nested structure, recurse deeper
        parseNestedJsonValues(valueNode, resultMap, newPath, isLiteralMap);
      } else {
        // This is a leaf value, create the Column and add to map
        Column column = new Column(newPath);

        if (isLiteralMap) {
          Literal literal = parseJsonValueToLiteral(valueNode);
          if (literal != null) {
            // Safe unchecked cast: when isLiteralMap=true, caller guarantees resultMap is
            // Map<Column, Literal>
            @SuppressWarnings("unchecked")
            Map<Column, Literal> literalMap = (Map<Column, Literal>) resultMap;
            literalMap.put(column, literal);
          }
        } else {
          if (valueNode.isNumber()) {
            @SuppressWarnings("unchecked")
            // Safe unchecked cast: when isLiteralMap=false, caller guarantees resultMap is
            // Map<Column, Long>
            Map<Column, Long> longMap = (Map<Column, Long>) resultMap;
            longMap.put(column, valueNode.asLong());
          }
        }
      }
    }
  }

  /**
   * Helper method to convert JSON node value to Literal based on the node type. This mirrors the
   * type handling in writeJsonValue method.
   */
  private static Literal parseJsonValueToLiteral(JsonNode valueNode) {
    if (valueNode == null || valueNode.isNull()) {
      return null;
    }

    if (valueNode.isBoolean()) {
      return Literal.ofBoolean(valueNode.asBoolean());
    } else if (valueNode.isInt()) {
      return Literal.ofInt(valueNode.asInt());
    } else if (valueNode.isLong()) {
      return Literal.ofLong(valueNode.asLong());
    } else if (valueNode.isNumber()) {
      // All decimal numbers are parsed as Double since JSON doesn't distinguish float vs double
      return Literal.ofDouble(valueNode.asDouble());
    } else if (valueNode.isTextual()) {
      String textValue = valueNode.asText();

      // Handle special float/double values that are stored as strings
      if ("NaN".equals(textValue)) {
        return Literal.ofDouble(Double.NaN);
      } else if ("Infinity".equals(textValue)) {
        return Literal.ofDouble(Double.POSITIVE_INFINITY);
      } else if ("-Infinity".equals(textValue)) {
        return Literal.ofDouble(Double.NEGATIVE_INFINITY);
      }

      // Try to parse as date (YYYY-MM-DD format)
      try {
        LocalDate date = LocalDate.parse(textValue, ISO_LOCAL_DATE);
        return Literal.ofDate((int) date.toEpochDay());
      } catch (Exception e) {
        // Not a date, continue
      }

      // Try to parse as timestamp
      try {
        if (textValue.contains("T") && textValue.contains(":")) {
          if (textValue.endsWith("Z")
              || textValue.contains("+")
              || textValue.matches(".*-\\d{2}:\\d{2}$")) {
            // Has timezone info - TimestampType
            OffsetDateTime offsetDateTime =
                OffsetDateTime.parse(textValue, DataFileStatistics.TIMESTAMP_FORMATTER);
            long epochMicros = ChronoUnit.MICROS.between(EPOCH, offsetDateTime);
            return Literal.ofTimestamp(epochMicros);
          } else {
            // No timezone info - TimestampNTZType
            LocalDateTime localDateTime =
                LocalDateTime.parse(textValue, DateTimeFormatter.ISO_LOCAL_DATE_TIME);
            long epochMicros = ChronoUnit.MICROS.between(EPOCH.toLocalDateTime(), localDateTime);
            return Literal.ofTimestampNtz(epochMicros);
          }
        }
      } catch (Exception e) {
        // Not a timestamp, continue
      }

      // Handle binary data stored as UTF-8 string (inverse of BinaryType serialization)
      try {
        // This is tricky - we can't easily distinguish between actual strings
        // and binary data that was converted to string. For now, default to string.
        return Literal.ofString(textValue);
      } catch (Exception e) {
        return Literal.ofString(textValue);
      }
    } else if (valueNode.isBigDecimal()) {
      BigDecimal decimal = valueNode.decimalValue();
      return Literal.ofDecimal(decimal, decimal.precision(), decimal.scale());
    }

    // Fallback - try to convert to string
    return Literal.ofString(valueNode.asText());
  }
}
