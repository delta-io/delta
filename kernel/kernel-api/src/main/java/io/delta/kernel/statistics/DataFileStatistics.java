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

import static java.time.ZoneOffset.UTC;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static java.time.temporal.ChronoUnit.MILLIS;

import com.fasterxml.jackson.core.JsonGenerator;
import io.delta.kernel.expressions.Column;
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.internal.util.JsonUtils;
import io.delta.kernel.types.*;
import io.delta.kernel.utils.JsonUtil;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;

/** Statistics about data file in a Delta Lake table. */
public class DataFileStatistics {
  private StructType dataSchema;
  private final long numRecords;
  private final Map<Column, Literal> minValues;
  private final Map<Column, Literal> maxValues;
  private final Map<Column, Long> nullCounts;

  public static final int MICROSECONDS_PER_SECOND = 1_000_000;
  public static final int NANOSECONDS_PER_MICROSECOND = 1_000;

  private static final DateTimeFormatter TIMESTAMP_FORMATTER =
      DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSX");

  /**
   * Create a new instance of {@link DataFileStatistics}.
   *
   * @param dataSchema Schema of the data file.
   * @param numRecords Number of records in the data file.
   * @param minValues Map of column to minimum value of it in the data file. If the data file has
   *     all nulls for the column, the value will be null or not present in the map.
   * @param maxValues Map of column to maximum value of it in the data file. If the data file has
   *     all nulls for the column, the value will be null or not present in the map.
   * @param nullCounts Map of column to number of nulls in the data file.
   */
  public DataFileStatistics(
      StructType dataSchema,
      long numRecords,
      Map<Column, Literal> minValues,
      Map<Column, Literal> maxValues,
      Map<Column, Long> nullCounts) {
    this.dataSchema = dataSchema;
    this.numRecords = numRecords;
    this.minValues = Collections.unmodifiableMap(minValues);
    this.maxValues = Collections.unmodifiableMap(maxValues);
    this.nullCounts = Collections.unmodifiableMap(nullCounts);
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
  public Map<Column, Long> getNullCounts() {
    return nullCounts;
  }

  public String serializeAsJson() {
    return JsonUtil.generate(
        gen -> {
          gen.writeStartObject();
          gen.writeNumberField("numRecords", numRecords);

          // Only write detailed statistics if dataSchema is available
          if (dataSchema != null) {
            gen.writeObjectFieldStart("minValues");

            writeJsonValues(
                gen,
                dataSchema,
                minValues,
                new Column(new String[0]),
                (g, v) -> writeJsonValue(g, v));
            gen.writeEndObject();

            gen.writeObjectFieldStart("maxValues");
            writeJsonValues(
                gen,
                dataSchema,
                maxValues,
                new Column(new String[0]),
                (g, v) -> writeJsonValue(g, v));
            gen.writeEndObject();

            gen.writeObjectFieldStart("nullCounts");
            writeJsonValues(
                gen, dataSchema, nullCounts, new Column(new String[0]), (g, v) -> g.writeNumber(v));
            gen.writeEndObject();
          }

          gen.writeEndObject();
        });
  }

  private <T> void writeJsonValues(
      JsonGenerator generator,
      StructType schema,
      Map<Column, T> values,
      Column parentColPath,
      JsonUtil.JsonValueWriter<T> writer)
      throws IOException {
    if (schema == null) {
      return;
    }
    for (StructField field : schema.fields()) {
      Column colPath = parentColPath.append(field.getName());
      if (field.getDataType() instanceof StructType) {
        generator.writeObjectFieldStart(field.getName());
        writeJsonValues(generator, (StructType) field.getDataType(), values, colPath, writer);
        generator.writeEndObject();
      } else {
        T value = values.get(colPath);
        if (value != null) {
          generator.writeFieldName(field.getName());
          writer.write(generator, value);
        }
      }
    }
  }

  private void writeJsonValue(JsonGenerator generator, Literal literal) throws IOException {
    if (literal == null || literal.getValue() == null) {
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
      generator.writeNumber(((Number) value).floatValue());
    } else if (type instanceof DoubleType) {
      generator.writeNumber(((Number) value).doubleValue());
    } else if (type instanceof StringType) {
      generator.writeString((String) value);
    } else if (type instanceof BinaryType) {
      generator.writeString(new String((byte[]) value, StandardCharsets.UTF_8));
    } else if (type instanceof DecimalType) {
      generator.writeNumber((BigDecimal) value);
    } else if (type instanceof DateType) {
      generator.writeString(
          LocalDate.ofEpochDay(((Number) value).longValue()).format(ISO_LOCAL_DATE));
    } else if (type instanceof TimestampType || type instanceof TimestampNTZType) {
      long epochMicros = (long) value;
      long epochSeconds = epochMicros / MICROSECONDS_PER_SECOND;
      int nanoAdjustment =
          (int) (epochMicros % MICROSECONDS_PER_SECOND) * NANOSECONDS_PER_MICROSECOND;
      if (nanoAdjustment < 0) {
        nanoAdjustment += MICROSECONDS_PER_SECOND * NANOSECONDS_PER_MICROSECOND;
      }
      Instant instant = Instant.ofEpochSecond(epochSeconds, nanoAdjustment);
      generator.writeString(
          TIMESTAMP_FORMATTER.format(ZonedDateTime.ofInstant(instant.truncatedTo(MILLIS), UTC)));
    } else {
      throw new IllegalArgumentException("Unsupported stats data type: " + type);
    }
  }

  @Override
  public String toString() {
    return serializeAsJson();
  }

  /**
   * Utility method to deserialize statistics from a JSON string. For now only the number of records
   * is deserialized, the rest of the statistics are not supported yet.
   *
   * @param json Data statistics JSON string to deserialize.
   * @return An {@link Optional} containing the deserialized {@link DataFileStatistics} if present.
   */
  public static Optional<DataFileStatistics> deserializeFromJson(String json) {
    Map<String, String> keyValueMap = JsonUtils.parseJSONKeyValueMap(json);

    // For now just deserialize the number of records
    String numRecordsStr = keyValueMap.get("numRecords");
    if (numRecordsStr == null) {
      return Optional.empty();
    }
    long numRecords = Long.parseLong(numRecordsStr);
    // TODO: Add support for inferring the statsSchema/dataSchema later.
    DataFileStatistics stats =
        new DataFileStatistics(
            null,
            numRecords,
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap());
    return Optional.of(stats);
  }
}
