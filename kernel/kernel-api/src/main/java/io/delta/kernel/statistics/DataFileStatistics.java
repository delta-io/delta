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
import static java.time.ZoneOffset.UTC;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static java.time.temporal.ChronoUnit.MILLIS;

import com.fasterxml.jackson.core.JsonGenerator;
import io.delta.kernel.exceptions.KernelException;
import io.delta.kernel.expressions.Column;
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.internal.DeltaErrors;
import io.delta.kernel.internal.util.JsonUtils;
import io.delta.kernel.types.*;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/** Statistics about data file in a Delta Lake table. */
public class DataFileStatistics {
  private static final DateTimeFormatter TIMESTAMP_FORMATTER =
      DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSX");

  public static final int MICROSECONDS_PER_SECOND = 1_000_000;
  public static final int NANOSECONDS_PER_MICROSECOND = 1_000;

  private final long numRecords;
  private final Map<Column, Literal> minValues;
  private final Map<Column, Literal> maxValues;
  private final Map<Column, Long> nullCounts;

  /**
   * Create a new instance of {@link DataFileStatistics}. The minValues, maxValues, and nullCounts
   * are all required fields. This class is primarily used to serialize stats to JSON with type
   * checking when constructing file actions and NOT used during data skipping.
   *
   * @param numRecords Number of records in the data file.
   * @param minValues Map of column to minimum value of it in the data file. If the data file has
   *     all nulls for the column, the value will be null or not present in the map.
   * @param maxValues Map of column to maximum value of it in the data file. If the data file has
   *     all nulls for the column, the value will be null or not present in the map.
   * @param nullCounts Map of column to number of nulls in the data file.
   */
  public DataFileStatistics(
      long numRecords,
      Map<Column, Literal> minValues,
      Map<Column, Literal> maxValues,
      Map<Column, Long> nullCounts) {
    Objects.requireNonNull(minValues, "minValues must not be null to serialize stats.");
    Objects.requireNonNull(maxValues, "maxValues must not be null to serialize stats.");
    Objects.requireNonNull(nullCounts, "nullCounts must not be null to serialize stats.");

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
          gen.writeNumberField("numRecords", numRecords);

          if (physicalSchema != null) {
            gen.writeObjectFieldStart("minValues");
            writeJsonValues(
                gen,
                physicalSchema,
                minValues,
                new Column(new String[0]),
                (g, v) -> writeJsonValue(g, v));
            gen.writeEndObject();

            gen.writeObjectFieldStart("maxValues");
            writeJsonValues(
                gen,
                physicalSchema,
                maxValues,
                new Column(new String[0]),
                (g, v) -> writeJsonValue(g, v));
            gen.writeEndObject();

            gen.writeObjectFieldStart("nullCounts");
            writeJsonValues(
                gen,
                physicalSchema,
                nullCounts,
                new Column(new String[0]),
                (g, v) -> g.writeNumber(v));
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
        && Objects.equals(nullCounts, that.nullCounts);
  }

  @Override
  public int hashCode() {
    int result = Long.hashCode(numRecords);
    result = 31 * result + Objects.hash(minValues.keySet());
    result = 31 * result + Objects.hash(maxValues.keySet());
    result = 31 * result + Objects.hash(nullCounts.keySet());
    return result;
  }

  @Override
  public String toString() {
    return String.format(
        "DataFileStatistics(numRecords=%s, minValues=%s, maxValues=%s, nullCounts=%s)",
        numRecords, minValues, maxValues, nullCounts);
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
      throw unsupportedStatsDataType(type);
    }
  }
}
