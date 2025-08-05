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
package io.delta.kernel.internal.columndefaults;

import io.delta.kernel.data.Row;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.tablefeatures.TableFeatureSupport;
import io.delta.kernel.internal.tablefeatures.TableFeatures;
import io.delta.kernel.internal.util.SchemaIterable;
import io.delta.kernel.types.*;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Utilities class for TableFeature "allowColumnDefaults". NOTE: As of Aug 2025, kernel only
 * supports reading Delta tables with the table feature, or make metadata change to the table.
 * Writing actual data to the table, or modify the default values is not allowed.
 */
public class ColumnDefaults {

  private static final String DEFAULT_VALUE_METADATA_KEY = "CURRENT_DEFAULT";

  /** Don't allow data writes to tables with the feature enabled */
  public static void blockIfEnabled(Row transactionState) {
    if (TableFeatureSupport.supports(
        transactionState, TableFeatures.ALLOW_COLUMN_DEFAULTS_W_FEATURE)) {
      throw new UnsupportedOperationException(
          "Writing with Column Default values is not supported yet.");
    }
  }

  /**
   * Validate Column Default value changes in the provided metadata. 1. Only the added/changed
   * default value is checked. 2. Kernel only supports literal default values. See
   * {validateLiteral}.
   */
  public static void validateChange(Metadata oldMetadata, Metadata newMetadata) {
    Map<String, StructField> oldDefaults = extractFieldsWithDefaultValues(oldMetadata.getSchema());
    Map<String, StructField> newDefaults = extractFieldsWithDefaultValues(newMetadata.getSchema());

    StructField defaultField = new StructField("default_field", IntegerType.INTEGER, false);

    newDefaults.forEach(
        (path, field) -> {
          String newDefaultValue = field.getMetadata().getString(DEFAULT_VALUE_METADATA_KEY);
          StructField existingField = oldDefaults.getOrDefault(path, defaultField);
          if (!Objects.equals(
              existingField.getMetadata().getString(DEFAULT_VALUE_METADATA_KEY), newDefaultValue)) {
            validateLiteral(field.getDataType(), newDefaultValue);
          }
        });
  }

  private static Map<String, StructField> extractFieldsWithDefaultValues(StructType schema) {
    Map<String, StructField> result = new HashMap<>();
    for (SchemaIterable.SchemaElement element : new SchemaIterable(schema)) {
      StructField field = element.getField();
      if (field.getMetadata().get(DEFAULT_VALUE_METADATA_KEY) != null) {
        result.put(element.getNamePath(), field);
      }
    }
    return result;
  }

  private static void validateLiteral(DataType type, String value) {
    try {
      String stripped = stripQuotes(value, false);
      if ((type instanceof StringType || type instanceof BinaryType)) {
        stripQuotes(value, true);
      } else if (type instanceof LongType) {
        Long.parseLong(stripped);
      } else if (type instanceof IntegerType) {
        Integer.parseInt(stripped);
      } else if (type instanceof ShortType) {
        Short.parseShort(stripped);
      } else if (type instanceof FloatType) {
        Float.parseFloat(stripped);
      } else if (type instanceof DoubleType) {
        Double.parseDouble(stripped);
      } else if (type instanceof DecimalType) {
        DecimalType dtype = (DecimalType) type;
        BigDecimal input = new BigDecimal(stripped);
        if (input.scale() > dtype.getScale() || input.precision() > dtype.getPrecision()) {
          throw new IllegalArgumentException("invalid default value " + value + " for " + type);
        }
      } else if (type instanceof BooleanType) {
        Boolean.parseBoolean(stripped);
      } else if (type instanceof DateType) {
        LocalDate.parse(stripped, DateTimeFormatter.ISO_LOCAL_DATE);
      } else if (type instanceof TimestampType) {
        OffsetDateTime.parse(stripped, DateTimeFormatter.ISO_DATE_TIME);
      } else if (type instanceof TimestampNTZType) {
        LocalDateTime.parse(stripped, DateTimeFormatter.ISO_LOCAL_DATE_TIME);
      } else {
        throw new UnsupportedOperationException(
            "Kernel does not support column defaults for " + type.toString());
      }
    } catch (NumberFormatException | DateTimeParseException e) {
      throw new IllegalArgumentException("invalid default value " + value + " for " + type);
    }
  }

  /**
   * Remove the quotes from input string.
   *
   * @param input input to remove quotes
   * @param require require the input to have quotes
   * @return string with enclosing quotes removed
   */
  private static String stripQuotes(String input, boolean require) {
    if (input.length() > 1
        && ((input.charAt(0) == '\'' && input.charAt(input.length() - 1) == '\'')
            || (input.charAt(0) == '"' && input.charAt(input.length() - 1) == '"'))) {
      return input.substring(1, input.length() - 1);
    }
    if (require) {
      throw new IllegalArgumentException("String literal not enclosed in quotes: " + input);
    }
    return input;
  }
}
