/*
 * Copyright (2025) The Delta Lake Project Authors.
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
import io.delta.kernel.exceptions.KernelException;
import io.delta.kernel.internal.DeltaErrors;
import io.delta.kernel.internal.data.TransactionStateRow;
import io.delta.kernel.internal.util.SchemaIterable;
import io.delta.kernel.types.*;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.stream.Stream;

/**
 * Utilities class for TableFeature "allowColumnDefaults". NOTE: As of Aug 2025, kernel only
 * supports reading Delta tables with the table feature, or modifying table metadata. Writing actual
 * data to the table is not allowed.
 */
public class ColumnDefaults {

  private static final String DEFAULT_VALUE_METADATA_KEY = "CURRENT_DEFAULT";

  /** Don't allow data writes to tables with default values */
  public static void blockWriteIfEnabled(Row transactionState) {
    if (extractFieldsWithDefaultValues(TransactionStateRow.getLogicalSchema(transactionState))
        .findAny()
        .isPresent()) {
      throw new UnsupportedOperationException(
          "Writing with Column Default values is not supported yet.");
    }
  }

  /**
   * Validate Column Default values in the provided metadata. Kernel only supports literal default
   * values. See {validateLiteral}.
   *
   * @param schema target table schema
   * @param isEnabled When the feature is disabled, no column default is allowed.
   * @param isIcebergCompatV3Enabled Kernel currently requires IcebergCompatV3 to be enabled when
   *     using Column Defaults
   * @throws KernelException when the table contains invalid default value
   */
  public static void validateSchema(
      StructType schema, boolean isEnabled, boolean isIcebergCompatV3Enabled) {
    if (isEnabled && !isIcebergCompatV3Enabled) {
      throw DeltaErrors.defaultValueRequireIcebergV3();
    }
    Stream<StructField> defaultValues = extractFieldsWithDefaultValues(schema);
    if (!isEnabled) {
      if (defaultValues.findAny().isPresent()) {
        throw DeltaErrors.defaultValueRequiresTableFeature();
      }
    } else {
      // This check will be relaxed once kernel supports default values with expression
      defaultValues.forEach(
          field -> {
            String defaultValue = getRawDefaultValue(field);
            try {
              validateLiteral(field.getDataType(), defaultValue);
            } catch (IllegalArgumentException e) {
              throw DeltaErrors.nonLiteralDefaultValue(defaultValue);
            } catch (UnsupportedOperationException e) {
              throw DeltaErrors.unsupportedDataTypeForDefaultValue(
                  field.getName(), field.getDataType().toString());
            }
          });
    }
  }

  /**
   * Validate that the schema only contains literal default values as a requirement of
   * IcebergCompat.
   *
   * @param schema table schema
   */
  public static void validateSchemaForIcebergCompat(StructType schema, String compatVersion) {
    extractFieldsWithDefaultValues(schema)
        .forEach(
            field -> {
              String defaultValue = getRawDefaultValue(field);
              try {
                validateLiteral(field.getDataType(), defaultValue);
              } catch (IllegalArgumentException e) {
                throw DeltaErrors.icebergCompatRequiresLiteralDefaultValue(
                    compatVersion, defaultValue);
              }
            });
  }

  private static Stream<StructField> extractFieldsWithDefaultValues(StructType schema) {
    return new SchemaIterable(schema)
        .stream()
            .map(SchemaIterable.SchemaElement::getField)
            .filter(f -> getRawDefaultValue(f) != null);
  }

  public static String getRawDefaultValue(StructField field) {
    return field.getMetadata().getString(DEFAULT_VALUE_METADATA_KEY);
  }

  /**
   * Validate that the provided default value is a literal (not an expression) and can be cast to
   * the given data type. We only support a limited set of literals. Example:
   *
   * <ul>
   *   <li>'CURRENT_VALUE()' is a valid String literal. CURRENT_VALUE (no quotes) is not.
   *   <li>4.95 and '4.95' are both valid Double/Float literals.
   *   <li>'2022-01-01' is a valid Date literal, '09/01/2022' is not.
   * </ul>
   *
   * @throws IllegalArgumentException if the value is not a literal value
   * @throws UnsupportedOperationException when kernel does not support column defaults for the data
   *     type
   */
  private static void validateLiteral(DataType type, String value) {
    String stripped = stripQuotes(value, false);
    if ((type instanceof StringType || type instanceof BinaryType)) {
      // String literals are required to be enclosed in quotes
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
      try {
        LocalDate.parse(stripped, DateTimeFormatter.ISO_LOCAL_DATE);
      } catch (DateTimeParseException e) {
        throw new IllegalArgumentException(e);
      }
    } else if (type instanceof TimestampType) {
      try {
        OffsetDateTime.parse(stripped, DateTimeFormatter.ISO_DATE_TIME);
      } catch (DateTimeParseException e) {
        throw new IllegalArgumentException(e);
      }
    } else if (type instanceof TimestampNTZType) {
      try {
        LocalDateTime.parse(stripped, DateTimeFormatter.ISO_LOCAL_DATE_TIME);
      } catch (DateTimeParseException e) {
        throw new IllegalArgumentException(e);
      }
    } else {
      throw new UnsupportedOperationException(
          "Kernel does not support column defaults for " + type.toString());
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
