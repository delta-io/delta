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

import static io.delta.kernel.internal.tablefeatures.TableFeatures.ALLOW_COLUMN_DEFAULTS_W_FEATURE;

import io.delta.kernel.data.Row;
import io.delta.kernel.exceptions.KernelException;
import io.delta.kernel.internal.TableConfig;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.internal.data.TransactionStateRow;
import io.delta.kernel.internal.tablefeatures.TableFeatures;
import io.delta.kernel.internal.util.SchemaIterable;
import io.delta.kernel.types.*;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Utilities class for TableFeature "allowColumnDefaults". NOTE: As of Aug 2025, kernel only
 * supports reading Delta tables with the table feature, or modifying table metadata. Writing actual
 * data to the table is not allowed.
 */
public class ColumnDefaults {

  private static final String DEFAULT_VALUE_METADATA_KEY = "CURRENT_DEFAULT";

  public static boolean isEnabled(Map<String, String> properties) {
    return TableFeatures.isFeaturePropertyOverridden(properties, ALLOW_COLUMN_DEFAULTS_W_FEATURE)
        && TableConfig.ICEBERG_COMPAT_V3_ENABLED.fromMetadata(properties);
  }

  public static boolean isEnabled(Protocol protocol, Metadata metadata) {
    return protocol.supportsFeature(ALLOW_COLUMN_DEFAULTS_W_FEATURE)
        && TableConfig.ICEBERG_COMPAT_V3_ENABLED.fromMetadata(metadata);
  }

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
   * Validate Column Default values in the provided metadata.
   *
   * <ul>
   *   <li>Kernel only supports literal default values. See {validateLiteral}.
   * </ul>
   *
   * @param schema target table schema
   * @param isEnabled When the feature is disabled, no column default is allowed. When it's enabled,
   *     only literal default value is allowed
   * @throws IllegalArgumentException when the table contains invalid default value
   */
  public static void validate(StructType schema, boolean isEnabled) {
    Stream<StructField> defaultValues = extractFieldsWithDefaultValues(schema);
    if (!isEnabled) {
      if (defaultValues.findAny().isPresent()) {
        throw new KernelException(
            "This table does not enable table features for setting column defaults");
      }
    } else {
      // Validate the default value
      defaultValues.forEach(
          field -> {
            try {
              validateLiteral(field.getDataType(), getRawDefaultValue(field));
            } catch (IllegalArgumentException | UnsupportedOperationException e) {
              throw new KernelException("This table contains unsupported default values", e);
            }
          });
    }
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
   */
  private static void validateLiteral(DataType type, String value) {
    try {
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
