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
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.internal.data.TransactionStateRow;
import io.delta.kernel.internal.icebergcompat.IcebergCompatV3MetadataValidatorAndUpdater;
import io.delta.kernel.internal.util.ColumnMapping;
import io.delta.kernel.internal.util.SchemaIterable;
import io.delta.kernel.types.*;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.*;

/**
 * Utilities class for TableFeature "allowColumnDefaults". NOTE: As of Aug 2025, kernel only
 * supports reading Delta tables with the table feature, or modifying table metadata. Writing actual
 * data to the table is not allowed.
 */
public class ColumnDefaults {

  private static final String DEFAULT_VALUE_METADATA_KEY = "CURRENT_DEFAULT";

  /** Don't allow data writes to tables with default values */
  public static void blockWriteIfEnabled(Row transactionState) {
    if (!extractFieldsWithDefaultValues(TransactionStateRow.getLogicalSchema(transactionState))
        .isEmpty()) {
      throw new UnsupportedOperationException(
          "Writing with Column Default values is not supported yet.");
    }
  }

  /**
   * Validate Column Default value changes in the provided metadata.
   *
   * <ul>
   *   <li>Only the added/changed default value is validated.
   *   <li>Kernel only supports literal default values. See {validateLiteral}.
   * </ul>
   */
  public static void validateChange(
      Protocol newProtocol, Optional<Metadata> oldMetadataOpt, Metadata newMetadata) {
    boolean featureEnabled = newProtocol.supportsFeature(ALLOW_COLUMN_DEFAULTS_W_FEATURE);
    boolean v3Enabled =
        IcebergCompatV3MetadataValidatorAndUpdater.isIcebergCompatEnabled(newMetadata);
    // Default value changes relies on Schema evolution, which requires ColumnMapping.
    // Thus if ColumnMapping is not present, we assume there's no schema evolution.
    if (oldMetadataOpt.isPresent()
        && !ColumnMapping.isColumnMappingModeEnabled(
            ColumnMapping.getColumnMappingMode(newMetadata.getConfiguration()))) {
      return;
    }
    Map<String, StructField> oldDefaults =
        oldMetadataOpt
            .map(metadata -> extractFieldsWithDefaultValues(metadata.getSchema()))
            .orElse(Collections.emptyMap());
    Map<String, StructField> newDefaults = extractFieldsWithDefaultValues(newMetadata.getSchema());

    StructField defaultField = new StructField("default_field", IntegerType.INTEGER, false);

    // Validate the default value if a column is newly added or the default value is changed.
    newDefaults.forEach(
        (path, field) -> {
          String newDefaultValue = getRawDefaultValue(field);
          StructField existingField = oldDefaults.getOrDefault(path, defaultField);
          if (!Objects.equals(getRawDefaultValue(existingField), newDefaultValue)) {
            if (!featureEnabled) {
              throw new KernelException(
                  "This table does not enable table feature for setting column defaults");
            }
            // We don't allow this feature to be used without V3 in Kernel
            if (!v3Enabled) {
              throw new KernelException(
                  "Kernel only supports changing Default Values on tables with "
                      + "IcebergCompatV3 enabled");
            }
            validateLiteral(field.getDataType(), newDefaultValue);
          }
        });
  }

  private static Map<String, StructField> extractFieldsWithDefaultValues(StructType schema) {
    Map<String, StructField> result = new HashMap<>();
    for (SchemaIterable.SchemaElement element : new SchemaIterable(schema)) {
      StructField field = element.getField();
      if (getRawDefaultValue(field) != null) {
        String fieldKey = null;
        try {
          fieldKey = String.valueOf(ColumnMapping.getColumnId(field));
        } catch (IllegalArgumentException e) {
          fieldKey = element.getNamePath();
        }
        result.put(fieldKey, field);
      }
    }
    return result;
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
