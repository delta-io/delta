/*
 * Copyright (2026) The Delta Lake Project Authors.
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

package io.delta.flink.table;

import io.delta.kernel.types.ArrayType;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.MapType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/** Internal helpers for validating append-only schema updates. */
final class SchemaEvolutionUtils {

  private SchemaEvolutionUtils() {}

  static Optional<StructType> buildAdditiveSchema(
      StructType currentSchema, StructType targetSchema) {
    if (targetSchema.length() < currentSchema.length()) {
      throw new IllegalArgumentException(
          String.format(
              "Target schema is stale: current schema has %d fields but target schema has %d",
              currentSchema.length(), targetSchema.length()));
    }

    for (int ordinal = 0; ordinal < currentSchema.length(); ordinal++) {
      StructField currentField = currentSchema.at(ordinal);
      StructField targetField = targetSchema.at(ordinal);
      if (!logicallyEqual(currentField, targetField)) {
        throw new IllegalArgumentException(
            String.format(
                "Unsupported schema change at ordinal %d. Current field: %s; target field: %s. "
                    + "Only nullable top-level columns appended to the schema are supported",
                ordinal, currentField, targetField));
      }
    }

    if (targetSchema.length() == currentSchema.length()) {
      return Optional.empty();
    }

    List<StructField> fields = new ArrayList<>(currentSchema.fields());
    for (StructField newField :
        targetSchema.fields().subList(currentSchema.length(), targetSchema.length())) {
      if (!newField.isNullable()) {
        throw new IllegalArgumentException(
            String.format("New column '%s' must be nullable", newField.getName()));
      }
      fields.add(newField);
    }
    return Optional.of(new StructType(fields));
  }

  static boolean logicallyEqual(DataType currentType, DataType targetType) {
    if (currentType instanceof StructType && targetType instanceof StructType) {
      StructType currentStruct = (StructType) currentType;
      StructType targetStruct = (StructType) targetType;
      if (currentStruct.length() != targetStruct.length()) {
        return false;
      }
      for (int ordinal = 0; ordinal < currentStruct.length(); ordinal++) {
        if (!logicallyEqual(currentStruct.at(ordinal), targetStruct.at(ordinal))) {
          return false;
        }
      }
      return true;
    }
    if (currentType instanceof ArrayType && targetType instanceof ArrayType) {
      return logicallyEqual(
          ((ArrayType) currentType).getElementField(), ((ArrayType) targetType).getElementField());
    }
    if (currentType instanceof MapType && targetType instanceof MapType) {
      MapType currentMap = (MapType) currentType;
      MapType targetMap = (MapType) targetType;
      return logicallyEqual(currentMap.getKeyField(), targetMap.getKeyField())
          && logicallyEqual(currentMap.getValueField(), targetMap.getValueField());
    }
    return currentType.equals(targetType);
  }

  private static boolean logicallyEqual(StructField currentField, StructField targetField) {
    return currentField.getName().equals(targetField.getName())
        && currentField.isNullable() == targetField.isNullable()
        && logicallyEqual(currentField.getDataType(), targetField.getDataType());
  }
}
