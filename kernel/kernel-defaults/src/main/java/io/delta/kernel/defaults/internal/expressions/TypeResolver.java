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
package io.delta.kernel.defaults.internal.expressions;

import static io.delta.kernel.defaults.internal.expressions.ImplicitCastExpression.canCastTo;

import io.delta.kernel.types.*;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

/**
 * Utility class for resolving common types among multiple operands using implicit cast rules. This
 * class determines the narrowest common type that all operands can be cast to, following the same
 * type precedence hierarchy as {@link ImplicitCastExpression}.
 */
final class TypeResolver {

  /** Type precedence hierarchy for numeric types (narrowest to widest). */
  private static final List<String> NUMERIC_TYPE_PRECEDENCE =
      Arrays.asList("byte", "short", "integer", "long", "float", "double");

  /**
   * Finds the narrowest common type that all given types can be cast to.
   *
   * @param valueType The type of the value expression (left side of IN)
   * @param listTypes The types of the IN list elements
   * @return The common type that all operands can be cast to
   * @throws IllegalArgumentException if no common type can be found
   */
  static DataType findCommonType(DataType valueType, List<DataType> listTypes) {
    if (listTypes.isEmpty()) {
      return valueType;
    }

    // Start with the value type as the candidate common type
    DataType commonType = valueType;

    // Check each list element type
    for (DataType listType : listTypes) {
      commonType = findCommonTypeBetweenTwo(commonType, listType);
    }

    return commonType;
  }

  /**
   * Finds the common type between two data types.
   *
   * @param type1 First data type
   * @param type2 Second data type
   * @return The common type that both can be cast to
   * @throws IllegalArgumentException if no common type exists
   */
  private static DataType findCommonTypeBetweenTwo(DataType type1, DataType type2) {
    // If types are equivalent, no casting needed
    if (type1.equivalent(type2)) {
      return type1;
    }

    // Both must be numeric types for implicit casting to work
    if (!isNumericType(type1) || !isNumericType(type2)) {
      throw new IllegalArgumentException(
          String.format(
              "Cannot find common type between non-numeric types: %s and %s", type1, type2));
    }

    // Find the wider type based on precedence
    int precedence1 = getTypePrecedence(type1);
    int precedence2 = getTypePrecedence(type2);

    if (precedence1 > precedence2) {
      // type1 is wider, check if type2 can be cast to type1
      if (canCastTo(type2, type1)) {
        return type1;
      }
    } else {
      // type2 is wider, check if type1 can be cast to type2
      if (canCastTo(type1, type2)) {
        return type2;
      }
    }

    throw new IllegalArgumentException(
        String.format("Cannot find common type between %s and %s", type1, type2));
  }

  /**
   * Checks if the given data type is a numeric type that supports implicit casting.
   *
   * @param type The data type to check
   * @return true if the type is numeric and supports implicit casting
   */
  static boolean isNumericType(DataType type) {
    String typeString = type.toString();
    return NUMERIC_TYPE_PRECEDENCE.contains(typeString);
  }

  /**
   * Gets the precedence index for a numeric type. Higher index means wider type.
   *
   * @param type The numeric data type
   * @return The precedence index (0 for byte, 5 for double)
   * @throws IllegalArgumentException if the type is not a supported numeric type
   */
  private static int getTypePrecedence(DataType type) {
    String typeString = type.toString();
    int precedence = NUMERIC_TYPE_PRECEDENCE.indexOf(typeString);
    if (precedence == -1) {
      throw new IllegalArgumentException("Unsupported numeric type: " + typeString);
    }
    return precedence;
  }

  /**
   * Checks if the given type can be cast to any of the types in the list.
   *
   * @param fromType The source type
   * @param toTypes The list of target types
   * @return Optional containing the first compatible target type, or empty if none found
   */
  static Optional<DataType> findCompatibleType(DataType fromType, List<DataType> toTypes) {
    for (DataType toType : toTypes) {
      if (fromType.equivalent(toType) || canCastTo(fromType, toType)) {
        return Optional.of(toType);
      }
    }
    return Optional.empty();
  }

  private TypeResolver() {
    // Utility class - prevent instantiation
  }
}
