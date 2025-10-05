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

import static io.delta.kernel.defaults.internal.DefaultEngineErrors.unsupportedExpressionException;
import static io.delta.kernel.defaults.internal.expressions.DefaultExpressionUtils.*;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.expressions.Expression;
import io.delta.kernel.expressions.In;
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.internal.util.Preconditions;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.internal.util.VectorUtils;
import io.delta.kernel.types.*;
import io.delta.kernel.types.CollationIdentifier;
import java.util.*;

/** Utility methods to evaluate {@code IN} expression. */
public class InExpressionEvaluator {

  /** Validates and transforms the {@code IN} expression. */
  static In validateAndTransform(
      In in,
      Expression valueExpression,
      DataType valueDataType,
      List<Expression> inListExpressions,
      List<DataType> inListDataTypes) {
    validateArgumentCount(in, inListExpressions);
    validateInListElementsAreLiterals(in, inListExpressions);

    // Use implicit casting for type compatibility
    DataType commonType = resolveCommonType(in, valueDataType, inListDataTypes);
    Expression transformedValue = applyCastIfNeeded(valueExpression, valueDataType, commonType);
    List<Expression> transformedList =
        applyListCasts(inListExpressions, inListDataTypes, commonType);

    validateCollation(
        in, commonType, transformedList, Collections.nCopies(transformedList.size(), commonType));

    if (in.getCollationIdentifier().isPresent()) {
      return new In(transformedValue, transformedList, in.getCollationIdentifier().get());
    } else {
      return new In(transformedValue, transformedList);
    }
  }

  /** Evaluates the IN expression on the given column vectors. */
  static ColumnVector eval(List<ColumnVector> childrenVectors) {
    return new InColumnVector(childrenVectors);
  }

  ////////////////////
  // Private Helper //
  ////////////////////

  private static void validateArgumentCount(In in, List<Expression> inListExpressions) {
    Objects.requireNonNull(inListExpressions);
    if (inListExpressions.isEmpty()) {
      throw unsupportedExpressionException(
          in,
          "IN expression requires at least 1 element in the IN list. "
              + "Example usage: column IN (value1, value2, ...). "
              + "Empty IN lists are not supported.");
    }
  }

  private static void validateInListElementsAreLiterals(In in, List<Expression> inListExpressions) {
    for (int i = 0; i < inListExpressions.size(); i++) {
      Expression child = inListExpressions.get(i);
      if (!(child instanceof Literal)) {
        throw unsupportedExpressionException(
            in,
            String.format(
                "IN expression requires all list elements to be literals. "
                    + "Non-literal expression found at position %d: %s. "
                    + "Only constant values are currently supported in IN lists. "
                    + "Consider using subqueries or other expressions for dynamic values.",
                i + 1, child.getClass().getSimpleName()));
      }
    }
  }

  /** Resolves the common type for all operands in the IN expression using implicit cast rules. */
  private static DataType resolveCommonType(
      In in, DataType valueDataType, List<DataType> inListDataTypes) {
    // Check for nested types which are not supported
    if (valueDataType.isNested()) {
      throw unsupportedExpressionException(
          in,
          String.format(
              "IN expression does not support nested types. Value type: %s. "
                  + "Only primitive types (numeric, string, boolean, date, timestamp, binary) "
                  + "are supported.",
              valueDataType));
    }

    for (int i = 0; i < inListDataTypes.size(); i++) {
      DataType listElementType = inListDataTypes.get(i);
      if (listElementType.isNested()) {
        throw unsupportedExpressionException(
            in,
            String.format(
                "IN expression does not support nested types at position %d. Element type: %s. "
                    + "Only primitive types (numeric, string, boolean, date, timestamp, binary) "
                    + "are supported.",
                i + 1, listElementType));
      }
    }

    try {
      return TypeResolver.findCommonType(valueDataType, inListDataTypes);
    } catch (IllegalArgumentException e) {
      // Provide enhanced error messages with specific guidance
      throw createTypeCompatibilityError(in, valueDataType, inListDataTypes, e);
    }
  }

  /**
   * Creates a detailed error message for type compatibility issues in IN expressions. Provides
   * specific guidance about implicit casting and supported type combinations.
   */
  private static UnsupportedOperationException createTypeCompatibilityError(
      In in,
      DataType valueDataType,
      List<DataType> inListDataTypes,
      IllegalArgumentException originalError) {

    // Find the first incompatible type to provide specific guidance
    for (int i = 0; i < inListDataTypes.size(); i++) {
      DataType listElementType = inListDataTypes.get(i);

      if (!valueDataType.equivalent(listElementType)) {
        // Check if both are numeric types
        if (TypeResolver.isNumericType(valueDataType)
            && TypeResolver.isNumericType(listElementType)) {
          // This should not happen as numeric types should be compatible, but provide guidance
          return unsupportedExpressionException(
              in,
              String.format(
                  "Cannot find common numeric type for IN expression. "
                      + "Value type: %s, incompatible element at position %d: %s. "
                      + "This may indicate a precision or range compatibility issue. "
                      + "Supported numeric type hierarchy: "
                      + "byte → short → integer → long → float → double.",
                  valueDataType, i + 1, listElementType));
        }

        // Check if one is numeric and the other is not
        if (TypeResolver.isNumericType(valueDataType)
            || TypeResolver.isNumericType(listElementType)) {
          return unsupportedExpressionException(
              in,
              String.format(
                  "Cannot find common type for IN expression. "
                      + "Value type: %s, incompatible element at position %d: %s. "
                      + "No implicit cast available between numeric and non-numeric types. "
                      + "Consider using explicit casting or ensuring all operands are "
                      + "of compatible types.",
                  valueDataType, i + 1, listElementType));
        }

        // Both are non-numeric types
        return unsupportedExpressionException(
            in,
            String.format(
                "Cannot find common type for IN expression. "
                    + "Value type: %s, incompatible element at position %d: %s. "
                    + "Non-numeric types must be exactly equivalent "
                    + "(no implicit casting available). "
                    + "Supported type families: numeric (byte, short, integer, long, "
                    + "float, double), string, boolean, date, timestamp, binary.",
                valueDataType, i + 1, listElementType));
      }
    }

    // Fallback to original error message if we couldn't identify the specific issue
    return unsupportedExpressionException(
        in,
        String.format(
            "Cannot find common type for IN expression. %s "
                + "Ensure all operands are of compatible types. "
                + "Implicit casting is supported within numeric types "
                + "(byte → short → integer → long → float → double). "
                + "Other types must be exactly equivalent.",
            originalError.getMessage()));
  }

  /** Applies an implicit cast to the given expression if needed. */
  private static Expression applyCastIfNeeded(
      Expression expression, DataType fromType, DataType toType) {
    if (fromType.equivalent(toType)) {
      return expression;
    }
    return new ImplicitCastExpression(expression, toType);
  }

  /** Applies implicit casts to all expressions in the list if needed. */
  private static List<Expression> applyListCasts(
      List<Expression> expressions, List<DataType> fromTypes, DataType toType) {
    List<Expression> transformedExpressions = new ArrayList<>();
    for (int i = 0; i < expressions.size(); i++) {
      Expression transformed = applyCastIfNeeded(expressions.get(i), fromTypes.get(i), toType);
      transformedExpressions.add(transformed);
    }
    return transformedExpressions;
  }

  /** Validates that collation is only used with string types and the collation is UTF8Binary. */
  private static void validateCollation(
      In in,
      DataType valueDataType,
      List<Expression> inListExpressions,
      List<DataType> inListDataTypes) {
    in.getCollationIdentifier()
        .ifPresent(
            collationIdentifier -> {
              checkIsUTF8BinaryCollation(in, collationIdentifier);
              validateStringTypesForCollation(
                  in, valueDataType, inListExpressions, inListDataTypes);
            });
  }

  private static void validateStringTypesForCollation(
      In in,
      DataType valueDataType,
      List<Expression> inListExpressions,
      List<DataType> inListDataTypes) {
    checkIsStringType(
        valueDataType, in, "'IN' with collation expects STRING type for the value expression");
    for (int i = 0; i < inListDataTypes.size(); i++) {
      if (!isNullLiteral(inListExpressions.get(i))) {
        checkIsStringType(
            inListDataTypes.get(i),
            in,
            "'IN' with collation expects STRING type for all list elements");
      }
    }
  }

  private static void checkIsUTF8BinaryCollation(In in, CollationIdentifier collationIdentifier) {
    if (!"SPARK.UTF8_BINARY".equals(collationIdentifier.toString())) {
      throw unsupportedExpressionException(
          in,
          String.format(
              "Unsupported collation: \"%s\". "
                  + "Default Engine supports only \"SPARK.UTF8_BINARY\" collation "
                  + "for IN expressions. "
                  + "Please use UTF8_BINARY collation or remove collation specification.",
              collationIdentifier));
    }
  }

  private static void checkIsStringType(DataType dataType, In in, String message) {
    if (!(dataType instanceof StringType)) {
      throw unsupportedExpressionException(
          in,
          String.format(
              "%s Found type: %s. "
                  + "Collation can only be applied to STRING types in IN expressions.",
              message, dataType));
    }
  }

  /**
   * Compares two values for equality after implicit casting has been applied. Since all values have
   * been cast to the same type, we can use simple equality comparison.
   */
  private static boolean compareValues(Object value1, Object value2) {
    // After implicit casting, both values are of the same type, so we can use Objects.equals
    return Objects.equals(value1, value2);
  }

  /** Column vector implementation for IN expression evaluation. */
  private static class InColumnVector implements ColumnVector {
    private final ColumnVector valueVector;
    private final List<ColumnVector> inListVectors;

    InColumnVector(List<ColumnVector> childrenVectors) {
      this.valueVector = childrenVectors.get(0);
      this.inListVectors = childrenVectors.subList(1, childrenVectors.size());

      // Validate type compatibility once during construction rather than for each row
      DataType valueType = valueVector.getDataType();
      for (ColumnVector inListVector : inListVectors) {
        Preconditions.checkArgument(
            valueType.equivalent(inListVector.getDataType()),
            String.format(
                "Type mismatch in IN expression: value type %s is not equivalent to "
                    + "list element type %s",
                valueType, inListVector.getDataType()));
      }
    }

    @Override
    public DataType getDataType() {
      return BooleanType.BOOLEAN;
    }

    @Override
    public int getSize() {
      return valueVector.getSize();
    }

    @Override
    public void close() {
      Utils.closeCloseables(valueVector);
      inListVectors.forEach(Utils::closeCloseables);
    }

    @Override
    public boolean getBoolean(int rowId) {
      Optional<Boolean> result = evaluateInLogic(rowId);
      Preconditions.checkArgument(
          result.isPresent(), "This method is expected to be called only when isNullAt is false");
      return result.get();
    }

    @Override
    public boolean isNullAt(int rowId) {
      return !evaluateInLogic(rowId).isPresent();
    }

    private Optional<Boolean> evaluateInLogic(int rowId) {
      if (valueVector.isNullAt(rowId)) {
        return Optional.empty();
      }

      Object valueToFind =
          VectorUtils.getValueAsObject(valueVector, valueVector.getDataType(), rowId);

      // Track if we encounter any null values in the IN list
      // SQL semantics:
      // - If value matches any element, return true (e.g., 5 IN {0, 4, null, 5} = true)
      // - If value is null OR (no matches found AND any null in list), return null
      //   (e.g., null IN {1, 2} = null, 3 IN {1, null, 2} = null)
      // - If value is not null AND no matches found AND no nulls in list, return false
      //   (e.g., 3 IN {1, 2} = false)
      boolean foundNull = false;

      for (ColumnVector inListVector : inListVectors) {
        if (inListVector.isNullAt(rowId)) {
          foundNull = true;
        } else {
          Object inListValue =
              VectorUtils.getValueAsObject(inListVector, inListVector.getDataType(), rowId);
          if (compareValues(valueToFind, inListValue)) {
            return Optional.of(true);
          }
        }
      }
      return foundNull ? Optional.empty() : Optional.of(false);
    }
  }

  private InExpressionEvaluator() {}
}
