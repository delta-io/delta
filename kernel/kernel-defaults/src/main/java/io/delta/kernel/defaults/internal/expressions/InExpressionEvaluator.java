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
import java.math.BigDecimal;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** Utility methods to evaluate {@code IN} expression. */
public class InExpressionEvaluator {

  private static final Set<Class<? extends DataType>> NUMERIC_TYPES =
      Stream.of(
              ByteType.class,
              ShortType.class,
              IntegerType.class,
              LongType.class,
              FloatType.class,
              DoubleType.class,
              DecimalType.class)
          .collect(Collectors.toSet());

  private static final Set<Class<? extends DataType>> TIMESTAMP_TYPES =
      Stream.of(TimestampType.class, TimestampNTZType.class).collect(Collectors.toSet());

  private static final Map<Class<? extends DataType>, BiFunction<Object, Object, Integer>>
      COMPARATORS = createComparatorMap();

  private static Map<Class<? extends DataType>, BiFunction<Object, Object, Integer>>
      createComparatorMap() {
    Map<Class<? extends DataType>, BiFunction<Object, Object, Integer>> map = new HashMap<>();
    map.put(BooleanType.class, (v1, v2) -> Boolean.compare((Boolean) v1, (Boolean) v2));
    map.put(
        ByteType.class,
        (v1, v2) -> Byte.compare(((Number) v1).byteValue(), ((Number) v2).byteValue()));
    map.put(
        ShortType.class,
        (v1, v2) -> Short.compare(((Number) v1).shortValue(), ((Number) v2).shortValue()));
    map.put(
        IntegerType.class,
        (v1, v2) -> Integer.compare(((Number) v1).intValue(), ((Number) v2).intValue()));
    map.put(
        DateType.class,
        (v1, v2) -> Integer.compare(((Number) v1).intValue(), ((Number) v2).intValue()));
    map.put(
        LongType.class,
        (v1, v2) -> Long.compare(((Number) v1).longValue(), ((Number) v2).longValue()));
    map.put(
        TimestampType.class,
        (v1, v2) -> Long.compare(((Number) v1).longValue(), ((Number) v2).longValue()));
    map.put(
        TimestampNTZType.class,
        (v1, v2) -> Long.compare(((Number) v1).longValue(), ((Number) v2).longValue()));
    map.put(
        FloatType.class,
        (v1, v2) -> Float.compare(((Number) v1).floatValue(), ((Number) v2).floatValue()));
    map.put(
        DoubleType.class,
        (v1, v2) -> Double.compare(((Number) v1).doubleValue(), ((Number) v2).doubleValue()));
    map.put(
        DecimalType.class,
        (v1, v2) -> BIGDECIMAL_COMPARATOR.compare((BigDecimal) v1, (BigDecimal) v2));
    map.put(StringType.class, (v1, v2) -> STRING_COMPARATOR.compare((String) v1, (String) v2));
    map.put(BinaryType.class, (v1, v2) -> BINARY_COMPARTOR.compare((byte[]) v1, (byte[]) v2));
    return Collections.unmodifiableMap(map);
  }

  /** Validates and transforms the {@code IN} expression. */
  static In validateAndTransform(
      In in,
      Expression valueExpression,
      DataType valueDataType,
      List<Expression> inListExpressions,
      List<DataType> inListDataTypes) {
    validateArgumentCount(in, inListExpressions);
    validateInListElementsAreLiterals(in, inListExpressions);
    validateTypeCompatibility(in, valueDataType, inListDataTypes);
    validateCollation(in, valueDataType, inListExpressions, inListDataTypes);
    if (in.getCollationIdentifier().isPresent()) {
      return new In(valueExpression, inListExpressions, in.getCollationIdentifier().get());
    } else {
      return new In(valueExpression, inListExpressions);
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
    if (inListExpressions.isEmpty()) {
      throw unsupportedExpressionException(
          in,
          "IN expression requires at least 1 element in the IN list. "
              + "Example usage: column IN (value1, value2, ...)");
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
                    + "Only constant values are currently supported in IN lists.",
                i + 1, child.getClass().getSimpleName()));
      }
    }
  }

  private static void validateTypeCompatibility(
      In in, DataType valueDataType, List<DataType> inListDataTypes) {
    for (int i = 0; i < inListDataTypes.size(); i++) {
      DataType listElementType = inListDataTypes.get(i);
      if (!areTypesCompatible(valueDataType, listElementType)) {
        throw unsupportedExpressionException(
            in,
            String.format(
                "IN expression requires all elements to have compatible types. "
                    + "Value type: %s, incompatible element type at position %d: %s. "
                    + "Consider casting the incompatible element to a compatible type.",
                valueDataType, i + 1, listElementType));
      }
    }
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

  private static boolean isNullLiteral(Expression expression) {
    return expression instanceof Literal && ((Literal) expression).getValue() == null;
  }

  private static void checkIsUTF8BinaryCollation(In in, CollationIdentifier collationIdentifier) {
    if (!"SPARK.UTF8_BINARY".equals(collationIdentifier.toString())) {
      throw unsupportedExpressionException(
          in,
          String.format(
              "Unsupported collation: \"%s\". "
                  + "Default Engine supports just \"SPARK.UTF8_BINARY\" collation.",
              collationIdentifier));
    }
  }

  private static void checkIsStringType(DataType dataType, In in, String message) {
    if (!(dataType instanceof StringType)) {
      throw unsupportedExpressionException(in, message);
    }
  }

  private static boolean areTypesCompatible(DataType type1, DataType type2) {
    return type1.equivalent(type2) || belongToSameTypeGroup(type1, type2);
  }

  private static boolean belongToSameTypeGroup(DataType type1, DataType type2) {
    return (NUMERIC_TYPES.contains(type1.getClass()) && NUMERIC_TYPES.contains(type2.getClass()))
        || (TIMESTAMP_TYPES.contains(type1.getClass())
            && TIMESTAMP_TYPES.contains(type2.getClass()));
  }

  private static boolean compareValues(Object value1, Object value2, DataType valueType) {
    if (value1 == null || value2 == null) {
      return false;
    }
    // For numeric types, convert both values to the same type for comparison
    if (NUMERIC_TYPES.contains(valueType.getClass())) {
      return compareNumericValues(value1, value2, valueType);
    }
    return getComparator(valueType).apply(value1, value2) == 0;
  }

  /**
   * Compares numeric values by converting both to the target type. This method handles mixed
   * numeric type comparisons by converting both values to the target type and using the appropriate
   * comparator.
   */
  private static boolean compareNumericValues(
      Object value1, Object value2, DataType targetTypeToCompare) {
    Number num1 = (Number) value1;
    Number num2 = (Number) value2;
    Object convertedValue1 = convertToTargetTypeForCompare(num1, targetTypeToCompare);
    Object convertedValue2 = convertToTargetTypeForCompare(num2, targetTypeToCompare);
    return getComparator(targetTypeToCompare).apply(convertedValue1, convertedValue2) == 0;
  }

  private static Object convertToTargetTypeForCompare(Number value, DataType targetType) {
    if (targetType instanceof ByteType) {
      return value.byteValue();
    } else if (targetType instanceof ShortType) {
      return value.shortValue();
    } else if (targetType instanceof IntegerType || targetType instanceof DateType) {
      return value.intValue();
    } else if (targetType instanceof LongType) {
      return value.longValue();
    } else if (targetType instanceof FloatType) {
      return value.floatValue();
    } else if (targetType instanceof DoubleType) {
      return value.doubleValue();
    } else if (targetType instanceof DecimalType) {
      // For decimal, convert to BigDecimal for precise comparison
      return (value instanceof BigDecimal) ? (BigDecimal) value : new BigDecimal(value.toString());
    }
    return value;
  }

  private static BiFunction<Object, Object, Integer> getComparator(DataType dataType) {
    BiFunction<Object, Object, Integer> comparator = COMPARATORS.get(dataType.getClass());
    if (comparator == null) {
      throw new UnsupportedOperationException(
          "No comparator available for data type: " + dataType.getClass().getSimpleName());
    }
    return comparator;
  }

  /** Column vector implementation for IN expression evaluation. */
  private static class InColumnVector implements ColumnVector {
    private final ColumnVector valueVector;
    private final List<ColumnVector> inListVectors;

    InColumnVector(List<ColumnVector> childrenVectors) {
      this.valueVector = childrenVectors.get(0);
      this.inListVectors = childrenVectors.subList(1, childrenVectors.size());
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
      // SQL semantics: if value is null OR any comparison is null, result is null
      // Only return false if value is not null and no matches found and no nulls encountered
      boolean foundNull = false;

      for (ColumnVector inListVector : inListVectors) {
        if (inListVector.isNullAt(rowId)) {
          foundNull = true;
        } else {
          Object inListValue =
              VectorUtils.getValueAsObject(inListVector, inListVector.getDataType(), rowId);
          if (compareValues(valueToFind, inListValue, valueVector.getDataType())) {
            return Optional.of(true);
          }
        }
      }

      return foundNull ? Optional.empty() : Optional.of(false);
    }
  }

  private InExpressionEvaluator() {}
}
