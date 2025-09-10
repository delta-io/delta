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
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.internal.util.VectorUtils;
import io.delta.kernel.types.*;
import java.math.BigDecimal;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/** Utility methods to evaluate {@code IN} expression. */
public class InExpressionEvaluator {

  private static final int MIN_ARGUMENT_COUNT = 2;

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

  private InExpressionEvaluator() {}

  /** Validates and transforms the {@code IN} expression. */
  static Predicate validateAndTransform(
      Predicate in, List<Expression> childrenExpressions, List<DataType> childrenOutputTypes) {

    validateArgumentCount(in, childrenExpressions);
    validateLiteralListChildren(in, childrenExpressions);
    validateTypeCompatibility(in, childrenOutputTypes);
    validateCollation(in, childrenExpressions, childrenOutputTypes);

    return createPredicate(in.getName(), childrenExpressions, in.getCollationIdentifier());
  }

  /** Evaluates the IN expression on the given column vectors. */
  static ColumnVector eval(List<ColumnVector> childrenVectors) {
    return new InColumnVector(childrenVectors);
  }

  // Private validation methods
  private static void validateArgumentCount(Predicate in, List<Expression> childrenExpressions) {
    if (childrenExpressions.size() < MIN_ARGUMENT_COUNT) {
      throw unsupportedExpressionException(
          in,
          "IN expression requires at least 2 arguments: value and at least one element "
              + "in the list. Example usage: column IN (value1, value2, ...)");
    }
  }

  private static void validateLiteralListChildren(
      Predicate in, List<Expression> childrenExpressions) {
    // Skip the first child (value expression) and validate that all list elements are literals
    for (int i = 1; i < childrenExpressions.size(); i++) {
      Expression child = childrenExpressions.get(i);
      if (!(child instanceof Literal)) {
        throw unsupportedExpressionException(
            in,
            String.format(
                "IN expression requires all list elements to be literals. "
                    + "Non-literal expression found at position %d: %s. "
                    + "Only constant values are currently supported in IN lists.",
                i, child.getClass().getSimpleName()));
      }
    }
  }

  private static void validateTypeCompatibility(Predicate in, List<DataType> childrenOutputTypes) {
    DataType valueType = childrenOutputTypes.get(0);

    IntStream.range(1, childrenOutputTypes.size())
        .forEach(
            i -> {
              DataType listElementType = childrenOutputTypes.get(i);
              if (!areTypesCompatible(valueType, listElementType)) {
                throw unsupportedExpressionException(
                    in,
                    String.format(
                        "IN expression requires all elements to have compatible types. "
                            + "Value type: %s, incompatible element type at position %d: %s. "
                            + "Consider casting the incompatible element to a compatible type.",
                        valueType, i, listElementType));
              }
            });
  }

  private static void validateCollation(
      Predicate in, List<Expression> childrenExpressions, List<DataType> childrenOutputTypes) {
    in.getCollationIdentifier()
        .ifPresent(
            collationIdentifier -> {
              checkIsUTF8BinaryCollation(in, collationIdentifier);
              validateStringTypesForCollation(in, childrenExpressions, childrenOutputTypes);
            });
  }

  private static void validateStringTypesForCollation(
      Predicate in, List<Expression> childrenExpressions, List<DataType> childrenOutputTypes) {

    checkIsStringType(
        childrenOutputTypes.get(0),
        in,
        "'IN' with collation expects STRING type for the value expression");

    for (int i = 1; i < childrenOutputTypes.size(); i++) {
      if (!isNullLiteral(childrenExpressions.get(i))) {
        checkIsStringType(
            childrenOutputTypes.get(i),
            in,
            "'IN' with collation expects STRING type for all list elements");
      }
    }
  }

  private static boolean isNullLiteral(Expression expression) {
    return expression instanceof Literal && ((Literal) expression).getValue() == null;
  }

  // Type compatibility logic
  private static boolean areTypesCompatible(DataType type1, DataType type2) {
    return type1.equivalent(type2) || belongToSameTypeGroup(type1, type2);
  }

  private static boolean belongToSameTypeGroup(DataType type1, DataType type2) {
    return (NUMERIC_TYPES.contains(type1.getClass()) && NUMERIC_TYPES.contains(type2.getClass()))
        || (TIMESTAMP_TYPES.contains(type1.getClass())
            && TIMESTAMP_TYPES.contains(type2.getClass()));
  }

  // Static comparator cache for performance optimization
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

  // Comparison logic
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

  private static boolean compareNumericValues(Object value1, Object value2, DataType targetType) {
    // Convert both values to the target type for comparison
    Number num1 = (Number) value1;
    Number num2 = (Number) value2;

    if (targetType instanceof ByteType) {
      return num1.byteValue() == num2.byteValue();
    } else if (targetType instanceof ShortType) {
      return num1.shortValue() == num2.shortValue();
    } else if (targetType instanceof IntegerType || targetType instanceof DateType) {
      return num1.intValue() == num2.intValue();
    } else if (targetType instanceof LongType) {
      return num1.longValue() == num2.longValue();
    } else if (targetType instanceof FloatType) {
      return Float.compare(num1.floatValue(), num2.floatValue()) == 0;
    } else if (targetType instanceof DoubleType) {
      return Double.compare(num1.doubleValue(), num2.doubleValue()) == 0;
    } else if (targetType instanceof DecimalType) {
      // For decimal, convert to BigDecimal for precise comparison
      BigDecimal bd1 =
          (value1 instanceof BigDecimal) ? (BigDecimal) value1 : new BigDecimal(num1.toString());
      BigDecimal bd2 =
          (value2 instanceof BigDecimal) ? (BigDecimal) value2 : new BigDecimal(num2.toString());
      return bd1.compareTo(bd2) == 0;
    }

    // Fallback to object comparison
    return value1.equals(value2);
  }

  private static BiFunction<Object, Object, Integer> getComparator(DataType dataType) {
    BiFunction<Object, Object, Integer> comparator = COMPARATORS.get(dataType.getClass());
    if (comparator != null) {
      return comparator;
    }
    // Throw exception for unsupported types to surface programming errors early
    throw new UnsupportedOperationException(
        "No comparator available for data type: " + dataType.getClass().getSimpleName());
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
      return evaluateInLogic(rowId).orElse(false);
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
}
