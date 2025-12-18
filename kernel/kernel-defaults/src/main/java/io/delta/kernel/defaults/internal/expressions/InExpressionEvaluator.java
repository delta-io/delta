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
import java.util.*;
import java.util.function.BiFunction;

/** Utility methods to evaluate {@code IN} expression. */
public class InExpressionEvaluator {

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
    // TODO: [delta-io/delta#5227] Try to reuse Implicit cast and simplify comparison logic
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
    Objects.requireNonNull(inListExpressions);
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
    // Check for nested types which are not supported
    if (valueDataType.isNested()) {
      throw unsupportedExpressionException(
          in, String.format("IN expression does not support nested types.", valueDataType));
    }

    for (int i = 0; i < inListDataTypes.size(); i++) {
      DataType listElementType = inListDataTypes.get(i);
      if (!valueDataType.equivalent(listElementType)) {
        throw unsupportedExpressionException(
            in,
            String.format(
                "IN expression requires all list elements to match the value type. "
                    + "Value type: %s, but found incompatible element type at position %d: %s. "
                    + "Consider casting the incompatible element to the value type.",
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

  private static boolean compareValues(Object value1, Object value2, DataType valueType) {
    Preconditions.checkArgument(value1 != null || value2 != null);
    if (value1 == null || value2 == null) {
      return false;
    }
    return getComparator(valueType).apply(value1, value2) == 0;
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
