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
package io.delta.kernel.defaults.internal.expressions;

import static io.delta.kernel.internal.util.Preconditions.checkArgument;

import io.delta.kernel.data.ArrayValue;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.MapValue;
import io.delta.kernel.expressions.Expression;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.types.*;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.IntPredicate;
import java.util.stream.Collectors;

/** Utility methods used by the default expression evaluator. */
class DefaultExpressionUtils {

  static final Comparator<BigDecimal> BIGDECIMAL_COMPARATOR = Comparator.naturalOrder();
  static final Comparator<byte[]> BINARY_COMPARTOR =
      (leftOp, rightOp) -> {
        int i = 0;
        while (i < leftOp.length && i < rightOp.length) {
          if (leftOp[i] != rightOp[i]) {
            return Byte.toUnsignedInt(leftOp[i]) - Byte.toUnsignedInt(rightOp[i]);
          }
          i++;
        }
        return Integer.compare(leftOp.length, rightOp.length);
      };
  static final Comparator<String> STRING_COMPARATOR =
      (leftOp, rightOp) -> {
        byte[] leftBytes = leftOp.getBytes(StandardCharsets.UTF_8);
        byte[] rightBytes = rightOp.getBytes(StandardCharsets.UTF_8);
        return BINARY_COMPARTOR.compare(leftBytes, rightBytes);
      };

  private DefaultExpressionUtils() {}

  /**
   * Utility method that calculates the nullability result from given two vectors. Result is null if
   * at least one side is a null.
   */
  static boolean[] evalNullability(ColumnVector left, ColumnVector right) {
    int numRows = left.getSize();
    boolean[] nullability = new boolean[numRows];
    for (int rowId = 0; rowId < numRows; rowId++) {
      nullability[rowId] = left.isNullAt(rowId) || right.isNullAt(rowId);
    }
    return nullability;
  }

  /**
   * Wraps a child vector as a boolean {@link ColumnVector} with the given value and nullability
   * accessors.
   */
  static ColumnVector booleanWrapperVector(
      ColumnVector childVector,
      Function<Integer, Boolean> valueAccessor,
      Function<Integer, Boolean> nullabilityAccessor) {

    return new ColumnVector() {

      @Override
      public DataType getDataType() {
        return BooleanType.BOOLEAN;
      }

      @Override
      public int getSize() {
        return childVector.getSize();
      }

      @Override
      public void close() {
        childVector.close();
      }

      @Override
      public boolean isNullAt(int rowId) {
        return nullabilityAccessor.apply(rowId);
      }

      @Override
      public boolean getBoolean(int rowId) {
        return valueAccessor.apply(rowId);
      }
    };
  }

  /**
   * Utility method for getting value comparator
   *
   * @param left
   * @param right
   * @param booleanComparator
   * @return
   */
  static IntPredicate getComparator(
      ColumnVector left, ColumnVector right, IntPredicate booleanComparator, Optional<Collation> collation) {
    checkArgument(
        left.getSize() == right.getSize(), "Left and right operand have different vector sizes.");

    DataType dataType = left.getDataType();
    IntPredicate vectorValueComparator;
    if (dataType instanceof BooleanType) {
      vectorValueComparator =
          rowId ->
              booleanComparator.test(
                  Boolean.compare(left.getBoolean(rowId), right.getBoolean(rowId)));
    } else if (dataType instanceof ByteType) {
      vectorValueComparator =
          rowId -> booleanComparator.test(Byte.compare(left.getByte(rowId), right.getByte(rowId)));
    } else if (dataType instanceof ShortType) {
      vectorValueComparator =
          rowId ->
              booleanComparator.test(Short.compare(left.getShort(rowId), right.getShort(rowId)));
    } else if (dataType instanceof IntegerType || dataType instanceof DateType) {
      vectorValueComparator =
          rowId -> booleanComparator.test(Integer.compare(left.getInt(rowId), right.getInt(rowId)));
    } else if (dataType instanceof LongType
        || dataType instanceof TimestampType
        || dataType instanceof TimestampNTZType) {
      vectorValueComparator =
          rowId -> booleanComparator.test(Long.compare(left.getLong(rowId), right.getLong(rowId)));
    } else if (dataType instanceof FloatType) {
      vectorValueComparator =
          rowId ->
              booleanComparator.test(Float.compare(left.getFloat(rowId), right.getFloat(rowId)));
    } else if (dataType instanceof DoubleType) {
      vectorValueComparator =
          rowId ->
              booleanComparator.test(Double.compare(left.getDouble(rowId), right.getDouble(rowId)));
    } else if (dataType instanceof DecimalType) {
      vectorValueComparator =
          rowId ->
              booleanComparator.test(
                  BIGDECIMAL_COMPARATOR.compare(left.getDecimal(rowId), right.getDecimal(rowId)));
    } else if (dataType instanceof StringType) {
      if (collation.isPresent() && collation.get() != Collation.DEFAULT_COLLATION) {
        vectorValueComparator =
            rowId ->
                booleanComparator.test(
                    collation.get().getComparator().compare(
                        left.getString(rowId),
                        right.getString(rowId)));
      } else {
        vectorValueComparator =
            rowId ->
                booleanComparator.test(
                    STRING_COMPARATOR.compare(
                        left.getString(rowId),
                        right.getString(rowId)));
      }
    } else if (dataType instanceof BinaryType) {
      vectorValueComparator =
          rowId ->
              booleanComparator.test(
                  BINARY_COMPARTOR.compare(left.getBinary(rowId), right.getBinary(rowId)));
    } else {
      throw new UnsupportedOperationException(dataType + " can not be compared.");
    }

    return vectorValueComparator;
  }

  /**
   * Utility method to create a column vector that lazily evaluate the comparator ex. (ie. ==, >=,
   * <=......) for left and right column vector according to the natural ordering of numbers
   *
   * <p>Only primitive data types are supported.
   */
  static ColumnVector comparatorVector(
          ColumnVector left, ColumnVector right, IntPredicate booleanComparator, Optional<Collation> collation) {
    IntPredicate vectorValueComparator = getComparator(left, right, booleanComparator, collation);

    return new ColumnVector() {

      @Override
      public DataType getDataType() {
        return BooleanType.BOOLEAN;
      }

      @Override
      public void close() {
        Utils.closeCloseables(left, right);
      }

      @Override
      public int getSize() {
        return left.getSize();
      }

      @Override
      public boolean isNullAt(int rowId) {
        return left.isNullAt(rowId) || right.isNullAt(rowId);
      }

      @Override
      public boolean getBoolean(int rowId) {
        if (isNullAt(rowId)) {
          return false;
        }
        return vectorValueComparator.test(rowId);
      }
    };
  }

  /**
   * Utility method to create a null safe column vector that lazily evaluate the comparator ex. (ie.
   * <=>) for left and right column vector according to the natural ordering of numbers
   *
   * <p>Only primitive data types are supported.
   */
  static ColumnVector nullSafeComparatorVector(
      ColumnVector left, ColumnVector right, IntPredicate booleanComparator, Optional<Collation> collation) {
    IntPredicate vectorValueComparator = getComparator(left, right, booleanComparator, collation);
    return new ColumnVector() {
      @Override
      public DataType getDataType() {
        return BooleanType.BOOLEAN;
      }

      @Override
      public void close() {
        Utils.closeCloseables(left, right);
      }

      @Override
      public int getSize() {
        return left.getSize();
      }

      @Override
      public boolean isNullAt(int rowId) {
        // Nullsafe comparator can never return null
        return false;
      }

      /**
       * Null safe comparator follows the truth table in Comparison Operators part of following link
       * https://spark.apache.org/docs/latest/sql-ref-null-semantics.html
       *
       * <p>If either left or right is null, return false If both left and right is null, return
       * true else compare the non null value of left and right
       *
       * @param rowId
       * @return
       */
      @Override
      public boolean getBoolean(int rowId) {
        if (left.isNullAt(rowId) && right.isNullAt(rowId)) {
          return true;
        } else if (left.isNullAt(rowId) || right.isNullAt(rowId)) {
          return false;
        }
        return vectorValueComparator.test(rowId);
      }
    };
  }

  static Expression childAt(Expression expression, int index) {
    return expression.getChildren().get(index);
  }

  /**
   * Combines a list of column vectors into one column vector based on the resolution of idxToReturn
   *
   * @param vectors List of ColumnVectors of the same data type with length >= 1
   * @param idxToReturn Function that takes in a rowId and returns the index of the column vector to
   *     use as the return value
   */
  static ColumnVector combinationVector(
      List<ColumnVector> vectors, Function<Integer, Integer> idxToReturn) {
    return new ColumnVector() {
      // Store the last lookup value to avoid multiple looks up for same rowId.
      // The general pattern is call `isNullAt(rowId)` followed by `getBoolean(rowId)` or
      // some other value accessor. So the cache of one value is enough.
      private int lastLookupRowId = -1;
      private ColumnVector lastLookupVector = null;

      @Override
      public DataType getDataType() {
        return vectors.get(0).getDataType();
      }

      @Override
      public int getSize() {
        return vectors.get(0).getSize();
      }

      @Override
      public void close() {
        Utils.closeCloseables(vectors.toArray(new ColumnVector[0]));
      }

      @Override
      public boolean isNullAt(int rowId) {
        return getVector(rowId).isNullAt(rowId);
      }

      @Override
      public boolean getBoolean(int rowId) {
        return getVector(rowId).getBoolean(rowId);
      }

      @Override
      public byte getByte(int rowId) {
        return getVector(rowId).getByte(rowId);
      }

      @Override
      public short getShort(int rowId) {
        return getVector(rowId).getShort(rowId);
      }

      @Override
      public int getInt(int rowId) {
        return getVector(rowId).getInt(rowId);
      }

      @Override
      public long getLong(int rowId) {
        return getVector(rowId).getLong(rowId);
      }

      @Override
      public float getFloat(int rowId) {
        return getVector(rowId).getFloat(rowId);
      }

      @Override
      public double getDouble(int rowId) {
        return getVector(rowId).getDouble(rowId);
      }

      @Override
      public byte[] getBinary(int rowId) {
        return getVector(rowId).getBinary(rowId);
      }

      @Override
      public String getString(int rowId) {
        return getVector(rowId).getString(rowId);
      }

      @Override
      public BigDecimal getDecimal(int rowId) {
        return getVector(rowId).getDecimal(rowId);
      }

      @Override
      public MapValue getMap(int rowId) {
        return getVector(rowId).getMap(rowId);
      }

      @Override
      public ArrayValue getArray(int rowId) {
        return getVector(rowId).getArray(rowId);
      }

      @Override
      public ColumnVector getChild(int ordinal) {
        return combinationVector(
            vectors.stream().map(v -> v.getChild(ordinal)).collect(Collectors.toList()),
            idxToReturn);
      }

      private ColumnVector getVector(int rowId) {
        if (rowId == lastLookupRowId) {
          return lastLookupVector;
        }
        lastLookupRowId = rowId;
        lastLookupVector = vectors.get(idxToReturn.apply(rowId));
        return lastLookupVector;
      }
    };
  }
}
