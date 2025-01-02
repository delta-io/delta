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

import static io.delta.kernel.defaults.internal.DefaultEngineErrors.unsupportedExpressionException;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.expressions.Expression;
import io.delta.kernel.expressions.ScalarExpression;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.types.*;
import java.util.*;

/** Utility methods to evaluate {@code substring} expression. */
public class SubstringEvaluator {

  private SubstringEvaluator() {}

  /** Validates and transforms the {@code substring} expression. */
  static ScalarExpression validateAndTransform(
      ScalarExpression substring,
      List<Expression> childrenExpressions,
      List<DataType> childrenOutputTypes) {
    int childrenSize = substring.getChildren().size();
    if (childrenSize < 2 || childrenSize > 3) {
      throw unsupportedExpressionException(
          substring,
          "Invalid number of inputs to SUBSTRING expression. "
              + "Example usage: SUBSTRING(column, pos), SUBSTRING(column, pos, len)");
    }

    // TODO: support binary type.
    if (!StringType.STRING.equals(childrenOutputTypes.get(0))) {
      throw unsupportedExpressionException(
          substring, "Invalid type of first input of SUBSTRING: expects STRING");
    }

    Expression posExpression = childrenExpressions.get(1);
    DefaultExpressionUtils.checkIntegerLiteral(
        posExpression, /* context= */ "Invalid `pos` argument type for SUBSTRING", substring);
    if (childrenSize == 3) {
      Expression lengthExpression = childrenExpressions.get(2);
      DefaultExpressionUtils.checkIntegerLiteral(
          lengthExpression, /* context= */ "Invalid `len` argument type for SUBSTRING", substring);
    }
    return new ScalarExpression(substring.getName(), childrenExpressions);
  }

  /**
   * Evaluates the {@code substring} expression for given input column vector, builds a column
   * vector with substring applied to each row.
   */
  static ColumnVector eval(List<ColumnVector> childrenVectors) {
    return new ColumnVector() {
      final ColumnVector input = childrenVectors.get(0);
      final ColumnVector positionVector = childrenVectors.get(1);
      final Optional<ColumnVector> lengthVector =
          childrenVectors.size() > 2 ? Optional.of(childrenVectors.get(2)) : Optional.empty();

      @Override
      public DataType getDataType() {
        return StringType.STRING;
      }

      @Override
      public int getSize() {
        return input.getSize();
      }

      @Override
      public void close() {
        // Utils.closeCloseables method will ignore the null element.
        Utils.closeCloseables(input, positionVector, lengthVector.orElse(null));
      }

      @Override
      public boolean isNullAt(int rowId) {
        if (rowId < 0 || rowId >= getSize()) {
          throw new IllegalArgumentException(
              String.format(
                  "Unexpected rowId %d, expected between 0 and the size of the column vector",
                  rowId));
        }
        return input.isNullAt(rowId);
      }

      @Override
      public String getString(int rowId) {
        if (isNullAt(rowId)) {
          return null;
        }

        String inputString = input.getString(rowId);
        int position = positionVector.getInt(rowId);
        Optional<Integer> length = lengthVector.map(columnVector -> columnVector.getInt(rowId));
        if (position > getStringLengthWithCodePoint(inputString)
            || (length.isPresent() && length.get() < 1)) {
          return "";
        }
        int startPosition = buildStartPosition(inputString, position);
        int startIndex = Math.max(startPosition, 0);
        return length
            .map(
                len -> {
                  // endIndex should be less than the length of input string, but positive.
                  // e.g. Substring("aaa", -100, 95), should be read as Substring("aaa", 0, 0)
                  int endIndex =
                      Math.min(
                          getStringLengthWithCodePoint(inputString),
                          Math.max(startPosition + len, 0));
                  return subStringWithCodePoint(inputString, startIndex, Optional.of(endIndex));
                })
            .orElse(subStringWithCodePoint(inputString, startIndex, Optional.empty()));
      }
    };
  }

  /**
   * Computes the start position following Hive and SQL's one-based indexing for substring.
   *
   * @param pos, pos can be positive, in which case the startIndex is computed from the left end of
   *     the string. Otherwise, the startIndex is computed from the right end of the string.
   * @return the position of inputString to compute the substring. The returned value can fall
   *     beyond the input index valid range. For example, pos could be larger than the inputString's
   *     length or inputString.length() + pos could be negative.
   */
  private static int buildStartPosition(String inputString, int pos) {
    // Handles the negative position (substring("abc", -2, 1), the start position should be 1("b"))
    if (pos < 0) {
      return getStringLengthWithCodePoint(inputString) + pos;
    }
    // Pos is 1 based and pos = 0 is treated as 1.
    return Math.max(pos - 1, 0);
  }

  /** Returns code point based string length for handling surrogate pairs. */
  private static int getStringLengthWithCodePoint(String s) {
    return s.codePointCount(/* beginIndex = */ 0, s.length());
  }

  private static String subStringWithCodePoint(String s, int start, Optional<Integer> end) {
    int startIndex = s.offsetByCodePoints(/* beginIndex = */ 0, start);
    return end.map(e -> s.substring(startIndex, s.offsetByCodePoints(0, e)))
        .orElse(s.substring(startIndex));
  }
}
