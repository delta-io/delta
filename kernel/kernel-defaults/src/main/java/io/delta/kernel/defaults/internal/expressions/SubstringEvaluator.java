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
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.expressions.ScalarExpression;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.types.*;
import java.util.*;

/** Utility methods to evaluate {@code substring} expression. */
public class SubstringEvaluator {

  private static final Set<DataType> SUBSTRING_SUPPORTED_TYPE =
      Collections.unmodifiableSet(
          new HashSet<>(Arrays.asList(StringType.STRING, BinaryType.BINARY)));

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

    if (!SUBSTRING_SUPPORTED_TYPE.contains(childrenOutputTypes.get(0))) {
      throw unsupportedExpressionException(
          substring, "Invalid type of first input of SUBSTRING: expects BINARY or STRING");
    }

    Expression posExpression = childrenExpressions.get(1);
    if (!isIntegerLiteral(posExpression)) {
      throw unsupportedExpressionException(
          substring,
          "Invalid type of second input of SUBSTRING: "
              + "expects an integral numeric expression specifying the starting position.");
    }

    if (childrenSize == 3) {
      Expression lengthExpression = childrenExpressions.get(2);
      if (!isIntegerLiteral(lengthExpression)) {
        throw unsupportedExpressionException(
            substring, "Invalid type of third input of SUBSTRING: expects an integral numeric.");
      }
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
        if (lengthVector.isPresent()) {
          Utils.closeCloseables(input, positionVector, lengthVector.get());
        } else {
          Utils.closeCloseables(input, positionVector);
        }
      }

      @Override
      public boolean isNullAt(int rowId) {
        return input.isNullAt(rowId);
      }

      @Override
      public String getString(int rowId) {
        if (isNullAt(rowId) || rowId < 0 || rowId >= getSize()) {
          return null;
        }

        String inputString =
            input.getDataType() == BinaryType.BINARY
                ? new String(input.getBinary(rowId))
                : input.getString(rowId);
        int position = positionVector.getInt(rowId);
        Optional<Integer> length = lengthVector.map(columnVector -> columnVector.getInt(rowId));

        if (position > inputString.length()) {
          return "";
        }

        int startPosition = buildStartPosition(inputString, position);

        if (!length.isPresent()) {
          return inputString.substring(Math.max(startPosition, 0));
        }

        if (length.get() < 1) {
          return "";
        }

        int startIndex = Math.max(startPosition, 0);
        // endIndex should be less than the length of input string, but positive.
        // e.g. Substring("aaa", -100, 95), should be read as Substring("aaa", 0, 0)
        int endIndex = Math.min(inputString.length(), Math.max(startPosition + length.get(), 0));
        return inputString.substring(startIndex, endIndex);
      }
    };
  }

  /**
   * Returns the index of the start position given inputString and parameter pos. The return value
   * is not normalized, i.e. could be less than 0.
   */
  private static int buildStartPosition(String inputString, int pos) {
    if (pos < 0) {
      return inputString.length() + pos;
    }
    // Pos is 1 based and pos = 0 is treated as 1.
    return Math.max(pos - 1, 0);
  }

  private static boolean isIntegerLiteral(Expression expression) {
    if (!(expression instanceof Literal)) {
      return false;
    }
    Literal literal = (Literal) expression;
    return IntegerType.INTEGER.equals(literal.getDataType());
  }

  private SubstringEvaluator() {}
}
