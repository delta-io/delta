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
import static io.delta.kernel.defaults.internal.expressions.DefaultExpressionUtils.*;
import static io.delta.kernel.types.StringType.STRING;
import static java.lang.String.format;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.expressions.CollatedPredicate;
import io.delta.kernel.expressions.Expression;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.DataType;
import java.util.List;

public class StartsWithExpressionEvaluator {

  /** Validates and transforms the {@code starts_with} expression. */
  static Predicate validateAndTransform(
      Predicate startsWith,
      List<Expression> childrenExpressions,
      List<DataType> childrenOutputTypes) {
    checkArgsCount(
        startsWith,
        /* expectedCount= */ 2,
        startsWith.getName(),
        "Example usage: STARTS_WITH(column, 'test')");
    for (DataType dataType : childrenOutputTypes) {
      checkIsStringType(dataType, startsWith, "'STARTS_WITH' expects STRING type inputs");
    }

    // TODO: support non literal as the second input of starts with.
    checkIsLiteral(
        childrenExpressions.get(1),
        startsWith,
        "'STARTS_WITH' expects literal as the second input");

    if (startsWith instanceof CollatedPredicate) {
      CollatedPredicate collatedPredicate = (CollatedPredicate) startsWith;
      if (!collatedPredicate.getCollationIdentifier().equals(STRING.getCollationIdentifier())) {
        String msg =
                format("Unsupported collation: \"%s\". Default Engine supports just \"SPARK.UTF8_BINARY\" collation.", collatedPredicate.getCollationIdentifier());
        throw unsupportedExpressionException(startsWith, msg);
      }

      return new CollatedPredicate(
          startsWith.getName(),
          childrenExpressions.get(0),
          childrenExpressions.get(1),
          ((CollatedPredicate) startsWith).getCollationIdentifier());
    }
    return new Predicate(startsWith.getName(), childrenExpressions);
  }

  static ColumnVector eval(List<ColumnVector> childrenVectors) {
    return new ColumnVector() {
      final ColumnVector left = childrenVectors.get(0);
      final ColumnVector right = childrenVectors.get(1);

      @Override
      public DataType getDataType() {
        return BooleanType.BOOLEAN;
      }

      @Override
      public int getSize() {
        return left.getSize();
      }

      @Override
      public void close() {
        Utils.closeCloseables(left, right);
      }

      @Override
      public boolean getBoolean(int rowId) {
        if (isNullAt(rowId)) {
          // The return value is undefined and can be anything, if the slot for rowId is null.
          return false;
        }
        return left.getString(rowId).startsWith(right.getString(rowId));
      }

      @Override
      public boolean isNullAt(int rowId) {
        return left.isNullAt(rowId) || right.isNullAt(rowId);
      }
    };
  }
}
