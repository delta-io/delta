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
package io.delta.kernel.defaults.internal.expressions;

import static io.delta.kernel.defaults.internal.DefaultEngineErrors.unsupportedExpressionException;
import static java.lang.String.format;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.expressions.Expression;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.StructType;

/** Utility methods to evaluate {@code ST_INTERSECT_BOXES} expression. */
class STIntersectBoxesEvaluator {
  private STIntersectBoxesEvaluator() {}

  /**
   * Validate and transform the {@code ST_INTERSECT_BOXES} expression with given validated and
   * transformed inputs.
   */
  static Predicate validateAndTransform(
      Predicate predicate,
      Expression left,
      DataType leftType,
      Expression right,
      DataType rightType) {

    // Validate that both operands are struct types with the expected bounding box schema
    validateBoundingBoxType(leftType, predicate, "left");
    validateBoundingBoxType(rightType, predicate, "right");

    return new Predicate(predicate.getName(), left, right);
  }

  /**
   * Utility method to evaluate the {@code ST_INTERSECT_BOXES} on given bounding box vectors.
   *
   * @param leftVector {@link ColumnVector} of bounding box struct type (xmin, ymin, xmax, ymax).
   * @param rightVector {@link ColumnVector} of bounding box struct type (xmin, ymin, xmax, ymax).
   * @return result {@link ColumnVector} containing boolean intersection results.
   */
  static ColumnVector eval(ColumnVector leftVector, ColumnVector rightVector) {
    // Extract child column vectors once (outside the row loop for efficiency)
    // Each struct has 4 children: xmin, ymin, xmax, ymax (all doubles)
    ColumnVector leftXmin = leftVector.getChild(0);
    ColumnVector leftYmin = leftVector.getChild(1);
    ColumnVector leftXmax = leftVector.getChild(2);
    ColumnVector leftYmax = leftVector.getChild(3);

    ColumnVector rightXmin = rightVector.getChild(0);
    ColumnVector rightYmin = rightVector.getChild(1);
    ColumnVector rightXmax = rightVector.getChild(2);
    ColumnVector rightYmax = rightVector.getChild(3);

    // Create a wrapper ColumnVector that lazily evaluates the intersection for each row
    return new ColumnVector() {
      @Override
      public DataType getDataType() {
        return BooleanType.BOOLEAN;
      }

      @Override
      public int getSize() {
        return leftVector.getSize();
      }

      @Override
      public void close() {
        Utils.closeCloseables(leftVector, rightVector);
      }

      @Override
      public boolean isNullAt(int rowId) {
        // Result is null if either bounding box is null
        if (leftVector.isNullAt(rowId) || rightVector.isNullAt(rowId)) {
          return true;
        }

        // Result is null if any coordinate is null
        return leftXmin.isNullAt(rowId)
            || leftYmin.isNullAt(rowId)
            || leftXmax.isNullAt(rowId)
            || leftYmax.isNullAt(rowId)
            || rightXmin.isNullAt(rowId)
            || rightYmin.isNullAt(rowId)
            || rightXmax.isNullAt(rowId)
            || rightYmax.isNullAt(rowId);
      }

      @Override
      public boolean getBoolean(int rowId) {
        // Extract double values for this row
        double xmin1 = leftXmin.getDouble(rowId);
        double ymin1 = leftYmin.getDouble(rowId);
        double xmax1 = leftXmax.getDouble(rowId);
        double ymax1 = leftYmax.getDouble(rowId);

        double xmin2 = rightXmin.getDouble(rowId);
        double ymin2 = rightYmin.getDouble(rowId);
        double xmax2 = rightXmax.getDouble(rowId);
        double ymax2 = rightYmax.getDouble(rowId);

        // Two bounding boxes intersect if they overlap on both X and Y axes
        boolean xOverlap = xmin1 <= xmax2 && xmax1 >= xmin2;
        boolean yOverlap = ymin1 <= ymax2 && ymax1 >= ymin2;

        return xOverlap && yOverlap;
      }
    };
  }

  private static void validateBoundingBoxType(DataType dataType, Predicate predicate, String side) {
    if (!(dataType instanceof StructType)) {
      String msg =
          format(
              "ST_INTERSECT_BOXES expects struct type inputs for %s operand, but got %s",
              side, dataType);
      throw unsupportedExpressionException(predicate, msg);
    }

    StructType structType = (StructType) dataType;
    // Expect struct with fields: xmin, ymin, xmax, ymax (all doubles)
    if (structType.length() != 4) {
      String msg =
          format(
              "ST_INTERSECT_BOXES expects bounding box struct with 4 fields "
                  + "(xmin, ymin, xmax, ymax) for %s operand, but got %d fields",
              side, structType.length());
      throw unsupportedExpressionException(predicate, msg);
    }

    // Validate field names and types
    String[] expectedFields = {"xmin", "ymin", "xmax", "ymax"};
    for (int i = 0; i < expectedFields.length; i++) {
      io.delta.kernel.types.StructField field = structType.at(i);
      if (!field.getName().equals(expectedFields[i])) {
        String msg =
            format(
                "ST_INTERSECT_BOXES expects bounding box field '%s' at position %d "
                    + "for %s operand, but got '%s'",
                expectedFields[i], i, side, field.getName());
        throw unsupportedExpressionException(predicate, msg);
      }
      if (!(field.getDataType() instanceof io.delta.kernel.types.DoubleType)) {
        String msg =
            format(
                "ST_INTERSECT_BOXES expects double type for field '%s' "
                    + "for %s operand, but got %s",
                field.getName(), side, field.getDataType());
        throw unsupportedExpressionException(predicate, msg);
      }
    }
  }
}
