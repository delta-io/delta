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
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.DoubleType;
import io.delta.kernel.types.StringType;
import java.util.List;

/**
 * Evaluator utilities for {@code ST_INTERSECT_BOXES} and {@code ST_INTERSECT_BOXES_ON_STATS}.
 *
 * <p>{@code ST_INTERSECT_BOXES(geoCol, xmin, ymin, xmax, ymax)} is the user-facing predicate. It is
 * <em>not</em> evaluable against row data; attempting to do so throws an exception.
 *
 * <p>{@code ST_INTERSECT_BOXES_ON_STATS(minStatCol, maxStatCol, xmin, ymin, xmax, ymax)} is the
 * internal stats-evaluation form.
 *
 * <ul>
 *   <li>{@code minStatCol} — string column whose per-file value is a WKT POINT representing the min
 *       bounding-box corner, e.g. {@code "POINT (xmin ymin)"}
 *   <li>{@code maxStatCol} — string column whose per-file value is a WKT POINT representing the max
 *       bounding-box corner, e.g. {@code "POINT (xmax ymax)"}
 *   <li>The 4 double literals are the query bounding box extracted from {@code ST_INTERSECT_BOXES}
 * </ul>
 *
 * <p>Supported POINT formats (WKT):
 *
 * <ul>
 *   <li>{@code POINT (x y)}
 *   <li>{@code POINT Z (x y z)} — z ignored
 *   <li>{@code POINT M (x y m)} — m ignored
 *   <li>{@code POINT ZM (x y z m)} — z and m ignored
 * </ul>
 */
class STIntersectBoxesEvaluator {
  private STIntersectBoxesEvaluator() {}

  // -------------------------------------------------------------------------
  // ST_INTERSECT_BOXES — user-facing, must NOT be evaluated against row data
  // -------------------------------------------------------------------------

  /**
   * Throws because {@code ST_INTERSECT_BOXES} must be translated to {@code
   * ST_INTERSECT_BOXES_ON_STATS} before evaluation. If this is called it means the predicate was
   * incorrectly passed as a row filter.
   */
  static Predicate validateAndTransformSTIntersectBoxes(Predicate predicate) {
    throw unsupportedExpressionException(
        predicate,
        "ST_INTERSECT_BOXES is a data-skipping-only predicate and cannot be evaluated against"
            + " row data. Translate it to ST_INTERSECT_BOXES_ON_STATS via"
            + " DataSkippingUtils.constructDataSkippingFilter before evaluation.");
  }

  // -------------------------------------------------------------------------
  // ST_INTERSECT_BOXES_ON_STATS — internal stats-evaluation form
  // -------------------------------------------------------------------------

  /**
   * Validate and transform {@code ST_INTERSECT_BOXES_ON_STATS}. Expects:
   *
   * <ul>
   *   <li>children[0]: resolved to {@link StringType} (minValues stat column — WKT POINT)
   *   <li>children[1]: resolved to {@link StringType} (maxValues stat column — WKT POINT)
   *   <li>children[2..5]: each a {@link Literal} of type {@link DoubleType} (query bbox)
   * </ul>
   */
  static Predicate validateAndTransformSTIntersectBoxesOnStats(
      Predicate predicate, DataType minColType, DataType maxColType) {

    List<?> children = predicate.getChildren();
    if (children.size() != 6) {
      throw unsupportedExpressionException(
          predicate,
          format(
              "ST_INTERSECT_BOXES_ON_STATS requires exactly 6 arguments"
                  + " (minStatCol, maxStatCol, xmin, ymin, xmax, ymax), but got %d",
              children.size()));
    }
    if (!(minColType instanceof StringType)) {
      throw unsupportedExpressionException(
          predicate,
          format(
              "ST_INTERSECT_BOXES_ON_STATS: first argument (min stat column) must be"
                  + " StringType, but got %s",
              minColType));
    }
    if (!(maxColType instanceof StringType)) {
      throw unsupportedExpressionException(
          predicate,
          format(
              "ST_INTERSECT_BOXES_ON_STATS: second argument (max stat column) must be"
                  + " StringType, but got %s",
              maxColType));
    }
    for (int i = 2; i <= 5; i++) {
      Object arg = children.get(i);
      if (!(arg instanceof Literal) || !(((Literal) arg).getDataType() instanceof DoubleType)) {
        throw unsupportedExpressionException(
            predicate,
            format(
                "ST_INTERSECT_BOXES_ON_STATS: argument %d must be a double Literal, but got %s",
                i, arg));
      }
    }
    return new Predicate(predicate.getName(), predicate.getChildren());
  }

  /**
   * Evaluate {@code ST_INTERSECT_BOXES_ON_STATS} against a stats batch.
   *
   * @param minVector string {@link ColumnVector} of WKT POINT min-corner values per file
   * @param maxVector string {@link ColumnVector} of WKT POINT max-corner values per file
   * @param predicate carries the 4 query-bbox literals as children[2..5]
   */
  static ColumnVector evalOnStats(
      ColumnVector minVector, ColumnVector maxVector, Predicate predicate) {
    List<?> children = predicate.getChildren();
    double qXmin = (Double) ((Literal) children.get(2)).getValue();
    double qYmin = (Double) ((Literal) children.get(3)).getValue();
    double qXmax = (Double) ((Literal) children.get(4)).getValue();
    double qYmax = (Double) ((Literal) children.get(5)).getValue();

    return new ColumnVector() {
      @Override
      public DataType getDataType() {
        return BooleanType.BOOLEAN;
      }

      @Override
      public int getSize() {
        return minVector.getSize();
      }

      @Override
      public void close() {
        Utils.closeCloseables(minVector, maxVector);
      }

      @Override
      public boolean isNullAt(int rowId) {
        return minVector.isNullAt(rowId) || maxVector.isNullAt(rowId);
      }

      @Override
      public boolean getBoolean(int rowId) {
        double[] minXY = parsePointXY(minVector.getString(rowId));
        double[] maxXY = parsePointXY(maxVector.getString(rowId));
        double sXmin = minXY[0], sYmin = minXY[1];
        double sXmax = maxXY[0], sYmax = maxXY[1];
        return (sXmin <= qXmax && sXmax >= qXmin) && (sYmin <= qYmax && sYmax >= qYmin);
      }
    };
  }

  // -------------------------------------------------------------------------
  // Private helpers
  // -------------------------------------------------------------------------

  /**
   * Parse a WKT POINT string and return {@code [x, y]}.
   *
   * <p>Supported formats (case-sensitive {@code "POINT"} prefix):
   *
   * <pre>
   *   POINT (x y)
   *   POINT Z (x y z)    — z ignored
   *   POINT M (x y m)    — m ignored
   *   POINT ZM (x y z m) — z and m ignored
   * </pre>
   *
   * <p>No geometry library is required; this is purely string tokenization.
   */
  static double[] parsePointXY(String wkt) {
    if (wkt == null || !wkt.startsWith("POINT")) {
      throw new IllegalArgumentException(
          format("Cannot parse geo stats POINT. Expected WKT POINT format, got: %s", wkt));
    }
    int parenStart = wkt.indexOf('(');
    int parenEnd = wkt.lastIndexOf(')');
    if (parenStart < 0 || parenEnd <= parenStart) {
      throw new IllegalArgumentException(
          format("Cannot parse geo stats POINT — missing parentheses: %s", wkt));
    }
    String inner = wkt.substring(parenStart + 1, parenEnd).trim();
    String[] parts = inner.split("\\s+");
    if (parts.length < 2) {
      throw new IllegalArgumentException(
          format("Cannot parse geo stats POINT — need at least x and y coordinates: %s", wkt));
    }
    try {
      return new double[] {Double.parseDouble(parts[0]), Double.parseDouble(parts[1])};
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
          format("Cannot parse geo stats POINT coordinates: %s", wkt), e);
    }
  }
}
