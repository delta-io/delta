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
package io.delta.kernel.expressions;

import static io.delta.kernel.internal.util.Preconditions.checkArgument;

import io.delta.kernel.annotation.Evolving;
import io.delta.kernel.types.DoubleType;
import java.util.Arrays;
import java.util.List;

/**
 * Predicate representing a geometry column intersects a query bounding box.
 *
 * <p>Used by DB engines that have {@code ST_INTERSECTS(col, geom_literal)}: the engine extracts the
 * bounding box of the geometry literal and constructs this predicate. Delta Kernel uses it purely
 * for data skipping — it is translated to {@link #STATS_NAME} during stats-predicate construction
 * and must never be evaluated against row data.
 *
 * <p>Signature: {@code ST_INTERSECT_BOXES(geoColumn, xmin, ymin, xmax, ymax)}
 *
 * <ul>
 *   <li>{@code geoColumn} — reference to the geometry column in the data schema
 *   <li>{@code xmin, ymin, xmax, ymax} — bounding box of the query geometry literal (doubles)
 * </ul>
 *
 * @since 4.0.0
 */
@Evolving
public class STIntersectBoxes extends Predicate {

  public static final String NAME = "ST_INTERSECT_BOXES";

  /**
   * Create a predicate for data-skipping a geometry column against a query bounding box.
   *
   * @param geoColumn reference to the geometry column
   * @param queryXmin west boundary of the query bounding box
   * @param queryYmin south boundary of the query bounding box
   * @param queryXmax east boundary of the query bounding box
   * @param queryYmax north boundary of the query bounding box
   */
  public STIntersectBoxes(
      Column geoColumn, double queryXmin, double queryYmin, double queryXmax, double queryYmax) {
    super(
        NAME,
        Arrays.asList(
            geoColumn,
            Literal.ofDouble(queryXmin),
            Literal.ofDouble(queryYmin),
            Literal.ofDouble(queryXmax),
            Literal.ofDouble(queryYmax)));
    validate();
  }

  /** Returns the geometry column reference (first argument). */
  public Column getGeoColumn() {
    return (Column) getChildren().get(0);
  }

  /** Returns the west boundary of the query bounding box. */
  public double getQueryXmin() {
    return (Double) ((Literal) getChildren().get(1)).getValue();
  }

  /** Returns the south boundary of the query bounding box. */
  public double getQueryYmin() {
    return (Double) ((Literal) getChildren().get(2)).getValue();
  }

  /** Returns the east boundary of the query bounding box. */
  public double getQueryXmax() {
    return (Double) ((Literal) getChildren().get(3)).getValue();
  }

  /** Returns the north boundary of the query bounding box. */
  public double getQueryYmax() {
    return (Double) ((Literal) getChildren().get(4)).getValue();
  }

  // -------------------------------------------------------------------------
  // Internal helpers
  // -------------------------------------------------------------------------

  /**
   * The predicate name used in stats-evaluation context. Instances of this name are created
   * internally by data-skipping filter construction and are never exposed to end users.
   */
  public static final String STATS_NAME = "ST_INTERSECT_BOXES_ON_STATS";

  private void validate() {
    List<Expression> ch = getChildren();
    checkArgument(
        ch.size() == 5,
        "ST_INTERSECT_BOXES requires exactly 5 arguments (geoColumn, xmin, ymin, xmax, ymax),"
            + " but got %s",
        ch.size());
    checkArgument(
        ch.get(0) instanceof Column,
        "ST_INTERSECT_BOXES: first argument must be a Column, but got %s",
        ch.get(0).getClass().getSimpleName());
    for (int i = 1; i <= 4; i++) {
      Expression arg = ch.get(i);
      checkArgument(
          arg instanceof Literal && ((Literal) arg).getDataType() instanceof DoubleType,
          "ST_INTERSECT_BOXES: argument %d must be a double Literal, but got %s",
          i,
          arg);
    }
  }
}
