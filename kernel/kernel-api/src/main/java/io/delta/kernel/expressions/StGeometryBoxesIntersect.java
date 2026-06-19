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

import io.delta.kernel.types.GeometryType;
import java.util.Arrays;

/**
 * A predicate that tests whether the bounding box of a geometry column intersects a given query
 * bounding box.
 *
 * <p>This expression is not directly evaluatable. It is transformed to an internal
 * ST_GEOMETRY_BOXES_INTERSECT_ON_STATS predicate during data-skipping filter construction, which
 * evaluates the intersection of the column's min/max bounding box against the query box.
 *
 * @since 4.0.0
 */
public class StGeometryBoxesIntersect extends Predicate {

  public static final String NAME = "ST_GEOMETRY_BOXES_INTERSECT";

  /**
   * @param column the geometry column to test
   * @param queryMin lower-left corner of the query bounding box as a GeometryType WKT POINT literal
   * @param queryMax upper-right corner of the query bounding box as a GeometryType WKT POINT
   *     literal
   */
  public StGeometryBoxesIntersect(Column column, Literal queryMin, Literal queryMax) {
    super(NAME, Arrays.asList(column, queryMin, queryMax));
    checkArgument(
        queryMin.getDataType() instanceof GeometryType,
        "queryMin must be a GeometryType WKT literal, got: %s",
        queryMin.getDataType());
    checkArgument(
        queryMax.getDataType() instanceof GeometryType,
        "queryMax must be a GeometryType WKT literal, got: %s",
        queryMax.getDataType());
  }

  public Column getColumn() {
    return (Column) getChildren().get(0);
  }

  public Literal getQueryMin() {
    return (Literal) getChildren().get(1);
  }

  public Literal getQueryMax() {
    return (Literal) getChildren().get(2);
  }
}
