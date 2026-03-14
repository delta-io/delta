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
package io.delta.kernel.expressions;

import static io.delta.kernel.internal.util.Preconditions.checkArgument;

import io.delta.kernel.types.StringType;
import java.util.Arrays;

/**
 * A predicate that tests whether the bounding box of a geometry column intersects a given query
 * bounding box.
 *
 * <p>This expression is not directly evaluatable. It is transformed to an internal
 * ST_INTERSECTS_BOXES_ON_STATS predicate during data-skipping filter construction, which evaluates
 * the intersection of the column's min/max bounding box against the query box.
 *
 * @since 4.0.0
 */
public class StIntersectsBoxes extends Predicate {

  public static final String NAME = "ST_INTERSECTS_BOXES";

  /**
   * @param column the geometry or geography column to test
   * @param queryMin lower-left corner of the query bounding box as a WKT POINT string literal
   * @param queryMax upper-right corner of the query bounding box as a WKT POINT string literal
   */
  public StIntersectsBoxes(Column column, Literal queryMin, Literal queryMax) {
    super(NAME, Arrays.asList(column, queryMin, queryMax));
    checkArgument(
        queryMin.getDataType() instanceof StringType,
        "queryMin must be a StringType WKT literal, got: %s",
        queryMin.getDataType());
    checkArgument(
        queryMax.getDataType() instanceof StringType,
        "queryMax must be a StringType WKT literal, got: %s",
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
