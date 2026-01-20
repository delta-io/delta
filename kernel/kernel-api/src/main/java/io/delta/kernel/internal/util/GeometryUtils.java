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
package io.delta.kernel.internal.util;

import static io.delta.kernel.internal.util.Preconditions.checkArgument;

import io.delta.kernel.data.Row;
import io.delta.kernel.internal.data.GenericRow;
import io.delta.kernel.types.*;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Utility methods for working with geometry types and WKT (Well-Known Text) format. */
public class GeometryUtils {

  /** Pattern for parsing WKT POINT format: POINT(x y) */
  private static final Pattern POINT_PATTERN =
      Pattern.compile("POINT\\s*\\(\\s*([+-]?\\d+\\.?\\d*)\\s+([+-]?\\d+\\.?\\d*)\\s*\\)");

  private GeometryUtils() {}

  /**
   * Parse a WKT POINT string into coordinates.
   *
   * @param wkt WKT POINT string in format "POINT(x y)"
   * @return array with [x, y] coordinates
   * @throws IllegalArgumentException if the WKT string is null or has invalid format
   */
  public static double[] parsePoint(String wkt) {
    checkArgument(wkt != null, "WKT string cannot be null");

    Matcher matcher = POINT_PATTERN.matcher(wkt);
    checkArgument(
        matcher.matches(),
        "Invalid WKT POINT format: '%s'. Expected format is 'POINT(x y)' "
            + "where x and y are numeric values.",
        wkt);

    double x = Double.parseDouble(matcher.group(1));
    double y = Double.parseDouble(matcher.group(2));

    return new double[] {x, y};
  }

  /**
   * Create a WKT POINT string from coordinates.
   *
   * @param x X coordinate
   * @param y Y coordinate
   * @return WKT POINT string in format "POINT(x y)"
   */
  public static String createPoint(double x, double y) {
    return String.format(Locale.US, "POINT(%.17g %.17g)", x, y);
  }

  /**
   * Parse min and max WKT POINT strings into a bounding box struct.
   *
   * @param minWkt WKT POINT string for the minimum corner (xmin, ymin)
   * @param maxWkt WKT POINT string for the maximum corner (xmax, ymax)
   * @return Row representing bounding box struct with fields {xmin, ymin, xmax, ymax}
   * @throws IllegalArgumentException if either WKT string is null or has invalid format
   */
  public static Row parseToBoundingBox(String minWkt, String maxWkt) {
    double[] minCoords = parsePoint(minWkt);
    double[] maxCoords = parsePoint(maxWkt);

    StructType boundingBoxSchema =
        new StructType()
            .add("xmin", DoubleType.DOUBLE, false)
            .add("ymin", DoubleType.DOUBLE, false)
            .add("xmax", DoubleType.DOUBLE, false)
            .add("ymax", DoubleType.DOUBLE, false);

    Map<Integer, Object> values = new HashMap<>();
    values.put(0, minCoords[0]); // xmin
    values.put(1, minCoords[1]); // ymin
    values.put(2, maxCoords[0]); // xmax
    values.put(3, maxCoords[1]); // ymax

    return new GenericRow(boundingBoxSchema, values);
  }

  /**
   * Create min and max WKT POINT strings from a bounding box.
   *
   * @param xmin Minimum X coordinate
   * @param ymin Minimum Y coordinate
   * @param xmax Maximum X coordinate
   * @param ymax Maximum Y coordinate
   * @return array with [minWkt, maxWkt] where each is a WKT POINT string
   */
  public static String[] createBoundingBoxWKT(double xmin, double ymin, double xmax, double ymax) {
    String minWkt = createPoint(xmin, ymin);
    String maxWkt = createPoint(xmax, ymax);
    return new String[] {minWkt, maxWkt};
  }

  /**
   * Extract coordinates from a bounding box Row.
   *
   * @param boundingBox Row with fields {xmin, ymin, xmax, ymax}
   * @return array with [xmin, ymin, xmax, ymax] coordinates
   * @throws IllegalArgumentException if the row is null or doesn't have the expected fields
   */
  public static double[] extractBoundingBoxCoordinates(Row boundingBox) {
    checkArgument(boundingBox != null, "Bounding box cannot be null");

    StructType schema = boundingBox.getSchema();
    checkArgument(schema.length() == 4, "Bounding box must have 4 fields");

    double xmin = boundingBox.getDouble(0);
    double ymin = boundingBox.getDouble(1);
    double xmax = boundingBox.getDouble(2);
    double ymax = boundingBox.getDouble(3);

    return new double[] {xmin, ymin, xmax, ymax};
  }

  /**
   * Test if two bounding boxes intersect.
   *
   * @param bbox1 First bounding box [xmin, ymin, xmax, ymax]
   * @param bbox2 Second bounding box [xmin, ymin, xmax, ymax]
   * @return true if the bounding boxes intersect, false otherwise
   */
  public static boolean boundingBoxesIntersect(double[] bbox1, double[] bbox2) {
    checkArgument(bbox1 != null && bbox1.length == 4, "First bounding box must have 4 coordinates");
    checkArgument(
        bbox2 != null && bbox2.length == 4, "Second bounding box must have 4 coordinates");

    double xmin1 = bbox1[0];
    double ymin1 = bbox1[1];
    double xmax1 = bbox1[2];
    double ymax1 = bbox1[3];

    double xmin2 = bbox2[0];
    double ymin2 = bbox2[1];
    double xmax2 = bbox2[2];
    double ymax2 = bbox2[3];

    // Two bounding boxes intersect if they overlap on both X and Y axes
    boolean xOverlap = xmin1 <= xmax2 && xmax1 >= xmin2;
    boolean yOverlap = ymin1 <= ymax2 && ymax1 >= ymin2;

    return xOverlap && yOverlap;
  }

  /**
   * Get the struct type for a bounding box.
   *
   * @return StructType with fields {xmin, ymin, xmax, ymax}
   */
  public static StructType getBoundingBoxSchema() {
    return new StructType()
        .add("xmin", DoubleType.DOUBLE, false)
        .add("ymin", DoubleType.DOUBLE, false)
        .add("xmax", DoubleType.DOUBLE, false)
        .add("ymax", DoubleType.DOUBLE, false);
  }
}
