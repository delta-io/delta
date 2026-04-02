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
package io.delta.kernel.internal.util;

import java.util.Objects;
import java.util.OptionalDouble;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Utility for parsing and formatting WKT POINT strings for geometry/geography stats. */
public class GeometryUtils {
  private GeometryUtils() {}

  // Matches: POINT [ZM|Z|M] (coords) - (case-insensitive, flexible whitespace)
  private static final Pattern POINT_WKT_PATTERN =
      Pattern.compile("^POINT\\s*(ZM|Z|M)?\\s*\\(([^)]+)\\)$", Pattern.CASE_INSENSITIVE);

  // Matches: POINT [Z|M|ZM] EMPTY - (case-insensitive, flexible whitespace)
  private static final Pattern POINT_EMPTY_PATTERN =
      Pattern.compile("^POINT\\s*(ZM|Z|M)?\\s+EMPTY$", Pattern.CASE_INSENSITIVE);

  /**
   * Formats x/y (and optional z/m) coordinates as a WKT POINT string. Uses standard OGC WKT format:
   * "POINT(1.0 2.0)", "POINT Z (1.0 2.0 3.0)", "POINT ZM (1.0 2.0 3.0 4.0)".
   */
  public static String formatPointWKT(double x, double y, OptionalDouble z, OptionalDouble m) {
    boolean hasZ = z.isPresent();
    boolean hasM = m.isPresent();
    if (Double.isNaN(x) && Double.isNaN(y)) {
      StringBuilder sb = new StringBuilder("POINT");
      if (hasZ == hasM) {
        if (hasZ) sb.append(" ZM");
      } else {
        sb.append(hasZ ? " Z" : " M");
      }
      return sb.append(" EMPTY").toString();
    }
    StringBuilder sb = new StringBuilder("POINT");
    if (hasZ == hasM) {
      if (hasZ) sb.append(" ZM ");
    } else {
      sb.append(hasZ ? " Z " : " M ");
    }
    sb.append("(").append(x).append(" ").append(y);
    z.ifPresent(v -> sb.append(" ").append(v));
    m.ifPresent(v -> sb.append(" ").append(v));
    return sb.append(")").toString();
  }

  /**
   * Validates that the given string is a well-formed WKT POINT. Throws KernelException if the
   * string is null or not a valid POINT WKT.
   */
  public static void validatePointWKT(String wkt) {
    try {
      parsePointXY(wkt);
    } catch (IllegalArgumentException e) {
      throw new io.delta.kernel.exceptions.KernelException(
          String.format("Geospatial stats must be a valid POINT WKT but got: %s", wkt), e);
    }
  }

  /**
   * Parses only the x and y coordinates from a WKT POINT string. Returns double[]{x, y}. Z and M
   * coordinates are validated but not returned because stats bounding boxes only use 2D
   * coordinates.
   *
   * <p>Supported formats: POINT(x y), POINT Z (x y z), POINT M (x y m), POINT ZM (x y z m), POINT
   * EMPTY, POINT Z EMPTY, POINT M EMPTY, POINT ZM EMPTY.
   *
   * <p>All EMPTY variants return {NaN, NaN}.
   *
   * @throws IllegalArgumentException if not a valid POINT WKT
   */
  public static double[] parsePointXY(String wkt) {
    Objects.requireNonNull(wkt, "WKT POINT string cannot be null");
    String trimmed = wkt.trim();

    if (POINT_EMPTY_PATTERN.matcher(trimmed).matches()) {
      return new double[] {Double.NaN, Double.NaN};
    }

    Matcher matcher = POINT_WKT_PATTERN.matcher(trimmed);
    if (!matcher.matches()) {
      throw new IllegalArgumentException("Invalid WKT POINT string: " + wkt);
    }

    String modifier = matcher.group(1);
    String coordStr = matcher.group(2).trim();
    String[] parts = coordStr.split("\\s+");

    String mod = (modifier == null) ? "" : modifier.toUpperCase();
    int expectedCount;
    switch (mod) {
      case "ZM":
        expectedCount = 4;
        break;
      case "Z":
      case "M":
        expectedCount = 3;
        break;
      default:
        expectedCount = 2;
        break;
    }

    if (parts.length != expectedCount) {
      throw new IllegalArgumentException(
          String.format(
              "POINT %s expects %d coordinates but got %d: %s",
              mod, expectedCount, parts.length, wkt));
    }

    try {
      double x = Double.parseDouble(parts[0]);
      double y = Double.parseDouble(parts[1]);
      if (!Double.isFinite(x) || !Double.isFinite(y)) {
        throw new IllegalArgumentException("POINT coordinates must be finite numbers: " + wkt);
      }
      for (int i = 2; i < parts.length; i++) {
        double v = Double.parseDouble(parts[i]);
        if (!Double.isFinite(v)) {
          throw new IllegalArgumentException("POINT coordinates must be finite numbers: " + wkt);
        }
      }
      return new double[] {x, y};
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Invalid coordinate in WKT POINT string: " + wkt, e);
    }
  }

  /**
   * Validates that the given WKT POINT is a valid geography point with coordinates in range:
   * longitude in [-180, 180] and latitude in [-90, 90]. Throws KernelException if invalid.
   */
  public static void validateGeographyPointWKT(String wkt) {
    double[] xy;
    try {
      xy = parsePointXY(wkt);
    } catch (IllegalArgumentException e) {
      throw new io.delta.kernel.exceptions.KernelException(
          String.format("Geospatial stats must be a valid POINT WKT" + " but got: %s", wkt), e);
    }
    validateGeographyCoordinates(xy[0], xy[1], wkt);
  }

  /**
   * Validates that longitude is in [-180, 180] and latitude is in [-90, 90]. Throws KernelException
   * if out of range.
   */
  public static void validateGeographyCoordinates(double lon, double lat, String wkt) {
    // POINT EMPTY produces NaN -- skip range check
    if (Double.isNaN(lon) && Double.isNaN(lat)) {
      return;
    }
    if (lon < -180 || lon > 180 || lat < -90 || lat > 90) {
      throw new io.delta.kernel.exceptions.KernelException(
          String.format(
              "Geography coordinates out of range: longitude"
                  + " must be in [-180, 180], latitude must be"
                  + " in [-90, 90], but got: %s",
              wkt));
    }
  }
}
