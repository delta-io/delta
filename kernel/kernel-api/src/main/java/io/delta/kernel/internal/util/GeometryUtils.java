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

import io.delta.kernel.data.PointVal;
import java.util.OptionalDouble;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Utility for parsing WKT POINT strings into {@link PointVal}. */
public class GeometryUtils {
  private GeometryUtils() {}

  // Matches: POINT [ZM|Z|M] (coords)
  private static final Pattern POINT_WKT_PATTERN =
      Pattern.compile("^POINT\\s*(ZM|Z|M)?\\s*\\(([^)]+)\\)$", Pattern.CASE_INSENSITIVE);

  /**
   * Formats x/y (and optional z/m) coordinates as a WKT POINT string. Examples: "POINT (1.0 2.0)",
   * "POINT Z(1.0 2.0 3.0)", "POINT ZM(1.0 2.0 3.0 4.0)".
   */
  public static String formatPointWKT(double x, double y, OptionalDouble z, OptionalDouble m) {
    boolean hasZ = z.isPresent();
    boolean hasM = m.isPresent();
    StringBuilder sb = new StringBuilder("POINT ");
    if (hasZ && hasM) {
      sb.append("ZM");
    } else if (hasZ) {
      sb.append("Z");
    } else if (hasM) {
      sb.append("M");
    }
    sb.append("(").append(x).append(" ").append(y);
    z.ifPresent(v -> sb.append(" ").append(v));
    m.ifPresent(v -> sb.append(" ").append(v));
    return sb.append(")").toString();
  }

  /**
   * Parses a WKT POINT string into a {@link PointVal}.
   *
   * <p>Supported formats:
   *
   * <ul>
   *   <li>{@code POINT (x y)}
   *   <li>{@code POINT Z(x y z)}
   *   <li>{@code POINT M(x y m)}
   *   <li>{@code POINT ZM(x y z m)}
   * </ul>
   *
   * @throws IllegalArgumentException if the input is null or does not match a valid POINT WKT
   */
  public static PointVal parsePoint(String wkt) {
    if (wkt == null) {
      throw new IllegalArgumentException("WKT POINT string cannot be null");
    }
    Matcher matcher = POINT_WKT_PATTERN.matcher(wkt.trim());
    if (!matcher.matches()) {
      throw new IllegalArgumentException("Invalid WKT POINT string: " + wkt);
    }

    String modifier = matcher.group(1);
    String coordStr = matcher.group(2).trim();
    String[] parts = coordStr.split("\\s+");

    String mod = modifier == null ? "" : modifier.toUpperCase();
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
      OptionalDouble z = OptionalDouble.empty();
      OptionalDouble m = OptionalDouble.empty();

      if ("Z".equals(mod)) {
        z = OptionalDouble.of(Double.parseDouble(parts[2]));
      } else if ("M".equals(mod)) {
        m = OptionalDouble.of(Double.parseDouble(parts[2]));
      } else if ("ZM".equals(mod)) {
        z = OptionalDouble.of(Double.parseDouble(parts[2]));
        m = OptionalDouble.of(Double.parseDouble(parts[3]));
      }

      return new PointVal(x, y, z, m);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Invalid coordinate in WKT POINT string: " + wkt, e);
    }
  }
}
