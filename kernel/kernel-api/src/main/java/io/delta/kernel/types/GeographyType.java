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
package io.delta.kernel.types;

import io.delta.kernel.annotation.Evolving;
import java.util.Objects;

/**
 * The data type representing geography values. A Geography must have a fixed Spatial Reference
 * System Identifier (SRID) that defines the coordinate system.
 *
 * <p>The SRID is specified as a string (e.g., "EPSG:4326" for WGS84, "4326", or any other format).
 * The engine is responsible for validating and interpreting the SRID value. The default SRID is
 * "EPSG:4326" which represents the WGS84 coordinate system commonly used for geographic coordinates
 * (latitude/longitude).
 *
 * <p>Geography values are stored in Well-Known Binary (WKB) or Extended Well-Known Binary (EWKB)
 * format in Parquet files, and statistics are stored in Well-Known Text (WKT) format in Delta log
 * files.
 *
 * @since 3.0.0
 */
@Evolving
public final class GeographyType extends DataType {
  /** Default SRID representing WGS84 coordinate system (latitude/longitude). */
  public static final String DEFAULT_SRID = "EPSG:4326";

  private final String srid;

  /**
   * Create a GeographyType with the specified SRID.
   *
   * @param srid the Spatial Reference System Identifier (any non-null, non-empty string)
   * @throws IllegalArgumentException if the SRID is null or empty
   */
  public GeographyType(String srid) {
    if (srid == null || srid.isEmpty()) {
      throw new IllegalArgumentException("SRID cannot be null or empty");
    }
    this.srid = srid;
  }

  /** Create a GeographyType with the default SRID (EPSG:4326 for WGS84). */
  public GeographyType() {
    this(DEFAULT_SRID);
  }

  /**
   * Get the Spatial Reference System Identifier.
   *
   * @return the SRID string
   */
  public String getSRID() {
    return srid;
  }

  @Override
  public boolean isNested() {
    return false;
  }

  @Override
  public String toString() {
    return String.format("Geography(%s)", srid);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    GeographyType that = (GeographyType) o;
    return srid.equals(that.srid);
  }

  @Override
  public int hashCode() {
    return Objects.hash(srid);
  }
}
