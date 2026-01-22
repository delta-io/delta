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
package io.delta.kernel.types;

import io.delta.kernel.annotation.Evolving;
import java.util.Objects;

/**
 * The data type representing geography values. A Geography must have a fixed Spatial Reference
 * System Identifier (SRID) that defines the coordinate system and an algorithm that determines how
 * geometric calculations are performed.
 *
 * <p>The SRID is specified as a string and the algorithm defines the calculation method. The engine
 * is responsible for validating and interpreting the SRID and algorithm values.
 *
 * @since 3.0.0
 */
@Evolving
public final class GeographyType extends DataType {
  public static final String DEFAULT_SRID = "OGC:CRS84";
  public static final String DEFAULT_ALGORITHM = "spherical";

  private final String srid;
  private final String algorithm;

  /** Create a GeographyType with the default SRID and algorithm. */
  public GeographyType() {
    this(DEFAULT_SRID, DEFAULT_ALGORITHM);
  }

  /**
   * Create a GeographyType with the specified SRID and default algorithm.
   *
   * @param srid the Spatial Reference System Identifier (any non-null, non-empty string)
   * @throws IllegalArgumentException if the SRID is null or empty
   */
  public GeographyType(String srid) {
    this(srid, DEFAULT_ALGORITHM);
  }

  /**
   * Create a GeographyType with the specified SRID and algorithm.
   *
   * @param srid the Spatial Reference System Identifier (any non-null, non-empty string)
   * @param algorithm the algorithm for geometric calculations (any non-null, non-empty string)
   * @throws IllegalArgumentException if the SRID or algorithm is null or empty
   */
  public GeographyType(String srid, String algorithm) {
    if (srid == null || srid.isEmpty()) {
      throw new IllegalArgumentException("SRID cannot be null or empty");
    }
    if (algorithm == null || algorithm.isEmpty()) {
      throw new IllegalArgumentException("Algorithm cannot be null or empty");
    }
    this.srid = srid;
    this.algorithm = algorithm;
  }

  /**
   * Get the Spatial Reference System Identifier.
   *
   * @return the SRID string
   */
  public String getSRID() {
    return srid;
  }

  /**
   * Get the algorithm for geometric calculations.
   *
   * @return the algorithm string
   */
  public String getAlgorithm() {
    return algorithm;
  }

  @Override
  public boolean isNested() {
    return false;
  }

  /**
   * Serialize this GeographyType to its string representation for JSON serialization. Format:
   * "geography(<srid>, <algorithm>)"
   *
   * @return the serialized string representation
   */
  public String toJson() {
    return String.format("geography(%s, %s)", srid, algorithm);
  }

  @Override
  public String toString() {
    return String.format("Geography(srid=%s, algorithm=%s)", srid, algorithm);
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
    return srid.equals(that.srid) && algorithm.equals(that.algorithm);
  }

  @Override
  public int hashCode() {
    return Objects.hash(srid, algorithm);
  }
}
