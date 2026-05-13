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
 * The data type representing geometry values. A Geometry must have a fixed Spatial Reference System
 * Identifier (CRS) that defines the coordinate system.
 *
 * <p>The CRS is specified as a string The engine is responsible for validating and interpreting
 * the CRS value.
 *
 * @since 3.0.0
 */
@Evolving
public final class GeometryType extends DataType {

  public static final String DEFAULT_CRS = "OGC:CRS84";

  private final String crs;

  /** Returns a GeometryType with the default CRS. */
  public static GeometryType ofDefault() {
    return new GeometryType(DEFAULT_CRS);
  }

  /**
   * Returns a GeometryType with the specified CRS.
   *
   * @param crs the Spatial Reference System Identifier (any non-null, non-empty string)
   */
  public static GeometryType ofCRS(String crs) {
    return new GeometryType(crs);
  }

  /**
   * Create a GeometryType with the specified CRS.
   *
   * @param crs the Spatial Reference System Identifier (any non-null, non-empty string)
   * @throws IllegalArgumentException if the CRS is null or empty
   */
  public GeometryType(String crs) {
    if (crs == null || crs.isEmpty()) {
      throw new IllegalArgumentException("CRS cannot be null or empty");
    }
    this.crs = crs;
  }

  /**
   * Get the Spatial Reference System Identifier.
   *
   * @return the CRS string
   */
  public String getCRS() {
    return crs;
  }

  @Override
  public boolean isNested() {
    return false;
  }

  /**
   * Serialize this GeometryType to its string representation with minimal info.
   *
   * @return the serialized string representation
   */
  public String simpleString() {
    return String.format("geometry(%s)", crs);
  }

  @Override
  public String toString() {
    return String.format("Geometry(crs=%s)", crs);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    GeometryType that = (GeometryType) o;
    return crs.equals(that.crs);
  }

  @Override
  public int hashCode() {
    return Objects.hash(crs);
  }
}
