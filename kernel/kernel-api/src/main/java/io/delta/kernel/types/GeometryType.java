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
 * Identifier (SRID) that defines the coordinate system.
 *
 * <p>The SRID is specified as a string The engine is responsible for validating and interpreting
 * the SRID value.
 *
 * @since 3.0.0
 */
@Evolving
public final class GeometryType extends DataType {

  public static final String DEFAULT_SRID = "OGC:CRS84";

  private final String srid;

  /** Create a GeometryType with the default SRID. */
  public GeometryType() {
    this(DEFAULT_SRID);
  }

  /**
   * Create a GeometryType with the specified SRID.
   *
   * @param srid the Spatial Reference System Identifier (any non-null, non-empty string)
   * @throws IllegalArgumentException if the SRID is null or empty
   */
  public GeometryType(String srid) {
    if (srid == null || srid.isEmpty()) {
      throw new IllegalArgumentException("SRID cannot be null or empty");
    }
    this.srid = srid;
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

  /**
   * Serialize this GeometryType to its string representation with minimal info.
   *
   * @return the serialized string representation
   */
  public String simpleString() {
    return String.format("geometry(%s)", srid);
  }

  @Override
  public String toString() {
    return String.format("Geometry(srid=%s)", srid);
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
    return srid.equals(that.srid);
  }

  @Override
  public int hashCode() {
    return Objects.hash(srid);
  }
}
