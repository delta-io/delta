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
package io.delta.kernel.data;

import io.delta.kernel.annotation.Evolving;
import java.util.OptionalDouble;

/**
 * Represents a parsed WKT POINT value used for geometry/geography column statistics. Supports 2D
 * (x, y), 3D (x, y, z), measured (x, y, m), and 4D (x, y, z, m) variants.
 *
 * @since 3.4.0
 */
@Evolving
public class PointVal {
  private final double x;
  private final double y;
  private final OptionalDouble z;
  private final OptionalDouble m;

  public PointVal(double x, double y, OptionalDouble z, OptionalDouble m) {
    this.x = x;
    this.y = y;
    this.z = z;
    this.m = m;
  }

  public double getX() {
    return x;
  }

  public double getY() {
    return y;
  }

  /** Present for Z and ZM variants. */
  public OptionalDouble getZ() {
    return z;
  }

  /** Present for M and ZM variants. */
  public OptionalDouble getM() {
    return m;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof PointVal)) return false;
    PointVal that = (PointVal) o;
    return Double.compare(x, that.x) == 0
        && Double.compare(y, that.y) == 0
        && z.equals(that.z)
        && m.equals(that.m);
  }

  @Override
  public int hashCode() {
    int result = Double.hashCode(x);
    result = 31 * result + Double.hashCode(y);
    result = 31 * result + z.hashCode();
    result = 31 * result + m.hashCode();
    return result;
  }
}
