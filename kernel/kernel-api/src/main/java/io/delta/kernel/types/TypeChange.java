/*
 * Copyright (2025) The Delta Lake Project Authors.
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

import java.util.Objects;

/**
 * Represents a type change for a field, containing the original and new primitive types.
 *
 * <p>Type changes are actually persisted in metadata attached to StructFields but the rules for
 * where the metadata is attached depend on if the change is for nested arrays/maps or primitive
 * types.
 */
public class TypeChange {
  private final DataType from;
  private final DataType to;

  public TypeChange(DataType from, DataType to) {
    this.from = Objects.requireNonNull(from, "from type cannot be null");
    this.to = Objects.requireNonNull(to, "to type cannot be null");
  }

  public DataType getFrom() {
    return from;
  }

  public DataType getTo() {
    return to;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    TypeChange that = (TypeChange) o;
    return Objects.equals(from, that.from) && Objects.equals(to, that.to);
  }

  @Override
  public int hashCode() {
    return Objects.hash(from, to);
  }

  @Override
  public String toString() {
    return String.format("TypeChange(from=%s,to=%s)", from, to);
  }
}
