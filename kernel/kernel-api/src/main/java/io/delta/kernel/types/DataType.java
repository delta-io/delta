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
import java.util.function.Predicate;

/**
 * Base class for all data types.
 *
 * @since 3.0.0
 */
@Evolving
public abstract class DataType {

  /**
   * Are the data types same? The metadata, collations or column names could be different.
   *
   * <p>Should be used for schema comparisons during schema evolution.
   *
   * @param dataType
   * @return
   */
  public boolean equivalent(DataType dataType) {
    return equals(dataType);
  }

  /**
   * Checks whether the given {@code dataType} is compatible as an input for this type. The
   * collations could be different.
   *
   * <p>This method should be used for schema comparisons when validating input type compatibility.
   * It should not be used in other contexts, such as during the read path.
   *
   * @param dataType
   * @return
   */
  public boolean isInputCompatible(DataType dataType) {
    return equals(dataType);
  }

  /**
   * Returns true iff this data is a nested data type (it logically parameterized by other types).
   *
   * <p>For example StructType, ArrayType, MapType are nested data types.
   */
  public abstract boolean isNested();

  /**
   * Returns true if the provided {@code predicate} matches this type or any nested child type.
   *
   * @param predicate
   * @return
   */
  public boolean existsRecursively(Predicate<DataType> predicate) {
    return predicate != null && predicate.test(this);
  }

  @Override
  public abstract int hashCode();

  @Override
  public abstract boolean equals(Object obj);

  @Override
  public abstract String toString();
}
