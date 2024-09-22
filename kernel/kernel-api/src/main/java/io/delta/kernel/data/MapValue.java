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

/** Abstraction to represent a single map value in a {@link ColumnVector}. */
public interface MapValue {
  /** The number of elements in the map */
  int getSize();

  /**
   * A {@link ColumnVector} containing the keys. There are exactly {@link MapValue#getSize()} keys
   * in the vector, and each key maps one-to-one to the value at the same index in {@link
   * MapValue#getValues()}.
   */
  ColumnVector getKeys();

  /**
   * A {@link ColumnVector} containing the values. There are exactly {@link MapValue#getSize()}
   * values in the vector, and maps one-to-one to the keys in {@link MapValue#getKeys()}
   */
  ColumnVector getValues();
}
