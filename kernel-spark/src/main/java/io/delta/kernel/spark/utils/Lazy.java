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
package io.delta.kernel.spark.utils;

import java.util.Optional;
import java.util.function.Supplier;

/**
 * A lazy initialization utility that defers computation until first access.
 *
 * <p>This class is thread-safe for single-threaded access but not for concurrent access from
 * multiple threads. For concurrent scenarios, external synchronization is required.
 *
 * @param <T> the type of the lazily computed value
 */
public class Lazy<T> {
  private final Supplier<T> supplier;
  private Optional<T> instance = Optional.empty();

  public Lazy(Supplier<T> supplier) {
    this.supplier = supplier;
  }

  /**
   * Returns the lazily computed value, computing it on first access.
   *
   * @return the computed value
   */
  public T get() {
    if (!instance.isPresent()) {
      instance = Optional.of(supplier.get());
    }
    return instance.get();
  }

  /**
   * Returns true if the value has been computed, false otherwise.
   *
   * @return true if computed, false if not yet computed
   */
  public boolean isPresent() {
    return instance.isPresent();
  }
}
