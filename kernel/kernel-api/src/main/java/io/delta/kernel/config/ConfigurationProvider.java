/*
 * Copyright (2024) The Delta Lake Project Authors.
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

package io.delta.kernel.config;

import io.delta.kernel.annotation.Evolving;
import java.util.NoSuchElementException;
import java.util.Optional;

/**
 * Provides per-session configuration values for Delta Kernel, specific to the current execution.
 *
 * <p>These values are not Delta table properties. Delta table properties are stored in the Delta
 * log metadata and apply globally to all readers and writers interacting with the table.
 *
 * <p>Instead, the configuration values provided by this interface are scoped to a single instance
 * of Delta Kernel during its execution.
 *
 * @since 3.3.0
 */
@Evolving
public interface ConfigurationProvider {
  /**
   * Retrieves the configuration value for the given key.
   *
   * @param key the configuration key
   * @return the configuration value associated with the key
   * @throws NoSuchElementException if the key is not found
   */
  String get(String key);

  /**
   * Retrieves the configuration value for the given key. If it doesn't exist, returns {@link
   * Optional#empty}.
   *
   * @param key the configuration key
   * @return the configuration value associated with the key, if it exists
   */
  Optional<String> getOptional(String key);

  /**
   * Checks if the configuration provider contains the given key.
   *
   * @param key the configuration key
   * @return {@code true} if the key exists, else {@code false}
   */
  boolean contains(String key);
}
