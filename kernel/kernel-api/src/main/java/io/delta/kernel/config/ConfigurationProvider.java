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
 * A generic interface for retrieving configuration values.
 *
 * @since 3.3.0
 */
@Evolving
public interface ConfigurationProvider {
  /**
   * Retrieves the configuration value for the given key.
   *
   * @param key the configuration key
   * @return the configuration value associated with the key, if it exists
   * @throws NoSuchElementException if the key is not found
   */
  String get(String key) throws NoSuchElementException;

  /**
   * Retrieves the configuration value for the given key. If it doesn't exist, returns {@link
   * Optional#empty}.
   *
   * @param key the configuration key
   * @return the configuration value associated with the key, if it exists
   */
  Optional<String> getOptional(String key);

  /**
   * Retrieves the configuration value for the given key, ensuring that the value is not null.
   *
   * @param key the configuration key
   * @return the configuration value associated with the key, guaranteed to be non-null
   * @throws NoSuchElementException if the key is not found in the configuration
   * @throws IllegalStateException if the key exists but its value is null
   */
  default String getNonNull(String key) throws NoSuchElementException, IllegalStateException {
      final String value = get(key);
      if (value == null) throw new IllegalStateException(String.format("%s is null", key));
      return value;
  }
}
