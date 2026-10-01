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
package io.delta.kernel.exceptions;

import io.delta.kernel.annotation.Evolving;
import java.util.Collections;
import java.util.Set;

/**
 * Base exception thrown when Kernel encounters unsupported table features.
 *
 * @since 4.1.0
 */
@Evolving
public class UnsupportedTableFeatureException extends KernelException {

  private final String tablePath;
  private final Set<String> unsupportedFeatures;

  public UnsupportedTableFeatureException(
      String tablePath, Set<String> unsupportedFeatures, String message) {
    super(message);
    this.tablePath = tablePath;
    this.unsupportedFeatures =
        unsupportedFeatures != null
            ? Collections.unmodifiableSet(unsupportedFeatures)
            : Collections.emptySet();
  }

  public UnsupportedTableFeatureException(
      String tablePath, String unsupportedFeature, String message) {
    this(tablePath, Collections.singleton(unsupportedFeature), message);
  }

  /**
   * @return the table path where the unsupported features were encountered, or null if not
   *     applicable
   */
  public String getTablePath() {
    return tablePath;
  }

  /** @return an unmodifiable set of unsupported feature names */
  public Set<String> getUnsupportedFeatures() {
    return unsupportedFeatures;
  }
}
