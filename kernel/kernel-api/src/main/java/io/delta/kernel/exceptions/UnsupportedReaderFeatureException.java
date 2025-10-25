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
import java.util.Set;

/**
 * Exception thrown when Kernel encounters unsupported reader table features.
 *
 * @since 4.1.0
 */
@Evolving
public class UnsupportedReaderFeatureException extends UnsupportedTableFeatureException {

  public UnsupportedReaderFeatureException(String tablePath, Set<String> readerFeatures) {
    super(
        tablePath,
        readerFeatures,
        String.format(
            "Unsupported Delta reader features: table `%s` requires reader table features [%s] "
                + "which is unsupported by this version of Delta Kernel.",
            tablePath, String.join(", ", readerFeatures)));
  }
}
