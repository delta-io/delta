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
package org.apache.spark.sql.delta.v2.read.metadata;

import org.apache.spark.sql.execution.datasources.PartitionedFile;

final class RowTrackingMetadataValueUtils {

  private RowTrackingMetadataValueUtils() {}

  /**
   * Returns the required row-tracking long constant from {@code file}. Fails fast with a clear
   * {@link IllegalStateException} instead of an NPE on {@code longValue()} when the constant is
   * missing. {@code requiredConstantDescription} is spliced into the exception message; callers
   * pass descriptions of the form {@code "<name> constant"}.
   */
  static long requireRowTrackingLong(
      PartitionedFile file, String key, String requiredConstantDescription) {
    Object rawValue = file.otherConstantMetadataColumnValues().get(key).getOrElse(() -> null);
    if (rawValue == null) {
      throw new IllegalStateException(
          "Missing '"
              + key
              + "' on PartitionedFile '"
              + file.filePath()
              + "' for row tracking - every Delta file with row tracking enabled must carry a "
              + requiredConstantDescription
              + ".");
    }
    return ((Number) rawValue).longValue();
  }
}
