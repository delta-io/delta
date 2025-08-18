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

package io.delta.unity.utils;

import java.util.function.Supplier;
import org.slf4j.Logger;

/** Utility class for timing operations and logging their execution duration. */
public class OperationTimer {
  private OperationTimer() {}

  /** Times an operation and logs the duration. */
  public static <T> T timeOperation(
      Logger logger, String operationName, String ucTableId, Supplier<T> operation) {
    final long startTime = System.nanoTime();
    try {
      final T result = operation.get();
      final long durationMs = nanoToMs(System.nanoTime() - startTime);
      logger.info("[{}] {} completed in {} ms", ucTableId, operationName, durationMs);
      return result;
    } catch (Exception e) {
      final long durationMs = nanoToMs(System.nanoTime() - startTime);
      logger.warn(
          "[{}] {} failed after {} ms: {}", ucTableId, operationName, durationMs, e.getMessage());
      throw e;
    }
  }

  private static long nanoToMs(long nanoTime) {
    return nanoTime / 1_000_000;
  }
}
