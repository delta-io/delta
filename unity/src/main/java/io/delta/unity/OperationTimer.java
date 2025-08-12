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

package io.delta.unity;

import java.util.function.Supplier;
import org.slf4j.Logger;

public final class OperationTimer {

  private OperationTimer() {}

  @FunctionalInterface
  public interface ThrowingSupplier<T, E extends Exception> {
    T get() throws E;
  }

  /** Times an operation that throws a checked exception of type E and logs the duration. */
  @SuppressWarnings("unchecked")
  public static <T, E extends Exception> T timeOperation(
      Logger logger, String operationName, String ucTableId, ThrowingSupplier<T, E> operation)
      throws E {
    final long startTime = System.nanoTime();
    try {
      final T result = operation.get();
      final long durationMs = (System.nanoTime() - startTime) / 1_000_000;
      logger.info("[{}] {} completed in {} ms", ucTableId, operationName, durationMs);
      return result;
    } catch (Exception e) {
      final long durationMs = (System.nanoTime() - startTime) / 1_000_000;
      logger.warn(
          "[{}] {} failed after {} ms: {}", ucTableId, operationName, durationMs, e.getMessage());
      throw (E) e; // Safe cast since operation can only throw E
    }
  }

  /** Times an operation and logs the duration. */
  public static <T> T timeUncheckedOperation(
      Logger logger, String operationName, String ucTableId, Supplier<T> operation) {
    return timeOperation(logger, operationName, ucTableId, operation::get);
  }
}
