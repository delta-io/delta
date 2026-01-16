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

package io.delta.flink.table;

import java.util.function.Predicate;

/** Utility methods and common exception types for exception inspection and handling. */
public class ExceptionUtils {

  /**
   * Creates a predicate that applies the given predicate recursively to an exception and its causal
   * chain.
   *
   * <p>The returned predicate returns {@code true} if the supplied predicate matches the exception
   * itself or any of its causes.
   *
   * @param pred the predicate to apply to each exception in the causal chain
   * @return a predicate that recursively checks the exception and its causes
   */
  public static Predicate<Throwable> recursiveCheck(Predicate<Throwable> pred) {
    return new Predicate<Throwable>() {
      @Override
      public boolean test(Throwable e) {
        if (e == null) {
          return false;
        }
        if (pred.test(e)) {
          return true;
        }
        return test(e.getCause());
      }
    };
  }

  /** Exception indicating that a requested resource does not exist. */
  public static class ResourceNotFoundException extends RuntimeException {
    public ResourceNotFoundException(String message) {
      super(message);
    }
  }

  /** Exception indicating that a resource already exists and cannot be created again. */
  public static class ResourceAlreadyExistException extends RuntimeException {
    public ResourceAlreadyExistException(String message) {
      super(message);
    }
  }
}
