/*
 * Copyright (2021) The Delta Lake Project Authors.
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

public class ExceptionUtils {

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

  public static class ResourceNotFoundException extends RuntimeException {
    public ResourceNotFoundException(String message) {
      super(message);
    }
  }

  public static class ResourceAlreadyExistException extends RuntimeException {
    public ResourceAlreadyExistException(String message) {
      super(message);
    }
  }
}
