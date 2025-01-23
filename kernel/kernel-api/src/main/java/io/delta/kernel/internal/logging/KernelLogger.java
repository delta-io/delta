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

package io.delta.kernel.internal.logging;

import java.util.function.Supplier;
import org.slf4j.Logger;

public class KernelLogger {

  private final Logger loggerImpl;
  private final String prefix;

  public KernelLogger(Logger loggerImpl, String tablePath) {
    this.loggerImpl = loggerImpl;
    this.prefix = String.format("[%s]: ", tablePath);
  }

  public void debug(String msg) {
    loggerImpl.debug(prefix + msg);
  }

  public void debug(String msg, Object... args) {
    loggerImpl.debug(prefix + msg, args);
  }

  public void debug(Supplier<String> message) {
    if (loggerImpl.isDebugEnabled()) {
      loggerImpl.debug(prefix + message.get());
    }
  }

  public void info(String msg) {
    loggerImpl.info(prefix + msg);
  }

  public void info(String msg, Object... args) {
    loggerImpl.info(prefix + msg, args);
  }

  public void warn(String msg) {
    loggerImpl.warn(prefix + msg);
  }

  public void warn(String msg, Object... args) {
    loggerImpl.warn(prefix + msg, args);
  }

  public void error(String msg) {
    loggerImpl.error(prefix + msg);
  }

  public void error(String msg, Object... args) {
    loggerImpl.error(prefix + msg, args);
  }

  public <T> T timeOperation(String operationName, Supplier<T> supplier) {
    final long start = System.currentTimeMillis();
    try {
      return supplier.get();
    } finally {
      final long end = System.currentTimeMillis();
      info("Operation '{}' took {} ms", operationName, (end - start));
    }
  }
}
