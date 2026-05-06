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
package io.delta.kernel.exceptions;

import io.delta.kernel.annotation.Evolving;
import java.util.Optional;

/**
 * Exception thrown when the requested start version is not found in the Delta log. This can happen
 * when the requested start version has been cleaned up due to log retention policies.
 *
 * @since 4.1.0
 */
@Evolving
public class StartVersionNotFoundException extends KernelException {

  private final String tablePath;
  private final long startVersionRequested;
  private final Optional<Long> earliestAvailableVersion;

  public StartVersionNotFoundException(
      String tablePath, long startVersionRequested, Optional<Long> earliestAvailableVersion) {
    super(buildMessage(tablePath, startVersionRequested, earliestAvailableVersion));
    this.tablePath = tablePath;
    this.startVersionRequested = startVersionRequested;
    this.earliestAvailableVersion = earliestAvailableVersion;
  }

  /** @return the table path where the start version was not found */
  public String getTablePath() {
    return tablePath;
  }

  /** @return the start version that was requested but not found */
  public long getStartVersionRequested() {
    return startVersionRequested;
  }

  /**
   * @return the earliest available version in the Delta log, or empty if no versions are available
   */
  public Optional<Long> getEarliestAvailableVersion() {
    return earliestAvailableVersion;
  }

  private static String buildMessage(
      String tablePath, long startVersionRequested, Optional<Long> earliestAvailableVersion) {
    String message =
        String.format(
            "%s: Requested table changes beginning with startVersion=%s but no log file found for "
                + "version %s.",
            tablePath, startVersionRequested, startVersionRequested);
    if (earliestAvailableVersion.isPresent()) {
      message =
          message
              + String.format(" Earliest available version is %s", earliestAvailableVersion.get());
    }
    return message;
  }
}
