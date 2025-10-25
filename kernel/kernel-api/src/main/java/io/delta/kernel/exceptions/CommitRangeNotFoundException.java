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
import java.util.Optional;

/**
 * Exception thrown when Kernel cannot find any commit files in the requested version range. This
 * can happen when the commit range is empty or when the requested versions don't exist in the
 * table.
 *
 * @since 4.1.0
 */
@Evolving
public class CommitRangeNotFoundException extends KernelException {

  private final String tablePath;
  private final long startVersion;
  private final Optional<Long> endVersion;

  public CommitRangeNotFoundException(
      String tablePath, long startVersion, Optional<Long> endVersion) {
    super(
        String.format(
            "%s: Requested table changes between [%s, %s] but no log files found in the requested"
                + " version range.",
            tablePath, startVersion, endVersion));
    this.tablePath = tablePath;
    this.startVersion = startVersion;
    this.endVersion = endVersion;
  }

  /** @return the table path where the commit range was not found */
  public String getTablePath() {
    return tablePath;
  }

  /** @return the start version of the requested commit range */
  public long getStartVersion() {
    return startVersion;
  }

  /**
   * @return the end version of the requested commit range, or empty if no end version was specified
   */
  public Optional<Long> getEndVersion() {
    return endVersion;
  }
}
