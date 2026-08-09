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
package io.delta.spark.internal.v2.exception;

/** Exception thrown when a requested version is not available in the Delta log. */
public class VersionNotFoundException extends RuntimeException {

  private final long userVersion;
  private final long earliest;
  private final long latest;

  public VersionNotFoundException(long userVersion, long earliest, long latest) {
    super(
        String.format(
            "Cannot time travel Delta table to version %d. Available versions: [%d, %d].",
            userVersion, earliest, latest));
    this.userVersion = userVersion;
    this.earliest = earliest;
    this.latest = latest;
  }

  public long getUserVersion() {
    return userVersion;
  }

  public long getEarliest() {
    return earliest;
  }

  public long getLatest() {
    return latest;
  }
}
