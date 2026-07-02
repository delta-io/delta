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

/** Exception thrown when a requested time travel timestamp is outside the table's commit range. */
public class TimestampOutOfRangeException extends RuntimeException {

  private final long userMillis;
  private final long commitMillis;
  private final boolean afterLatest;

  public TimestampOutOfRangeException(long userMillis, long commitMillis, boolean afterLatest) {
    super(
        String.format(
            "Cannot time travel Delta table to the requested timestamp (%d ms); it is %s the "
                + "available commit range (nearest commit at %d ms).",
            userMillis,
            afterLatest ? "after the latest commit" : "before the earliest commit",
            commitMillis));
    this.userMillis = userMillis;
    this.commitMillis = commitMillis;
    this.afterLatest = afterLatest;
  }

  public long getUserMillis() {
    return userMillis;
  }

  public long getCommitMillis() {
    return commitMillis;
  }

  /**
   * {@code true} if the request is after the latest commit; {@code false} if it is before the
   * earliest (recreatable) commit.
   */
  public boolean isAfterLatest() {
    return afterLatest;
  }
}
