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
package io.delta.kernel.internal.replay;

import static io.delta.kernel.internal.util.Preconditions.checkArgument;

import java.util.Objects;
import java.util.Optional;

/** {@code PaginationContext} carries pagination-related information. */
public class PaginationContext {

  public static PaginationContext forPageWithPageToken(
      long pageSize,
      String lastReadLogFileName,
      long lastReturnedRowIndex,
      Optional<Long> lastReadSidecarFileIdx) {
    Objects.requireNonNull(lastReadLogFileName, "lastReadLogFileName is null");
    Objects.requireNonNull(lastReadSidecarFileIdx, "lastReadSidecarFileIdx is null");
    return new PaginationContext(
        pageSize,
        Optional.of(lastReadLogFileName),
        Optional.of(lastReturnedRowIndex),
        lastReadSidecarFileIdx);
  }

  public static PaginationContext forFirstPage(long pageSize) {
    return new PaginationContext(
        pageSize,
        Optional.empty() /* lastReadLogFileName */,
        Optional.empty() /* lastReturnedRowIndex */,
        Optional.empty() /* lastReadSidecarFileIdx */);
  }

  /** maximum number of ScanFiles to return in the current page */
  private final long pageSize;

  /**Optional Page Token*/
  private final PageToken pageToken;

  // TODO: add cached log replay hashsets related info

  private PaginationContext(
      long pageSize,
      Optional<PageToken> pageToken) {
    checkArgument(pageSize > 0, "Page size must be greater than zero!");
    this.pageSize = pageSize;
  }

  public long getPageSize() {
    return pageSize;
  }

  public Optional<String> getLastReadLogFileName() {
    return lastReadLogFileName;
  }

  public Optional<Long> getLastReturnedRowIndex() {
    return lastReturnedRowIndex;
  }

  public Optional<Long> getLastReadSidecarFileIdx() {
    return lastReadSidecarFileIdx;
  }
}
