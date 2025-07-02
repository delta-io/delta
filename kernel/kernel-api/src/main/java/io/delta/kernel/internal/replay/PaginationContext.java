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

  // ===== Variables from page token (is empty when getting first page because page token is empty)

  /**
   * The name of the last log file read in the previous page. If not present, this is the first
   * page.
   */
  private final Optional<String> lastReadLogFileName;

  /**
   * The index of the last row that was returned from the last read log file during the previous
   * page. This row index is relative to the file. The current page should begin from the row
   * immediately after this row index.
   */
  private final Optional<Long> lastReturnedRowIndex;

  /**
   * The index of the last sidecar checkpoint file read in the previous page. This index is based on
   * the ordering of sidecar files in the V2 manifest checkpoint file. If present, it must represent
   * the final sidecar file that was read and must correspond to the same file as
   * `lastReadLogFileName`.
   */
  private final Optional<Long> lastReadSidecarFileIdx;

  // TODO: add cached log replay hashsets related info

  private PaginationContext(
      long pageSize,
      Optional<String> lastReadLogFileName,
      Optional<Long> lastReturnedRowIndex,
      Optional<Long> lastReadSidecarFileIdx) {
    checkArgument(pageSize > 0, "Page size must be greater than zero!");
    this.pageSize = pageSize;
    this.lastReadLogFileName = lastReadLogFileName;
    this.lastReturnedRowIndex = lastReturnedRowIndex;
    this.lastReadSidecarFileIdx = lastReadSidecarFileIdx;
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
