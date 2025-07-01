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

import java.util.Optional;

/** {@code PaginationContext} carries pagination-related information. */
public class PaginationContext {

  // ===== Variables from page token (is empty when getting first page because page token is empty)

  /**
   * The name of the last log file read in the previous page. If not present, this is the first
   * page.
   */
  public final Optional<String> lastReadLogFileName;
  /**
   * The index of the last row that was returned from the last read log file during the previous
   * page. This row index is relative to the file. The current page should begin from the row
   * immediately after this row index.
   */
  public final Optional<Long> lastReturnedRowIndex;
  /**
   * The index of the last sidecar checkpoint file read in the previous page. This index is based on
   * the ordering of sidecar files in the V2 manifest checkpoint file. If present, it must represent
   * the final sidecar file that was read and must correspond to the same file as
   * `lastReadLogFileName`.
   */
  public final Optional<Long> lastReadSidecarFileIdx;

  // ===== Non-page-token related info =====

  /** maximum number of ScanFiles to return in the current page */
  public final long pageSize;

  // TODO: add cached log replay hashsets related info

  private PaginationContext(
      Optional<String> lastReadLogFileName,
      Optional<Long> lastReturnedRowIndex,
      Optional<Long> lastReadSidecarFileIdx,
      long pageSize) {
    this.lastReadLogFileName = lastReadLogFileName;
    this.lastReturnedRowIndex = lastReturnedRowIndex;
    this.lastReadSidecarFileIdx = lastReadSidecarFileIdx;
    this.pageSize = pageSize;
  }

  /** Factory for non first page, where page token is available */
  public static PaginationContext forPageWithPageToken(
      String lastReadLogFileName,
      long lastReturnedRowIndex,
      Optional<Long> lastReadSidecarFileIdx,
      long pageSize) {
    return new PaginationContext(
        Optional.of(lastReadLogFileName),
        Optional.of(lastReturnedRowIndex),
        lastReadSidecarFileIdx,
        pageSize);
  }

  /** Factory for the very first page, where no page token is available */
  public static PaginationContext forFirstPage(long pageSize) {
    return new PaginationContext(Optional.empty(), Optional.empty(), Optional.empty(), pageSize);
  }
}
