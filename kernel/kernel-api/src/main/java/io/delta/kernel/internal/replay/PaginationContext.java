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

import io.delta.kernel.Meta;
import java.util.Objects;
import java.util.Optional;

/** {@code PaginationContext} carries pagination-related information. */
public class PaginationContext {

  public static PaginationContext forPageWithPageToken(
      String tablePath, long tableVersion, long pageSize, PageToken pageToken) {
    Objects.requireNonNull(pageToken, "page token is null");
    Objects.requireNonNull(tablePath, "table path is null");
    checkArgument(
        tablePath.equals(pageToken.getTablePath()),
        "Invalid page token: token table path does not match the requested table path. "
            + "Expected: %s, Found: %s",
        tablePath,
        pageToken.getTablePath());
    checkArgument(
        tableVersion == pageToken.getTableVersion(),
        "Invalid page token: token table version does not match the requested table version. "
            + "Expected: %d, Found: %d",
        tableVersion,
        pageToken.getTableVersion());
    checkArgument(
        Meta.KERNEL_VERSION.equals(pageToken.getKernelVersion()),
        "Invalid page token: token kernel version does not match the requested kernel version. "
            + "Expected: %s, Found: %s",
        Meta.KERNEL_VERSION,
        pageToken.getKernelVersion());
    return new PaginationContext(tablePath, tableVersion, pageSize, Optional.of(pageToken));
  }

  public static PaginationContext forFirstPage(String tablePath, long tableVersion, long pageSize) {
    Objects.requireNonNull(tablePath, "table path is null");
    return new PaginationContext(
        tablePath, tableVersion, pageSize, Optional.empty() /* page token */);
  }

  private final String tablePath;

  private final long tableVersion;

  // TODO: add hash value of log segment and predicate

  /** maximum number of ScanFiles to return in the current page */
  private final long pageSize;

  /** Optional Page Token */
  private final Optional<PageToken> pageToken;

  // TODO: add cached log replay hashsets related info

  private PaginationContext(
      String tablePath, long tableVersion, long pageSize, Optional<PageToken> pageToken) {
    checkArgument(pageSize > 0, "Page size must be greater than zero!");
    this.tablePath = tablePath;
    this.tableVersion = tableVersion;
    this.pageSize = pageSize;
    this.pageToken = pageToken;
  }

  public String getTablePath() {
    return tablePath;
  }

  public long getTableVersion() {
    return tableVersion;
  }

  public long getPageSize() {
    return pageSize;
  }

  public Optional<String> getLastReadLogFilePath() {
    if (!pageToken.isPresent()) return Optional.empty();
    return Optional.of(pageToken.get().getLastReadLogFilePath());
  }

  public Optional<Long> getLastReturnedRowIndex() {
    if (!pageToken.isPresent()) return Optional.empty();
    return Optional.of(pageToken.get().getLastReturnedRowIndex());
  }

  public Optional<Long> getLastReadSidecarFileIdx() {
    if (!pageToken.isPresent()) return Optional.empty();
    return pageToken.get().getLastReadSidecarFileIdx();
  }
}
