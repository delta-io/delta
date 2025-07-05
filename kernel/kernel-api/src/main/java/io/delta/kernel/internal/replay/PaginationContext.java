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

  public static PaginationContext forPageWithPageToken(long pageSize, PageToken pageToken) {
    Objects.requireNonNull(pageToken, "page token is null");
    return new PaginationContext(pageSize, Optional.of(pageToken));
  }

  public static PaginationContext forFirstPage(long pageSize) {
    return new PaginationContext(pageSize, Optional.empty() /* page token */);
  }

  /** maximum number of ScanFiles to return in the current page */
  private final long pageSize;

  /** Optional Page Token */
  private final Optional<PageToken> pageToken;

  // TODO: add cached log replay hashsets related info

  private PaginationContext(long pageSize, Optional<PageToken> pageToken) {
    checkArgument(pageSize > 0, "Page size must be greater than zero!");
    this.pageSize = pageSize;
    this.pageToken = pageToken;
  }

  public long getPageSize() {
    return pageSize;
  }

  public Optional<String> getLastReadLogFileName() {
    if (!pageToken.isPresent()) return Optional.empty();
    return Optional.of(pageToken.get().getLastReadLogFileName());
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
