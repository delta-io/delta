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
package io.delta.kernel;

import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.utils.CloseableIterator;

/**
 * An iterator over {@link FilteredColumnarBatch}, each representing a batch of Scan Files in a
 * paginated scan. This iterator also exposes the page token that can be used to resume the scan
 * from the exact position current page ends in a subsequent request.
 *
 * <p>This interface extends {@link CloseableIterator} and should be closed when the iteration is
 * complete.
 */
public interface PaginatedAddFilesIterator extends CloseableIterator<FilteredColumnarBatch> {
  /**
   * Returns a page token representing the starting position of next page. This token can be used to
   * resume the scan from the exact position current page ends in a subsequent request. Page token
   * also contains metadata for validation purpose, such as detecting changes in query parameters or
   * the underlying log files.
   *
   * <p>The page token represents the position of current iterator at the time it's called. If the
   * iterator is only partially consumed, the returned token will always point to the beginning of
   * the next unconsumed {@link FilteredColumnarBatch}. This method will return Option.empty() if
   * all data in the Scan is consumed (no more non-empty pages remain).
   *
   * <p>Note: Each page is expected to fully include each {@link FilteredColumnarBatch}. That is,
   * pages do not split batches — the token always refers to the start of the next full batch, never
   * to a position within a batch.
   */
  Row getCurrentPageToken();
}
