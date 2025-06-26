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
import io.delta.kernel.engine.Engine;
import io.delta.kernel.utils.CloseableIterator;
import java.util.Optional;

/**
 * Extension of {@link Scan} that supports pagination.
 *
 * <p>This interface allows consumers to retrieve scan results in discrete, ordered pages rather
 * than all at once. This is particularly useful for large datasets where materializing the full
 * result set would be expensive in terms of memory or compute resources.
 *
 * <p>Pagination is achieved via a combination of {@code pageSize} and {@code pageToken}. The {@code
 * pageSize} controls how many Scan files are returned in each page, while the {@code pageToken}
 * encodes the location of next batch to read and is used to resume the scan from exactly where the
 * last page ended. For the first page, the {@code pageToken} should be {@code Optional.empty()}.
 *
 * <p>Consumers typically use {@link PaginatedScan} in a loop: they call {@code getScanFiles()} to
 * retrieve an iterator over the current page's scan files. After consuming the iterator, users
 * should call {@code getCurrentPageToken()} to retrieve a token to pass into the next page request.
 * This allows users to scan the dataset incrementally, resuming from where they left off.
 */
public interface PaginatedScan extends Scan {

  /**
   * Get an iterator of Scan files for the current page.
   *
   * @param engine {@link Engine} instance to use in Delta Kernel.
   * @return iterator of {@link FilteredColumnarBatch}s for the current page.
   */
  @Override
  CloseableIterator<FilteredColumnarBatch> getScanFiles(Engine engine);

  /**
   * Returns a page token representing the current position in the ScanFiles iterator, allowing the
   * scan to be resumed from that point in a subsequent request. If the iterator is partially
   * consumed, the token corresponds to the next unconsumed element.
   *
   * <p>Note: Since {@code getScanFiles()} returns an iterator over batches, this page token assumes
   * that the current batch has been fully consumed. As a result, the token points to the start of
   * the next batch.
   *
   * <p>If the scan has been fully consumed (i.e., no more pages remain), this method returns {@code
   * Optional.empty()}.
   *
   * <p>The returned token also includes metadata for validation purposes, such as detecting changes
   * in query parameters or underlying delta log content between requests.
   *
   * <p>Callers should invoke {@code getCurrentPageToken()} only after finishing consumption of the
   * current iterator. If they don't consume the entire iterator; then the returned page token will
   * always point to the beginning of the next batch in the iterator.
   *
   * @return an {@link Optional} {@link Row} encoding the current scan position and validation
   *     metadata.
   */
  Optional<Row> getCurrentPageToken();
}
