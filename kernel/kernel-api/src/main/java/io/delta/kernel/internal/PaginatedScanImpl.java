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

package io.delta.kernel.internal;

import io.delta.kernel.PaginatedScan;
import io.delta.kernel.PaginatedScanFilesIterator;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.internal.replay.PageToken;
import io.delta.kernel.internal.replay.PaginatedScanFilesIteratorImpl;
import io.delta.kernel.internal.replay.PaginationContext;
import io.delta.kernel.utils.CloseableIterator;
import java.util.Optional;

/** Implementation of {@link PaginatedScan} */
public class PaginatedScanImpl implements PaginatedScan {
  private final PaginationContext paginationContext;
  private final ScanImpl baseScan;
  private final long pageSize;
  private final Optional<PageToken> pageTokenOpt;

  public PaginatedScanImpl(
      ScanImpl baseScan,
      String tablePath,
      long tableVersion,
      long pageSize,
      Optional<Row> pageTokenRowOpt) {
    this.baseScan = baseScan;
    this.pageSize = pageSize;
    this.pageTokenOpt = pageTokenRowOpt.map(PageToken::fromRow);
    // TODO: get hash value of predicate & log segment and check values in pagination context
    this.paginationContext =
        pageTokenOpt
            .map(
                token ->
                    PaginationContext.forPageWithPageToken(
                        tablePath, tableVersion, pageSize, token))
            .orElseGet(() -> PaginationContext.forFirstPage(tablePath, tableVersion, pageSize));
  }

  @Override
  public Optional<Predicate> getRemainingFilter() {
    return baseScan.getRemainingFilter();
  }

  @Override
  public Row getScanState(Engine engine) {
    return baseScan.getScanState(engine);
  }

  @Override
  public PaginatedScanFilesIterator getScanFiles(Engine engine) {
    return this.getScanFiles(engine, false /* include stats */);
  }

  public PaginatedScanFilesIterator getScanFiles(Engine engine, boolean includeStates) {
    CloseableIterator<FilteredColumnarBatch> filteredScanFilesIter =
        baseScan.getScanFiles(engine, includeStates, Optional.of(paginationContext));
    return new PaginatedScanFilesIteratorImpl(filteredScanFilesIter, paginationContext);
  }
}
