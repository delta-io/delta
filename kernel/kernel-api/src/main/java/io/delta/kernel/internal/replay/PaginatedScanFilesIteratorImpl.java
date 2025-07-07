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
import io.delta.kernel.PaginatedScanFilesIterator;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.utils.CloseableIterator;
import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Implementation of {@link PaginatedScanFilesIterator} */
public class PaginatedScanFilesIteratorImpl implements PaginatedScanFilesIterator {

  private static final Logger logger =
      LoggerFactory.getLogger(PaginatedScanFilesIteratorImpl.class);

  /**
   * Filtered ScanFiles iterator retrieved from baseScan but without batches from skipped log files
   */
  private final CloseableIterator<FilteredColumnarBatch> filteredScanFilesIter;

  /** Maximum number of ScanFiles to include in current page */
  private final long pageSize;

  /** Total number of scan files returned in current page */
  private long numScanFilesReturned;

  /**
   * The name of the last log file that was read. This value corresponds to the one saved in the
   * page token;
   */
  private String lastReadLogFileName = null;

  /**
   * The index of the last row returned from the last log file that was read.
   *
   * <p>For example, if the last page contains 3 batches from the same log file, and each batch has
   * 10 rows, this value will be 29 (since row indices start at 0).
   *
   * <p>This value corresponds to the one saved in the page token;
   */
  private long lastReturnedRowIndex = -1;

  private Optional<FilteredColumnarBatch> currentBatch = Optional.empty();
  private boolean closed = false;
  /**
   * Constructs a paginated iterator over scan files on top of a given filtered scan files iterator
   * and pagination context.
   *
   * @param filteredScanFilesIter The underlying scan files iterator with data skipping and
   *     partition pruning applied. This iterator serves as the source of filtered scan results for
   *     pagination.
   * @param paginationContext The pagination context that carries pagination-related information,
   *     such as the maximum number of files to return in a page.
   */
  public PaginatedScanFilesIteratorImpl(
      CloseableIterator<FilteredColumnarBatch> filteredScanFilesIter,
      PaginationContext paginationContext) {
    this.filteredScanFilesIter = filteredScanFilesIter;
    this.pageSize = paginationContext.getPageSize();
  }

  @Override
  public Row getCurrentPageToken() {
    // TODO: change value for data validation here
    return new PageToken(
            lastReadLogFileName,
            lastReturnedRowIndex,
            Optional.empty() /* sidecar file index */,
            Meta.KERNEL_VERSION,
            null /* table path */,
            -1 /* table version */,
            -1 /* predicate hash */,
            -1 /* log segment hash */)
        .toRow();
  }

  @Override
  public boolean hasNext() {
    if (closed) {
      return false;
    }
    if (!currentBatch.isPresent()) {
      prepareNext();
    }
    return currentBatch.isPresent();
  }

  private void prepareNext() {
    if (currentBatch.isPresent()) return;
    if (numScanFilesReturned >= pageSize) return;

    if (!filteredScanFilesIter.hasNext()) return; // base iterator is empty

    FilteredColumnarBatch batch = filteredScanFilesIter.next();
    checkArgument(batch.getFilePath().isPresent(), "file path doesn't exist!");
    String filePath = batch.getFilePath().get();
    if (!filePath.equals(lastReadLogFileName)) {
      lastReadLogFileName = filePath;
      logger.info("filePath " + filePath);
      lastReturnedRowIndex = -1;
    }
    checkArgument(
        batch.getPreComputedNumSelectedRows().isPresent(),
        "pre-computed number of selected rows doesn't exist!");
    long numSelectedAddFilesInBatch = batch.getPreComputedNumSelectedRows().get();
    long numRowsInBatch = batch.getData().getSize();

    currentBatch = Optional.of(batch);
    numScanFilesReturned += numSelectedAddFilesInBatch;
    lastReturnedRowIndex += numRowsInBatch;

    logger.info("total numScanFilesReturned: " + numScanFilesReturned);
    logger.info("numSelectedAddFilesInBatch: " + numSelectedAddFilesInBatch);
    logger.info("numTotalAddFilesInBatch: " + batch.getData().getColumnVector(0).getSize());
    logger.info("numRowsInBatch: " + numRowsInBatch);
  }

  @Override
  public FilteredColumnarBatch next() {
    if (closed) {
      throw new IllegalStateException("Can't call `next` on a closed iterator.");
    }
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    final FilteredColumnarBatch ret = currentBatch.get();
    currentBatch = Optional.empty();
    return ret;
  }

  @Override
  public void close() throws IOException {
    closed = true;
  }
}
