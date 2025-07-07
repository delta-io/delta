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

import io.delta.kernel.PaginatedScanFilesIterator;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.utils.CloseableIterator;
import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.Optional;

import static io.delta.kernel.internal.util.Preconditions.checkArgument;

/** Implementation of {@link PaginatedScanFilesIterator} */
public class PaginatedScanFilesIteratorImpl implements PaginatedScanFilesIterator {

  private final CloseableIterator<FilteredColumnarBatch> filteredScanFilesIter;
  private final long pageSize;

  private long numScanFilesReturned;
  private String currentLogFileName = null;
  private long currentRowIdxInFile = -1;
  private Optional<FilteredColumnarBatch> nextBatch;
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
    //TODO: change value for data validation here
    return new PageToken(currentLogFileName, currentRowIdxInFile, Optional.empty(),
        null,null,-1,-1,-1).toRow();
  }

  @Override
  public boolean hasNext() {
    if (closed) {
      return false;
    }
    if (!nextBatch.isPresent()) {
      prepareNext();
    }
    return nextBatch.isPresent();
  }

  private void prepareNext() {
    if(nextBatch.isPresent()) return;
    if (numScanFilesReturned >= pageSize) return;

    if (!filteredScanFilesIter.hasNext()) return; // base iterator is empty

    FilteredColumnarBatch batch = filteredScanFilesIter.next();
    checkArgument(batch.getFilePath().isPresent(), "file path doesn't exist!");
    String filePath = batch.getFilePath().get();
    if (!filePath.equals(currentLogFileName)) {
      currentLogFileName = filePath;
      System.out.println("filePath " + filePath);
      currentRowIdxInFile = 0; // row idx starts from 1
    }
    checkArgument(batch.getPreComputedNumSelectedRows().isPresent(), "pre-computed number of selected rows doesn't exist!");
    long numSelectedAddFilesInBatch = batch.getPreComputedNumSelectedRows().get();
    long numRowsInBatch = batch.getData().getSize();

    nextBatch = Optional.of(batch);
    numScanFilesReturned += numSelectedAddFilesInBatch;
    currentRowIdxInFile += numRowsInBatch;

    //TODO: change to logger
    System.out.println("total numScanFilesReturned: " + numScanFilesReturned);
    System.out.println("numSelectedAddFilesInBatch: " + numSelectedAddFilesInBatch);
    System.out.println("numTotalAddFilesInBatch: " + batch.getData().getColumnVector(0).getSize());
    System.out.println("numRowsInBatch: " + numRowsInBatch);
  }

  @Override
  public FilteredColumnarBatch next() {
    if (closed) {
      throw new IllegalStateException("Can't call `next` on a closed iterator.");
    }
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    final FilteredColumnarBatch ret = nextBatch.get();
    nextBatch = Optional.empty();
    return ret;
  }

  @Override
  public void close() throws IOException {
    closed = true;
  }
}
