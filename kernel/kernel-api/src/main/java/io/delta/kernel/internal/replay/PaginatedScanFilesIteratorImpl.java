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

/** Implementation of {@link PaginatedScanFilesIterator} */
public class PaginatedScanFilesIteratorImpl implements PaginatedScanFilesIterator {

  private final CloseableIterator<FilteredColumnarBatch> filteredScanFilesIter;
  private final long pageSize;

  private long numScanFilesReturned;
  private String lastLogFileName = null;
  private long rowIdxInLastFile = -1;
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
    throw new UnsupportedOperationException("Not implemented");
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
    String fileName = batch.getFilePath().get();
    if (!fileName.equals(lastLogFileName)) {
      lastLogFileName = fileName;
      System.out.println("filePath " + fileName);
      rowIdxInLastFile = 0; // row idx starts from 1
    }
    long numActiveAddFiles = batch.getPreComputedNumSelectedRows().get();
    long rowNum = batch.getData().getSize();

    nextBatch = Optional.of(batch);
    numScanFilesReturned += numActiveAddFiles;
    rowIdxInLastFile += rowNum;

    System.out.println("numAddFilesReturned: " + numScanFilesReturned);
    System.out.println("numActiveAddFiles: " + numActiveAddFiles);
    System.out.println("numTotalAddFiles: " + batch.getData().getColumnVector(0).getSize());
    System.out.println("numOfRows: " + rowNum);
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
