package io.delta.kernel.internal.replay;

import io.delta.kernel.PaginatedScanFilesIterator;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.utils.CloseableIterator;
import java.io.IOException;

public class PaginatedScanFilesIteratorImpl implements PaginatedScanFilesIterator {

  private final CloseableIterator<FilteredColumnarBatch> originalIterator;
  private final long pageSize; // max num of files to return in this page

  public PaginatedScanFilesIteratorImpl(
      CloseableIterator<FilteredColumnarBatch> originalIterator,
      PaginationContext paginationContext) {
    this.originalIterator = originalIterator;
    this.pageSize = paginationContext.getPageSize();
  }

  @Override
  public Row getCurrentPageToken() {
    return null;
  }

  @Override
  public boolean hasNext() {
    return false;
  }

  @Override
  public FilteredColumnarBatch next() {
    return null;
  }

  @Override
  public void close() throws IOException {}
}
