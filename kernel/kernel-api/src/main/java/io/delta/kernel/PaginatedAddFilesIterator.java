package io.delta.kernel;

import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.utils.CloseableIterator;

/**
 *
* */
public interface PaginatedAddFilesIterator extends CloseableIterator<FilteredColumnarBatch> {
  /** get Current Page Token */
  Row getCurrentPageToken();
}
