package io.delta.kernel;

import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.utils.CloseableIterator;

/** Extension of {@link Scan} that supports pagination. */
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
   * Returns the page token representing the current position of the iterator. If this method is
   * called while the iterator is partially consumed, the returned token corresponds to the next
   * unconsumed element. This token can be passed back in a subsequent paginated scan to resume
   * reading from the same position.
   *
   * <p>The token may also contain additional metadata for validation purposes (e.g., to detect
   * mismatches caused by query params or log segment changes).
   *
   * @return a {@link Row} encoding the current scan position and optional validation info.
   */
  Row getCurrentPageToken();

  /**
   * Returns the log replay hashsets as a {@link ColumnarBatch}. This data can optionally be cached
   * by the caller and injected into subsequent page requests to improve performance.
   *
   * <p>Each column in the {@link ColumnarBatch} represents a specific attribute of the log replay
   * hashsets.
   *
   * @return a {@link ColumnarBatch} containing the log replay hashsets.
   */
  ColumnarBatch getLogReplayHashsets();
}
