package io.delta.kernel;

import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.utils.CloseableIterator;
import java.util.Optional;

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
   * Returns a page token representing the current position in the iterator. If the iterator is
   * partially consumed, the token corresponds to the next unconsumed element, allowing the scan to
   * be resumed from that point in a subsequent paginated request.
   *
   * <p>If the scan has been fully consumed (i.e., no more pages remain), this method returns {@code
   * Optional.empty()}.
   *
   * <p>The returned token may also include metadata for validation purposes, such as detecting
   * changes in query parameters or log segments between requests.
   *
   * @return an {@link Optional} {@link Row} encoding the current scan position and validation
   *     metadata.
   */
  Optional<Row> getCurrentPageToken();

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
