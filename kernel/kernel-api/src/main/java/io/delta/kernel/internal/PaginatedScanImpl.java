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

public class PaginatedScanImpl implements PaginatedScan {
  private final long pageSize;
  private final Optional<PageToken> pageToken;
  private final ScanImpl baseScan;

  public PaginatedScanImpl(ScanImpl baseScan, Optional<Row> pageTokenInRow, long pageSize) {
    this.baseScan = baseScan;
    this.pageToken = pageTokenInRow.map(PageToken::fromRow);
    this.pageSize = pageSize;
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
    return this.getScanFiles(engine, false);
  }

  public PaginatedScanFilesIterator getScanFiles(Engine engine, boolean includeStates) {
    PaginationContext paginationContext =
        pageToken
            .map(token -> PaginationContext.forPageWithPageToken(pageSize, token))
            .orElseGet(() -> PaginationContext.forFirstPage(pageSize));
    CloseableIterator<FilteredColumnarBatch> scanFileIter =
        baseScan.getScanFiles(engine, includeStates, Optional.of(paginationContext));
    return new PaginatedScanFilesIteratorImpl(scanFileIter, paginationContext);
  }
}
