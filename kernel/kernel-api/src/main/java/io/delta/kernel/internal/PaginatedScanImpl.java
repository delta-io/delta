package io.delta.kernel.internal;

import io.delta.kernel.PaginatedAddFilesIterator;
import io.delta.kernel.PaginatedScan;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.replay.*;
import io.delta.kernel.metrics.SnapshotReport;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import java.util.Optional;

public class PaginatedScanImpl implements PaginatedScan {

  private final long pageSize;
  private final Optional<PageToken> pageToken;
  private final ScanImpl baseScan;
  private PaginatedAddFilesIterator paginatedIter;

  public PaginatedScanImpl(
      StructType snapshotSchema,
      StructType readSchema,
      Protocol protocol,
      Metadata metadata,
      LogReplay logReplay,
      Optional<Predicate> filter,
      Path dataPath,
      SnapshotReport snapshotReport,
      Optional<Row> pageTokenInRow,
      long pageSize) {
    baseScan =
        new ScanImpl(
            snapshotSchema,
            readSchema,
            protocol,
            metadata,
            logReplay,
            filter,
            dataPath,
            snapshotReport);
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
  public PaginatedAddFilesIterator getScanFiles(Engine engine) {
    System.out.println("try fetching scan iter 1 ");
    return this.getScanFiles(engine, false);
  }

  public PaginatedAddFilesIterator getScanFiles(Engine engine, boolean includeStates) {
    PaginationContext paginationContext = new PaginationContext(pageSize, pageToken);
    System.out.println("try fetching scan iter 2");
    CloseableIterator<FilteredColumnarBatch> scanFileIter =
        baseScan.getScanFiles(engine, includeStates, paginationContext);
    System.out.println("successfully fetch iterator");
    return new PaginatedAddFilesIteratorImpl(scanFileIter, paginationContext);
  }
}
