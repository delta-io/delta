package io.delta.kernel.internal;

import io.delta.kernel.PaginatedScan;
import io.delta.kernel.data.ColumnarBatch;
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
import java.util.HashSet;
import java.util.Optional;

public class PaginatedScanImpl implements PaginatedScan {

  private final long pageSize;
  private final PageToken pageToken;
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
      Row pageTokenInRow,
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
    assert pageTokenInRow != null;
    this.pageToken = decodePageToken(pageTokenInRow);
    // TODO: validation 1. LopReplay.getLogSegment() 2. data Path 3. snapshotReport.getVersion();
    // [maybe can use snapshotReport.getCheckpointVersion()]
    // TODO: not sure how to check: 1. predicate(filter) 2. Kernel Version ID
    this.pageSize = pageSize;
  }

  @Override
  public CloseableIterator<FilteredColumnarBatch> getScanFiles(Engine engine) {
    return this.getScanFiles(engine, false);
  }

  @Override
  public Optional<Predicate> getRemainingFilter() {
    return baseScan.getRemainingFilter();
  }

  @Override
  public Row getScanState(Engine engine) {
    return baseScan.getScanState(engine);
  }

  public CloseableIterator<FilteredColumnarBatch> getScanFiles(
      Engine engine, boolean includeStates) {
    // TODO: update code here
    boolean isHashSetCached = false;
    HashSet<LogReplayUtils.UniqueFileActionTuple> tombstonesFromJson = new HashSet<>();
    HashSet<LogReplayUtils.UniqueFileActionTuple> addFilesFromJson = new HashSet<>();

    PaginationContext paginationContext =
        new PaginationContext(
            pageToken.getStartingFileName(),
            pageToken.getRowIndex(),
            pageToken.getSidecarIdx(),
            pageSize,
            isHashSetCached,
            tombstonesFromJson,
            addFilesFromJson);

    CloseableIterator<FilteredColumnarBatch> scanFileIter =
        baseScan.getScanFiles(engine, includeStates, paginationContext);
    this.paginatedIter = new PaginatedAddFilesIterator(scanFileIter, paginationContext);
    return paginatedIter;
  }

  // TODO: implement following methods
  private PageToken decodePageToken(Row pageTokenInRow) {
    return PageToken.fromRow(pageTokenInRow);
  }

  @Override
  public Row getNewPageToken() {
    return paginatedIter.getNewPageToken().getRow();
  }

  @Override
  public ColumnarBatch getTombStoneHashsets() {
    return null;
  }
}
