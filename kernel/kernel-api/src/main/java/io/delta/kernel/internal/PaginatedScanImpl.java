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

  private final ScanImpl baseScan;

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
  }

  @Override
  public CloseableIterator<FilteredColumnarBatch> getScanFiles(Engine engine) {
    return baseScan.getScanFiles(engine);
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
  public Row getCurrentPageToken() {
    return null;
  }

  @Override
  public ColumnarBatch getLogReplayHashsets() {
    return null;
  }
}
