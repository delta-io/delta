package io.delta.kernel.internal;

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
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

public class PaginatedScanImpl implements PaginatedScan {

  private final String startingLogFileName;
  private final long numOfAddFilesToSkip;
  private final long pageSize;
  private PageToken pageToken;
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
      String pageTokenString,
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
    this.pageToken = decodePageToken(pageTokenString);
    // TODO: if we need check table version and predicates here?
    assert pageToken != null;
    this.startingLogFileName = pageToken.getStartingFileName();
    this.numOfAddFilesToSkip = pageToken.getRowIndex();
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

    // hash set related
    boolean isHashSetCached = false; // default to be false
    HashSet<LogReplayUtils.UniqueFileActionTuple> tombstonesFromJson = new HashSet<>();
    HashSet<LogReplayUtils.UniqueFileActionTuple> addFilesFromJson = new HashSet<>();

    PaginationContext paginationContext =
        new PaginationContext(
            startingLogFileName,
            numOfAddFilesToSkip,
            pageToken.getSidecarIdx(),
            pageSize,
            isHashSetCached,
            tombstonesFromJson,
            addFilesFromJson);

    CloseableIterator<FilteredColumnarBatch> scanFileIter =
        baseScan.getScanFiles(engine, includeStates, paginationContext);
    PaginatedAddFilesIterator paginatedIter =
        new PaginatedAddFilesIterator(scanFileIter, paginationContext);
    this.pageToken =
        new PageToken(
            paginatedIter.getNextStartingLogFileName(),
            paginatedIter.getNumAddFilesToSkipForNextPage(),
            0,
            0); // update page token
    return paginatedIter;
  }

  // TODO: remove these two methods, page token received should be of Row Type
  private PageToken decodePageToken(String pageTokenString) {
    return null;
  }

  private String encodePageToken(PageToken pageToken) {
    return "ada";
  }

  // TODO: implement following methods
  @Override
  public String getNewPageTokenString() {
    return encodePageToken(this.pageToken);
  }

  @Override
  public Set<LogReplayUtils.UniqueFileActionTuple> getTombStoneFromJson() {
    return null;
  }

  @Override
  public Set<LogReplayUtils.UniqueFileActionTuple> getAddFilesFromJson() {
    return null;
  }
}
