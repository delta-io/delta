package io.delta.kernel.internal.replay;

import java.util.HashSet;

public class PaginationContext {
  // TODO: wrap all these into a PageToken object
  public final String startingLogFileName;
  public final long rowIdx;
  public final long sidecarIdx;
  public final long pageSize;
  public final boolean isHashSetCached;
  public final HashSet<LogReplayUtils.UniqueFileActionTuple> tombstonesFromJson;
  public final HashSet<LogReplayUtils.UniqueFileActionTuple> addFilesFromJson;

  public PaginationContext(
      String startingLogFileName,
      long rowIdx,
      long sidecarIdx,
      long pageSize,
      boolean isHashSetCached,
      HashSet<LogReplayUtils.UniqueFileActionTuple> tombstonesFromJson,
      HashSet<LogReplayUtils.UniqueFileActionTuple> addFilesFromJson) {
    this.startingLogFileName = startingLogFileName;
    this.rowIdx = rowIdx;
    this.sidecarIdx = sidecarIdx;
    this.isHashSetCached = isHashSetCached;
    this.tombstonesFromJson = tombstonesFromJson;
    this.addFilesFromJson = addFilesFromJson;
    this.pageSize = pageSize;
  }

  public static final PaginationContext EMPTY =
      new PaginationContext(null, 0, 0, 0, false, null, null);
}
