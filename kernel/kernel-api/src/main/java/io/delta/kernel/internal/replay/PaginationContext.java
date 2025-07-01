package io.delta.kernel.internal.replay;

public class PaginationContext {
  public final String startingLogFileName;
  public final long rowIdx;
  public final long sidecarIdx;
  public final long pageSize;

  // TODO: add cached hashsets related info
  public PaginationContext(
      String startingLogFileName, long rowIdx, long sidecarIdx, long pageSize) {
    this.startingLogFileName = startingLogFileName;
    this.rowIdx = rowIdx;
    this.sidecarIdx = sidecarIdx;
    this.pageSize = pageSize;
  }
}
