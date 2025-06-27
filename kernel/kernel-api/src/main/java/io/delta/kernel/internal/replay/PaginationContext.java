package io.delta.kernel.internal.replay;

import java.util.Optional;

public class PaginationContext {
  public final long pageSize;
  public String lastReadLogFileName = null;
  public long lastReadRowIdxInFile = -1;

  public PaginationContext(long pageSize,  Optional<PageToken> pageToken) {
    this.pageSize = pageSize;
    if(pageToken.isPresent()) {
      this.lastReadLogFileName = pageToken.get().getStartingFileName();
      this.lastReadRowIdxInFile =  pageToken.get().getRowIndex();
    }
  }

  public static final PaginationContext EMPTY = new PaginationContext(-1, Optional.empty());
}