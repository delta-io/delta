package io.delta.kernel.internal.replay;

public class PaginationContext {
  public final long pageSize;

  public PaginationContext(long pageSize) {
    this.pageSize = pageSize;
  }

  public static final PaginationContext EMPTY =
      new PaginationContext(0);
}