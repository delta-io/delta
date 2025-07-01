package io.delta.kernel.internal.transaction;

public class CommitRebaseState {
  public final long latestCommitVersion;
  public final long latestCommitTimestamp;

  public CommitRebaseState(long latestCommitVersion, long latestCommitTimestamp) {
    this.latestCommitVersion = latestCommitVersion;
    this.latestCommitTimestamp = latestCommitTimestamp;
  }
}
