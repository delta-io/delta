package io.delta.kernel.transaction;

import io.delta.kernel.commit.CommitPayload;
import io.delta.kernel.data.Row;
import io.delta.kernel.internal.files.ParsedLogData;
import io.delta.kernel.utils.CloseableIterator;
import java.util.Optional;

public interface TransactionCommitPhase {
  CommitPayload getCommitPayload(
      CloseableIterator<Row> dataActions,
      Optional<CommitPayload> prevCommitPayload,
      Optional<ParsedLogData> conflictWinningCommits,
      Optional<Long> conflictMaxRatifiedTableVersion);
}
