package io.delta.kernel.transaction;

import io.delta.kernel.commit.CommitPayload;
import io.delta.kernel.commit.Committer;
import io.delta.kernel.data.Row;
import io.delta.kernel.expressions.Column;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import java.util.List;

public interface Transaction {
  StructType getSchema();

  List<Column> getPartitionColumns();

  long getReadTableVersion();

  Row getTransactionState();

  Committer getCommitter();

  CommitPayload getCommitPayload(CloseableIterator<Row> dataActions);

  // TODO: CommitPayload resolveConflictsAndRebase(
  //         CloseableIterator<Row> dataActions,
  //         List<ParsedDeltaData> winningCommits,
  //         Optional<Long> latestTableVersion);
}
