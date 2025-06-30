package io.delta.kernel.transaction;

import io.delta.kernel.commit.CommitPayload;
import io.delta.kernel.data.Row;
import io.delta.kernel.utils.CloseableIterator;

public interface Transaction {
//  StructType getSchema();
//
//  List<Column> getPartitionColumns();
//
//  long getReadTableVersion();

  Row getTransactionState();

  void addDomainMetadata(String domain, String config);

  void removeDomainMetadata(String domain);

  CommitPayload getCommitPayload(CloseableIterator<Row> dataActions);

  // TODO: CommitPayload resolveConflictsAndRebase(
  //         CloseableIterator<Row> dataActions,
  //         List<ParsedDeltaData> winningCommits,
  //         Optional<Long> latestTableVersion);
}
