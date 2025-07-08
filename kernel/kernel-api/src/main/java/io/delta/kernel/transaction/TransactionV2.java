package io.delta.kernel.transaction;

import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.files.ParsedLogData;
import io.delta.kernel.utils.CloseableIterator;
import java.util.List;
import java.util.Optional;

public interface TransactionV2 {
  Row getTransactionState();

  void addDomainMetadata(String domain, String config);

  void removeDomainMetadata(String domain);

  CommitContext getInitialCommitContext(Engine engine, CloseableIterator<Row> dataActions);

  CommitContext resolveConflictsAndRebase(
      Engine engine,
      CloseableIterator<Row> prevFinalizedActions,
      CommitContext prevCommitContext,
      Optional<List<ParsedLogData>> winningCommitsOpt,
      Optional<Long> maxRatifiedVersionOpt);
}
