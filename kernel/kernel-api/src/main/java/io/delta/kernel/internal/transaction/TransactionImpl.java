package io.delta.kernel.internal.transaction;

import io.delta.kernel.Meta;
import io.delta.kernel.Operation;
import io.delta.kernel.commit.CommitPayload;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.expressions.Column;
import io.delta.kernel.internal.actions.CommitInfo;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.internal.files.ParsedLogData;
import io.delta.kernel.transaction.Transaction;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class TransactionImpl implements Transaction {

  public TransactionImpl(
      String engineInfo,
      Operation operation,
      Protocol txnProtocol,
      Metadata txnMetadata,
      boolean updateProtocol,
      boolean updateMetadata) {}

  @Override
  public StructType getSchema() {
    return null;
  }

  @Override
  public List<Column> getPartitionColumns() {
    return List.of();
  }

  @Override
  public long getReadTableVersion() {
    return 0;
  }

  @Override
  public Row getTransactionState() {
    return null;
  }

  @Override
  public void addDomainMetadata(String domain, String config) {}

  @Override
  public void removeDomainMetadata(String domain) {}

  @Override
  public CommitPayload getCommitPayload(CloseableIterator<Row> dataActions) {
    return null;
  }

  private CommitPayload getCommitPayloadHelper(
      CloseableIterator<Row> dataActions,
      Optional<CommitPayload> prevCommitPayload,
      List<ParsedLogData> winningCommits,
      long maxRatifiedVersion) {
    // TODO: resolve conflicts and rebase

  }

  private CommitInfo generateCommitAction(Engine engine) {
    long commitAttemptStartTime = clock.getTimeMillis();
    return new CommitInfo(
        generateInCommitTimestampForFirstCommitAttempt(engine, commitAttemptStartTime),
        commitAttemptStartTime, /* timestamp */
        "Kernel-" + Meta.KERNEL_VERSION + "/" + engineInfo, /* engineInfo */
        operation.getDescription(), /* description */
        getOperationParameters(), /* operationParameters */
        isBlindAppend(), /* isBlindAppend */
        txnId.toString(), /* txnId */
        Collections.emptyMap() /* operationMetrics */);
  }
}
