package io.delta.kernel.internal.transaction;

import io.delta.kernel.commit.CommitPayload;
import io.delta.kernel.data.Row;
import io.delta.kernel.expressions.Column;
import io.delta.kernel.transaction.Transaction;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import java.util.List;

public class TransactionImpl implements Transaction {

  public TransactionImpl() {}

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
}
