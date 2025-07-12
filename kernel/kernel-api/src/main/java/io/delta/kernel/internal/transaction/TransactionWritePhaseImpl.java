package io.delta.kernel.internal.transaction;

import io.delta.kernel.data.Row;
import io.delta.kernel.transaction.TransactionWritePhase;

public class TransactionWritePhaseImpl implements TransactionWritePhase {

  @Override
  public Row getTransactionState() {
    return null;
  }

  @Override
  public void addDomainMetadata(String domain, String config) {

  }

  @Override
  public void removeDomainMetadata(String domain) {

  }

  @Override
  public TransactionWritePhase readyToCommit() {
    return null;
  }
}
