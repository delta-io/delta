package io.delta.kernel.transaction;

import io.delta.kernel.data.Row;

public interface TransactionWritePhase {
  
  Row getTransactionState();

  void addDomainMetadata(String domain, String config);

  void removeDomainMetadata(String domain);

  TransactionWritePhase readyToCommit();
}
