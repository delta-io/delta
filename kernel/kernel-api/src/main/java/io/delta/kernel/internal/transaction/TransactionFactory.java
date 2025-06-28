package io.delta.kernel.internal.transaction;

import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;

public class TransactionFactory {
  private TransactionFactory() {}

  public static TransactionImpl createTransaction(
      String tablePath,
      long readTableVersion,
      long readTableTimestamp,
      Protocol baseProtocol,
      Metadata baseMetadata) {
    return null;
  }
}
