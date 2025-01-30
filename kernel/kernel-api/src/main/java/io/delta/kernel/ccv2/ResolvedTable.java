package io.delta.kernel.ccv2;

import io.delta.kernel.Operation;
import io.delta.kernel.Snapshot;
import io.delta.kernel.Transaction;
import io.delta.kernel.TransactionBuilder;
import io.delta.kernel.ccv2.internal.ResolvedTableImpl;
import io.delta.kernel.exceptions.TableNotFoundException;

public interface ResolvedTable {

  /** Create a {@link ResolvedTable} object using the given {@link ResolvedMetadata}. */
  static ResolvedTable forResolvedMetadata(ResolvedMetadata resolvedMetadata) {
    return new ResolvedTableImpl(resolvedMetadata);
  }

  /**
   * @return the {@link Snapshot} of the table at the current version.
   * @throws TableNotFoundException if the table is not found
   */
  Snapshot getSnapshot();

  /**
   * Create a {@link TransactionBuilder} which can create a {@link Transaction} object to mutate the
   * table.
   */
  TransactionBuilder createTransactionBuilder(String engineInfo, Operation operation);
}
