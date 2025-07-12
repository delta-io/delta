package io.delta.kernel.internal.transaction;

import io.delta.kernel.Operation;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.actions.DomainMetadata;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.internal.files.ParsedLogData;
import io.delta.kernel.internal.rowtracking.RowTracking;
import io.delta.kernel.internal.tablefeatures.TableFeatures;
import io.delta.kernel.internal.util.Clock;
import io.delta.kernel.transaction.CommitContext;
import io.delta.kernel.transaction.TransactionV2;
import io.delta.kernel.utils.CloseableIterator;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public class TransactionV2Impl implements TransactionV2 {

  enum TransactionType {
    CREATE, REPLACE, UPDATE
  }

  class TransactionState {
    public final String tablePath;
    public final String logPath;
    public final String engineInfo;
    public final Operation operation;
    public final Protocol txnProtocol;
    public final Metadata txnMetadata;
    public final boolean isProtocolUpdate;
    public final boolean isMetadataUpdate;
    public final TransactionType type;
    public final Optional<TransactionDataSource> readSourceOpt;
    public final Clock clock;

    public final String txnId;

    public TransactionState(
        String tablePath,
        String logPath,
        String engineInfo,
        Operation operation,
        Protocol txnProtocol,
        Metadata txnMetadata,
        boolean isProtocolUpdate,
        boolean isMetadataUpdate,
        TransactionType type,
        Optional<TransactionDataSource> readSourceOpt,
        Clock clock) {
      this.tablePath = tablePath;
      this.logPath = logPath;
      this.engineInfo = engineInfo;
      this.operation = operation;
      this.txnProtocol = txnProtocol;
      this.txnMetadata = txnMetadata;
      this.isProtocolUpdate = isProtocolUpdate;
      this.isMetadataUpdate = isMetadataUpdate;
      this.type = type;
      this.readSourceOpt = readSourceOpt;
      this.clock = clock;

      this.txnId = UUID.randomUUID().toString();
    }
  }

  private final TransactionState txnState;

  // TODO: We can think of the best way to init the txnState. I am trying to avoid lots of duplicate
  //       long constructors (e.g. TransactionV2Impl(11 params) and also TransactionState(11 params)
  public TransactionV2Impl(TransactionState txnState) {
    this.txnState = txnState;
  }

  @Override
  public Row getTransactionState() {
    return null;
  }

  // TODO: how does this impact / relate to the immutable txnState?
  @Override
  public void addDomainMetadata(String domain, String config) {}

  @Override
  public void removeDomainMetadata(String domain) {}

  // TODO: should we freeze the txn state only after all of the domain metadatas have been added/removed?
  @Override
  public CommitContext getInitialCommitContext(Engine engine, CloseableIterator<Row> dataActions) {
//    if (TableFeatures.isRowTrackingSupported(protocol)) {
//      List<DomainMetadata> updatedDomainMetadata =
//          RowTracking.updateRowIdHighWatermarkIfNeeded(
//              readSnapshot,
//              protocol,
//              Optional.empty() /* winningTxnRowIdHighWatermark */,
//              dataActions,
//              resolvedDomainMetadatas);
//      domainMetadataState.setComputedDomainMetadatas(updatedDomainMetadata);
//      dataActions =
//          RowTracking.assignBaseRowIdAndDefaultRowCommitVersion(
//              readSnapshot,
//              protocol,
//              Optional.empty() /* winningTxnRowIdHighWatermark */,
//              Optional.empty() /* prevCommitVersion */,
//              commitAsVersion,
//              dataActions);
    }
    // TODO: apply row tracking updates to the data action
    return CommitContextImpl.createInitialCommitContext(engine, txnState, dataActions);
  }

  @Override
  public CommitContext resolveConflictsAndRebase(
      CloseableIterator<Row> prevFinalizedActions,
      CommitContext prevCommitContext,
      Optional<List<ParsedLogData>> winningCommitsOpt,
      Optional<Long> maxRatifiedVersionOpt) {
    return null;
  }
}
