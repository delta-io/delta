package io.delta.kernel.internal.transaction;

import static io.delta.kernel.internal.TableConfig.IN_COMMIT_TIMESTAMPS_ENABLED;
import static io.delta.kernel.internal.actions.SingleAction.*;
import static io.delta.kernel.internal.util.Preconditions.checkArgument;

import io.delta.kernel.Meta;
import io.delta.kernel.commit.CommitMetadata;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.actions.CommitInfo;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.replay.ConflictChecker;
import io.delta.kernel.internal.util.VectorUtils;
import io.delta.kernel.transaction.CommitContext;
import io.delta.kernel.utils.CloseableIterator;
import java.util.*;
import java.util.stream.Collectors;

public class CommitContextImpl implements CommitContext {

  public static CommitContextImpl createInitialCommitContext(
      Engine engine,
      TransactionV2Impl.TransactionState txnState,
      CloseableIterator<Row> dataActions) {
    return new CommitContextImpl(
        engine, txnState, Optional.of(dataActions), Optional.empty() /* rebaseContextOpt */);
  }

  public static CommitContextImpl createCommitContextAfterRebase(
      Engine engine,
      TransactionV2Impl.TransactionState txnState,
      CommitContextImpl prevCommitContext,
      ConflictChecker.TransactionRebaseState rebaseState) {
    return new CommitContextImpl(
        engine,
        txnState,
        Optional.empty(),
        Optional.of(new RebaseContext(prevCommitContext, rebaseState)));
  }

  static class RebaseContext {
    public final CommitContextImpl prevCommitContext;
    public final ConflictChecker.TransactionRebaseState rebaseState;

    public RebaseContext(
        CommitContextImpl prevCommitContext,
        ConflictChecker.TransactionRebaseState rebaseState) {
      this.prevCommitContext = prevCommitContext;
      this.rebaseState = rebaseState;
    }
  }

  private final TransactionV2Impl.TransactionState txnState;
  private final CloseableIterator<Row> dataActionsForCommit; // finalized**Data**Actions --> any domain metadata/rebasing must have **already** been applied
  private final Optional<RebaseContext> rebaseContextOpt;
  private final long commitAsVersion;
  private final long commitTimestampNotICT;
  private final Optional<Long> inCommitTimestampOpt;
  private final Metadata metadata;

  public CommitContextImpl(
      Engine engine,
      TransactionV2Impl.TransactionState txnState,
      Optional<CloseableIterator<Row>> dataActionsOpt,
      Optional<RebaseContext> rebaseContextOpt) {
    checkArgument(
        dataActionsOpt.isPresent() ^ rebaseContextOpt.isPresent(),
        "Exactly one of dataActionsOpt or rebaseContextOpt must be present");
    this.txnState = txnState;
    this.dataActionsForCommit =
        dataActionsOpt.orElse(rebaseContextOpt.get().rebaseState.getUpdatedDataActionsIter());
    this.rebaseContextOpt = rebaseContextOpt;

    this.commitAsVersion = getCommitAsVersion();
    this.commitTimestampNotICT = txnState.clock.getTimeMillis();
    this.inCommitTimestampOpt = getICT(engine);
    this.metadata = txnState.txnMetadata;

    getUpdatedMetadataWithICTEnablementInfoIfApplicable();
  }

  @Override
  public CloseableIterator<Row> getFinalizedActions() {
    final List<Row> metadataActions = new ArrayList<>();
    metadataActions.add(createCommitInfoSingleAction(getCommitInfo()));
    if (shouldUpdateMetadata) {
      metadataActions.add(createMetadataSingleAction(metadata.toRow()));
    }
    if (txnState.isProtocolUpdate) {
      metadataActions.add(createProtocolSingleAction(txnState.txnProtocol.toRow()));
    }
    return null;
  }

  @Override
  public CommitMetadata getCommitMetadata() {
    return null;
  }

  // ===== commitAsVersion =====

  private long getCommitAsVersion() {
    if (rebaseContextOpt.isPresent()) return getCommitAsVersionAfterRebase();
    return getCommitAsVersionForFirstCommit();
  }

  private long getCommitAsVersionAfterRebase() {
    return rebaseContextOpt.get().rebaseState.getLatestVersion() + 1;
  }

  private long getCommitAsVersionForFirstCommit() {
    if (txnState.readSourceOpt.isPresent()) {
      return txnState.readSourceOpt.get().getVersion() + 1;
    }
    return 0L;
  }

  // ===== inCommitTimestampOpt =====

  private Optional<Long> getICT(Engine engine) {
    if (rebaseContextOpt.isPresent()) return getICTAfterRebase();
    return getICTForFirstCommit(engine);
  }

  private Optional<Long> getICTAfterRebase() {
    final Optional<Long> prevAttemptICT =
        rebaseContextOpt.get().prevCommitContext.inCommitTimestampOpt;

    if (prevAttemptICT.isPresent()) {
      return Optional.of(
          Math.max(prevAttemptICT.get(), rebaseContextOpt.get().rebaseState.getLatestCommitTimestamp() + 1)
      );
    }

    return Optional.empty(); // ICT not enabled
  }

  private Optional<Long> getICTForFirstCommit(Engine engine) {
    if (IN_COMMIT_TIMESTAMPS_ENABLED.fromMetadata(txnState.txnMetadata)) {
      long readSourceTimestamp = txnState.readSourceOpt.map(s -> s.getTimestamp(engine)).orElse(Long.MIN_VALUE);
      return Optional.of(Math.max(commitTimestampNotICT, readSourceTimestamp + 1));
    } else {
      return Optional.empty();
    }
  }

  public Metadata getUpdatedMetadataWithICTEnablementInfoIfApplicable() {
    // TODO: just call InCommitTimestampUtils.didCurrentTransactionEnableICT

    if (!inCommitTimestampOpt.isPresent()) {
      return txnState.txnMetadata;
    }

    // TODO call InCommitTimestampUtils.getUpdatedMetadataWithICTEnablementInfo(
    //             engine, inCommitTimestamp, readSnapshot, metadata, lastCommitVersion + 1L);
    return txnState.txnMetadata;
  }

  private CommitInfo getCommitInfo(Engine engine) {
    return new CommitInfo(
        inCommitTimestampOpt,
        commitTimestampNotICT, /* timestamp */ // TODO: why don't we just use the ICT?
        "Kernel-" + Meta.KERNEL_VERSION + "/" + txnState.engineInfo, /* engineInfo */
        txnState.operation.getDescription(), /* description */
        getOperationParameters(), /* operationParameters */
        true, /* isBlindAppend */ // TODO: call back into the txn? or should this be in txnState?
        txnState.txnId, /* txnId */
        Collections.emptyMap() /* operationMetrics */);
  }

  private Map<String, String> getOperationParameters() {
    if (isCreateOrReplace()) {
      List<String> partitionCols = VectorUtils.toJavaList(updatedMetadata.getPartitionColumns());
      String partitionBy =
          partitionCols.stream()
              .map(col -> "\"" + col + "\"")
              .collect(Collectors.joining(",", "[", "]"));
      return Collections.singletonMap("partitionBy", partitionBy);
    }
    return Collections.emptyMap();
  }

  private boolean isCreateOrReplace() {
    return txnState.type == TransactionV2Impl.TransactionType.CREATE ||
           txnState.type == TransactionV2Impl.TransactionType.REPLACE;
  }
}
