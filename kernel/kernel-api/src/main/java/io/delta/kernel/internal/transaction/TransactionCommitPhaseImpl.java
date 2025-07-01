package io.delta.kernel.internal.transaction;


import static io.delta.kernel.internal.TableConfig.IN_COMMIT_TIMESTAMPS_ENABLED;

import io.delta.kernel.Meta;
import io.delta.kernel.Operation;
import io.delta.kernel.commit.CommitPayload;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.actions.CommitInfo;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.internal.util.Clock;
import io.delta.kernel.internal.util.InCommitTimestampUtils;
import io.delta.kernel.transaction.TransactionCommitPhase;
import io.delta.kernel.utils.CloseableIterator;
import java.util.Collections;
import java.util.Optional;

public class TransactionCommitPhaseImpl implements TransactionCommitPhase {

  public final String tablePath;
  public final String logPath;
  public final String engineInfo;
  public final Operation operation;
  public final Protocol txnProtocol;
  public final Metadata txnMetadata;
  public final boolean isProtocolUpdate;
  public final boolean isMetadataUpdate;
  public final Optional<TransactionDataSource> readSourceOpt;
  private final long txnCommitTimeMs;

  public TransactionCommitPhaseImpl(
      String tablePath,
      String logPath,
      String engineInfo,
      Operation operation,
      Protocol txnProtocol,
      Metadata txnMetadata,
      boolean isProtocolUpdate,
      boolean isMetadataUpdate,
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
    this.readSourceOpt = readSourceOpt;
    this.txnCommitTimeMs = clock.getTimeMillis();
  }

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
  public CommitPayload getCommitPayload(Engine engine, CloseableIterator<Row> dataActions) {
    final long commitAsVersion = readSourceOpt.map(x -> x.getVersion()).orElse(0L);
    return null;
  }

  // TODO: Take in prevCommitPayloadOpt
  // TODO: Take in winningCommits optional list
  // TODO: Take in maxRatifiedVersion optional
  private CommitPayload getCommitPayloadImpl(
      Engine engine,
      CloseableIterator<Row> dataActions,
      Optional<CommitPayload> prevCommitPayloadOpt) {
    // TODO: Detect conflicts
    // TODO: Rebase
    final CommitContext commitContext = new CommitContext(readSourceOpt, Optional.empty(), Optional.empty());
    final long commitAsVersion = commitContext.getCommitAsVersion();
  }

  private class CommitContext {

    private final Optional<CommitRebaseState> rebaseStateOpt;
    private final Optional<CommitPayload> prevCommitPayloadOpt;

    public long commitAsVersion;
    public Optional<Long> inCommitTimestampOpt;
    public Optional<Metadata> updatedMetadataWithICTEnablementInfoOpt;

    // TODO: either
    // (a) ALL empty (CREATE)
    // (b) readSourceOpt is present (1st commit, UPDATE or REPLACE)
    // (c) rebaseStateOpt AND prevCommitPayloadOpt is present (2nd commit) -- readSourceOpt could still be empty
    // i.e. rebaseStateOpt an prevCommitPayloadOpt are coupled together
    private CommitContext(
        Engine engine,
        Optional<CommitRebaseState> rebaseStateOpt,
        Optional<CommitPayload> prevCommitPayloadOpt) {
      this.rebaseStateOpt = rebaseStateOpt;
      this.prevCommitPayloadOpt = prevCommitPayloadOpt;

      this.commitAsVersion = loadCommitAsVersion();
      this.inCommitTimestampOpt = loadInCommitTimestamp(engine);
      this.updatedMetadataWithICTEnablementInfoOpt = loadUpdatedMetadataWithICTEnablementInfo();
    }

    private Optional<Metadata> loadUpdatedMetadataWithICTEnablementInfo(Engine engine) {
      return inCommitTimestampOpt.map { ict =>
          InCommitTimestampUtils.getUpdatedMetadataWithICTEnablementInfo(
              engine, ict, readSnapshot, txnMetadata, lastCommitVersion + 1L);
        }
    }

    private long loadCommitAsVersion() {
      if (rebaseStateOpt.isPresent()) return loadCommitAsVersionAfterRebase();
      return loadCommitAsVersionForFirstCommit();
    }

    private long loadCommitAsVersionAfterRebase() {
      return rebaseStateOpt.get().latestCommitVersion + 1;
    }

    private long loadCommitAsVersionForFirstCommit() {
      return readSourceOpt.map(x -> x.getVersion() + 1).orElse(0L);
    }

    private Optional<Long> loadInCommitTimestamp(Engine engine) {
      if (rebaseStateOpt.isPresent()) return getInCommitTimestampAfterRebase();
      return getInCommitTimestampForFirstCommit(engine);
    }

    private Optional<Long> getInCommitTimestampAfterRebase() {
      final Optional<Long> prevAttemptICT =
          prevCommitPayloadOpt.get().getCommitInfo().getInCommitTimestamp();

      if (prevAttemptICT.isPresent()) {
        return Optional.of(
            Math.max(prevAttemptICT.get(), rebaseStateOpt.get().latestCommitTimestamp + 1)
        );
      }

      return Optional.empty(); // ICT not enabled
    }

    private Optional<Long> getInCommitTimestampForFirstCommit(Engine engine) {
      if (IN_COMMIT_TIMESTAMPS_ENABLED.fromMetadata(txnMetadata)) {
        long lastCommitTimestamp = readSourceOpt.get().getTimestamp(engine);
        return Optional.of(Math.max(txnCommitTimeMs, lastCommitTimestamp + 1));
      } else {
        return Optional.empty();
      }
    }
  }

  private CommitInfo generateCommitAction(
      Engine engine,
      TransactionCommitPhaseImpl txn,
      long commitAttemptStartTime) {
    return new CommitInfo(
        generateInCommitTimestampForFirstCommitAttempt(engine, commitAttemptStartTime),
        commitAttemptStartTime, /* timestamp */
        "Kernel-" + Meta.KERNEL_VERSION + "/" + txn.engineInfo, /* engineInfo */
        operation.getDescription(), /* description */
        getOperationParameters(), /* operationParameters */
        isBlindAppend(), /* isBlindAppend */
        txnId.toString(), /* txnId */
        Collections.emptyMap() /* operationMetrics */);
  }
}
