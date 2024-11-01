/*
 * Copyright (2023) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.delta.kernel.internal.replay;

import static io.delta.kernel.internal.DeltaErrors.wrapEngineExceptionThrowsIO;
import static io.delta.kernel.internal.TableConfig.IN_COMMIT_TIMESTAMPS_ENABLED;
import static io.delta.kernel.internal.actions.SingleAction.*;
import static io.delta.kernel.internal.util.FileNames.deltaFile;
import static io.delta.kernel.internal.util.Preconditions.checkArgument;
import static io.delta.kernel.internal.util.Preconditions.checkState;
import static io.delta.kernel.utils.CloseableIterable.inMemoryIterable;
import static java.lang.String.format;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.exceptions.ConcurrentWriteException;
import io.delta.kernel.internal.*;
import io.delta.kernel.internal.actions.CommitInfo;
import io.delta.kernel.internal.actions.DomainMetadata;
import io.delta.kernel.internal.actions.SetTransaction;
import io.delta.kernel.internal.util.DomainMetadataUtils;
import io.delta.kernel.internal.util.FileNames;
import io.delta.kernel.utils.CloseableIterable;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;
import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Class containing the conflict resolution logic when writing to a Delta table.
 *
 * <p>Currently, the support is to allow blind appends. Later on this can be extended to add support
 * for read-after-write scenarios.
 */
public class ConflictChecker {
  private static final int PROTOCOL_ORDINAL = CONFLICT_RESOLUTION_SCHEMA.indexOf("protocol");
  private static final int METADATA_ORDINAL = CONFLICT_RESOLUTION_SCHEMA.indexOf("metaData");
  private static final int TXN_ORDINAL = CONFLICT_RESOLUTION_SCHEMA.indexOf("txn");
  private static final int COMMITINFO_ORDINAL = CONFLICT_RESOLUTION_SCHEMA.indexOf("commitInfo");

  // Snapshot of the table read by the transaction that encountered the conflict
  // (a.k.a the losing transaction)
  private final SnapshotImpl snapshot;

  // Losing transaction
  private final TransactionImpl transaction;
  private final long attemptVersion;

  // Data actions from the losing transaction
  CloseableIterable<Row> dataActions;

  private ConflictChecker(
      SnapshotImpl snapshot,
      TransactionImpl transaction,
      long attemptVersion,
      CloseableIterable<Row> dataActions) {
    this.snapshot = snapshot;
    this.transaction = transaction;
    this.attemptVersion = attemptVersion;
    this.dataActions = dataActions;
  }

  /**
   * Resolve conflicts between the losing transaction and the winning transactions and return a
   * rebase state that the losing transaction needs to rebase against before attempting the commit.
   *
   * @param engine {@link Engine} instance to use
   * @param snapshot {@link SnapshotImpl} of the table when the losing transaction has started
   * @param transaction {@link TransactionImpl} that encountered the conflict (a.k.a the losing
   *     transaction)
   * @param dataActions {@link CloseableIterable} of {@link Row}s that represent the losing
   *     transaction's data actions
   * @return {@link TransactionRebaseState} that the losing transaction needs to rebase against
   * @throws ConcurrentWriteException if there are logical conflicts between the losing transaction
   *     and the winning transactions that cannot be resolved.
   */
  public static TransactionRebaseState resolveConflicts(
      Engine engine,
      SnapshotImpl snapshot,
      long attemptVersion,
      TransactionImpl transaction,
      CloseableIterable<Row> dataActions)
      throws ConcurrentWriteException {
    checkArgument(transaction.isBlindAppend(), "Current support is for blind appends only.");
    return new ConflictChecker(snapshot, transaction, attemptVersion, dataActions)
        .resolveConflicts(engine);
  }

  public TransactionRebaseState resolveConflicts(Engine engine) throws ConcurrentWriteException {
    List<FileStatus> winningCommits = getWinningCommitFiles(engine);
    AtomicReference<Optional<CommitInfo>> winningCommitInfoOpt =
        new AtomicReference<>(Optional.empty());

    // no winning commits. why did we get the transaction conflict?
    checkState(!winningCommits.isEmpty(), "No winning commits found.");

    FileStatus lastWinningTxn = winningCommits.get(winningCommits.size() - 1);
    long lastWinningVersion = FileNames.deltaVersion(lastWinningTxn.getPath());
    // Read the actions from the winning commits
    try (ActionsIterator actionsIterator =
        new ActionsIterator(engine, winningCommits, CONFLICT_RESOLUTION_SCHEMA, Optional.empty())) {

      actionsIterator.forEachRemaining(
          actionBatch -> {
            checkArgument(!actionBatch.isFromCheckpoint()); // no checkpoints should be read
            ColumnarBatch batch = actionBatch.getColumnarBatch();
            if (actionBatch.getVersion() == lastWinningVersion) {
              Optional<CommitInfo> commitInfo =
                  getCommitInfo(batch.getColumnVector(COMMITINFO_ORDINAL));
              winningCommitInfoOpt.set(commitInfo);
            }

            handleProtocol(batch.getColumnVector(PROTOCOL_ORDINAL));
            handleMetadata(batch.getColumnVector(METADATA_ORDINAL));
            handleTxn(batch.getColumnVector(TXN_ORDINAL));
            handleDomainMetadata(batch);
          });
    } catch (IOException ioe) {
      throw new UncheckedIOException("Error reading actions from winning commits.", ioe);
    }

    // if we get here, we have successfully rebased (i.e no logical conflicts)
    // against the winning transactions
    return new TransactionRebaseState(
        lastWinningVersion,
        getLastCommitTimestamp(
            engine, lastWinningVersion, lastWinningTxn, winningCommitInfoOpt.get()));
  }

  /**
   * Class containing the rebase state from winning transactions that the current transaction needs
   * to rebase against before attempting the commit.
   *
   * <p>Currently, the rebase state is just the latest winning version of the table. In future once
   * we start supporting read-after-write, domain metadata, row tracking, etc., we will have more
   * state to add. For example read-after-write will need to know the files deleted in the winning
   * transactions to make sure the same files are not deleted by the current (losing) transaction.
   */
  public static class TransactionRebaseState {
    private final long latestVersion;
    private final long latestCommitTimestamp;

    public TransactionRebaseState(long latestVersion, long latestCommitTimestamp) {
      this.latestVersion = latestVersion;
      this.latestCommitTimestamp = latestCommitTimestamp;
    }

    /**
     * Return the latest winning version of the table.
     *
     * @return latest winning version of the table.
     */
    public long getLatestVersion() {
      return latestVersion;
    }

    /**
     * Return the latest commit timestamp of the table. For ICT enabled tables, this is the ICT of
     * the latest winning transaction commit file. For non-ICT enabled tables, this is the
     * modification time of the latest winning transaction commit file.
     *
     * @return latest commit timestamp of the table.
     */
    public long getLatestCommitTimestamp() {
      return latestCommitTimestamp;
    }
  }

  /**
   * Any protocol changes between the losing transaction and the winning transactions are not
   * allowed. In future once we start supporting more table features on the write side, this can be
   * changed to handle safe protocol changes. For now the write support in Kernel is supported at a
   * very first version of the protocol.
   *
   * @param protocolVector protocol rows from the winning transactions
   */
  private void handleProtocol(ColumnVector protocolVector) {
    for (int rowId = 0; rowId < protocolVector.getSize(); rowId++) {
      if (!protocolVector.isNullAt(rowId)) {
        throw DeltaErrors.protocolChangedException(attemptVersion);
      }
    }
  }

  /**
   * Any metadata changes between the losing transaction and the winning transactions are not
   * allowed.
   *
   * @param metadataVector metadata rows from the winning transactions
   */
  private void handleMetadata(ColumnVector metadataVector) {
    for (int rowId = 0; rowId < metadataVector.getSize(); rowId++) {
      if (!metadataVector.isNullAt(rowId)) {
        throw DeltaErrors.metadataChangedException();
      }
    }
  }

  /**
   * Checks whether each of the current transaction's {@link DomainMetadata} conflicts with the
   * winning transaction at any domain.
   *
   * <ol>
   *   1. Accept the current transaction if its set of metadata domains does not overlap with the
   *   winning transaction's set of metadata domains.
   *   <p>2. Otherwise, fail the current transaction unless each conflicting domain is associated
   *   with a domain-specific way of resolving the conflict.
   * </ol>
   *
   * @param actionBatch action batch from the winning transactions
   */
  private void handleDomainMetadata(ColumnarBatch actionBatch) {
    // Extract the domain metadata map from the winning transaction.
    Map<String, DomainMetadata> winningDomainMetadataMap =
        DomainMetadataUtils.extractDomainMetadataMap(
            inMemoryIterable(actionBatch.getRows()), CONFLICT_RESOLUTION_SCHEMA);

    // Get the ordinal of the domainMetadata action from the full schema, which is used when writing
    // out the single action to the Delta Log
    final int domainMetadataOrdinal = FULL_SCHEMA.indexOf("domainMetadata");

    // Use try-with-resources to ensure that the CloseableIterable is closed after the loop
    try (CloseableIterable<Row> closeableDataActions = dataActions) {
      for (Row action : closeableDataActions) {
        // Skip non-domainMetadata actions
        if (action.isNullAt(domainMetadataOrdinal)) continue;

        // Extract DomainMetadata action that the losing transaction trying to commit
        DomainMetadata domainMetadata =
            DomainMetadata.fromRow(action.getStruct(domainMetadataOrdinal));

        // Try to resolve the conflict, if not possible, throw a ConcurrentTransaction exception
        resolveDomainMetadataConflict(domainMetadata, winningDomainMetadataMap);
      }
    } catch (IOException ioe) {
      throw new UncheckedIOException(ioe);
    }
  }

  private void resolveDomainMetadataConflict(
      DomainMetadata domainMetadataAttempt, Map<String, DomainMetadata> winningDomainMetadataMap) {

    String domain = domainMetadataAttempt.getDomain();
    DomainMetadata winningDomainMetadata = winningDomainMetadataMap.get(domain);
    if (winningDomainMetadata == null) {
      // No conflict
      return;
    } else {
      // Conflict - check if the conflict can be resolved

      // Currently, we don't have any domain-specific way of resolving the conflict.
      // Domain-specific ways of resolving the conflict can be added here (e.g. for Row Tracking)
      throw new ConcurrentWriteException(
          "A concurrent writer added a domainMetadata action for the same domain: "
              + domain
              + ". No domain-specific conflict resolution available for this domain."
              + "Attempted domainMetadata: "
              + domainMetadataAttempt
              + ". Winning domainMetadata: "
              + winningDomainMetadata);
    }
  }

  /**
   * Get the commit info from the winning transactions.
   *
   * @param commitInfoVector commit info rows from the winning transactions
   * @return the commit info
   */
  private Optional<CommitInfo> getCommitInfo(ColumnVector commitInfoVector) {
    for (int rowId = 0; rowId < commitInfoVector.getSize(); rowId++) {
      if (!commitInfoVector.isNullAt(rowId)) {
        return Optional.of(CommitInfo.fromColumnVector(commitInfoVector, rowId));
      }
    }
    return Optional.empty();
  }

  private void handleTxn(ColumnVector txnVector) {
    // Check if the losing transaction has any txn identifier. If it does, go through the
    // winning transactions and make sure that the losing transaction is valid from a
    // idempotent perspective.
    Optional<SetTransaction> losingTxnIdOpt = transaction.getSetTxnOpt();
    losingTxnIdOpt.ifPresent(
        losingTxnId -> {
          for (int rowId = 0; rowId < txnVector.getSize(); rowId++) {
            SetTransaction winningTxn = SetTransaction.fromColumnVector(txnVector, rowId);
            if (winningTxn != null
                && winningTxn.getAppId().equals(losingTxnId.getAppId())
                && winningTxn.getVersion() >= losingTxnId.getVersion()) {
              throw DeltaErrors.concurrentTransaction(
                  losingTxnId.getAppId(), losingTxnId.getVersion(), winningTxn.getVersion());
            }
          }
        });
  }

  private List<FileStatus> getWinningCommitFiles(Engine engine) {
    String firstWinningCommitFile =
        deltaFile(snapshot.getLogPath(), snapshot.getVersion(engine) + 1);

    try (CloseableIterator<FileStatus> files =
        wrapEngineExceptionThrowsIO(
            () -> engine.getFileSystemClient().listFrom(firstWinningCommitFile),
            "Listing from %s",
            firstWinningCommitFile)) {
      // Select all winning transaction commit files.
      List<FileStatus> winningCommitFiles = new ArrayList<>();
      while (files.hasNext()) {
        FileStatus file = files.next();
        if (FileNames.isCommitFile(file.getPath())) {
          winningCommitFiles.add(file);
        }
      }

      return ensureNoGapsInWinningCommits(winningCommitFiles);
    } catch (FileNotFoundException nfe) {
      // no winning commits. why did we get here?
      throw new IllegalStateException("No winning commits found.", nfe);
    } catch (IOException ioe) {
      throw new UncheckedIOException("Error listing files from " + firstWinningCommitFile, ioe);
    }
  }

  /**
   * Get the last commit timestamp of the table. For ICT enabled tables, this is the ICT of the
   * latest winning transaction commit file. For non-ICT enabled tables, this is the modification
   * time of the latest winning transaction commit file.
   *
   * @param engine {@link Engine} instance to use
   * @param lastWinningVersion last winning version of the table
   * @param lastWinningTxn the last winning transaction commit file
   * @param winningCommitInfoOpt winning commit info
   * @return last commit timestamp of the table
   */
  private long getLastCommitTimestamp(
      Engine engine,
      long lastWinningVersion,
      FileStatus lastWinningTxn,
      Optional<CommitInfo> winningCommitInfoOpt) {
    if (snapshot.getVersion(engine) == -1
        || !IN_COMMIT_TIMESTAMPS_ENABLED.fromMetadata(snapshot.getMetadata())) {
      return lastWinningTxn.getModificationTime();
    } else {
      return CommitInfo.getRequiredInCommitTimestamp(
          winningCommitInfoOpt, String.valueOf(lastWinningVersion), snapshot.getDataPath());
    }
  }

  private static List<FileStatus> ensureNoGapsInWinningCommits(List<FileStatus> winningCommits) {
    long lastVersion = -1;
    for (FileStatus commit : winningCommits) {
      long version = FileNames.deltaVersion(commit.getPath());
      checkState(
          lastVersion == -1 || version == lastVersion + 1,
          format(
              "Gaps in Delta log commit files. Expected version %d but got %d",
              (lastVersion + 1), version));
      lastVersion = version;
    }
    return winningCommits;
  }
}
