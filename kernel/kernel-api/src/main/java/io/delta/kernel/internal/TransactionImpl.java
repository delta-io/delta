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
package io.delta.kernel.internal;

import static io.delta.kernel.internal.DeltaErrors.wrapEngineExceptionThrowsIO;
import static io.delta.kernel.internal.TableConfig.*;
import static io.delta.kernel.internal.actions.SingleAction.*;
import static io.delta.kernel.internal.util.Preconditions.checkArgument;
import static io.delta.kernel.internal.util.Preconditions.checkState;
import static io.delta.kernel.internal.util.Utils.toCloseableIterator;

import io.delta.kernel.*;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.exceptions.ConcurrentWriteException;
import io.delta.kernel.expressions.Column;
import io.delta.kernel.internal.actions.*;
import io.delta.kernel.internal.data.TransactionStateRow;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.lang.ListUtils;
import io.delta.kernel.internal.metrics.TransactionMetrics;
import io.delta.kernel.internal.metrics.TransactionReportImpl;
import io.delta.kernel.internal.replay.ConflictChecker;
import io.delta.kernel.internal.replay.ConflictChecker.TransactionRebaseState;
import io.delta.kernel.internal.rowtracking.RowTracking;
import io.delta.kernel.internal.util.*;
import io.delta.kernel.metrics.TransactionReport;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterable;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.*;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransactionImpl implements Transaction {
  private static final Logger logger = LoggerFactory.getLogger(TransactionImpl.class);

  public static final int DEFAULT_READ_VERSION = 1;
  public static final int DEFAULT_WRITE_VERSION = 2;

  private final UUID txnId = UUID.randomUUID();

  private final boolean isNewTable; // the transaction is creating a new table
  private final String engineInfo;
  private final Operation operation;
  private final Path dataPath;
  private final Path logPath;
  private final Protocol protocol;
  private final SnapshotImpl readSnapshot;
  private final Optional<SetTransaction> setTxnOpt;
  private final boolean shouldUpdateProtocol;
  private final Clock clock;
  private List<DomainMetadata> domainMetadatas = new ArrayList<>();
  private Metadata metadata;
  private boolean shouldUpdateMetadata;
  private int maxRetries;

  private boolean closed; // To avoid trying to commit the same transaction again.

  private long commitAsVersion;
  private Optional<CloseableIterable<Row>> dataActionsIterable;

  public TransactionImpl(
      boolean isNewTable,
      Path dataPath,
      Path logPath,
      SnapshotImpl readSnapshot,
      String engineInfo,
      Operation operation,
      Protocol protocol,
      Metadata metadata,
      Optional<SetTransaction> setTxnOpt,
      boolean shouldUpdateMetadata,
      boolean shouldUpdateProtocol,
      int maxRetries,
      Clock clock) {
    this.isNewTable = isNewTable;
    this.dataPath = dataPath;
    this.logPath = logPath;
    this.readSnapshot = readSnapshot;
    this.engineInfo = engineInfo;
    this.operation = operation;
    this.protocol = protocol;
    this.metadata = metadata;
    this.setTxnOpt = setTxnOpt;
    this.shouldUpdateMetadata = shouldUpdateMetadata;
    this.shouldUpdateProtocol = shouldUpdateProtocol;
    this.maxRetries = maxRetries;
    this.clock = clock;

    this.commitAsVersion = readSnapshot.getVersion() + 1;
    this.dataActionsIterable = Optional.empty();
  }

  @Override
  public Row getTransactionState(Engine engine) {
    return TransactionStateRow.of(metadata, dataPath.toString());
  }

  @Override
  public List<String> getPartitionColumns(Engine engine) {
    return VectorUtils.toJavaList(metadata.getPartitionColumns());
  }

  @Override
  public StructType getSchema(Engine engine) {
    return readSnapshot.getSchema();
  }

  public Optional<SetTransaction> getSetTxnOpt() {
    return setTxnOpt;
  }

  /**
   * Internal API to add domain metadata actions for this transaction. Visible for testing.
   *
   * @param domainMetadatas List of domain metadata to be added to the transaction.
   */
  public void addDomainMetadatas(List<DomainMetadata> domainMetadatas) {
    this.domainMetadatas.addAll(domainMetadatas);
  }

  public List<DomainMetadata> getDomainMetadatas() {
    return domainMetadatas;
  }

  public Protocol getProtocol() {
    return protocol;
  }

  @Override
  public Optional<Protocol> getUpdatedProtocol() {
    if (shouldUpdateProtocol || isNewTable) {
      return Optional.of(protocol);
    }
    return Optional.empty();
  }

  @Override
  public Optional<Metadata> getUpdatedMetadata() {
    if (shouldUpdateMetadata || isNewTable) {
      return Optional.of(metadata);
    }
    return Optional.empty();
  }

  @Override
  public long getCommitAsVersion() {
    return commitAsVersion;
  }

  @Override
  public void setInitialDataActions(Engine engine, CloseableIterable<Row> dataActions) {
    this.dataActionsIterable = Optional.of(dataActions);
  }

  @Override
  public CloseableIterator<Row> getFinalizedActions(Engine engine) {
    checkState(dataActionsIterable.isPresent(), "Initial data actions not set.");

    CloseableIterable<Row> dataActions = dataActionsIterable.get();

    final long commitAsVersion = readSnapshot.getVersion() + 1;

    // Generate the commit action with the inCommitTimestamp if ICT is enabled.
    CommitInfo attemptCommitInfo = generateCommitAction(engine);
    updateMetadataWithICTIfRequired(
        engine, attemptCommitInfo.getInCommitTimestamp(), readSnapshot.getVersion());

    // If row tracking is supported, assign base row IDs and default row commit versions to any
    // AddFile actions that do not yet have them. If the row ID high watermark changes, emit a
    // DomainMetadata action to update it.
    if (TableFeatures.isRowTrackingSupported(protocol)) {
      domainMetadatas =
          RowTracking.updateRowIdHighWatermarkIfNeeded(
              readSnapshot,
              protocol,
              Optional.empty() /* winningTxnRowIdHighWatermark */,
              dataActions,
              domainMetadatas);
      dataActions =
          RowTracking.assignBaseRowIdAndDefaultRowCommitVersion(
              readSnapshot,
              protocol,
              Optional.empty() /* winningTxnRowIdHighWatermark */,
              Optional.empty() /* prevCommitVersion */,
              commitAsVersion,
              dataActions);
    }

    List<Row> metadataActions = new ArrayList<>();
    metadataActions.add(createCommitInfoSingleAction(attemptCommitInfo.toRow()));
    if (shouldUpdateMetadata || isNewTable) {
      this.metadata =
          ColumnMapping.updateColumnMappingMetadata(
              metadata,
              ColumnMapping.getColumnMappingMode(metadata.getConfiguration()),
              isNewTable);
      metadataActions.add(createMetadataSingleAction(metadata.toRow()));
    }
    if (shouldUpdateProtocol || isNewTable) {
      // In the future, we need to add metadata and action when there are any changes to them.
      metadataActions.add(createProtocolSingleAction(protocol.toRow()));
    }
    setTxnOpt.ifPresent(setTxn -> metadataActions.add(createTxnSingleAction(setTxn.toRow())));

    // Check for duplicate domain metadata and if the protocol supports
    DomainMetadataUtils.validateDomainMetadatas(domainMetadatas, protocol);

    domainMetadatas.forEach(
        dm -> metadataActions.add(createDomainMetadataSingleAction(dm.toRow())));

    final CloseableIterator<Row> stageDataIter = dataActions.iterator();

    // Create a new CloseableIterator that will return the metadata actions followed by the
    // data actions.
    CloseableIterator<Row> dataAndMetadataActions =
        toCloseableIterator(metadataActions.iterator()).combine(stageDataIter);

    if (commitAsVersion == 0) {

      // TODO: has to be a better place to do this
      try {
        // New table, create a delta log directory
        if (!wrapEngineExceptionThrowsIO(
            () -> engine.getFileSystemClient().mkdirs(logPath.toString()),
            "Creating directories for path %s",
            logPath)) {
          throw new RuntimeException("Failed to create delta log directory: " + logPath);
        }

        final String uuidCommitsPath = new Path(logPath, "_commits").toString();
        if (!wrapEngineExceptionThrowsIO(
            () -> engine.getFileSystemClient().mkdirs(uuidCommitsPath),
            "Creating directories for path %s",
            uuidCommitsPath)) {
          throw new RuntimeException("Failed to create delta log directory: " + uuidCommitsPath);
        }
      } catch (IOException ex) {
        throw new UncheckedIOException(ex);
      }
    }

    return dataAndMetadataActions;
  }

  @Override
  public void resolveConflictsAndRebase(Engine engine, List<FileStatus> unbackfilledCommits) {
    // TODO: what about metadata-only commits?
    checkState(dataActionsIterable.isPresent(), "Initial data actions not set.");

    if (!unbackfilledCommits.isEmpty()) {
      logger.info(
          "[Table {}] Found unbackfilled commits: {}. Trying to resolve conflicts and rebase.",
          dataPath,
          unbackfilledCommits);

      this.commitAsVersion =
          FileNames.uuidCommitDeltaVersion(ListUtils.getLast(unbackfilledCommits).getPath()) + 1;
    } else {
      logger.info(
          "[Table {}] No unbackfilled commits found. Trying to resolve conflicts and rebase.",
          dataPath);
      TransactionRebaseState rebaseState =
          ConflictChecker.resolveConflicts(
              engine, readSnapshot, commitAsVersion, this, dataActionsIterable.get());
      this.commitAsVersion = rebaseState.getLatestVersion() + 1;
    }

    logger.info(
        "[Table {}] Resolved conflicts and rebased to commit as version {}.",
        dataPath,
        commitAsVersion);
  }

  @Override
  public TransactionCommitResult registerCommitSuccess(long commitVersion) {
    return null;
  }

  @Override
  public TransactionCommitResult commit(Engine engine, CloseableIterable<Row> dataActions)
      throws ConcurrentWriteException {
    throw new UnsupportedOperationException(
        "This method should not be called directly. Use finalizeActions instead.");
  }

  private TransactionRebaseState resolveConflicts(
      Engine engine,
      long commitAsVersion,
      CommitInfo attemptCommitInfo,
      int numTries,
      CloseableIterable<Row> dataActions) {
    logger.info(
        "Table {}, trying to resolve conflicts and retry commit. (tries/maxRetries: {}/{})",
        dataPath,
        numTries,
        maxRetries);
    TransactionRebaseState rebaseState =
        ConflictChecker.resolveConflicts(engine, readSnapshot, commitAsVersion, this, dataActions);
    long newCommitAsVersion = rebaseState.getLatestVersion() + 1;
    checkArgument(
        commitAsVersion < newCommitAsVersion,
        "New commit version %d should be greater than the previous commit attempt version %d.",
        newCommitAsVersion,
        commitAsVersion);
    Optional<Long> updatedInCommitTimestamp =
        getUpdatedInCommitTimestampAfterConflict(
            rebaseState.getLatestCommitTimestamp(), attemptCommitInfo.getInCommitTimestamp());
    updateMetadataWithICTIfRequired(
        engine, updatedInCommitTimestamp, rebaseState.getLatestVersion());
    attemptCommitInfo.setInCommitTimestamp(updatedInCommitTimestamp);
    return rebaseState;
  }

  private void updateMetadata(Metadata metadata) {
    logger.info(
        "Updated metadata from {} to {}", shouldUpdateMetadata ? this.metadata : "-", metadata);
    this.metadata = metadata;
    this.shouldUpdateMetadata = true;
  }

  private void updateMetadataWithICTIfRequired(
      Engine engine, Optional<Long> inCommitTimestampOpt, long lastCommitVersion) {
    // If ICT is enabled for the current transaction, update the metadata with the ICT
    // enablement info.
    inCommitTimestampOpt.ifPresent(
        inCommitTimestamp -> {
          Optional<Metadata> metadataWithICTInfo =
              InCommitTimestampUtils.getUpdatedMetadataWithICTEnablementInfo(
                  engine, inCommitTimestamp, readSnapshot, metadata, lastCommitVersion + 1L);
          metadataWithICTInfo.ifPresent(this::updateMetadata);
        });
  }

  private Optional<Long> getUpdatedInCommitTimestampAfterConflict(
      long winningCommitTimestamp, Optional<Long> attemptInCommitTimestamp) {
    if (attemptInCommitTimestamp.isPresent()) {
      long updatedInCommitTimestamp =
          Math.max(attemptInCommitTimestamp.get(), winningCommitTimestamp + 1);
      return Optional.of(updatedInCommitTimestamp);
    }
    return attemptInCommitTimestamp;
  }

  public boolean isBlindAppend() {
    // For now, Kernel just supports blind append.
    // Change this when read-after-write is supported.
    return true;
  }

  /**
   * Generates a timestamp which is greater than the commit timestamp of the readSnapshot. This can
   * result in an additional file read and that this will only happen if ICT is enabled.
   */
  private Optional<Long> generateInCommitTimestampForFirstCommitAttempt(
      Engine engine, long currentTimestamp) {
    if (IN_COMMIT_TIMESTAMPS_ENABLED.fromMetadata(metadata)) {
      long lastCommitTimestamp = readSnapshot.getTimestamp(engine);
      return Optional.of(Math.max(currentTimestamp, lastCommitTimestamp + 1));
    } else {
      return Optional.empty();
    }
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

  private boolean isReadyForCheckpoint(long newVersion) {
    int checkpointInterval = CHECKPOINT_INTERVAL.fromMetadata(metadata);
    return newVersion > 0 && newVersion % checkpointInterval == 0;
  }

  private Map<String, String> getOperationParameters() {
    if (isNewTable) {
      List<String> partitionCols = VectorUtils.toJavaList(metadata.getPartitionColumns());
      String partitionBy =
          partitionCols.stream()
              .map(col -> "\"" + col + "\"")
              .collect(Collectors.joining(",", "[", "]"));
      return Collections.singletonMap("partitionBy", partitionBy);
    }
    return Collections.emptyMap();
  }

  private void recordTransactionReport(
      Engine engine,
      Optional<Long> committedVersion,
      TransactionMetrics transactionMetrics,
      Optional<Exception> exception) {
    TransactionReport transactionReport =
        new TransactionReportImpl(
            dataPath.toString() /* tablePath */,
            operation.toString(),
            engineInfo,
            committedVersion,
            transactionMetrics,
            readSnapshot.getSnapshotReport(),
            exception);
    engine.getMetricsReporters().forEach(reporter -> reporter.report(transactionReport));
  }

  /**
   * Get the part of the schema of the table that needs the statistics to be collected per file.
   *
   * @param engine {@link Engine} instance to use.
   * @param transactionState State of the transaction
   * @return
   */
  public static List<Column> getStatisticsColumns(Engine engine, Row transactionState) {
    // TODO: implement this once we start supporting collecting stats
    return Collections.emptyList();
  }
}
