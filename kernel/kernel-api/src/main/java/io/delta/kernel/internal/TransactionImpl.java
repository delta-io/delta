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
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.FileAlreadyExistsException;
import java.util.*;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransactionImpl implements Transaction {
  private static final Logger logger = LoggerFactory.getLogger(TransactionImpl.class);

  public static final int DEFAULT_READ_VERSION = 1;
  public static final int DEFAULT_WRITE_VERSION = 2;

  /**
   * Number of retries for concurrent write exceptions to resolve conflicts and retry commit. In
   * Delta-Spark, for historical reasons the number of retries is really high (10m). We are starting
   * with a lower number for now. If this is not sufficient we can update it.
   */
  private static final int NUM_TXN_RETRIES = 200;

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
  private final List<DomainMetadata> domainMetadatas = new ArrayList<>();
  private Metadata metadata;
  private boolean shouldUpdateMetadata;

  private boolean closed; // To avoid trying to commit the same transaction again.

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
    this.clock = clock;
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
    return readSnapshot.getSchema(engine);
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

  @Override
  public TransactionCommitResult commit(Engine engine, CloseableIterable<Row> dataActions)
      throws ConcurrentWriteException {
    checkState(!closed, "Transaction is already attempted to commit. Create a new transaction.");
    TransactionMetrics transactionMetrics = new TransactionMetrics();
    try {
      TransactionCommitResult result =
          transactionMetrics.totalCommitTimer.time(
              () -> commitWithRetry(engine, dataActions, transactionMetrics));
      recordTransactionReport(
          engine,
          Optional.of(result.getVersion()) /* committedVersion */,
          transactionMetrics,
          Optional.empty() /* exception */);
      return result;
    } catch (Exception e) {
      recordTransactionReport(
          engine,
          Optional.empty() /* committedVersion */,
          transactionMetrics,
          Optional.of(e) /* exception */);
      throw e;
    }
  }

  private TransactionCommitResult commitWithRetry(
      Engine engine, CloseableIterable<Row> dataActions, TransactionMetrics transactionMetrics) {
    try {
      long commitAsVersion = readSnapshot.getVersion(engine) + 1;
      // Generate the commit action with the inCommitTimestamp if ICT is enabled.
      CommitInfo attemptCommitInfo = generateCommitAction(engine);
      updateMetadataWithICTIfRequired(
          engine, attemptCommitInfo.getInCommitTimestamp(), readSnapshot.getVersion(engine));

      // If row tracking is supported, assign base row IDs and default row commit versions to any
      // AddFile actions that do not yet have them. If the row ID high watermark changes, emit a
      // DomainMetadata action to update it.
      if (TableFeatures.isRowTrackingSupported(protocol)) {
        RowTracking.createNewHighWaterMarkIfNeeded(readSnapshot, dataActions)
            .ifPresent(domainMetadatas::add);
        dataActions =
            RowTracking.assignBaseRowIdAndDefaultRowCommitVersion(
                readSnapshot, commitAsVersion, dataActions);
      }

      int numRetries = 0;
      do {
        transactionMetrics.commitAttemptsCounter.increment();
        logger.info("Committing transaction as version = {}.", commitAsVersion);
        try {
          return doCommit(
              engine, commitAsVersion, attemptCommitInfo, dataActions, transactionMetrics);
        } catch (FileAlreadyExistsException fnfe) {
          logger.info(
              "Concurrent write detected when committing as version = {}. "
                  + "Trying to resolve conflicts and retry commit.",
              commitAsVersion);
          TransactionRebaseState rebaseState =
              ConflictChecker.resolveConflicts(engine, readSnapshot, commitAsVersion, this);
          long newCommitAsVersion = rebaseState.getLatestVersion() + 1;
          checkArgument(
              commitAsVersion < newCommitAsVersion,
              "New commit version %d should be greater than the previous commit "
                  + "attempt version %d.",
              newCommitAsVersion,
              commitAsVersion);
          commitAsVersion = newCommitAsVersion;
          Optional<Long> updatedInCommitTimestamp =
              getUpdatedInCommitTimestampAfterConflict(
                  rebaseState.getLatestCommitTimestamp(), attemptCommitInfo.getInCommitTimestamp());
          updateMetadataWithICTIfRequired(
              engine, updatedInCommitTimestamp, rebaseState.getLatestVersion());
          attemptCommitInfo.setInCommitTimestamp(updatedInCommitTimestamp);
        }
        numRetries++;
      } while (numRetries < NUM_TXN_RETRIES);
    } finally {
      closed = true;
    }

    // we have exhausted the number of retries, give up.
    logger.info("Exhausted maximum retries ({}) for committing transaction.", NUM_TXN_RETRIES);
    throw new ConcurrentWriteException();
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

  private TransactionCommitResult doCommit(
      Engine engine,
      long commitAsVersion,
      CommitInfo attemptCommitInfo,
      CloseableIterable<Row> dataActions,
      TransactionMetrics transactionMetrics)
      throws FileAlreadyExistsException {
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

    try (CloseableIterator<Row> stageDataIter = dataActions.iterator()) {
      // Create a new CloseableIterator that will return the metadata actions followed by the
      // data actions.
      CloseableIterator<Row> dataAndMetadataActions =
          toCloseableIterator(metadataActions.iterator()).combine(stageDataIter);

      if (commitAsVersion == 0) {
        // New table, create a delta log directory
        if (!wrapEngineExceptionThrowsIO(
            () -> engine.getFileSystemClient().mkdirs(logPath.toString()),
            "Creating directories for path %s",
            logPath)) {
          throw new RuntimeException("Failed to create delta log directory: " + logPath);
        }
      }

      // Counters may be partially incremented from previous tries, reset the action counters to 0
      transactionMetrics.resetActionCounters();

      // Write the staged data to a delta file
      wrapEngineExceptionThrowsIO(
          () -> {
            engine
                .getJsonHandler()
                .writeJsonFileAtomically(
                    FileNames.deltaFile(logPath, commitAsVersion),
                    dataAndMetadataActions.map(
                        action -> {
                          transactionMetrics.totalActionsCounter.increment();
                          if (!action.isNullAt(ADD_FILE_ORDINAL)) {
                            transactionMetrics.addFilesCounter.increment();
                          } else if (!action.isNullAt(REMOVE_FILE_ORDINAL)) {
                            transactionMetrics.removeFilesCounter.increment();
                          }
                          return action;
                        }),
                    false /* overwrite */);
            return null;
          },
          "Write file actions to JSON log file `%s`",
          FileNames.deltaFile(logPath, commitAsVersion));

      return new TransactionCommitResult(commitAsVersion, isReadyForCheckpoint(commitAsVersion));
    } catch (FileAlreadyExistsException e) {
      throw e;
    } catch (IOException ioe) {
      throw new UncheckedIOException(ioe);
    }
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
