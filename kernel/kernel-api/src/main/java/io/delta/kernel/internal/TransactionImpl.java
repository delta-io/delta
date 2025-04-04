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

import io.delta.kernel.Meta;
import io.delta.kernel.Operation;
import io.delta.kernel.Transaction;
import io.delta.kernel.TransactionCommitResult;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.exceptions.ConcurrentWriteException;
import io.delta.kernel.exceptions.DomainDoesNotExistException;
import io.delta.kernel.expressions.Column;
import io.delta.kernel.hook.PostCommitHook;
import io.delta.kernel.internal.actions.*;
import io.delta.kernel.internal.annotation.VisibleForTesting;
import io.delta.kernel.internal.checksum.CRCInfo;
import io.delta.kernel.internal.data.TransactionStateRow;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.hook.CheckpointHook;
import io.delta.kernel.internal.hook.ChecksumSimpleHook;
import io.delta.kernel.internal.metrics.TransactionMetrics;
import io.delta.kernel.internal.metrics.TransactionReportImpl;
import io.delta.kernel.internal.replay.ConflictChecker;
import io.delta.kernel.internal.replay.ConflictChecker.TransactionRebaseState;
import io.delta.kernel.internal.rowtracking.RowTracking;
import io.delta.kernel.internal.tablefeatures.TableFeatures;
import io.delta.kernel.internal.util.*;
import io.delta.kernel.internal.util.Clock;
import io.delta.kernel.internal.util.FileNames;
import io.delta.kernel.internal.util.InCommitTimestampUtils;
import io.delta.kernel.internal.util.VectorUtils;
import io.delta.kernel.metrics.TransactionMetricsResult;
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
  private final Map<String, DomainMetadata> domainMetadatasAdded = new HashMap<>();
  private final Set<String> domainMetadatasRemoved = new HashSet<>();
  private Optional<List<DomainMetadata>> domainMetadatas = Optional.empty();
  private Metadata metadata;
  private boolean shouldUpdateMetadata;
  private int maxRetries;

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
  }

  @Override
  public Row getTransactionState(Engine engine) {
    return TransactionStateRow.of(metadata, dataPath.toString(), maxRetries);
  }

  @Override
  public List<String> getPartitionColumns(Engine engine) {
    return VectorUtils.toJavaList(metadata.getPartitionColumns());
  }

  @Override
  public StructType getSchema(Engine engine) {
    return readSnapshot.getSchema();
  }

  @Override
  public long getReadTableVersion() {
    return readSnapshot.getVersion();
  }

  public Optional<SetTransaction> getSetTxnOpt() {
    return setTxnOpt;
  }

  @VisibleForTesting
  public void addDomainMetadataInternal(String domain, String config) {
    checkArgument(
        !domainMetadatasRemoved.contains(domain),
        "Cannot add a domain that is removed in this transaction");
    checkState(!closed, "Cannot add a domain metadata after the transaction has completed");
    // we override any existing value
    domainMetadatasAdded.put(domain, new DomainMetadata(domain, config, false /* removed */));
  }

  @Override
  public void addDomainMetadata(String domain, String config) {
    checkState(
        TableFeatures.isDomainMetadataSupported(protocol),
        "Unable to add domain metadata when the domain metadata table feature is disabled");
    checkArgument(
        DomainMetadata.isUserControlledDomain(domain),
        "Setting a system-controlled domain is not allowed: " + domain);
    addDomainMetadataInternal(domain, config);
  }

  @VisibleForTesting
  public void removeDomainMetadataInternal(String domain) {
    checkArgument(
        !domainMetadatasAdded.containsKey(domain),
        "Cannot remove a domain that is added in this transaction");
    checkState(!closed, "Cannot remove a domain after the transaction has completed");
    domainMetadatasRemoved.add(domain);
  }

  @Override
  public void removeDomainMetadata(String domain) {
    checkState(
        TableFeatures.isDomainMetadataSupported(protocol),
        "Unable to add domain metadata when the domain metadata table feature is disabled");
    checkArgument(
        DomainMetadata.isUserControlledDomain(domain),
        "Removing a system-controlled domain is not allowed: " + domain);
    removeDomainMetadataInternal(domain);
  }

  /**
   * Returns a list of the domain metadatas to commit. This consists of the domain metadatas added
   * in the transaction using {@link Transaction#addDomainMetadata(String, String)} and the
   * tombstones for the domain metadatas removed in the transaction using {@link
   * Transaction#removeDomainMetadata(String)}. The result is stored in {@code domainMetadatas}.
   *
   * @return A list of {@link DomainMetadata} containing domain metadata to be committed in this
   *     transaction.
   */
  public List<DomainMetadata> getDomainMetadatas() {
    // If we have already processed the domain metadatas, then return the list.
    if (domainMetadatas.isPresent()) {
      return domainMetadatas.get();
    }

    if (domainMetadatasAdded.isEmpty() && domainMetadatasRemoved.isEmpty()) {
      // If no domain metadatas are added or removed, return an empty list. This is to avoid
      // unnecessary loading of the domain metadatas from the snapshot (which is an expensive
      // operation).
      domainMetadatas = Optional.of(Collections.emptyList());
      return Collections.emptyList();
    }

    // Add all domain metadatas added in the transaction
    List<DomainMetadata> finalDomainMetadatas = new ArrayList<>(domainMetadatasAdded.values());

    // Generate the tombstones for the removed domain metadatas
    Map<String, DomainMetadata> snapshotDomainMetadataMap = readSnapshot.getDomainMetadataMap();
    for (String domainName : domainMetadatasRemoved) {
      // Note: we know domainName is not already in finalDomainMetadatas because we do not allow
      // removing and adding a domain with the same identifier in a single txn!
      if (snapshotDomainMetadataMap.containsKey(domainName)) {
        DomainMetadata domainToRemove = snapshotDomainMetadataMap.get(domainName);
        if (domainToRemove.isRemoved()) {
          // If the domain is already removed we throw an error to avoid any inconsistencies or
          // ambiguity. The snapshot read by the connector is inconsistent with the snapshot
          // loaded here as the domain to remove no longer exists.
          throw new DomainDoesNotExistException(
              dataPath.toString(), domainName, readSnapshot.getVersion());
        }
        finalDomainMetadatas.add(domainToRemove.removed());
      } else {
        // We must throw an error if the domain does not exist. Otherwise, there could be unexpected
        // behavior within conflict resolution. For example, consider the following
        // 1. Table has no domains set in V0
        // 2. txnA is started and wants to remove domain "foo"
        // 3. txnB is started and adds domain "foo" and commits V1 before txnA
        // 4. txnA needs to perform conflict resolution against the V1 commit from txnB
        // Conflict resolution should fail but since the domain does not exist we cannot create
        // a tombstone to mark it as removed and correctly perform conflict resolution.
        throw new DomainDoesNotExistException(
            dataPath.toString(), domainName, readSnapshot.getVersion());
      }
    }
    domainMetadatas = Optional.of(finalDomainMetadatas);
    return finalDomainMetadatas;
  }

  public Protocol getProtocol() {
    return protocol;
  }

  @Override
  public TransactionCommitResult commit(Engine engine, CloseableIterable<Row> dataActions)
      throws ConcurrentWriteException {
    checkState(!closed, "Transaction is already attempted to commit. Create a new transaction.");
    TransactionMetrics transactionMetrics = new TransactionMetrics();
    try {
      long committedVersion =
          transactionMetrics.totalCommitTimer.time(
              () -> commitWithRetry(engine, dataActions, transactionMetrics));
      recordTransactionReport(
          engine,
          Optional.of(committedVersion),
          transactionMetrics,
          Optional.empty() /* exception */);
      TransactionMetricsResult txnMetricsCaptured =
          transactionMetrics.captureTransactionMetricsResult();
      return new TransactionCommitResult(
          committedVersion,
          generatePostCommitHooks(committedVersion, txnMetricsCaptured),
          txnMetricsCaptured);
    } catch (Exception e) {
      recordTransactionReport(
          engine,
          Optional.empty() /* committedVersion */,
          transactionMetrics,
          Optional.of(e) /* exception */);
      throw e;
    }
  }

  private long commitWithRetry(
      Engine engine, CloseableIterable<Row> dataActions, TransactionMetrics transactionMetrics) {
    try {
      long commitAsVersion = readSnapshot.getVersion() + 1;
      // Generate the commit action with the inCommitTimestamp if ICT is enabled.
      CommitInfo attemptCommitInfo = generateCommitAction(engine);
      updateMetadataWithICTIfRequired(
          engine, attemptCommitInfo.getInCommitTimestamp(), readSnapshot.getVersion());
      List<DomainMetadata> resolvedDomainMetadatas = getDomainMetadatas();

      // If row tracking is supported, assign base row IDs and default row commit versions to any
      // AddFile actions that do not yet have them. If the row ID high watermark changes, emit a
      // DomainMetadata action to update it.
      if (TableFeatures.isRowTrackingSupported(protocol)) {
        List<DomainMetadata> updatedDomainMetadata =
            RowTracking.updateRowIdHighWatermarkIfNeeded(
                readSnapshot,
                protocol,
                Optional.empty() /* winningTxnRowIdHighWatermark */,
                dataActions,
                resolvedDomainMetadatas);
        domainMetadatas = Optional.of(updatedDomainMetadata);
        dataActions =
            RowTracking.assignBaseRowIdAndDefaultRowCommitVersion(
                readSnapshot,
                protocol,
                Optional.empty() /* winningTxnRowIdHighWatermark */,
                Optional.empty() /* prevCommitVersion */,
                commitAsVersion,
                dataActions);
      }

      int numTries = 0;
      while (numTries <= maxRetries) { // leq because the first is a try, not a retry
        logger.info("Committing transaction as version = {}.", commitAsVersion);
        try {
          transactionMetrics.commitAttemptsCounter.increment();
          return doCommit(
              engine, commitAsVersion, attemptCommitInfo, dataActions, transactionMetrics);
        } catch (FileAlreadyExistsException fnfe) {
          logger.info(
              "Concurrent write detected when committing as version = {}.", commitAsVersion);
          if (numTries < maxRetries) {
            // only try and resolve conflicts if we're going to retry
            TransactionRebaseState rebaseState =
                resolveConflicts(engine, commitAsVersion, attemptCommitInfo, numTries, dataActions);
            commitAsVersion = rebaseState.getLatestVersion() + 1;
            dataActions = rebaseState.getUpdatedDataActions();
            domainMetadatas = Optional.of(rebaseState.getUpdatedDomainMetadatas());
          }
        }
        numTries++;
      }
    } finally {
      closed = true;
    }

    // we have exhausted the number of retries, give up.
    logger.info("Exhausted maximum retries ({}) for committing transaction.", maxRetries);
    throw new ConcurrentWriteException();
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

  private long doCommit(
      Engine engine,
      long commitAsVersion,
      CommitInfo attemptCommitInfo,
      CloseableIterable<Row> dataActions,
      TransactionMetrics transactionMetrics)
      throws FileAlreadyExistsException {
    List<Row> metadataActions = new ArrayList<>();
    metadataActions.add(createCommitInfoSingleAction(attemptCommitInfo.toRow()));
    if (shouldUpdateMetadata || isNewTable) {
      metadataActions.add(createMetadataSingleAction(metadata.toRow()));
    }
    if (shouldUpdateProtocol || isNewTable) {
      // In the future, we need to add metadata and action when there are any changes to them.
      metadataActions.add(createProtocolSingleAction(protocol.toRow()));
    }
    setTxnOpt.ifPresent(setTxn -> metadataActions.add(createTxnSingleAction(setTxn.toRow())));

    List<DomainMetadata> resolvedDomainMetadatas = getDomainMetadatas();

    // Check for duplicate domain metadata and if the protocol supports
    DomainMetadataUtils.validateDomainMetadatas(resolvedDomainMetadatas, protocol);

    resolvedDomainMetadatas.forEach(
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

      // Action counters may be partially incremented from previous tries, reset the counters to 0
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
                          // TODO: handle RemoveFiles.
                          if (!action.isNullAt(ADD_FILE_ORDINAL)) {
                            transactionMetrics.addFilesCounter.increment();
                            transactionMetrics.addFilesSizeInBytesCounter.increment(
                                new AddFile(action.getStruct(ADD_FILE_ORDINAL)).getSize());
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

      return commitAsVersion;
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

  private List<PostCommitHook> generatePostCommitHooks(
      long committedVersion, TransactionMetricsResult txnMetrics) {
    List<PostCommitHook> postCommitHooks = new ArrayList<>();
    if (isReadyForCheckpoint(committedVersion)) {
      postCommitHooks.add(new CheckpointHook(dataPath, committedVersion));
    }

    buildPostCommitCrcInfoIfCurrentCrcAvailable(committedVersion, txnMetrics)
        .ifPresent(crcInfo -> postCommitHooks.add(new ChecksumSimpleHook(crcInfo, logPath)));

    return postCommitHooks;
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

  private Optional<CRCInfo> buildPostCommitCrcInfoIfCurrentCrcAvailable(
      long commitAtVersion, TransactionMetricsResult metricsResult) {
    if (isNewTable) {
      return Optional.of(
          new CRCInfo(
              commitAtVersion,
              metadata,
              protocol,
              metricsResult.getTotalAddFilesSizeInBytes(),
              metricsResult.getNumAddFiles(),
              Optional.of(txnId.toString())));
    }

    return readSnapshot
        .getCurrentCrcInfo()
        // in the case of a conflicting txn and successful retry the readSnapshot may not be
        // commitVersion - 1
        .filter(lastCrcInfo -> commitAtVersion == lastCrcInfo.getVersion() + 1)
        .map(
            lastCrcInfo ->
                new CRCInfo(
                    commitAtVersion,
                    metadata,
                    protocol,
                    // TODO: handle RemoveFiles for calculating table size and num of files.
                    lastCrcInfo.getTableSizeBytes() + metricsResult.getTotalAddFilesSizeInBytes(),
                    lastCrcInfo.getNumFiles() + metricsResult.getNumAddFiles(),
                    Optional.of(txnId.toString())));
  }

  /**
   * Get the part of the schema of the table that needs the statistics to be collected per file.
   *
   * @param transactionState State of the transaction.
   * @return
   */
  public static List<Column> getStatisticsColumns(Row transactionState) {
    int numIndexedCols =
        TableConfig.DATA_SKIPPING_NUM_INDEXED_COLS.fromMetadata(
            TransactionStateRow.getConfiguration(transactionState));

    // Get the list of partition columns to exclude
    Set<String> partitionColumns =
        new HashSet<>(TransactionStateRow.getPartitionColumnsList(transactionState));

    // Collect the leaf-level columns for statistics calculation.
    // This call selects only the first 'numIndexedCols' leaf columns from the logical schema,
    // excluding any column whose top-level name appears in 'partitionColumns'.
    // NOTE: Nested columns (i.e. each leaf within a StructType) count individually toward the
    // numIndexedCols limit (not Map/ArrayTypes - they're not stats compatible types).
    //
    // For example, given the following schema:
    //   root
    //     ├─ col1 (int)
    //     ├─ col2 (string)
    //     └─ col3 (struct)
    //           ├─ a (int)
    //           └─ b (double)
    //
    // And if 'numIndexedCols' is set to 2 with no partition columns to exclude, then the returned
    // stats columns
    // would be: [col1, col2]. If 'col1' were a partition column, the returned list would be:
    // [col2, col3.a] (assuming col3.a is encountered before col3.b).
    return SchemaUtils.collectLeafColumns(
        TransactionStateRow.getPhysicalSchema(transactionState), partitionColumns, numIndexedCols);
  }
}
