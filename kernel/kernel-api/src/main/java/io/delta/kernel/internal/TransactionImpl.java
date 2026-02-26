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

import static io.delta.kernel.internal.TableConfig.*;
import static io.delta.kernel.internal.actions.SingleAction.*;
import static io.delta.kernel.internal.util.Preconditions.checkArgument;
import static io.delta.kernel.internal.util.Preconditions.checkState;
import static io.delta.kernel.internal.util.Utils.toCloseableIterator;
import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;

import io.delta.kernel.*;
import io.delta.kernel.commit.CommitFailedException;
import io.delta.kernel.commit.CommitMetadata;
import io.delta.kernel.commit.Committer;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.exceptions.*;
import io.delta.kernel.expressions.Column;
import io.delta.kernel.hook.PostCommitHook;
import io.delta.kernel.internal.actions.*;
import io.delta.kernel.internal.annotation.VisibleForTesting;
import io.delta.kernel.internal.checksum.CRCInfo;
import io.delta.kernel.internal.clustering.ClusteringUtils;
import io.delta.kernel.internal.compaction.LogCompactionWriter;
import io.delta.kernel.internal.data.TransactionStateRow;
import io.delta.kernel.internal.files.ParsedDeltaData;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.hook.CheckpointHook;
import io.delta.kernel.internal.hook.ChecksumFullHook;
import io.delta.kernel.internal.hook.ChecksumSimpleHook;
import io.delta.kernel.internal.hook.LogCompactionHook;
import io.delta.kernel.internal.lang.Lazy;
import io.delta.kernel.internal.metrics.SnapshotQueryContext;
import io.delta.kernel.internal.metrics.TransactionMetrics;
import io.delta.kernel.internal.metrics.TransactionReportImpl;
import io.delta.kernel.internal.replay.ConflictChecker;
import io.delta.kernel.internal.replay.ConflictChecker.TransactionRebaseState;
import io.delta.kernel.internal.replay.LogReplay;
import io.delta.kernel.internal.rowtracking.RowTracking;
import io.delta.kernel.internal.rowtracking.RowTrackingMetadataDomain;
import io.delta.kernel.internal.snapshot.LogSegment;
import io.delta.kernel.internal.stats.FileSizeHistogram;
import io.delta.kernel.internal.tablefeatures.TableFeatures;
import io.delta.kernel.internal.util.*;
import io.delta.kernel.internal.util.Clock;
import io.delta.kernel.internal.util.InCommitTimestampUtils;
import io.delta.kernel.internal.util.VectorUtils;
import io.delta.kernel.metrics.TransactionMetricsResult;
import io.delta.kernel.metrics.TransactionReport;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterable;
import io.delta.kernel.utils.CloseableIterator;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.*;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransactionImpl implements Transaction {

  ///////////////////////////////
  // Static methods and fields //
  ///////////////////////////////

  /** Get the part of the schema of the table that needs the statistics to be collected per file. */
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

  private static final Logger logger = LoggerFactory.getLogger(TransactionImpl.class);
  public static final int DEFAULT_READ_VERSION = 1;
  public static final int DEFAULT_WRITE_VERSION = 2;
  /**
   * Default retries for concurrent write exceptions to resolve conflicts and retry commit. In
   * Delta-Spark, for historical reasons the number of retries is really high (10m). We are starting
   * with a lower number by default for now. If this is not sufficient we can update it.
   */
  private static final int DEFAULT_MAX_RETRIES = 200;

  /////////////////////
  // Instance fields //
  /////////////////////

  private final UUID txnId = UUID.randomUUID();

  /** If the transaction is defining a new table from scratch (i.e. create table, replace table) */
  private final boolean isCreateOrReplace;

  private final Path dataPath;
  private final Path logPath;
  private final Optional<SnapshotImpl> readSnapshotOpt;
  private final String engineInfo;
  private final Operation operation;
  private final Protocol protocol;
  private final boolean shouldUpdateProtocol;
  private Metadata metadata;
  private boolean shouldUpdateMetadata;
  private final Committer committer;
  private final Optional<SetTransaction> setTxnOpt;
  /**
   * The new clustering columns to write in the domain metadata in this transaction if provided.
   *
   * <ul>
   *   <li>Optional.empty() - do not update the clustering domain metadata in this txn
   *   <li>Optional.of([]) - update the clustering domain metadata to store an empty list in this
   *       txn
   *   <li>Optional.of([col1, col2]) - update the clustering domain metadata to store these columns
   *       in this txn
   * </ul>
   */
  private final Optional<List<Column>> newClusteringColumnsOpt;

  private int maxRetries;
  private final int logCompactionInterval;
  private final Clock clock;
  private final DomainMetadataState domainMetadataState = new DomainMetadataState();
  private Optional<CRCInfo> currentCrcInfo;
  private Optional<Long> providedRowIdHighWatermark = Optional.empty();
  private Supplier<Map<String, String>> committerProperties = Collections::emptyMap;
  private boolean closed; // To avoid trying to commit the same transaction again.

  public TransactionImpl(
      boolean isCreateOrReplace,
      Path dataPath,
      Optional<SnapshotImpl> readSnapshotOpt,
      String engineInfo,
      Operation operation,
      Optional<Protocol> newProtocol,
      Optional<Metadata> newMetadata,
      Committer committer,
      Optional<SetTransaction> setTxnOpt,
      Optional<List<Column>> newClusteringColumnsOpt,
      Optional<Integer> maxRetriesOpt,
      int logCompactionInterval,
      Clock clock) {
    checkArgument(isCreateOrReplace || readSnapshotOpt.isPresent());
    // For a new table, a protocol and metadata must be provided
    checkArgument(
        (newProtocol.isPresent() && newMetadata.isPresent()) || readSnapshotOpt.isPresent());
    // TODO: look into migrating entire class into just (newMetadata, newProtocol, readSnapshotOpt)

    this.isCreateOrReplace = isCreateOrReplace;
    this.dataPath = dataPath;
    this.logPath = new Path(dataPath, "_delta_log");
    this.readSnapshotOpt = readSnapshotOpt;
    this.engineInfo = engineInfo;
    this.operation = operation;
    this.protocol = newProtocol.orElseGet(() -> readSnapshotOpt.get().getProtocol());
    this.shouldUpdateProtocol = newProtocol.isPresent();
    this.metadata = newMetadata.orElseGet(() -> readSnapshotOpt.get().getMetadata());
    this.shouldUpdateMetadata = newMetadata.isPresent();
    this.committer = committer;
    this.setTxnOpt = setTxnOpt;
    this.newClusteringColumnsOpt = newClusteringColumnsOpt;
    this.maxRetries = maxRetriesOpt.orElse(DEFAULT_MAX_RETRIES);
    this.logCompactionInterval = logCompactionInterval;
    this.clock = clock;
    this.currentCrcInfo = readSnapshotOpt.flatMap(SnapshotImpl::getCurrentCrcInfo);
  }

  ////////////////
  // Public API //
  ////////////////

  @Override
  public Row getTransactionState(Engine engine) {
    return TransactionStateRow.of(metadata, protocol, dataPath.toString(), maxRetries);
  }

  @Override
  public Committer getCommitter() {
    return committer;
  }

  @Override
  public List<String> getPartitionColumns(Engine engine) {
    return VectorUtils.toJavaList(metadata.getPartitionColumns());
  }

  @Override
  public StructType getSchema(Engine engine) {
    return metadata.getSchema();
  }

  @Override
  public long getReadTableVersion() {
    return readSnapshotOpt.map(SnapshotImpl::getVersion).orElse(-1L);
  }

  @Override
  public void withCommitterProperties(Supplier<Map<String, String>> committerProperties) {
    this.committerProperties = requireNonNull(committerProperties, "committerProperties is null");
  }

  @Override
  public void addDomainMetadata(String domain, String config) {
    checkState(
        TableFeatures.isDomainMetadataSupported(protocol),
        "Unable to add domain metadata when the domain metadata table feature is disabled");
    checkArgument(
        DomainMetadata.isUserControlledDomain(domain)
            || DomainMetadata.isSystemDomainSupportedSetFromTxn(domain),
        "Setting a non-supported system-controlled domain is not allowed: " + domain);

    // Specific handling for system domain metadata
    if (DomainMetadata.isSystemDomainSupportedSetFromTxn(domain)) {
      handleSystemDomainMetadata(domain, config);
    } else {
      domainMetadataState.addDomain(domain, config);
    }
  }

  @Override
  public void removeDomainMetadata(String domain) {
    checkState(
        TableFeatures.isDomainMetadataSupported(protocol),
        "Unable to add domain metadata when the domain metadata table feature is disabled");
    checkArgument(
        DomainMetadata.isUserControlledDomain(domain),
        "Removing a system-controlled domain is not allowed: " + domain);
    domainMetadataState.removeDomain(domain);
  }

  @Override
  public TransactionCommitResult commit(Engine engine, CloseableIterable<Row> dataActions)
      throws ConcurrentWriteException {
    checkState(!closed, "Transaction is already attempted to commit. Create a new transaction.");
    // For a new table or when fileSizeHistogram is available in the CRC of the readSnapshotOpt
    // we update it in the commit. When it is not available we do nothing.
    TransactionMetrics txnMetrics =
        readSnapshotOpt
            .map(
                snapshot ->
                    TransactionMetrics.withExistingTableFileSizeHistogram(
                        snapshot.getCurrentCrcInfo().flatMap(CRCInfo::getFileSizeHistogram)))
            .orElse(TransactionMetrics.forNewTable());
    try {
      final Tuple2<ParsedDeltaData, Optional<Long>> committedDeltaAndIct =
          txnMetrics.totalCommitTimer.time(() -> commitWithRetry(engine, dataActions, txnMetrics));

      return buildTransactionCommitResult(
          engine, committedDeltaAndIct._1, txnMetrics, committedDeltaAndIct._2);
    } catch (Exception e) {
      recordTransactionReport(
          engine,
          Optional.empty() /* committedVersion */,
          getEffectiveClusteringColumns(),
          txnMetrics,
          Optional.of(e) /* exception */);
      throw e;
    }
  }

  //////////////////
  // Internal API //
  //////////////////

  @VisibleForTesting
  public void addDomainMetadataInternal(String domain, String config) {
    domainMetadataState.addDomain(domain, config);
  }

  @VisibleForTesting
  public void removeDomainMetadataInternal(String domain) {
    domainMetadataState.removeDomain(domain);
  }

  public Path getDataPath() {
    return dataPath;
  }

  public Path getLogPath() {
    return logPath;
  }

  public Protocol getProtocol() {
    return protocol;
  }

  public Optional<SetTransaction> getSetTxnOpt() {
    return setTxnOpt;
  }

  public Optional<List<Column>> getEffectiveClusteringColumns() {
    if (isCreateOrReplace) {
      // if isCreateOrReplace return the columns set in this txn
      return newClusteringColumnsOpt;
    } else { // since !isCreateOrReplace must be an update to an existing table
      if (newClusteringColumnsOpt.isPresent()) {
        // if the clustering columns are being updated in this txn return those
        return newClusteringColumnsOpt;
      } else {
        // else, return the current existing clustering columns (readSnapshotOpt must be present)
        return readSnapshotOpt.flatMap(SnapshotImpl::getPhysicalClusteringColumns);
      }
    }
  }

  ///////////////////////////////
  // Other getters and setters //
  ///////////////////////////////

  private boolean isReplaceTable() {
    return isCreateOrReplace && readSnapshotOpt.isPresent();
  }

  /**
   * Returns the maximum number of commit attempts, including the first attempt.
   *
   * <p>This is explicitly a method instead of a constant as the maxRetries variable is itself
   * mutable, and can for example be set to 0 when the rowIdHighWatermark is explicitly provided.
   */
  private int getMaxCommitAttempts() {
    return maxRetries + 1; // +1 because the first attempt is a try, not a retry.
  }

  private Optional<Boolean> isBlindAppend() {
    // TODO: for now we hard code this to false to avoid erroneously setting this to true for a
    //  non-blind-append operation. We should revisit how to safely set this to true for actual
    //  blind appends.
    return Optional.of(false);
  }

  private void updateMetadata(Metadata metadata) {
    logger.info(
        "Updated metadata from {} to {}", shouldUpdateMetadata ? this.metadata : "-", metadata);
    this.metadata = metadata;
    this.shouldUpdateMetadata = true;
  }

  private void handleSystemDomainMetadata(String domain, String config) {
    if (domain.equals(RowTrackingMetadataDomain.DOMAIN_NAME)) {
      if (!TableFeatures.isRowTrackingSupported(protocol)) {
        throw DeltaErrors.rowTrackingRequiredForRowIdHighWatermark(dataPath.toString(), config);
      }
      long providedHighWaterMark =
          RowTrackingMetadataDomain.fromJsonConfiguration(config).getRowIdHighWaterMark();
      checkArgument(providedHighWaterMark >= 0, "rowIdHighWatermark must be >= 0");
      this.providedRowIdHighWatermark = Optional.of(providedHighWaterMark);
      // Conflict resolution is disabled when providedRowIdHighWatermark is set,
      // because it must be updated according to the latest table state.
      maxRetries = 0;
    }
  }

  //////////////////////////////////
  // Commit Execution (Main Flow) //
  //////////////////////////////////

  /** Returns (commitDeltaData, inCommitTimestamp). */
  private Tuple2<ParsedDeltaData, Optional<Long>> commitWithRetry(
      Engine engine, CloseableIterable<Row> dataActions, TransactionMetrics transactionMetrics) {
    try {
      long commitAsVersion = getReadTableVersion() + 1;
      // Generate the commit action with the inCommitTimestamp if ICT is enabled.
      CommitInfo attemptCommitInfo = generateCommitAction(engine);
      updateMetadataWithICTIfRequired(
          engine, attemptCommitInfo.getInCommitTimestamp(), getReadTableVersion());
      List<DomainMetadata> resolvedDomainMetadatas =
          domainMetadataState.getComputedDomainMetadatasToCommit();

      // If row tracking is supported, assign base row IDs and default row commit versions to any
      // AddFile actions that do not yet have them. If the row ID high watermark changes, emit a
      // DomainMetadata action to update it.
      if (TableFeatures.isRowTrackingSupported(protocol)) {
        List<DomainMetadata> updatedDomainMetadata =
            RowTracking.updateRowIdHighWatermarkIfNeeded(
                readSnapshotOpt,
                protocol,
                Optional.empty() /* winningTxnRowIdHighWatermark */,
                dataActions,
                resolvedDomainMetadatas,
                providedRowIdHighWatermark);
        domainMetadataState.setComputedDomainMetadatas(updatedDomainMetadata);
        dataActions =
            RowTracking.assignBaseRowIdAndDefaultRowCommitVersion(
                readSnapshotOpt,
                protocol,
                Optional.empty() /* winningTxnRowIdHighWatermark */,
                Optional.empty() /* prevCommitVersion */,
                commitAsVersion,
                dataActions);
      }

      int attempt = 1;
      boolean seenRetryableNonConflictException = false;
      while (true) {
        // This loop exits upon either (a) commit success (return statement) or (b) commit failure.
        logger.info(
            "Attempting to commit transaction at table version {}. Attempt {}/{}",
            commitAsVersion,
            attempt,
            getMaxCommitAttempts());
        try {
          transactionMetrics.commitAttemptsCounter.increment();
          return doCommit(
              engine, commitAsVersion, attemptCommitInfo, dataActions, transactionMetrics);
        } catch (CommitFailedException cfe) {
          if (!cfe.isRetryable()) {
            // Case 1: Non-retryable exception. We must throw this. We don't expect connectors to
            //         be able to recover from this.
            throw DeltaErrors.nonRetryableCommitException(attempt, commitAsVersion, cfe);
          }
          if (attempt >= getMaxCommitAttempts()) {
            // Case 2: Despite the error being retryable, we have exhausted the maximum number of
            //         retries. We must throw here, too.
            throw new MaxCommitRetryLimitReachedException(commitAsVersion, maxRetries, cfe);
          }

          // We know the commit is retryable.

          if (!cfe.isConflict()) {
            // Case 3: No conflict => No conflict resolution needed. Just retry with same version.
            printLogForRetryableNonConflictException(attempt, commitAsVersion, cfe);
            seenRetryableNonConflictException = true;
          } else if (seenRetryableNonConflictException) {
            checkState(cfe.isRetryable() && cfe.isConflict(), "expect retryable and conflict");

            // Case 4: There is a conflict, and we have previously seen a retryable exception
            //         without conflict and then retried. This means that something like the
            //         following has happened:
            // - Commit Attempt #1: IOException => CFE(retryable=true, conflict=false). We set
            //         seenRetryableNonConflictException to true. Here, there's two possible cases:
            //         (A) N.json was written successfully (and we just never learned about it),
            //         or (B) N.json was not written.
            // - Commit Attempt #2: FileAlreadyExistsException => CFE(retryable=true, conflict=true)
            //         Should we retry this commit? If it's case (A), then we should not, as we are
            //         just conflicting with our previous commit attempt. If it's case (B), then we
            //         should retry, since we are conflicting with some *other* writer's commit. In
            //         the future we can add detection capabilities between these two cases (e.g.
            //         check if the CommitInfo action is present and has a txnId, else compare the
            //         other contents of the delta files).
            throw new CommitStateUnknownException(commitAsVersion, attempt, cfe);
          } else {
            checkState(cfe.isRetryable() && cfe.isConflict(), "expect retryable and conflict");
            // Case 5: There is a conflict, and we have not previously seen a retryable and
            //         non-conflict exception. We will resolve the conflict and retry.
            printLogForRetryableWithConflictException(attempt, commitAsVersion, cfe);

            TransactionRebaseState rebaseState =
                resolveConflicts(engine, commitAsVersion, attemptCommitInfo, attempt, dataActions);
            commitAsVersion = rebaseState.getLatestVersion() + 1;
            dataActions = rebaseState.getUpdatedDataActions();
            domainMetadataState.setComputedDomainMetadatas(rebaseState.getUpdatedDomainMetadatas());
            currentCrcInfo = rebaseState.getUpdatedCrcInfo();
          }
        }
        // We will be retrying the commit (either from case 3 or 5 above).
        //
        // Action counters may be partially incremented from previous tries, reset the counters
        // to 0 and drop fileSizeHistogram
        // TODO: [delta-io/delta#5047] reconcile fileSizeHistogram
        transactionMetrics.resetActionMetricsForRetry();
        attempt++;
      }
    } finally {
      closed = true;
    }
  }

  /** Returns (commitDeltaData, inCommitTimestamp). */
  private Tuple2<ParsedDeltaData, Optional<Long>> doCommit(
      Engine engine,
      long commitAsVersion,
      CommitInfo attemptCommitInfo,
      CloseableIterable<Row> dataActions,
      TransactionMetrics transactionMetrics)
      throws CommitFailedException {
    List<Row> metadataActions = new ArrayList<>();
    metadataActions.add(createCommitInfoSingleAction(attemptCommitInfo.toRow()));
    if (shouldUpdateMetadata) {
      metadataActions.add(createMetadataSingleAction(metadata.toRow()));
    }
    if (shouldUpdateProtocol) {
      // In the future, we need to add metadata and action when there are any changes to them.
      metadataActions.add(createProtocolSingleAction(protocol.toRow()));
    }
    setTxnOpt.ifPresent(setTxn -> metadataActions.add(createTxnSingleAction(setTxn.toRow())));

    List<DomainMetadata> resolvedDomainMetadatas =
        domainMetadataState.getComputedDomainMetadatasToCommit();

    // Check for duplicate domain metadata and if the protocol supports
    DomainMetadataUtils.validateDomainMetadatas(resolvedDomainMetadatas, protocol);

    resolvedDomainMetadatas.forEach(
        dm -> metadataActions.add(createDomainMetadataSingleAction(dm.toRow())));

    try (CloseableIterator<Row> userStageDataIter = dataActions.iterator()) {
      final CloseableIterator<Row> completeFileActionIter;
      if (isReplaceTable()) {
        // If this is a replace table operation we need to internally generate the remove file
        // actions to reset the table state
        completeFileActionIter = getRemoveActionsForReplace(engine).combine(userStageDataIter);
      } else {
        completeFileActionIter = userStageDataIter;
      }

      boolean isAppendOnlyTable = APPEND_ONLY_ENABLED.fromMetadata(metadata);

      // Create a new CloseableIterator that will return the metadata actions followed by the
      // data actions.
      CloseableIterator<Row> dataAndMetadataActions =
          toCloseableIterator(metadataActions.iterator())
              .combine(completeFileActionIter)
              .map(
                  action -> {
                    incrementMetricsForFileActionRow(transactionMetrics, action);
                    if (!action.isNullAt(REMOVE_FILE_ORDINAL)) {
                      RemoveFile removeFile = new RemoveFile(action.getStruct(REMOVE_FILE_ORDINAL));
                      if (isAppendOnlyTable && removeFile.getDataChange()) {
                        throw DeltaErrors.cannotModifyAppendOnlyTable(dataPath.toString());
                      }
                    }
                    return action;
                  });

      final CommitMetadata commitMetadata =
          new CommitMetadata(
              commitAsVersion,
              logPath.toString(),
              attemptCommitInfo,
              resolvedDomainMetadatas,
              committerProperties,
              readSnapshotOpt.map(x -> new Tuple2<>(x.getProtocol(), x.getMetadata())),
              shouldUpdateProtocol ? Optional.of(protocol) : Optional.empty(),
              shouldUpdateMetadata ? Optional.of(metadata) : Optional.empty(),
              readSnapshotOpt
                  .map(x -> x.getLogSegment().getMaxPublishedDeltaVersion())
                  .orElse(Optional.of(-1L)));

      DirectoryCreationUtils.createAllDeltaDirectoriesAsNeeded(
          engine, logPath, commitAsVersion, commitMetadata.getReadProtocolOpt(), protocol);

      return new Tuple2<>(
          committer.commit(engine, dataAndMetadataActions, commitMetadata).getCommitLogData(),
          attemptCommitInfo.getInCommitTimestamp());
    } catch (IOException ioe) {
      // Error closing the CloseableIterator of actions or error creating the delta log directory
      throw new UncheckedIOException(ioe);
    }
  }

  ////////////////////////////////
  // Commit Execution (Helpers) //
  ////////////////////////////////

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
        emptyMap() /* operationMetrics */);
  }

  /**
   * Generates a timestamp which is greater than the commit timestamp of the readSnapshotOpt. This
   * can result in an additional file read and that this will only happen if ICT is enabled.
   */
  private Optional<Long> generateInCommitTimestampForFirstCommitAttempt(
      Engine engine, long currentTimestamp) {
    if (IN_COMMIT_TIMESTAMPS_ENABLED.fromMetadata(metadata)) {
      if (readSnapshotOpt.isPresent()) {
        long lastCommitTimestamp = readSnapshotOpt.get().getTimestamp(engine);
        return Optional.of(Math.max(currentTimestamp, lastCommitTimestamp + 1));
      } else { // For a new table this is just the current timestamp
        return Optional.of(currentTimestamp);
      }
    } else {
      return Optional.empty();
    }
  }

  private Map<String, String> getOperationParameters() {
    if (isCreateOrReplace) {
      List<String> partitionCols = VectorUtils.toJavaList(metadata.getPartitionColumns());
      String partitionBy =
          partitionCols.stream()
              .map(col -> "\"" + col + "\"")
              .collect(Collectors.joining(",", "[", "]"));
      return Collections.singletonMap("partitionBy", partitionBy);
    }
    return emptyMap();
  }

  private void updateMetadataWithICTIfRequired(
      Engine engine, Optional<Long> inCommitTimestampOpt, long lastCommitVersion) {
    // If ICT is enabled for the current transaction, update the metadata with the ICT
    // enablement info.
    inCommitTimestampOpt.ifPresent(
        inCommitTimestamp -> {
          Optional<Metadata> metadataWithICTInfo =
              InCommitTimestampUtils.getUpdatedMetadataWithICTEnablementInfo(
                  engine, inCommitTimestamp, readSnapshotOpt, metadata, lastCommitVersion + 1L);
          metadataWithICTInfo.ifPresent(this::updateMetadata);
        });
  }

  private void incrementMetricsForFileActionRow(TransactionMetrics txnMetrics, Row fileActionRow) {
    txnMetrics.totalActionsCounter.increment();
    if (!fileActionRow.isNullAt(ADD_FILE_ORDINAL)) {
      txnMetrics.updateForAddFile(new AddFile(fileActionRow.getStruct(ADD_FILE_ORDINAL)).getSize());
    } else if (!fileActionRow.isNullAt(REMOVE_FILE_ORDINAL)) {
      RemoveFile removeFile = new RemoveFile(fileActionRow.getStruct(REMOVE_FILE_ORDINAL));
      long removeFileSize =
          removeFile.getSize().orElseThrow(DeltaErrorsInternal::missingRemoveFileSizeDuringCommit);
      txnMetrics.updateForRemoveFile(removeFileSize);
    }
  }

  /**
   * Returns the remove file rows needed to remove every active add file in the table. These rows
   * are already formatted as {@link SingleAction} rows and are ready to be committed.
   */
  private CloseableIterator<Row> getRemoveActionsForReplace(Engine engine) {
    checkArgument(
        readSnapshotOpt.isPresent(), "Cannot generate removes for a snapshot with version < 0");
    Scan scan = readSnapshotOpt.get().getScanBuilder().build();
    return Utils.intoRows(scan.getScanFiles(engine))
        .map(
            scanRow -> {
              AddFile add = new AddFile(scanRow.getStruct(InternalScanFileUtils.ADD_FILE_ORDINAL));
              return SingleAction.createRemoveFileSingleAction(
                  add.toRemoveFileRow(true /* dataChange */, Optional.empty()));
            });
  }

  /////////////////////////
  // Conflict Resolution //
  /////////////////////////

  private TransactionRebaseState resolveConflicts(
      Engine engine,
      long commitAsVersion,
      CommitInfo attemptCommitInfo,
      int attempt,
      CloseableIterable<Row> dataActions) {
    logger.info(
        "[{}] Trying to resolve conflicts and retry commit. Attempt {}/{}.",
        dataPath,
        attempt,
        getMaxCommitAttempts());
    TransactionRebaseState rebaseState =
        ConflictChecker.resolveConflicts(
            engine,
            readSnapshotOpt,
            commitAsVersion,
            this,
            domainMetadataState.getComputedDomainMetadatasToCommit(),
            dataActions);
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

  private Optional<Long> getUpdatedInCommitTimestampAfterConflict(
      long winningCommitTimestamp, Optional<Long> attemptInCommitTimestamp) {
    if (attemptInCommitTimestamp.isPresent()) {
      long updatedInCommitTimestamp =
          Math.max(attemptInCommitTimestamp.get(), winningCommitTimestamp + 1);
      return Optional.of(updatedInCommitTimestamp);
    }
    return attemptInCommitTimestamp;
  }

  ////////////////////////////
  // Post-Commit Processing //
  ////////////////////////////

  private TransactionCommitResult buildTransactionCommitResult(
      Engine engine,
      ParsedDeltaData committedDelta,
      TransactionMetrics txnMetrics,
      Optional<Long> committedIctOpt) {
    final long committedVersion = committedDelta.getVersion();

    final TransactionReport transactionReport =
        recordTransactionReport(
            engine,
            Optional.of(committedVersion),
            getEffectiveClusteringColumns(),
            txnMetrics,
            Optional.empty() /* exception */);

    final TransactionMetricsResult txnMetricsCaptured =
        txnMetrics.captureTransactionMetricsResult();

    final Optional<CRCInfo> postCommitCrcOpt =
        buildPostCommitCrcInfoIfCurrentCrcAvailable(committedVersion, txnMetricsCaptured);

    final Optional<SnapshotImpl> postCommitSnapshotOpt =
        buildPostCommitSnapshotOpt(engine, committedDelta, committedIctOpt, postCommitCrcOpt);

    return new TransactionCommitResult(
        committedVersion,
        generatePostCommitHooks(committedVersion, postCommitCrcOpt),
        transactionReport,
        postCommitSnapshotOpt);
  }

  private Optional<SnapshotImpl> buildPostCommitSnapshotOpt(
      Engine engine,
      ParsedDeltaData committedDelta,
      Optional<Long> committedIctOpt,
      Optional<CRCInfo> postCommitCrcOpt) {
    // TODO: Support building post-commit Snapshots after conflicts. If there was a conflict, then
    //       we'd need to keep track of each of the conflicting commit files in order to build the
    //       new LogSegment for our post-commit Snapshot. This is currently not done, today. Note
    //       that for catalogManaged tables, we would need the Committer to provide the conflicting
    //       commits as part of the CommitFailedException.
    if (committedDelta.getVersion() != getReadTableVersion() + 1) {
      return Optional.empty();
    }

    final LogSegment postCommitLogSegment = buildPostCommitLogSegment(committedDelta);
    final Lazy<LogSegment> lazyLogSegment = new Lazy<>(() -> postCommitLogSegment);
    final Lazy<Optional<CRCInfo>> lazyCrcInfo = new Lazy<>(() -> postCommitCrcOpt);
    final LogReplay logReplay = new LogReplay(engine, dataPath, lazyLogSegment, lazyCrcInfo);
    // TODO: SnapshotQueryContext.forPostCommitSnapshot
    final SnapshotQueryContext snapshotContext =
        SnapshotQueryContext.forVersionSnapshot(dataPath.toString(), committedDelta.getVersion());
    final SnapshotImpl postCommitSnapshot =
        new SnapshotImpl(
            dataPath,
            committedDelta.getVersion(),
            lazyLogSegment,
            logReplay,
            protocol,
            metadata,
            committer,
            snapshotContext,
            committedIctOpt);

    return Optional.of(postCommitSnapshot);
  }

  private LogSegment buildPostCommitLogSegment(ParsedDeltaData committedDelta) {
    if (readSnapshotOpt.isPresent()) {
      return readSnapshotOpt
          .get()
          .getLogSegment()
          .newWithAddedDeltas(Collections.singletonList(committedDelta));
    }

    return LogSegment.createForNewTable(logPath, committedDelta);
  }

  private List<PostCommitHook> generatePostCommitHooks(
      long committedVersion, Optional<CRCInfo> postCommitCrcOpt) {
    final List<PostCommitHook> postCommitHooks = new ArrayList<>();

    if (isReadyForCheckpoint(committedVersion)) {
      postCommitHooks.add(new CheckpointHook(dataPath, committedVersion));
    }

    if (postCommitCrcOpt.isPresent()) {
      postCommitHooks.add(new ChecksumSimpleHook(postCommitCrcOpt.get(), logPath));
    } else {
      postCommitHooks.add(new ChecksumFullHook(dataPath, committedVersion));
    }

    if (logCompactionInterval > 0
        && LogCompactionWriter.shouldCompact(committedVersion, logCompactionInterval)) {
      // add one here because commits start a 0
      long startVersion = committedVersion + 1 - logCompactionInterval;
      long minFileRetentionTimestampMillis =
          clock.getTimeMillis() - TOMBSTONE_RETENTION.fromMetadata(metadata);
      postCommitHooks.add(
          new LogCompactionHook(
              dataPath, logPath, startVersion, committedVersion, minFileRetentionTimestampMillis));
    }

    return postCommitHooks;
  }

  private boolean isReadyForCheckpoint(long newVersion) {
    int checkpointInterval = CHECKPOINT_INTERVAL.fromMetadata(metadata);
    return newVersion > 0 && newVersion % checkpointInterval == 0;
  }

  private Optional<CRCInfo> buildPostCommitCrcInfoIfCurrentCrcAvailable(
      long commitAtVersion, TransactionMetricsResult metricsResult) {
    if (isCreateOrReplace) {
      // We don't need to worry about conflicting transaction here since new tables always commit
      // metadata (and thus fail any conflicts)
      return Optional.of(
          new CRCInfo(
              commitAtVersion,
              metadata,
              protocol,
              metricsResult.getTotalAddFilesSizeInBytes(),
              metricsResult.getNumAddFiles(),
              Optional.of(txnId.toString()),
              domainMetadataState.getPostCommitDomainMetadatas(),
              metricsResult
                  .getTableFileSizeHistogram()
                  .map(FileSizeHistogram::fromFileSizeHistogramResult)));
    }

    return currentCrcInfo
        // Ensure current currentCrcInfo is exactly commitAtVersion - 1
        .filter(crcInfo -> commitAtVersion == crcInfo.getVersion() + 1)
        .map(
            lastCrcInfo ->
                new CRCInfo(
                    commitAtVersion,
                    metadata,
                    protocol,
                    lastCrcInfo.getTableSizeBytes()
                        + metricsResult.getTotalAddFilesSizeInBytes()
                        - metricsResult.getTotalRemoveFilesSizeInBytes(),
                    lastCrcInfo.getNumFiles()
                        + metricsResult.getNumAddFiles()
                        - metricsResult.getNumRemoveFiles(),
                    Optional.of(txnId.toString()),
                    domainMetadataState.getPostCommitDomainMetadatas(),
                    metricsResult
                        .getTableFileSizeHistogram()
                        .map(FileSizeHistogram::fromFileSizeHistogramResult)));
  }

  private TransactionReport recordTransactionReport(
      Engine engine,
      Optional<Long> committedVersion,
      Optional<List<Column>> clusteringColumnsOpt,
      TransactionMetrics transactionMetrics,
      Optional<Exception> exception) {
    TransactionReport transactionReport =
        new TransactionReportImpl(
            dataPath.toString() /* tablePath */,
            operation.toString(),
            engineInfo,
            committedVersion,
            clusteringColumnsOpt,
            transactionMetrics,
            readSnapshotOpt.map(SnapshotImpl::getSnapshotReport),
            exception);
    engine.getMetricsReporters().forEach(reporter -> reporter.report(transactionReport));

    return transactionReport;
  }

  /////////////////////
  // Logging Helpers //
  /////////////////////

  private void printLogForRetryableNonConflictException(
      int attempt, long commitAsVersion, CommitFailedException cfe) {
    logger.warn(
        "Commit attempt {} for table version {} failed with a retryable exception and without "
            + "conflict. Skipping conflict resolution and trying again. Exception: {}",
        attempt,
        commitAsVersion,
        cfe);
  }

  private void printLogForRetryableWithConflictException(
      int attempt, long commitAsVersion, CommitFailedException cfe) {
    logger.warn(
        "Commit attempt {} for version {} failed with a retryable exception due to a physical "
            + "conflict. Performing conflict resolution and trying again. Exception: {}",
        attempt,
        commitAsVersion,
        cfe);
  }

  ////////////////////
  // Helper Classes //
  ////////////////////

  /** Encapsulates the state of domain metadata within a transaction. */
  private class DomainMetadataState {
    private final Map<String, DomainMetadata> domainsToAdd = new HashMap<>();
    private final Set<String> domainsToRemove = new HashSet<>();
    private Optional<List<DomainMetadata>> computedMetadatas = Optional.empty();

    /** Adds a domain metadata. Invalidates any cached computed state. */
    public void addDomain(String domain, String config) {
      checkArgument(
          !domainsToRemove.contains(domain),
          "Cannot add a domain that is removed in this transaction");
      checkState(!closed, "Cannot add a domain metadata after the transaction has completed");

      // Add the domain and invalidate cache
      domainsToAdd.put(domain, new DomainMetadata(domain, config, false /* removed */));
      computedMetadatas = Optional.empty();
    }

    /** Marks a domain for removal. Invalidates any cached computed state. */
    public void removeDomain(String domain) {
      checkArgument(
          !domainsToAdd.containsKey(domain),
          "Cannot remove a domain that is added in this transaction");
      checkState(!closed, "Cannot remove a domain after the transaction has completed");

      // Mark for removal and invalidate cache
      domainsToRemove.add(domain);
      computedMetadatas = Optional.empty();
    }

    /**
     * Returns a list of the domain metadatas to commit. This consists of the domain metadatas added
     * in the transaction using {@link Transaction#addDomainMetadata(String, String)} and the
     * tombstones for the domain metadatas removed in the transaction using {@link
     * Transaction#removeDomainMetadata(String)}.
     *
     * @return A list of {@link DomainMetadata} containing domain metadata to be committed in this
     *     transaction.
     */
    public List<DomainMetadata> getComputedDomainMetadatasToCommit() {
      if (computedMetadatas.isPresent()) {
        return computedMetadatas.get();
      }

      generateClusteringDomainMetadataIfNeeded();
      if (isReplaceTable()) {
        // In the case of replace table we need to completely reset the table state by removing
        // any existing domain metadata
        readSnapshotOpt
            .get() // if replaceTable we know snapshot is present
            .getActiveDomainMetadataMap()
            .forEach(
                (domainName, domainMetadata) -> {
                  if (!domainsToAdd.containsKey(domainName)) {
                    // We only need to remove the domain if it is not added (& thus overwritten)
                    // in this current transaction. We cannot add and remove the same domain in
                    // one transaction.
                    removeDomain(domainName);
                  }
                });
      }
      // Add all domains added in the transaction
      List<DomainMetadata> result = new ArrayList<>(domainsToAdd.values());

      if (domainsToRemove.isEmpty()) {
        // If no domain metadatas are removed we don't need to load the existing domain metadatas
        // from the snapshot (which is an expensive operation)
        computedMetadatas = Optional.of(result);
        return result;
      }

      // Generate the tombstones for removed domains
      Map<String, DomainMetadata> snapshotDomainMetadataMap =
          readSnapshotOpt.map(SnapshotImpl::getActiveDomainMetadataMap).orElse(emptyMap());
      for (String domainName : domainsToRemove) {
        if (snapshotDomainMetadataMap.containsKey(domainName)) {
          // Note: we know domainName is not already in finalDomainMetadatas because we do not allow
          // removing and adding a domain with the same identifier in a single txn!
          DomainMetadata domainToRemove = snapshotDomainMetadataMap.get(domainName);
          checkState(
              !domainToRemove.isRemoved(),
              "snapshotDomainMetadataMap should only contain active domain metadata");
          result.add(domainToRemove.removed());
        } else {
          // We must throw an error if the domain does not exist. Otherwise, there could be
          // unexpected
          // behavior within conflict resolution. For example, consider the following
          // 1. Table has no domains set in V0
          // 2. txnA is started and wants to remove domain "foo"
          // 3. txnB is started and adds domain "foo" and commits V1 before txnA
          // 4. txnA needs to perform conflict resolution against the V1 commit from txnB
          // Conflict resolution should fail but since the domain does not exist we cannot create
          // a tombstone to mark it as removed and correctly perform conflict resolution.
          throw new DomainDoesNotExistException(
              dataPath.toString(), domainName, getReadTableVersion());
        }
      }

      computedMetadatas = Optional.of(result);
      return result;
    }

    /** Sets the computed domain metadata list directly. Used during conflict resolution. */
    public void setComputedDomainMetadatas(List<DomainMetadata> updatedDomainMetadatas) {
      computedMetadatas = Optional.of(updatedDomainMetadatas);
    }

    /**
     * Returns the set of active domain metadata of the table, removed domain metadata are excluded.
     */
    public Optional<Set<DomainMetadata>> getPostCommitDomainMetadatas() {
      if (!readSnapshotOpt.isPresent()) {
        return Optional.of(
            getComputedDomainMetadatasToCommit().stream()
                .filter(dm -> !dm.isRemoved())
                .collect(Collectors.toSet()));
      }
      return currentCrcInfo
          .flatMap(CRCInfo::getDomainMetadata)
          .map(
              oldDomainMetadata -> {
                Map<String, DomainMetadata> domainMetadataMap =
                    oldDomainMetadata.stream()
                        .collect(Collectors.toMap(DomainMetadata::getDomain, Function.identity()));
                getComputedDomainMetadatasToCommit()
                    .forEach(
                        domainMetadata -> {
                          if (domainMetadata.isRemoved()) {
                            domainMetadataMap.remove(domainMetadata.getDomain());
                          } else {
                            domainMetadataMap.put(domainMetadata.getDomain(), domainMetadata);
                          }
                        });
                return new HashSet<>(domainMetadataMap.values());
              });
    }

    /**
     * Generate the domain metadata for the clustering columns if they are present in the
     * transaction.
     */
    private void generateClusteringDomainMetadataIfNeeded() {
      if (TableFeatures.isClusteringTableFeatureSupported(protocol)
          && newClusteringColumnsOpt.isPresent()) {
        DomainMetadata clusteringDomainMetadata =
            ClusteringUtils.getClusteringDomainMetadata(newClusteringColumnsOpt.get());
        addDomain(
            clusteringDomainMetadata.getDomain(), clusteringDomainMetadata.getConfiguration());
      } else if (TableFeatures.isClusteringTableFeatureSupported(protocol)
          && isReplaceTable()
          && !newClusteringColumnsOpt.isPresent()) {
        // When clustering is in the writer features we require there to be a clustering domain
        // metadata present; when the table is no longer a clustered table this means we must have
        // a domain metadata with clusteringColumns=[]
        DomainMetadata emptyClusteringDomainMetadata =
            ClusteringUtils.getClusteringDomainMetadata(Collections.emptyList());
        addDomain(
            emptyClusteringDomainMetadata.getDomain(),
            emptyClusteringDomainMetadata.getConfiguration());
      }
    }
  }
}
