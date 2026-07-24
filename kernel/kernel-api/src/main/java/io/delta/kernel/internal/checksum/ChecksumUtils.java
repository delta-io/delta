/*
 * Copyright (2025) The Delta Lake Project Authors.
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
package io.delta.kernel.internal.checksum;

import static io.delta.kernel.internal.actions.SingleAction.CHECKPOINT_SCHEMA;
import static io.delta.kernel.internal.util.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.TableConfig;
import io.delta.kernel.internal.actions.*;
import io.delta.kernel.internal.data.StructRow;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.replay.ActionWrapper;
import io.delta.kernel.internal.replay.ActionsIterator;
import io.delta.kernel.internal.replay.CreateCheckpointIterator;
import io.delta.kernel.internal.snapshot.LogSegment;
import io.delta.kernel.internal.stats.FileSizeHistogram;
import io.delta.kernel.internal.tablefeatures.TableFeatures;
import io.delta.kernel.internal.util.Clock;
import io.delta.kernel.internal.util.FileNames;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;
import java.io.IOException;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Utility methods for computing and writing checksums for Delta tables. */
public class ChecksumUtils {

  private ChecksumUtils() {}

  private static final Logger logger = LoggerFactory.getLogger(ChecksumUtils.class);

  private static final int PROTOCOL_INDEX = CHECKPOINT_SCHEMA.indexOf("protocol");
  private static final int METADATA_INDEX = CHECKPOINT_SCHEMA.indexOf("metaData");
  // TODO: simplify the schema to only read size from addFile.
  private static final int ADD_INDEX = CHECKPOINT_SCHEMA.indexOf("add");
  private static final int REMOVE_INDEX = CHECKPOINT_SCHEMA.indexOf("remove");
  private static final int DOMAIN_METADATA_INDEX = CHECKPOINT_SCHEMA.indexOf("domainMetadata");
  private static final int TXN_INDEX = CHECKPOINT_SCHEMA.indexOf("txn");
  private static final int ADD_SIZE_INDEX = AddFile.FULL_SCHEMA.indexOf("size");
  private static final int ADD_DV_INDEX = AddFile.FULL_SCHEMA.indexOf("deletionVector");
  private static final int REMOVE_SIZE_INDEX = RemoveFile.FULL_SCHEMA.indexOf("size");
  private static final int REMOVE_DV_INDEX = RemoveFile.FULL_SCHEMA.indexOf("deletionVector");
  private static final int DV_CARDINALITY_INDEX =
      DeletionVectorDescriptor.READ_SCHEMA.indexOf("cardinality");
  // commitInfo is appended after CHECKPOINT_SCHEMA in incremental read
  private static final int COMMIT_INFO_INDEX = CHECKPOINT_SCHEMA.length();

  /**
   * The live {@link SetTransaction} list ({@link CRCInfo#getSetTransactions()}) is stored in the
   * checksum only when the table has at most this many distinct appIds.
   */
  public static final long DEFAULT_SET_TRANSACTIONS_IN_CRC_THRESHOLD = 100L;

  private static final Set<String> INCREMENTAL_SUPPORTED_OPS =
      Collections.unmodifiableSet(
          new HashSet<>(
              Arrays.asList(
                  "WRITE",
                  "MERGE",
                  "UPDATE",
                  "DELETE",
                  "OPTIMIZE",
                  "CREATE TABLE",
                  "REPLACE TABLE",
                  "CREATE TABLE AS SELECT",
                  "REPLACE TABLE AS SELECT",
                  "CREATE OR REPLACE TABLE AS SELECT")));

  /**
   * Computes the state of a Delta table and writes a checksum file for the provided snapshot's
   * version. If a checksum file already exists for this version, this method returns without any
   * changes.
   *
   * <p>The checksum file contains table statistics including:
   *
   * <ul>
   *   <li>Total table size in bytes
   *   <li>Total number of files
   *   <li>File size histogram
   *   <li>Domain metadata information
   *   <li>Deletion-vector metrics
   *   <li>Add file list (optional)
   * </ul>
   *
   * <p>If a checksum file already exists for exactly this version, the existing {@link CRCInfo} is
   * returned as-is.
   *
   * <p>Note: For very large tables, this operation may be expensive as it requires scanning the
   * table state to compute statistics.
   *
   * @param engine The Engine instance used to access the underlying storage
   * @param logSegmentAtVersion The LogSegment instance of the table at a specific version
   * @throws IOException If an I/O error occurs during checksum computation or writing
   */
  public static void computeStateAndWriteChecksum(Engine engine, LogSegment logSegmentAtVersion)
      throws IOException {
    computeStateAndWriteChecksum(engine, logSegmentAtVersion, System::currentTimeMillis);
  }

  public static void computeStateAndWriteChecksum(
      Engine engine, LogSegment logSegmentAtVersion, Clock clock) throws IOException {
    requireNonNull(engine);
    requireNonNull(logSegmentAtVersion);

    // Check for existing checksum for this version
    Optional<Long> lastSeenCrcVersion =
        logSegmentAtVersion
            .getLastSeenChecksum()
            .map(file -> FileNames.getFileVersion(new Path(file.getPath())));

    if (lastSeenCrcVersion.isPresent()
        && lastSeenCrcVersion.get().equals(logSegmentAtVersion.getVersion())) {
      logger.info("Checksum file already exists for version {}", logSegmentAtVersion.getVersion());
      return;
    }

    CRCInfo crcInfo = computeChecksum(engine, logSegmentAtVersion, clock);
    ChecksumWriter checksumWriter = new ChecksumWriter(logSegmentAtVersion.getLogPath());
    checksumWriter.writeCheckSum(engine, crcInfo);
  }

  /**
   * Computes the state of a Delta table at the provided snapshot's version.
   *
   * <p>The checksum contains table statistics including:
   *
   * <ul>
   *   <li>Total table size in bytes
   *   <li>Total number of files
   *   <li>File size histogram
   *   <li>Domain metadata information
   *   <li>Deletion-vector metrics
   *   <li>Add file list (optional)
   * </ul>
   *
   * <p>Note: For very large tables, this operation may be expensive as it requires scanning the
   * table state to compute statistics.
   *
   * <p>The returned {@link CRCInfo} does not carry an in-commit timestamp, which is not derivable
   * from the file actions this replay reads. A caller that holds the commit's {@code CommitInfo}
   * can inject it via {@link CRCInfo#withInCommitTimestamp(Optional)}.
   *
   * <p>The full list of {@link CRCInfo#getAllFiles() allFiles} is collected when the table is
   * within {@link CRCInfo#DEFAULT_ALL_FILES_IN_CRC_THRESHOLD}; use {@link #computeChecksum(Engine,
   * LogSegment, boolean)} to disable that collection.
   *
   * @param engine The Engine instance used to access the underlying storage
   * @param logSegmentAtVersion The LogSegment instance of the table at a specific version
   * @return The computed CRC info for the table at the given version
   * @throws IOException If an I/O error occurs during checksum computation
   */
  public static CRCInfo computeChecksum(Engine engine, LogSegment logSegmentAtVersion)
      throws IOException {
    return computeChecksum(engine, logSegmentAtVersion, System::currentTimeMillis);
  }

  /**
   * Variant of {@link #computeChecksum(Engine, LogSegment)} that takes an explicit {@link Clock}.
   * The clock supplies "now" for the {@code delta.setTransactionRetentionDuration} cutoff applied
   * by the full-replay path; it is only consulted when that property is set.
   */
  public static CRCInfo computeChecksum(Engine engine, LogSegment logSegmentAtVersion, Clock clock)
      throws IOException {
    return computeChecksum(engine, logSegmentAtVersion, clock, /* collectAllFiles */ true);
  }

  /**
   * Computes the state of a Delta table at the provided snapshot's version, optionally collecting
   * the full {@link AddFile} list.
   */
  public static CRCInfo computeChecksum(
      Engine engine, LogSegment logSegmentAtVersion, boolean collectAllFiles) throws IOException {
    return computeChecksum(engine, logSegmentAtVersion, System::currentTimeMillis, collectAllFiles);
  }

  /**
   * Computes the state of a Delta table at the provided snapshot's version, taking both an explicit
   * {@link Clock} (for the {@code delta.setTransactionRetentionDuration} cutoff) and a flag
   * controlling whether the full {@link AddFile} list is collected.
   */
  public static CRCInfo computeChecksum(
      Engine engine, LogSegment logSegmentAtVersion, Clock clock, boolean collectAllFiles)
      throws IOException {
    requireNonNull(engine);
    requireNonNull(logSegmentAtVersion);

    Optional<CRCInfo> lastSeenCrcInfo =
        logSegmentAtVersion
            .getLastSeenChecksum()
            .flatMap(file -> ChecksumReader.tryReadChecksumFile(engine, file));
    if (lastSeenCrcInfo.isPresent()
        && lastSeenCrcInfo.get().getVersion() == logSegmentAtVersion.getVersion()) {
      return lastSeenCrcInfo.get();
    }
    // Try to build CRC incrementally if possible
    Optional<CRCInfo> incrementallyBuiltCrc =
        lastSeenCrcInfo.isPresent()
            ? buildCrcInfoIncrementally(
                lastSeenCrcInfo.get(),
                engine,
                logSegmentAtVersion,
                /* canBuildWithOnlyRequiredFields */ false,
                collectAllFiles)
            : Optional.empty();

    // Use incrementally built CRC if available, otherwise do full log replay
    return incrementallyBuiltCrc.isPresent()
        ? incrementallyBuiltCrc.get()
        : buildCrcInfoWithFullLogReplay(engine, logSegmentAtVersion, clock, collectAllFiles);
  }

  /**
   * Attempts to incrementally compute the CRC at the segment's version by replaying subsequent
   * delta files from the given base CRC. Returns {@link Optional#empty()} when the base CRC is
   * absent or the incremental build is not possible.
   *
   * @param engine The engine to use for file operations
   * @param logSegment The log segment to process
   * @param lastSeenCrcInfo The last available CRC info to build upon, typically from {@link
   *     io.delta.kernel.internal.replay.LogReplay#getLastSeenCrcInfo()}
   * @return The incrementally-built CRC info, or empty
   */
  public static Optional<CRCInfo> tryBuildCrcIncrementally(
      Engine engine, LogSegment logSegment, Optional<CRCInfo> lastSeenCrcInfo) throws IOException {
    return lastSeenCrcInfo.isPresent()
        ? buildCrcInfoIncrementally(
            lastSeenCrcInfo.get(),
            engine,
            logSegment,
            /* canBuildWithOnlyRequiredFields */ true,
            /* collectAllFiles */ true)
        : Optional.empty();
  }

  /**
   * Builds CRC info by replaying the full log.
   *
   * @param engine The engine instance
   * @param logSegmentAtVersion The log segment at the target version
   * @param collectAllFiles whether to collect the full {@link AddFile} list (up to the threshold)
   * @return The complete CRC info
   */
  private static CRCInfo buildCrcInfoWithFullLogReplay(
      Engine engine, LogSegment logSegmentAtVersion, Clock clock, boolean collectAllFiles)
      throws IOException {

    StateTracker state = new StateTracker();
    state.collectSetTransactions = true;
    // Make the full-replay path collect every appId so retention filtering runs first. This
    // prevents expired appIds from being counted towards the threshold.
    state.abandonAboveThreshold = false;
    // Collect the full AddFile list up to the threshold; abandoned automatically if exceeded.
    state.collectAllFiles = collectAllFiles;

    // Process logs and update state
    try (CreateCheckpointIterator checkpointIterator =
        new CreateCheckpointIterator(
            // Set minFileRetentionTimestampMillis to infinite future to skip all removed files
            engine, logSegmentAtVersion, Instant.ofEpochMilli(Long.MAX_VALUE).toEpochMilli())) {

      // Process all checkpoint batches
      while (checkpointIterator.hasNext()) {
        FilteredColumnarBatch filteredBatch = checkpointIterator.next();
        ColumnarBatch batch = filteredBatch.getData();
        Optional<ColumnVector> selectionVector = filteredBatch.getSelectionVector();
        final int rowCount = batch.getSize();

        ColumnVector metadataVector = batch.getColumnVector(METADATA_INDEX);
        ColumnVector protocolVector = batch.getColumnVector(PROTOCOL_INDEX);
        ColumnVector removeVector = batch.getColumnVector(REMOVE_INDEX);
        ColumnVector addVector = batch.getColumnVector(ADD_INDEX);
        ColumnVector domainMetadataVector = batch.getColumnVector(DOMAIN_METADATA_INDEX);
        ColumnVector txnVector = batch.getColumnVector(TXN_INDEX);
        // Process all selected rows in a single pass for optimal performance
        for (int i = 0; i < rowCount; i++) {
          // Fields referenced in the lambda should be effectively final.
          int rowId = i;
          boolean isSelected =
              selectionVector
                  .map(vec -> !vec.isNullAt(rowId) && vec.getBoolean(rowId))
                  .orElse(true);
          if (!isSelected) continue;
          // Step 1: Ensure there are no remove records
          // We set minFileRetentionTimestampMillis to infinite future to skip all removed files,
          // so there should be no remove actions.
          checkState(
              removeVector.isNullAt(i),
              "unexpected remove row found when "
                  + "setting minFileRetentionTimestampMillis to infinite future");
          // Step 2: Process add files, domain metadata, metadata, protocol, and transactions
          processAddRecord(addVector, state, i);
          processDomainMetadataRecord(domainMetadataVector, state, i);
          processMetadataRecord(metadataVector, state, i);
          processProtocolRecord(protocolVector, state, i);
          processTxnRecord(txnVector, state, i);
        }
      }
    }
    // Get final metadata and protocol
    Metadata finalMetadata =
        state.metadataFromLog.orElseThrow(() -> new IllegalStateException("No metadata found"));

    Protocol finalProtocol =
        state.protocolFromLog.orElseThrow(() -> new IllegalStateException("No protocol found"));

    // Filter to only non-removed domain metadata
    Set<DomainMetadata> finalDomainMetadata = getNonRemovedDomainMetadata(state);

    // Deletion-vector metrics, present only for DV-capable tables.
    DvMetrics dvMetrics = dvMetricsFor(finalProtocol, state, /* seedReliable = */ true);

    return new CRCInfo(
        logSegmentAtVersion.getVersion(),
        finalMetadata,
        finalProtocol,
        state.tableSizeByte.longValue(),
        state.fileCount.longValue(),
        Optional.empty(),
        Optional.of(finalDomainMetadata),
        Optional.of(state.addedFileSizeHistogram),
        Optional.empty() /* inCommitTimestamp */,
        filterAndBoundSetTransactions(
            state.collectedSetTransactions(), finalMetadata, clock, state.setTransactionsThreshold),
        dvMetrics.numDeletedRecords,
        dvMetrics.numDeletionVectors,
        state.collectedAllFiles());
  }

  /**
   * Attempts to build CRC info incrementally from the last seen checksum. Falls back if incremental
   * computation is not possible.
   *
   * @param lastSeenCrcInfo The last available CRC info to build upon
   * @param engine The engine to use for file operations
   * @param logSegment The log segment to process
   * @param canBuildWithOnlyRequiredFields If true, allows building CRC with only required fields.
   *     Tolerates missing optional fields.
   * @param collectAllFiles If true, maintain the full {@link AddFile} list ({@link
   *     CRCInfo#getAllFiles()}) when the base CRC carries it; if false, leave it absent.
   * @return Optional containing the new CRC info, or empty if fallback is needed
   */
  private static Optional<CRCInfo> buildCrcInfoIncrementally(
      CRCInfo lastSeenCrcInfo,
      Engine engine,
      LogSegment logSegment,
      boolean canBuildWithOnlyRequiredFields,
      boolean collectAllFiles)
      throws IOException {
    long startTime = System.currentTimeMillis();
    if (!canBuildWithOnlyRequiredFields && !lastSeenCrcInfo.getDomainMetadata().isPresent()) {
      logger.info(
          "Falling back to full replay after {}ms: detected current crc missing domain metadata.",
          System.currentTimeMillis() - startTime);
      return Optional.empty();
    }
    if (!canBuildWithOnlyRequiredFields && !lastSeenCrcInfo.getFileSizeHistogram().isPresent()) {
      logger.info(
          "Falling back to full replay after {}ms: "
              + "detected current crc missing file size histogram.",
          System.currentTimeMillis() - startTime);
      return Optional.empty();
    }
    // Deletion-vector metrics can only be folded forward from a trustworthy base. If the base
    // table is DV-capable but its CRC omitted the DV metrics, seeding from 0 would undercount the
    // records/vectors already present at the base version. Fall back to full replay in that case.
    boolean baseSupportsDv =
        lastSeenCrcInfo.getProtocol().supportsFeature(TableFeatures.DELETION_VECTORS_RW_FEATURE);
    boolean baseHasDvMetrics =
        lastSeenCrcInfo.getNumDeletionVectors().isPresent()
            && lastSeenCrcInfo.getNumDeletedRecords().isPresent();
    boolean dvSeedReliable = !baseSupportsDv || baseHasDvMetrics;
    if (!canBuildWithOnlyRequiredFields && !dvSeedReliable) {
      logger.info(
          "Falling back to full replay after {}ms: "
              + "base crc missing deletion-vector metrics on a DV-enabled table.",
          System.currentTimeMillis() - startTime);
      return Optional.empty();
    }

    // Initialize state tracking
    StateTracker state = new StateTracker();
    // Maintain setTransactions incrementally only when the base CRC carries the list.
    state.collectSetTransactions = lastSeenCrcInfo.getSetTransactions().isPresent();
    // Obtain deletion-vector metrics from the base CRC to allow incremental folding.
    lastSeenCrcInfo.getNumDeletionVectors().ifPresent(n -> state.numDeletionVectors.add(n));
    lastSeenCrcInfo.getNumDeletedRecords().ifPresent(n -> state.numDeletedRecords.add(n));
    // Incrementally update allFiles only when the base CRC already has it.
    if (collectAllFiles) {
      lastSeenCrcInfo
          .getAllFiles()
          .ifPresent(
              baseFiles -> {
                state.collectAllFiles = true;
                baseFiles.forEach(f -> state.addFilesByIdentity.put(FileIdentity.of(f), f));
              });
    }

    // TODO: use compacted logs.
    List<FileStatus> deltaFiles =
        logSegment.getDeltas().stream()
            .filter(
                file ->
                    FileNames.getFileVersion(new Path(file.getPath()))
                        > lastSeenCrcInfo.getVersion())
            .sorted(
                Comparator.comparingLong(
                    (FileStatus file) -> FileNames.getFileVersion(new Path(file.getPath()))))
            .collect(Collectors.toList());
    validateDeltaContinuity(deltaFiles, lastSeenCrcInfo.getVersion());
    Collections.reverse(deltaFiles);
    // Create iterator for delta files newer than last CRC
    StructType readSchema = CHECKPOINT_SCHEMA.add("commitInfo", CommitInfo.FULL_SCHEMA);
    try (CloseableIterator<ActionWrapper> iterator =
        new ActionsIterator(engine, deltaFiles, readSchema, java.util.Optional.empty())) {
      Optional<Long> lastSeenVersion = Optional.empty();
      while (iterator.hasNext()) {
        ActionWrapper currentAction = iterator.next();
        ColumnarBatch batch = currentAction.getColumnarBatch();
        final int rowCount = batch.getSize();
        if (rowCount == 0) {
          continue;
        }
        ColumnVector addVector = batch.getColumnVector(ADD_INDEX);
        ColumnVector removeVector = batch.getColumnVector(REMOVE_INDEX);
        ColumnVector metadataVector = batch.getColumnVector(METADATA_INDEX);
        ColumnVector protocolVector = batch.getColumnVector(PROTOCOL_INDEX);
        ColumnVector domainMetadataVector = batch.getColumnVector(DOMAIN_METADATA_INDEX);
        ColumnVector txnVector = batch.getColumnVector(TXN_INDEX);
        ColumnVector commitInfoVector = batch.getColumnVector(COMMIT_INFO_INDEX);

        for (int i = 0; i < rowCount; i++) {
          long newVersion = currentAction.getVersion();

          // Detect version change
          if (!lastSeenVersion.isPresent() || newVersion != lastSeenVersion.get()) {
            // New version detected - current row must be commit info
            if (commitInfoVector.isNullAt(i)) {
              logger.info(
                  "Falling back to full replay: first row of version {} is not commit info",
                  newVersion);
              return Optional.empty();
            }

            CommitInfo commitInfo = CommitInfo.fromColumnVector(commitInfoVector, i);
            if (commitInfo == null
                || !commitInfo
                    .getOperation()
                    .filter(INCREMENTAL_SUPPORTED_OPS::contains)
                    .isPresent()) {
              logger.info(
                  "Falling back to full replay after {}ms: "
                      + "unsupported operation '{}' for version {}",
                  System.currentTimeMillis() - startTime,
                  commitInfo != null ? commitInfo.getOperation().orElse("null") : "null",
                  newVersion);
              return Optional.empty();
            }

            lastSeenVersion = Optional.of(newVersion);
            continue;
          }

          // Process the row
          if (!addVector.isNullAt(i)) {
            processAddRecord(addVector, state, i);
          }

          // Process remove file records
          if (!removeVector.isNullAt(i)) {
            ColumnVector sizeVector = removeVector.getChild(REMOVE_SIZE_INDEX);
            if (sizeVector.isNullAt(i)) {
              logger.info(
                  "Falling back to full replay after {}ms: "
                      + "detected remove without file size in version {}",
                  System.currentTimeMillis() - startTime,
                  newVersion);
              return Optional.empty();
            }
            long fileSize = sizeVector.getLong(i);
            state.tableSizeByte.add(-fileSize);
            state.removedFileSizeHistogram.insert(fileSize);
            state.fileCount.decrement();
            // Subtract the removed file's deletion-vector metrics.
            applyDeletionVectorMetrics(removeVector, REMOVE_DV_INDEX, state, i, DvDelta.REMOVE);

            if (state.collectAllFiles) {
              RemoveFile removeFile = new RemoveFile(StructRow.fromStructVector(removeVector, i));
              state.removeAddFile(
                  FileIdentity.of(removeFile.getPath(), removeFile.getDeletionVector()));
            }
          }

          // Process domain metadata, protocol, metadata, and transactions
          processDomainMetadataRecord(domainMetadataVector, state, i);
          processMetadataRecord(metadataVector, state, i);
          processProtocolRecord(protocolVector, state, i);
          processTxnRecord(txnVector, state, i);
        }
      }
    }

    lastSeenCrcInfo
        .getSetTransactions()
        .ifPresent(txns -> txns.forEach(state::recordSetTransaction));

    // Merge with existing domain metadata if available
    Optional<Set<DomainMetadata>> finalDomainMetadata;
    if (lastSeenCrcInfo.getDomainMetadata().isPresent()) {
      lastSeenCrcInfo
          .getDomainMetadata()
          .get()
          .forEach(
              dm -> {
                if (!state.domainMetadataMap.containsKey(dm.getDomain())) {
                  state.domainMetadataMap.put(dm.getDomain(), dm);
                }
              });
      finalDomainMetadata = Optional.of(getNonRemovedDomainMetadata(state));
    } else {
      finalDomainMetadata = Optional.empty();
    }
    logger.info(
        "Successfully completed incremental CRC computation in {} ms",
        System.currentTimeMillis() - startTime);
    Optional<FileSizeHistogram> finalHistogram =
        lastSeenCrcInfo
            .getFileSizeHistogram()
            .map(h -> state.addedFileSizeHistogram.plus(h).minus(state.removedFileSizeHistogram));

    Metadata finalMetadata = state.metadataFromLog.orElseGet(lastSeenCrcInfo::getMetadata);
    Protocol finalProtocol = state.protocolFromLog.orElseGet(lastSeenCrcInfo::getProtocol);

    // Omit setTransactions on incremental path since it cannot maintain the wall-clock retention
    // filter deterministically
    Optional<List<SetTransaction>> incrementalSetTransactions =
        isSetTransactionRetentionConfigured(finalMetadata)
            ? Optional.empty()
            : state.collectedSetTransactions();
    // Omit DV stats if the seed is unreliable (base CRC lacked DV metrics on a DV-enabled table).
    DvMetrics finalDvMetrics = dvMetricsFor(finalProtocol, state, dvSeedReliable);

    long finalNumFiles = state.fileCount.longValue() + lastSeenCrcInfo.getNumFiles();
    // Only keep the incrementally-maintained allFiles if it stayed consistent with the computed
    // file count. Else drop it.
    Optional<List<AddFile>> finalAllFiles =
        state.collectedAllFiles().filter(files -> files.size() == finalNumFiles);

    // Build and return the new CRC info
    return Optional.of(
        new CRCInfo(
            logSegment.getVersion(),
            finalMetadata,
            finalProtocol,
            state.tableSizeByte.longValue() + lastSeenCrcInfo.getTableSizeBytes(),
            finalNumFiles,
            Optional.empty(),
            finalDomainMetadata,
            finalHistogram,
            Optional.empty() /* inCommitTimestamp */,
            incrementalSetTransactions,
            finalDvMetrics.numDeletedRecords,
            finalDvMetrics.numDeletionVectors,
            finalAllFiles));
  }

  /** Processes an add file record and updates the state tracker. */
  private static void processAddRecord(ColumnVector addVector, StateTracker state, int rowId) {
    if (!addVector.isNullAt(rowId)) {
      ColumnVector sizeVector = addVector.getChild(ADD_SIZE_INDEX);
      checkState(!sizeVector.isNullAt(rowId), "Add record has null file size");

      long fileSize = sizeVector.getLong(rowId);
      checkState(fileSize >= 0, "Add record has negative file size: " + fileSize);

      state.tableSizeByte.add(fileSize);
      state.addedFileSizeHistogram.insert(fileSize);
      state.fileCount.increment();
      applyDeletionVectorMetrics(addVector, ADD_DV_INDEX, state, rowId, DvDelta.ADD);

      if (state.collectAllFiles) {
        state.recordAddFile(new AddFile(StructRow.fromStructVector(addVector, rowId)));
      }
    }
  }

  /** Processes a domain metadata record and updates the state tracker. */
  private static void processDomainMetadataRecord(
      ColumnVector domainMetadataVector, StateTracker state, int rowId) {
    if (!domainMetadataVector.isNullAt(rowId)) {
      DomainMetadata domainMetadata = DomainMetadata.fromColumnVector(domainMetadataVector, rowId);
      if (!state.domainMetadataMap.containsKey(domainMetadata.getDomain())) {
        state.domainMetadataMap.put(domainMetadata.getDomain(), domainMetadata);
      }
    }
  }

  /** Processes a SetTransaction (txn) record and updates the state tracker. */
  private static void processTxnRecord(ColumnVector txnVector, StateTracker state, int rowId) {
    if (state.collectSetTransactions && !txnVector.isNullAt(rowId)) {
      state.recordSetTransaction(SetTransaction.fromColumnVector(txnVector, rowId));
    }
  }

  private static boolean isSetTransactionRetentionConfigured(Metadata metadata) {
    return TableConfig.SET_TRANSACTION_RETENTION.fromMetadata(metadata).isPresent();
  }

  /**
   * Finalizes the setTransactions gathered by a full log replay: first drops expired transactions
   * per {@code delta.setTransactionRetentionDuration}, then bounds the surviving (live) set by the
   * {@code .crc}-size threshold.
   *
   * <p>Retention is applied <em>before</em> the threshold check so that expired appIds do not
   * consume the threshold budget.
   *
   * @param collected the setTransactions gathered during a full replay
   * @param metadata the effective table metadata at the checksum version
   * @param clock the clock supplying "now" for the retention cutoff
   * @param threshold the maximum number of live appIds storable in the {@code .crc}
   * @return the retention-filtered setTransactions, or empty if the live set exceeds the threshold
   */
  private static Optional<List<SetTransaction>> filterAndBoundSetTransactions(
      Optional<List<SetTransaction>> collected, Metadata metadata, Clock clock, long threshold) {
    if (!collected.isPresent()) {
      return collected;
    }
    List<SetTransaction> live = collected.get();
    Optional<Long> retentionMillis = TableConfig.SET_TRANSACTION_RETENTION.fromMetadata(metadata);
    if (retentionMillis.isPresent()) {
      long cutoff = clock.getTimeMillis() - retentionMillis.get();
      live =
          collected.get().stream()
              .filter(txn -> txn.getLastUpdated().map(updated -> updated > cutoff).orElse(false))
              .collect(Collectors.toList());
    }

    if (live.size() > threshold) {
      return Optional.empty();
    }
    return Optional.of(live);
  }

  /** Processes a metadata record and updates the state tracker. */
  private static void processMetadataRecord(
      ColumnVector metadataVector, StateTracker state, int rowId) {
    if (!metadataVector.isNullAt(rowId) && !state.metadataFromLog.isPresent()) {
      Metadata metadata = Metadata.fromColumnVector(metadataVector, rowId);
      checkState(metadata != null, "Metadata is null");
      state.metadataFromLog = Optional.of(metadata);
    }
  }

  /** Processes a protocol record and updates the state tracker. */
  private static void processProtocolRecord(
      ColumnVector protocolVector, StateTracker state, int rowId) {
    if (!protocolVector.isNullAt(rowId) && !state.protocolFromLog.isPresent()) {
      Protocol protocol = Protocol.fromColumnVector(protocolVector, rowId);
      checkState(protocol != null, "Protocol is null");
      state.protocolFromLog = Optional.of(protocol);
    }
  }

  /** Get non-removed domain metadata. */
  private static Set<DomainMetadata> getNonRemovedDomainMetadata(StateTracker state) {
    return state.domainMetadataMap.values().stream()
        .filter(dm -> !dm.isRemoved())
        .collect(Collectors.toSet());
  }

  /** Class for tracking state during log processing. */
  private static class StateTracker {
    Optional<Metadata> metadataFromLog = Optional.empty();
    Optional<Protocol> protocolFromLog = Optional.empty();
    LongAdder tableSizeByte = new LongAdder();
    LongAdder fileCount = new LongAdder();
    FileSizeHistogram addedFileSizeHistogram = FileSizeHistogram.createDefaultHistogram();
    FileSizeHistogram removedFileSizeHistogram = FileSizeHistogram.createDefaultHistogram();
    Map<String, DomainMetadata> domainMetadataMap = new HashMap<>();
    final Map<String, SetTransaction> setTransactionsByAppId = new LinkedHashMap<>();
    boolean collectSetTransactions = false;
    long setTransactionsThreshold = DEFAULT_SET_TRANSACTIONS_IN_CRC_THRESHOLD;
    // When true, abandon collection once the distinct-appId count exceeds the threshold. The
    // full-replay path disables this so retention filtering runs before the threshold check.
    boolean abandonAboveThreshold = true;

    // Deletion-vector metrics 2 fields.
    LongAdder numDeletionVectors = new LongAdder();
    LongAdder numDeletedRecords = new LongAdder();
    // AllFiles keyed on (path, dvId)
    final Map<FileIdentity, AddFile> addFilesByIdentity = new LinkedHashMap<>();
    // Identities whose action has already been applied during this computation. The incremental
    // path replays the delta range newest-commit-first, so the FIRST action seen for an identity
    // is authoritative and any older add/remove for the same identity must be ignored -- otherwise
    // a file added and removed within the range (in either order) is mis-resolved, since a plain
    // map put/remove carries no happens-before info under reverse iteration. In the full-replay
    // path each live identity is seen once, so this guard is a no-op there.
    final Set<FileIdentity> seenIdentities = new HashSet<>();
    boolean collectAllFiles = false;
    long allFilesThreshold = CRCInfo.DEFAULT_ALL_FILES_IN_CRC_THRESHOLD;

    /** Records a SetTransaction if collecting and its appId is not already seen. */
    void recordSetTransaction(SetTransaction txn) {
      if (!collectSetTransactions) {
        return;
      }
      if (!setTransactionsByAppId.containsKey(txn.getAppId())) {
        setTransactionsByAppId.put(txn.getAppId(), txn);
        if (abandonAboveThreshold && setTransactionsByAppId.size() > setTransactionsThreshold) {
          collectSetTransactions = false;
          setTransactionsByAppId.clear();
        }
      }
    }

    /** The collected setTransactions, or empty if collection was disabled or exceeded threshold. */
    Optional<List<SetTransaction>> collectedSetTransactions() {
      return collectSetTransactions
          ? Optional.of(new ArrayList<>(setTransactionsByAppId.values()))
          : Optional.empty();
    }

    /** Records a live AddFile if collecting, unless a newer action for its identity was seen. */
    void recordAddFile(AddFile addFile) {
      if (!collectAllFiles) {
        return;
      }
      FileIdentity identity = FileIdentity.of(addFile);
      // Newest-first: skip if this identity was already resolved by a later commit's action.
      if (!seenIdentities.add(identity)) {
        return;
      }
      addFilesByIdentity.put(identity, addFile);
      if (addFilesByIdentity.size() > allFilesThreshold) {
        collectAllFiles = false;
        addFilesByIdentity.clear();
        seenIdentities.clear();
      }
    }

    /** Removes a live AddFile by (path, dv) identity if collecting. */
    void removeAddFile(FileIdentity identity) {
      if (!collectAllFiles) {
        return;
      }
      // Newest-first: a remove that was superseded by a later add for the same identity (already
      // seen) must not delete that add's entry.
      if (!seenIdentities.add(identity)) {
        return;
      }
      addFilesByIdentity.remove(identity);
    }

    /** The collected allFiles list, or empty if collection was disabled or exceeded threshold. */
    Optional<List<AddFile>> collectedAllFiles() {
      return collectAllFiles
          ? Optional.of(new ArrayList<>(addFilesByIdentity.values()))
          : Optional.empty();
    }
  }

  /**
   * Identity of a file action for allFiles bookkeeping: its path together with its deletion-vector
   * unique id (empty when the file has no DV). Two actions match iff both agree, so an
   * add-with-new-DV and a remove-of-the-old-file for the same path are distinct entries.
   *
   * <p>TODO: Refactor to Record type if Java 16
   */
  private static final class FileIdentity {
    private final String path;
    private final Optional<String> dvId;

    private FileIdentity(String path, Optional<String> dvId) {
      this.path = path;
      this.dvId = dvId;
    }

    static FileIdentity of(AddFile addFile) {
      return new FileIdentity(
          addFile.getPath(),
          addFile.getDeletionVector().map(DeletionVectorDescriptor::getUniqueId));
    }

    static FileIdentity of(String path, Optional<DeletionVectorDescriptor> dv) {
      return new FileIdentity(path, dv.map(DeletionVectorDescriptor::getUniqueId));
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof FileIdentity)) {
        return false;
      }
      FileIdentity other = (FileIdentity) o;
      return path.equals(other.path) && dvId.equals(other.dvId);
    }

    @Override
    public int hashCode() {
      return Objects.hash(path, dvId);
    }
  }

  /**
   * Whether a file action contributes its deletion-vector metrics to the running state (an AddFile)
   * or retracts them (a RemoveFile).
   */
  private enum DvDelta {
    ADD(1),
    REMOVE(-1);

    final int sign;

    DvDelta(int sign) {
      this.sign = sign;
    }
  }

  /** Adds (ADD) or removes (REMOVE) an action's deletion-vector metrics from the state. */
  private static void applyDeletionVectorMetrics(
      ColumnVector fileVector, int dvChildIndex, StateTracker state, int rowId, DvDelta delta) {
    ColumnVector dvVector = fileVector.getChild(dvChildIndex);
    if (dvVector.isNullAt(rowId)) {
      return;
    }
    long cardinality = dvVector.getChild(DV_CARDINALITY_INDEX).getLong(rowId);
    state.numDeletionVectors.add(delta.sign);
    state.numDeletedRecords.add(delta.sign * cardinality);
  }

  /**
   * The deletion-vector metrics to store. Present ONLY when the resolved protocol supports the
   * deletion-vectors table feature AND the accumulated state is trustworthy.
   */
  private static DvMetrics dvMetricsFor(
      Protocol protocol, StateTracker state, boolean seedReliable) {
    boolean dvSupported = protocol.supportsFeature(TableFeatures.DELETION_VECTORS_RW_FEATURE);
    if (dvSupported && seedReliable) {
      return new DvMetrics(
          Optional.of(state.numDeletedRecords.longValue()),
          Optional.of(state.numDeletionVectors.longValue()));
    }
    return new DvMetrics(Optional.empty(), Optional.empty());
  }

  /** The pair of deletion-vector metrics stored in a {@link CRCInfo}. */
  private static class DvMetrics {
    final Optional<Long> numDeletedRecords;
    final Optional<Long> numDeletionVectors;

    DvMetrics(Optional<Long> numDeletedRecords, Optional<Long> numDeletionVectors) {
      this.numDeletedRecords = numDeletedRecords;
      this.numDeletionVectors = numDeletionVectors;
    }
  }

  private static void validateDeltaContinuity(List<FileStatus> deltas, long checksumVersion) {
    if (deltas.isEmpty()) {
      return;
    }

    long expectedVersion = checksumVersion + 1;
    for (FileStatus delta : deltas) {
      long version = FileNames.getFileVersion(new Path(delta.getPath()));
      if (version != expectedVersion) {
        throw new IllegalStateException(
            String.format(
                "Gap detected in delta files: expected version %d, found %d",
                expectedVersion, version));
      }
      expectedVersion++;
    }
  }
}
