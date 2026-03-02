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
import io.delta.kernel.internal.actions.*;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.replay.ActionWrapper;
import io.delta.kernel.internal.replay.ActionsIterator;
import io.delta.kernel.internal.replay.CreateCheckpointIterator;
import io.delta.kernel.internal.snapshot.LogSegment;
import io.delta.kernel.internal.stats.FileSizeHistogram;
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
  private static final int ADD_SIZE_INDEX = AddFile.FULL_SCHEMA.indexOf("size");
  private static final int REMOVE_SIZE_INDEX = RemoveFile.FULL_SCHEMA.indexOf("size");
  // commitInfo is appended after CHECKPOINT_SCHEMA in incremental read
  private static final int COMMIT_INFO_INDEX = CHECKPOINT_SCHEMA.length();

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
   * </ul>
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

    Optional<CRCInfo> lastSeenCrcInfo =
        logSegmentAtVersion
            .getLastSeenChecksum()
            .flatMap(file -> ChecksumReader.tryReadChecksumFile(engine, file));
    // Try to build CRC incrementally if possible
    Optional<CRCInfo> incrementallyBuiltCrc =
        lastSeenCrcInfo.isPresent()
            ? buildCrcInfoIncrementally(lastSeenCrcInfo.get(), engine, logSegmentAtVersion)
            : Optional.empty();

    // Use incrementally built CRC if available, otherwise do full log replay
    CRCInfo crcInfo =
        incrementallyBuiltCrc.isPresent()
            ? incrementallyBuiltCrc.get()
            : buildCrcInfoWithFullLogReplay(engine, logSegmentAtVersion);

    ChecksumWriter checksumWriter = new ChecksumWriter(logSegmentAtVersion.getLogPath());
    checksumWriter.writeCheckSum(engine, crcInfo);
  }

  /**
   * Builds CRC info by replaying the full log.
   *
   * @param engine The engine instance
   * @param logSegmentAtVersion The log segment at the target version
   * @return The complete CRC info
   */
  private static CRCInfo buildCrcInfoWithFullLogReplay(
      Engine engine, LogSegment logSegmentAtVersion) throws IOException {

    StateTracker state = new StateTracker();

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
          // Step 2: Process add files, domain metadata, metadata, and protocol
          processAddRecord(addVector, state, i);
          processDomainMetadataRecord(domainMetadataVector, state, i);
          processMetadataRecord(metadataVector, state, i);
          processProtocolRecord(protocolVector, state, i);
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

    return new CRCInfo(
        logSegmentAtVersion.getVersion(),
        finalMetadata,
        finalProtocol,
        state.tableSizeByte.longValue(),
        state.fileCount.longValue(),
        Optional.empty(),
        Optional.of(finalDomainMetadata),
        Optional.of(state.addedFileSizeHistogram));
  }

  /**
   * Attempts to build CRC info incrementally from the last seen checksum. Falls back if incremental
   * computation is not possible.
   *
   * @param lastSeenCrcInfo The last available CRC info to build upon
   * @param engine The engine to use for file operations
   * @param logSegment The log segment to process
   * @return Optional containing the new CRC info, or empty if fallback is needed
   */
  private static Optional<CRCInfo> buildCrcInfoIncrementally(
      CRCInfo lastSeenCrcInfo, Engine engine, LogSegment logSegment) throws IOException {
    long startTime = System.currentTimeMillis();
    // Can only build incrementally if we have domain metadata and file size histogram
    if (!lastSeenCrcInfo.getDomainMetadata().isPresent()) {
      logger.info(
          "Falling back to full replay after {}ms: detected current crc missing domain metadata.",
          System.currentTimeMillis() - startTime);
      return Optional.empty();
    }
    if (!lastSeenCrcInfo.getFileSizeHistogram().isPresent()) {
      logger.info(
          "Falling back to full replay after {}ms: "
              + "detected current crc missing file size histogram.",
          System.currentTimeMillis() - startTime);
      return Optional.empty();
    }

    // Initialize state tracking
    StateTracker state = new StateTracker();

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
          }

          // Process domain metadata, protocol, and metadata
          processDomainMetadataRecord(domainMetadataVector, state, i);
          processMetadataRecord(metadataVector, state, i);
          processProtocolRecord(protocolVector, state, i);
        }
      }
    }

    // Merge with existing domain metadata
    lastSeenCrcInfo
        .getDomainMetadata()
        .get()
        .forEach(
            dm -> {
              if (!state.domainMetadataMap.containsKey(dm.getDomain())) {
                state.domainMetadataMap.put(dm.getDomain(), dm);
              }
            });

    // Filter to only non-removed domain metadata
    Set<DomainMetadata> finalDomainMetadata = getNonRemovedDomainMetadata(state);
    logger.info(
        "Successfully completed incremental CRC computation in {} ms",
        System.currentTimeMillis() - startTime);
    // Build and return the new CRC info
    return Optional.of(
        new CRCInfo(
            logSegment.getVersion(),
            state.metadataFromLog.orElseGet(lastSeenCrcInfo::getMetadata),
            state.protocolFromLog.orElseGet(lastSeenCrcInfo::getProtocol),
            state.tableSizeByte.longValue() + lastSeenCrcInfo.getTableSizeBytes(),
            state.fileCount.longValue() + lastSeenCrcInfo.getNumFiles(),
            Optional.empty(),
            Optional.of(finalDomainMetadata),
            Optional.of(
                state
                    .addedFileSizeHistogram
                    .plus(lastSeenCrcInfo.getFileSizeHistogram().get())
                    .minus(state.removedFileSizeHistogram))));
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
