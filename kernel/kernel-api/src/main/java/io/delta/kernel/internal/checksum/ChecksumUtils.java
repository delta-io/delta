/*
 * Copyright (2024) The Delta Lake Project Authors.
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
import static io.delta.kernel.internal.replay.LogReplayUtils.getUniqueFileAction;
import static io.delta.kernel.internal.util.Preconditions.checkState;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.actions.*;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.replay.CreateCheckpointIterator;
import io.delta.kernel.internal.replay.LogReplayUtils;
import io.delta.kernel.internal.snapshot.LogSegment;
import io.delta.kernel.internal.stats.FileSizeHistogram;
import io.delta.kernel.internal.util.FileNames;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
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
  private static final int ADD_INDEX = CHECKPOINT_SCHEMA.indexOf("add");
  private static final int REMOVE_INDEX = CHECKPOINT_SCHEMA.indexOf("remove");
  private static final int DOMAIN_METADATA_INDEX = CHECKPOINT_SCHEMA.indexOf("domainMetadata");
  private static final int ADD_SIZE_INDEX = AddFile.FULL_SCHEMA.indexOf("size");
  private static final int REMOVE_SIZE_INDEX = RemoveFile.FULL_SCHEMA.indexOf("size");
  private static final int ADD_PATH_INDEX = AddFile.FULL_SCHEMA.indexOf("path");
  private static final int ADD_DV_INDEX = AddFile.FULL_SCHEMA.indexOf("deletionVector");

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
    if (engine == null || logSegmentAtVersion == null) {
      throw new IllegalArgumentException("Engine and logSegmentAtVersion cannot be null");
    }

    // Check for existing checksum for this version
    Optional<CRCInfo> lastSeenCrcInfo =
        logSegmentAtVersion
            .getLastSeenChecksum()
            .flatMap(file -> ChecksumReader.getCRCInfo(engine, file));

    Optional<Long> checksumVersion = lastSeenCrcInfo.map(CRCInfo::getVersion);
    if (checksumVersion.isPresent()
        && checksumVersion.get().equals(logSegmentAtVersion.getVersion())) {
      logger.info("Checksum file already exists for version {}", logSegmentAtVersion.getVersion());
      return;
    }

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
    try {
      checksumWriter.writeCheckSum(engine, crcInfo);
      logger.info(
          "Successfully wrote checksum file for version {}", logSegmentAtVersion.getVersion());
    } catch (FileAlreadyExistsException e) {
      logger.info("Checksum file already exists for version {}", logSegmentAtVersion.getVersion());
      // Checksum file has been created while we were computing it.
      // This is fine - the checksum now exists, which was our goal.
    }
  }

  private static Optional<CRCInfo> buildCrcInfoIncrementally(
      CRCInfo lastSeenCrcInfo, Engine engine, LogSegment logSegment) throws IOException {
    // Can only build incrementally if we have domain metadata and file size histogram
    if (!lastSeenCrcInfo.getDomainMetadata().isPresent()
        || !lastSeenCrcInfo.getFileSizeHistogram().isPresent()) {
      return Optional.empty();
    }

    // Initialize state tracking
    StateTracker state = new StateTracker();
    final Set<LogReplayUtils.UniqueFileActionTuple> addFilesFromJson = new HashSet<>();

    // Create a CreateCheckpointIterator with only delta file
    try (CreateCheckpointIterator checkpointIterator =
        new CreateCheckpointIterator(
            engine,
            new LogSegment(
                logSegment.getLogPath(),
                logSegment.getVersion(),
                logSegment.getDeltas().stream()
                    .filter(
                        file ->
                            FileNames.getFileVersion(new Path(file.getPath()))
                                > lastSeenCrcInfo.getVersion())
                    .collect(Collectors.toList()),
                new ArrayList<>(),
                new ArrayList<>(),
                Optional.empty(),
                logSegment.getLastCommitTimestamp()),
            0L)) {
      while (checkpointIterator.hasNext()) {
        FilteredColumnarBatch filteredColumnarBatch = checkpointIterator.next();
        ColumnarBatch batch = filteredColumnarBatch.getData();
        Optional<ColumnVector> selectionVector = filteredColumnarBatch.getSelectionVector();

        ColumnVector metadataVector = batch.getColumnVector(METADATA_INDEX);
        ColumnVector protocolVector = batch.getColumnVector(PROTOCOL_INDEX);
        ColumnVector removeVector = batch.getColumnVector(REMOVE_INDEX);
        ColumnVector addVector = batch.getColumnVector(ADD_INDEX);
        ColumnVector domainMetadataVector = batch.getColumnVector(DOMAIN_METADATA_INDEX);
        final int rowCount = batch.getSize();

        // Process all selected rows in a single pass for optimal performance
        for (int i = 0; i < rowCount; i++) {
          // Process add file records
          processAddRecord(addVector, state, Optional.of(addFilesFromJson), i);

          // Process remove file records
          if (!removeVector.isNullAt(i)) {
            ColumnVector sizeVector = removeVector.getChild(REMOVE_SIZE_INDEX);
            // Cannot determine if a remove file missing stats came before or after crc
            if (sizeVector.isNullAt(i)) {
              throw new IllegalStateException("Remove file missing size information");
            }
            long fileSize = sizeVector.getLong(i);
            state.tableSizeByte.add(-fileSize);
            state.removedFileSizeHistogram.insert(fileSize);
            state.fileCount.decrement();
          }

          int rowId = i;
          boolean isSelected =
              selectionVector
                  .map(vec -> !vec.isNullAt(rowId) && vec.getBoolean(rowId))
                  .orElse(true);
          if (!isSelected) continue;

          // Process domain metadata, protocol, and metadata
          processDomainMetadataRecord(domainMetadataVector, state, i);
          processMetadataRecord(metadataVector, state, i);
          processProtocolRecord(protocolVector, state, i);
        }
      }
    }

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

  /**
   * Builds CRC info by replaying the full log.
   *
   * @param engine The engine instance
   * @param logSegmentAtVersion The log segment at the target version
   * @return The complete CRC info
   */
  private static CRCInfo buildCrcInfoWithFullLogReplay(
      Engine engine, LogSegment logSegmentAtVersion) throws IOException {

    // Initialize state tracking
    StateTracker state = new StateTracker();

    // Process logs and update state
    try (CreateCheckpointIterator checkpointIterator =
        new CreateCheckpointIterator(
            engine, logSegmentAtVersion, Instant.ofEpochMilli(Long.MAX_VALUE).toEpochMilli())) {

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
          // Fields referenced in the lambda should be effectively final
          int rowId = i;
          boolean isSelected =
              selectionVector
                  .map(vec -> !vec.isNullAt(rowId) && vec.getBoolean(rowId))
                  .orElse(true);
          if (!isSelected) continue;

          // Ensure there are no remove records (we set minFileRetentionTimestampMillis to infinite
          // future)
          checkState(
              removeVector.isNullAt(i),
              "unexpected remove row found when "
                  + "setting minFileRetentionTimestampMillis to infinite future");

          // Process add files, domain metadata, metadata, and protocol
          processAddRecord(addVector, state, Optional.empty(), i);
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

  /** Process an add file record. */
  private static void processAddRecord(
      ColumnVector addVector,
      StateTracker state,
      Optional<Set<LogReplayUtils.UniqueFileActionTuple>> addFilesFromJson,
      int rowId) {

    if (!addVector.isNullAt(rowId)) {
      if (addFilesFromJson.isPresent()) {
        final LogReplayUtils.UniqueFileActionTuple key =
            getUniqueFileAction(
                addVector.getChild(ADD_PATH_INDEX), addVector.getChild(ADD_DV_INDEX), rowId);
        if (addFilesFromJson.get().contains(key)) {
          return;
        }
        addFilesFromJson.get().add(key);
      }

      // Get file size and update tracking information
      ColumnVector sizeVector = addVector.getChild(ADD_SIZE_INDEX);
      long fileSize = sizeVector.getLong(rowId);
      state.tableSizeByte.add(fileSize);
      state.addedFileSizeHistogram.insert(fileSize);
      state.fileCount.increment();
    }
  }

  /** Process a domain metadata record. */
  private static void processDomainMetadataRecord(
      ColumnVector domainMetadataVector, StateTracker state, int rowId) {
    if (!domainMetadataVector.isNullAt(rowId)) {
      DomainMetadata domainMetadata = DomainMetadata.fromColumnVector(domainMetadataVector, rowId);
      checkState(
          !state.domainMetadataMap.containsKey(domainMetadata.getDomain()),
          "unexpected duplicate domain metadata rows");
      // CreateCheckpointIterator will ensure only the last entry of domain metadata
      // got emit.
      state.domainMetadataMap.put(domainMetadata.getDomain(), domainMetadata);
    }
  }

  /** Process a metadata record. */
  private static void processMetadataRecord(
      ColumnVector metadataVector, StateTracker state, int rowId) {
    if (!metadataVector.isNullAt(rowId)) {
      checkState(!state.metadataFromLog.isPresent(), "unexpected duplicate selected metadata rows");
      Metadata metadata = Metadata.fromColumnVector(metadataVector, rowId);
      state.metadataFromLog = Optional.of(metadata);
    }
  }

  /** Process a protocol record. */
  private static void processProtocolRecord(
      ColumnVector protocolVector, StateTracker state, int rowId) {
    if (!protocolVector.isNullAt(rowId)) {
      checkState(!state.protocolFromLog.isPresent(), "unexpected duplicate selected protocol rows");
      Protocol protocol = Protocol.fromColumnVector(protocolVector, rowId);
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
}
