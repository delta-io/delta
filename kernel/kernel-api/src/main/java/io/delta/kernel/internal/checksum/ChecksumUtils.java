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
import static io.delta.kernel.internal.util.Preconditions.checkArgument;
import static io.delta.kernel.internal.util.Preconditions.checkState;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.actions.*;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.replay.CreateCheckpointIterator;
import io.delta.kernel.internal.snapshot.LogSegment;
import io.delta.kernel.internal.stats.FileSizeHistogram;
import io.delta.kernel.internal.util.FileNames;
import io.delta.kernel.utils.FileStatus;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ChecksumUtils {

  private ChecksumUtils() {}

  private static final Logger logger = LoggerFactory.getLogger(ChecksumUtils.class);

  private static final int PROTOCOL_INDEX = CHECKPOINT_SCHEMA.indexOf("protocol");
  private static final int METADATA_INDEX = CHECKPOINT_SCHEMA.indexOf("metaData");
  private static final int ADD_INDEX = CHECKPOINT_SCHEMA.indexOf("add");
  private static final int REMOVE_INDEX = CHECKPOINT_SCHEMA.indexOf("remove");
  private static final int DOMAIN_METADATA_INDEX = CHECKPOINT_SCHEMA.indexOf("domainMetadata");
  private static final int ADD_SIZE_INDEX = AddFile.FULL_SCHEMA.indexOf("size");

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

    // Step 1: Try to get the last checksum information
    Optional<CRCInfo> lastSeenCrcInfo =
        logSegmentAtVersion
            .getLastSeenChecksum()
            .flatMap(file -> ChecksumReader.getCRCInfo(engine, file));

    Optional<Long> checksumVersion = lastSeenCrcInfo.map(CRCInfo::getVersion);

    // If checksum already exists for this version, nothing to do
    if (checksumVersion.isPresent()
        && checksumVersion.get().equals(logSegmentAtVersion.getVersion())) {
      logger.info("Checksum file already exists for version {}", logSegmentAtVersion.getVersion());
      return;
    }

    // Step 2: Initialize state tracking variables
    boolean canUseLastChecksum =
        lastSeenCrcInfo.isPresent()
            && lastSeenCrcInfo.get().getDomainMetadata().isPresent()
            && lastSeenCrcInfo.get().getFileSizeHistogram().isPresent();

    StateTracker stateTracker = new StateTracker();
    long minLogToRead = 0;
    if (canUseLastChecksum) {
      stateTracker.tableSizeByte.add(lastSeenCrcInfo.get().getTableSizeBytes());
      stateTracker.fileCount.add(lastSeenCrcInfo.get().getNumFiles());
      stateTracker.fileSizeHistogram = lastSeenCrcInfo.get().getFileSizeHistogram().get();
      minLogToRead = lastSeenCrcInfo.get().getVersion() + 1;
    }

    // Step 3: Create filtered log segment from current version, this is because when crc could be
    // used,
    // we could only replay log until crc info + 1.
    LogSegment filteredLogSegment = createFilteredLogSegment(logSegmentAtVersion, minLogToRead);

    // Step 4: Process all logs and update state
    processLogs(engine, filteredLogSegment, stateTracker);

    // Step 5: Get final metadata and protocol for the checksum
    Metadata finalMetadata = getFinalMetadata(stateTracker.metadataFromLog, lastSeenCrcInfo);
    Protocol finalProtocol = getFinalProtocol(stateTracker.protocolFromLog, lastSeenCrcInfo);

    // Step 6: Combine domain metadata from previous checksum if needed
    Map<String, DomainMetadata> finalDomainMetadataMap =
        canUseLastChecksum
            ? combineWithDomainMetadataInCrc(
                stateTracker.domainMetadataMapFromLog, lastSeenCrcInfo.get())
            : stateTracker.domainMetadataMapFromLog;

    // Return only non-removed domain metadata
    Set<DomainMetadata> finalDomainMetadata =
        finalDomainMetadataMap.values().stream()
            .filter(dm -> !dm.isRemoved())
            .collect(Collectors.toSet());

    // Step 8: Write the computed checksum
    ChecksumWriter checksumWriter = new ChecksumWriter(logSegmentAtVersion.getLogPath());

    try {
      checksumWriter.writeCheckSum(
          engine,
          new CRCInfo(
              logSegmentAtVersion.getVersion(),
              finalMetadata,
              finalProtocol,
              stateTracker.tableSizeByte.longValue(),
              stateTracker.fileCount.longValue(),
              Optional.empty() /* txnId */,
              Optional.of(finalDomainMetadata),
              Optional.of(stateTracker.fileSizeHistogram)));

      logger.info(
          "Successfully wrote checksum file for version {}", logSegmentAtVersion.getVersion());
    } catch (FileAlreadyExistsException e) {
      logger.info("Checksum file already exists for version {}", logSegmentAtVersion.getVersion());
      // Checksum file has been created while we were computing it.
      // This is fine - the checksum now exists, which was our goal.
    }
  }

  /** Creates a filtered log segment containing only files after minLogToRead. */
  private static LogSegment createFilteredLogSegment(
      LogSegment logSegmentAtVersion, long minLogToRead) {

    List<FileStatus> filteredDeltas =
        logSegmentAtVersion.getDeltas().stream()
            .filter(file -> FileNames.getFileVersion(new Path(file.getPath())) >= minLogToRead)
            .collect(Collectors.toList());

    List<FileStatus> filteredCheckpoints =
        logSegmentAtVersion.getCheckpoints().stream()
            .filter(file -> FileNames.getFileVersion(new Path(file.getPath())) >= minLogToRead)
            .collect(Collectors.toList());

    return new LogSegment(
        logSegmentAtVersion.getLogPath(),
        logSegmentAtVersion.getVersion(),
        filteredDeltas,
        // TODO: Check if log compaction could further help.
        /* compactions= */ new ArrayList<>(),
        filteredCheckpoints,
        Optional.empty(),
        logSegmentAtVersion.getLastCommitTimestamp());
  }

  /** Processes all logs in the log segment and updates the state tracker. */
  private static void processLogs(
      Engine engine, LogSegment filteredLogSegment, StateTracker stateTracker) throws IOException {

    // Create an iterator that will only process files without considering removals
    try (CreateCheckpointIterator checkpointIterator =
        new CreateCheckpointIterator(
            engine,
            filteredLogSegment,
            // Set retention to infinite future to skip all removed files
            Instant.ofEpochMilli(Long.MAX_VALUE).toEpochMilli())) {

      // Process all checkpoint batches
      while (checkpointIterator.hasNext()) {
        FilteredColumnarBatch filteredBatch = checkpointIterator.next();
        processBatch(filteredBatch, stateTracker);
      }
    }
  }

  /** Processes a single batch from the checkpoint iterator. */
  private static void processBatch(FilteredColumnarBatch filteredBatch, StateTracker stateTracker) {
    ColumnarBatch batch = filteredBatch.getData();
    Optional<ColumnVector> selectionVector = filteredBatch.getSelectionVector();
    final int rowCount = batch.getSize();

    // Get column vectors for each action type
    ColumnVector metadataVector = batch.getColumnVector(METADATA_INDEX);
    ColumnVector protocolVector = batch.getColumnVector(PROTOCOL_INDEX);
    ColumnVector removeVector = batch.getColumnVector(REMOVE_INDEX);
    ColumnVector addVector = batch.getColumnVector(ADD_INDEX);
    ColumnVector domainMetadataVector = batch.getColumnVector(DOMAIN_METADATA_INDEX);

    // Process all selected rows in a single pass for optimal performance
    for (int i = 0; i < rowCount; i++) {
      int rowId = i;
      boolean isSelected = selectionVector.map(vec -> !vec.isNullAt(rowId)).orElse(true);
      if (!isSelected) continue;

      // Step 1: Ensure there are no remove records (should be filtered by infinite retention)
      checkState(
          removeVector.isNullAt(i),
          "Unexpected remove row found when "
              + "setting minFileRetentionTimestampMillis to infinite future");

      // Step 2: Process add file records
      if (!addVector.isNullAt(i)) {
        ColumnVector sizeVector = addVector.getChild(ADD_SIZE_INDEX);
        long fileSize = sizeVector.getLong(i);

        stateTracker.tableSizeByte.add(fileSize);
        stateTracker.fileSizeHistogram.insert(fileSize);
        stateTracker.fileCount.increment();
      }

      // Step 3: Process domain metadata records
      if (!domainMetadataVector.isNullAt(i)) {
        DomainMetadata domainMetadata = DomainMetadata.fromColumnVector(domainMetadataVector, i);
        if (domainMetadata != null) {
          String domain = domainMetadata.getDomain();
          if (!stateTracker.domainMetadataMapFromLog.containsKey(domain)) {
            stateTracker.domainMetadataMapFromLog.put(domain, domainMetadata);
          }
        }
      }

      // Step 4: Process metadata records
      if (!stateTracker.metadataFromLog.isPresent() && !metadataVector.isNullAt(i)) {
        Metadata metadata = Metadata.fromColumnVector(metadataVector, i);
        if (metadata != null) {
          stateTracker.metadataFromLog = Optional.of(metadata);
          logger.debug("Found and stored metadata at row {}", i);
        }
      }

      // Step 5: Process protocol records
      if (!stateTracker.protocolFromLog.isPresent() && !protocolVector.isNullAt(i)) {
        Protocol protocol = Protocol.fromColumnVector(protocolVector, i);
        if (protocol != null) {
          stateTracker.protocolFromLog = Optional.of(protocol);
          logger.debug("Found and stored protocol at row {}", i);
        }
      }
    }
  }

  /** Determines the final metadata to use in the checksum. */
  private static Metadata getFinalMetadata(
      Optional<Metadata> metadataFromLog, Optional<CRCInfo> lastSeenCrcInfo) {

    if (metadataFromLog.isPresent()) {
      logger.debug("Using metadata from log processing");
      return metadataFromLog.get();
    }

    if (lastSeenCrcInfo.isPresent() && lastSeenCrcInfo.get().getMetadata() != null) {
      logger.debug("Using metadata from previous checksum");
      return lastSeenCrcInfo.get().getMetadata();
    }

    throw new IllegalStateException("No metadata found in current log or previous checksum");
  }

  /** Determines the final protocol to use in the checksum. */
  private static Protocol getFinalProtocol(
      Optional<Protocol> protocolFromLog, Optional<CRCInfo> lastSeenCrcInfo) {

    if (protocolFromLog.isPresent()) {
      logger.debug("Using protocol from log processing");
      return protocolFromLog.get();
    }

    if (lastSeenCrcInfo.isPresent() && lastSeenCrcInfo.get().getProtocol() != null) {
      logger.debug("Using protocol from previous checksum");
      return lastSeenCrcInfo.get().getProtocol();
    }

    throw new IllegalStateException("No protocol found in current log or previous checksum");
  }

  /** Combines domain metadata from the current processing with previous checksum if needed. */
  private static Map<String, DomainMetadata> combineWithDomainMetadataInCrc(
      Map<String, DomainMetadata> domainMetadataMapFromLog, CRCInfo lastSeenCrcInfo) {
    checkArgument(lastSeenCrcInfo.getDomainMetadata().isPresent());
    Set<DomainMetadata> previousDomainMetadata = lastSeenCrcInfo.getDomainMetadata().get();

    for (DomainMetadata dm : previousDomainMetadata) {
      if (!domainMetadataMapFromLog.containsKey(dm.getDomain())) {
        domainMetadataMapFromLog.put(dm.getDomain(), dm);
      }
    }

    return domainMetadataMapFromLog;
  }

  /** Class for tracking state during log processing. */
  private static class StateTracker {
    Optional<Metadata> metadataFromLog = Optional.empty();
    Optional<Protocol> protocolFromLog = Optional.empty();
    LongAdder tableSizeByte = new LongAdder();
    LongAdder fileCount = new LongAdder();
    FileSizeHistogram fileSizeHistogram = FileSizeHistogram.createDefaultHistogram();
    Map<String, DomainMetadata> domainMetadataMapFromLog = new HashMap<>();
  }
}
