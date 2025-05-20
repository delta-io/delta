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

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.actions.AddFile;
import io.delta.kernel.internal.actions.DomainMetadata;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
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

    // Initialize state tracking
    StateTracker state = new StateTracker();
    long minLogToRead;

    // Use existing checksum data if available
    boolean canUseLastChecksum =
        lastSeenCrcInfo.isPresent()
            && lastSeenCrcInfo.get().getDomainMetadata().isPresent()
            && lastSeenCrcInfo.get().getFileSizeHistogram().isPresent();

    if (canUseLastChecksum) {
      state.tableSizeByte.add(lastSeenCrcInfo.get().getTableSizeBytes());
      state.fileCount.add(lastSeenCrcInfo.get().getNumFiles());
      state.fileSizeHistogram = lastSeenCrcInfo.get().getFileSizeHistogram().get();
      minLogToRead = lastSeenCrcInfo.get().getVersion() + 1;
    } else {
      minLogToRead = 0;
    }

    // Filter log segment to only process necessary files
    List<FileStatus> filteredDeltas =
        logSegmentAtVersion.getDeltas().stream()
            .filter(file -> FileNames.getFileVersion(new Path(file.getPath())) >= minLogToRead)
            .collect(Collectors.toList());

    List<FileStatus> filteredCheckpoints =
        logSegmentAtVersion.getCheckpoints().stream()
            .filter(file -> FileNames.getFileVersion(new Path(file.getPath())) >= minLogToRead)
            .collect(Collectors.toList());

    LogSegment filteredLogSegment =
        new LogSegment(
            logSegmentAtVersion.getLogPath(),
            logSegmentAtVersion.getVersion(),
            filteredDeltas,
            new ArrayList<>(),
            filteredCheckpoints,
            Optional.empty(),
            logSegmentAtVersion.getLastCommitTimestamp());

    // Process logs and update state
    try (CreateCheckpointIterator checkpointIterator =
        new CreateCheckpointIterator(
            engine, filteredLogSegment, Instant.ofEpochMilli(Long.MAX_VALUE).toEpochMilli())) {

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

          // Step 2: Process add file records - direct columnar access for better performance
          if (!addVector.isNullAt(i)) {
            // Direct access to size field in add file record for maximum performance
            ColumnVector sizeVector = addVector.getChild(ADD_SIZE_INDEX);
            long fileSize = sizeVector.getLong(i);

            state.tableSizeByte.add(fileSize);
            state.fileSizeHistogram.insert(fileSize);
            state.fileCount.increment();
          }

          // Step 3: Process domain metadata records
          if (!domainMetadataVector.isNullAt(i)) {
            DomainMetadata domainMetadata =
                DomainMetadata.fromColumnVector(domainMetadataVector, i);
            if (domainMetadata != null) {
              checkState(
                  !state.domainMetadataMap.containsKey(domainMetadata.getDomain()),
                  "unexpected duplicate domain metadata rows");
              // CreateCheckpointIterator will ensure only the last entry of domain metadata
              // got emit.
              state.domainMetadataMap.put(domainMetadata.getDomain(), domainMetadata);
            }
          }

          // Step 4: Process metadata records
          if (!state.metadataFromLog.isPresent() && !metadataVector.isNullAt(i)) {
            Metadata metadata = Metadata.fromColumnVector(metadataVector, i);
            if (metadata != null) {
              state.metadataFromLog = Optional.of(metadata);
            }
          }

          // Step 5: Process protocol records
          if (!state.protocolFromLog.isPresent() && !protocolVector.isNullAt(i)) {
            Protocol protocol = Protocol.fromColumnVector(protocolVector, i);
            if (protocol != null) {
              state.protocolFromLog = Optional.of(protocol);
            }
          }
        }
      }
    }

    // Get final metadata and protocol
    Metadata finalMetadata =
        state.metadataFromLog.orElseGet(
            () ->
                lastSeenCrcInfo
                    .map(CRCInfo::getMetadata)
                    .orElseThrow(() -> new IllegalStateException("No metadata found")));

    Protocol finalProtocol =
        state.protocolFromLog.orElseGet(
            () ->
                lastSeenCrcInfo
                    .map(CRCInfo::getProtocol)
                    .orElseThrow(() -> new IllegalStateException("No protocol found")));

    // Combine with previous domain metadata if available
    if (canUseLastChecksum) {
      lastSeenCrcInfo.get().getDomainMetadata().get().stream()
          .filter(dm -> !state.domainMetadataMap.containsKey(dm.getDomain()))
          .forEach(dm -> state.domainMetadataMap.put(dm.getDomain(), dm));
    }

    // Filter to only non-removed domain metadata
    Set<DomainMetadata> finalDomainMetadata =
        state.domainMetadataMap.values().stream()
            .filter(dm -> !dm.isRemoved())
            .collect(Collectors.toSet());

    // Write the checksum file
    ChecksumWriter checksumWriter = new ChecksumWriter(logSegmentAtVersion.getLogPath());
    try {
      checksumWriter.writeCheckSum(
          engine,
          new CRCInfo(
              logSegmentAtVersion.getVersion(),
              finalMetadata,
              finalProtocol,
              state.tableSizeByte.longValue(),
              state.fileCount.longValue(),
              Optional.empty(),
              Optional.of(finalDomainMetadata),
              Optional.of(state.fileSizeHistogram)));

      logger.info(
          "Successfully wrote checksum file for version {}", logSegmentAtVersion.getVersion());
    } catch (FileAlreadyExistsException e) {
      logger.info("Checksum file already exists for version {}", logSegmentAtVersion.getVersion());
    }
  }

  /** Class for tracking state during log processing. */
  private static class StateTracker {
    Optional<Metadata> metadataFromLog = Optional.empty();
    Optional<Protocol> protocolFromLog = Optional.empty();
    LongAdder tableSizeByte = new LongAdder();
    LongAdder fileCount = new LongAdder();
    FileSizeHistogram fileSizeHistogram = FileSizeHistogram.createDefaultHistogram();
    Map<String, DomainMetadata> domainMetadataMap = new HashMap<>();
  }
}
