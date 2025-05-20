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

  // Indices into child structs - directly accessing these fields is faster than using
  // accessor methods when processing large numbers of rows.
  private static final int ADD_SIZE_INDEX = AddFile.FULL_SCHEMA.indexOf("size");

  /** Class for tracking state during log processing. */
  private static class StateTracker {
    Optional<Metadata> metadataFromLog = Optional.empty();
    Optional<Protocol> protocolFromLog = Optional.empty();
    LongAdder tableSizeByte = new LongAdder();
    LongAdder fileCount = new LongAdder();
    FileSizeHistogram fileSizeHistogram = FileSizeHistogram.createDefaultHistogram();
    Map<String, DomainMetadata> domainMetadataMap = new HashMap<>();
  }

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

        for (int i = 0; i < rowCount; i++) {
          int rowId = i;
          boolean isSelected = selectionVector.map(vec -> !vec.isNullAt(rowId)).orElse(true);
          if (!isSelected) continue;

          checkState(removeVector.isNullAt(i), "Unexpected remove action found");

          if (!addVector.isNullAt(i)) {
            long fileSize = addVector.getChild(ADD_SIZE_INDEX).getLong(i);
            state.tableSizeByte.add(fileSize);
            state.fileSizeHistogram.insert(fileSize);
            state.fileCount.increment();
          }

          if (!domainMetadataVector.isNullAt(i)) {
            DomainMetadata domainMetadata =
                DomainMetadata.fromColumnVector(domainMetadataVector, i);
            if (domainMetadata != null) {
              String domain = domainMetadata.getDomain();
              if (!state.domainMetadataMap.containsKey(domain)) {
                state.domainMetadataMap.put(domain, domainMetadata);
              }
            }
          }

          if (!state.metadataFromLog.isPresent() && !metadataVector.isNullAt(i)) {
            Metadata metadata = Metadata.fromColumnVector(metadataVector, i);
            if (metadata != null) {
              state.metadataFromLog = Optional.of(metadata);
            }
          }

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
    Metadata finalMetadata;
    Protocol finalProtocol;

    if (state.metadataFromLog.isPresent()) {
      finalMetadata = state.metadataFromLog.get();
    } else if (lastSeenCrcInfo.isPresent() && lastSeenCrcInfo.get().getMetadata() != null) {
      finalMetadata = lastSeenCrcInfo.get().getMetadata();
    } else {
      throw new IllegalStateException("No metadata found in log or previous checksum");
    }

    if (state.protocolFromLog.isPresent()) {
      finalProtocol = state.protocolFromLog.get();
    } else if (lastSeenCrcInfo.isPresent() && lastSeenCrcInfo.get().getProtocol() != null) {
      finalProtocol = lastSeenCrcInfo.get().getProtocol();
    } else {
      throw new IllegalStateException("No protocol found in log or previous checksum");
    }

    // Combine domain metadata if needed
    if (canUseLastChecksum) {
      Set<DomainMetadata> previousDomainMetadata = lastSeenCrcInfo.get().getDomainMetadata().get();
      for (DomainMetadata dm : previousDomainMetadata) {
        if (!state.domainMetadataMap.containsKey(dm.getDomain())) {
          state.domainMetadataMap.put(dm.getDomain(), dm);
        }
      }
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
}
