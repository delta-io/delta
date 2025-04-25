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
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.internal.actions.AddFile;
import io.delta.kernel.internal.actions.DomainMetadata;
import io.delta.kernel.internal.replay.CreateCheckpointIterator;
import io.delta.kernel.internal.stats.FileSizeHistogram;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;

public class ChecksumUtils {

  private ChecksumUtils() {}

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
   * @param snapshot The SnapshotImpl instance representing the table at a specific version
   * @throws IOException If an I/O error occurs during checksum computation or writing
   */
  public static void computeStateAndWriteChecksum(Engine engine, SnapshotImpl snapshot)
      throws IOException {
    // If checksum already exists, nothing to do
    if (snapshot.getCurrentCrcInfo().isPresent()) {
      return;
    }

    // TODO: Optimize using last available crc after https://github.com/delta-io/delta/pull/4112
    LongAdder tableSizeByte = new LongAdder();
    LongAdder fileCount = new LongAdder();
    FileSizeHistogram fileSizeHistogram = FileSizeHistogram.createDefaultHistogram();
    Map<String, DomainMetadata> domainMetadataMap = new HashMap<>();
    ChecksumWriter checksumWriter = new ChecksumWriter(snapshot.getLogPath());

    try (CreateCheckpointIterator checkpointIterator =
        new CreateCheckpointIterator(
            engine,
            snapshot.getLogSegment(),
            // Set minFileRetentionTimestampMillis to infinite future to skip all removed files
            Instant.ofEpochMilli(Long.MAX_VALUE).toEpochMilli())) {

      // Process all checkpoint batches
      while (checkpointIterator.hasNext()) {
        FilteredColumnarBatch filteredBatch = checkpointIterator.next();
        ColumnarBatch batch = filteredBatch.getData();
        Optional<ColumnVector> selectionVector = filteredBatch.getSelectionVector();
        final int rowCount = batch.getSize();

        ColumnVector removeVector = batch.getColumnVector(REMOVE_INDEX);
        ColumnVector addVector = batch.getColumnVector(ADD_INDEX);
        ColumnVector domainMetadataVector = batch.getColumnVector(DOMAIN_METADATA_INDEX);

        // Process all selected rows in a single pass for optimal performance
        for (int i = 0; i < rowCount; i++) {
          // Check if this row is selected
          boolean isSelected =
              !selectionVector.isPresent()
                  || (!selectionVector.get().isNullAt(i) && selectionVector.get().getBoolean(i));
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

            tableSizeByte.add(fileSize);
            fileSizeHistogram.insert(fileSize);
            fileCount.increment();
          }

          // Step 3: Process domain metadata records
          if (!domainMetadataVector.isNullAt(i)) {
            DomainMetadata domainMetadata =
                DomainMetadata.fromColumnVector(domainMetadataVector, i);
            if (domainMetadata != null
                && !domainMetadataMap.containsKey(domainMetadata.getDomain())) {
              domainMetadataMap.put(domainMetadata.getDomain(), domainMetadata);
            }
          }
        }
      }

      // Write the checksum file
      try {
        checksumWriter.writeCheckSum(
            engine,
            new CRCInfo(
                snapshot.getVersion(),
                snapshot.getMetadata(),
                snapshot.getProtocol(),
                tableSizeByte.longValue(),
                fileCount.longValue(),
                Optional.empty() /* txnId */,
                Optional.of(
                    domainMetadataMap.values().stream()
                        .filter(domainMetadata -> !domainMetadata.isRemoved())
                        .collect(Collectors.toSet())),
                Optional.of(fileSizeHistogram)));
      } catch (FileAlreadyExistsException e) {
        // Checksum file has been created while we were computing it.
        // This is fine - the checksum now exists, which was our goal.
      }
    }
  }
}
