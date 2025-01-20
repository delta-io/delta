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
package io.delta.kernel.internal.rowtracking;

import static io.delta.kernel.internal.util.Preconditions.checkArgument;

import io.delta.kernel.data.Row;
import io.delta.kernel.internal.DeltaErrors;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.internal.TableFeatures;
import io.delta.kernel.internal.actions.*;
import io.delta.kernel.utils.CloseableIterable;
import io.delta.kernel.utils.CloseableIterator;
import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

/** A collection of helper methods for working with row tracking. */
public class RowTracking {
  private RowTracking() {
    // Empty constructor to prevent instantiation of this class
  }

  /**
   * Assigns base row IDs and default row commit versions to {@link AddFile} actions in the provided
   * {@code dataActions}. This method should be called when processing data actions during commit
   * preparation and before the actual commit.
   *
   * <p>This method should be called exactly once per transaction during commit preparation, i.e.,
   * not for each commit attempt. And it should only be called when the 'rowTracking' feature is
   * supported.
   *
   * <p>For {@link AddFile} actions missing a base row ID, assigns the current row ID high watermark
   * plus 1. The high watermark is then incremented by the number of records in the file. For
   * actions missing a default row commit version, assigns the specified commit version.
   *
   * @param snapshot the snapshot of the table that this transaction is reading at
   * @param commitVersion the version of the commit for default row commit version assignment
   * @param dataActions the {@link CloseableIterable} of data actions to process
   * @return an {@link CloseableIterable} of data actions with base row IDs and default row commit
   *     versions assigned
   */
  public static CloseableIterable<Row> assignBaseRowIdAndDefaultRowCommitVersion(
      SnapshotImpl snapshot, long commitVersion, CloseableIterable<Row> dataActions) {
    checkArgument(
        TableFeatures.isRowTrackingSupported(snapshot.getProtocol()),
        "Base row ID and default row commit version are assigned "
            + "only when feature 'rowTracking' is supported.");

    return new CloseableIterable<Row>() {
      @Override
      public void close() throws IOException {
        dataActions.close();
      }

      @Override
      public CloseableIterator<Row> iterator() {
        // Used to keep track of the current high watermark as we iterate through the data actions.
        // Use an AtomicLong to allow for updating the high watermark in the lambda.
        final AtomicLong currRowIdHighWatermark = new AtomicLong(readRowIdHighWaterMark(snapshot));
        return dataActions
            .iterator()
            .map(
                row -> {
                  // Non-AddFile actions are returned unchanged
                  if (row.isNullAt(SingleAction.ADD_FILE_ORDINAL)) {
                    return row;
                  }

                  AddFile addFile = new AddFile(row.getStruct(SingleAction.ADD_FILE_ORDINAL));

                  // Assign base row ID if missing
                  if (!addFile.getBaseRowId().isPresent()) {
                    final long numRecords = getNumRecordsOrThrow(addFile);
                    addFile = addFile.withNewBaseRowId(currRowIdHighWatermark.get() + 1L);
                    currRowIdHighWatermark.addAndGet(numRecords);
                  }

                  // Assign default row commit version if missing
                  if (!addFile.getDefaultRowCommitVersion().isPresent()) {
                    addFile = addFile.withNewDefaultRowCommitVersion(commitVersion);
                  }

                  return SingleAction.createAddFileSingleAction(addFile.toRow());
                });
      }
    };
  }

  /**
   * Returns a {@link DomainMetadata} action if the row ID high watermark has changed due to newly
   * processed {@link AddFile} actions.
   *
   * <p>This method should be called during commit preparation to prepare the domain metadata
   * actions for commit. It should be called only when the 'rowTracking' feature is supported.
   *
   * @param snapshot the snapshot of the table that this transaction is reading at
   * @param dataActions the iterable of data actions that may update the high watermark
   */
  public static Optional<DomainMetadata> createNewHighWaterMarkIfNeeded(
      SnapshotImpl snapshot, CloseableIterable<Row> dataActions) {
    checkArgument(
        TableFeatures.isRowTrackingSupported(snapshot.getProtocol()),
        "Row ID high watermark is updated only when feature 'rowTracking' is supported.");

    final long prevRowIdHighWatermark = readRowIdHighWaterMark(snapshot);
    // Use an AtomicLong to allow for updating the high watermark in the lambda
    final AtomicLong newRowIdHighWatermark = new AtomicLong(prevRowIdHighWatermark);

    dataActions.forEach(
        row -> {
          if (!row.isNullAt(SingleAction.ADD_FILE_ORDINAL)) {
            AddFile addFile = new AddFile(row.getStruct(SingleAction.ADD_FILE_ORDINAL));
            if (!addFile.getBaseRowId().isPresent()) {
              newRowIdHighWatermark.addAndGet(getNumRecordsOrThrow(addFile));
            }
          }
        });

    return (newRowIdHighWatermark.get() != prevRowIdHighWatermark)
        ? Optional.of(new RowTrackingMetadataDomain(newRowIdHighWatermark.get()).toDomainMetadata())
        : Optional.empty();
  }

  /**
   * Reads the current row ID high watermark from the snapshot, or returns a default value if
   * missing.
   */
  private static long readRowIdHighWaterMark(SnapshotImpl snapshot) {
    return RowTrackingMetadataDomain.fromSnapshot(snapshot)
        .map(RowTrackingMetadataDomain::getRowIdHighWaterMark)
        .orElse(RowTrackingMetadataDomain.MISSING_ROW_ID_HIGH_WATERMARK);
  }

  /**
   * Get the number of records from the AddFile's statistics. It errors out if statistics are
   * missing.
   */
  private static long getNumRecordsOrThrow(AddFile addFile) {
    return addFile.getNumRecords().orElseThrow(DeltaErrors::missingNumRecordsStatsForRowTracking);
  }
}
