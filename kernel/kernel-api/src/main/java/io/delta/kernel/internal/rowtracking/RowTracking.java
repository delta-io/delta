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

import io.delta.kernel.data.Row;
import io.delta.kernel.internal.DeltaErrors;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.internal.TableFeatures;
import io.delta.kernel.internal.actions.*;
import io.delta.kernel.utils.CloseableIterable;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.DataFileStatistics;
import java.io.IOException;
import java.util.Optional;

/** A collection of helper methods for working with row tracking. */
public class RowTracking {
  private RowTracking() {
    // Empty constructor to prevent instantiation of this class
  }

  private static final int ADD_FILE_ORDINAL = SingleAction.FULL_SCHEMA.indexOf("add");

  /**
   * Assigns base row IDs and default row commit versions to {@link AddFile} actions before
   * committing. If row tracking is not supported by the given protocol, returns the input actions
   * unchanged.
   *
   * <p>When needed, it sets each {@link AddFile} action’s base row ID based on the current high
   * watermark and increments the watermark accordingly. If a default row commit version is missing,
   * it assigns the provided commit version.
   *
   * @param protocol the protocol to check for row tracking support
   * @param snapshot the current snapshot of the table
   * @param commitVersion the version of the commit for default row commit version assignment
   * @param dataActions the {@link CloseableIterable} of data actions to process
   * @return an {@link CloseableIterable} of data actions with base row IDs and default row commit
   *     versions assigned
   */
  public static CloseableIterable<Row> assignBaseRowIdAndDefaultRowCommitVersion(
      Protocol protocol,
      SnapshotImpl snapshot,
      long commitVersion,
      CloseableIterable<Row> dataActions) {
    if (!TableFeatures.isRowTrackingSupported(protocol)) {
      return dataActions;
    }

    return new CloseableIterable<Row>() {
      @Override
      public void close() throws IOException {
        dataActions.close();
      }

      @Override
      public CloseableIterator<Row> iterator() {
        // Used to keep track of the current high watermark as we iterate through the data actions.
        // Use a one-element array here to allow for mutation within the lambda.
        final long[] currRowIdHighWatermark = {readRowIdHighWaterMark(snapshot)};
        return dataActions
            .iterator()
            .map(
                row -> {
                  // Non-AddFile actions are returned unchanged
                  if (row.isNullAt(ADD_FILE_ORDINAL)) {
                    return row;
                  }

                  AddFile addFile = AddFile.fromRow(row.getStruct(ADD_FILE_ORDINAL));

                  // Assign base row ID if missing
                  if (!addFile.getBaseRowId().isPresent()) {
                    final long numRecords = getNumRecords(addFile);
                    addFile = addFile.withNewBaseRowId(currRowIdHighWatermark[0] + 1L);
                    currRowIdHighWatermark[0] += numRecords;
                  }

                  // Assign default row commit version if missing
                  if (!addFile.getDefaultRowCommitVersion().isPresent()) {
                    addFile = addFile.withNewDefaultRowCommitVersion(commitVersion);
                  }

                  // Return a new AddFile row with assigned baseRowId/defaultRowCommitVersion
                  return SingleAction.createAddFileSingleAction(addFile.toRow());
                });
      }
    };
  }

  /**
   * Emits a {@link DomainMetadata} action if the row ID high watermark has changed due to newly
   * processed {@link AddFile} actions.
   *
   * @param protocol the protocol to check for row tracking support
   * @param snapshot the current snapshot of the table
   * @param dataActions the iterable of data actions that may update the high watermark
   */
  public static Optional<DomainMetadata> createNewHighWaterMarkIfNeeded(
      Protocol protocol, SnapshotImpl snapshot, CloseableIterable<Row> dataActions) {
    if (!TableFeatures.isRowTrackingSupported(protocol)) {
      return Optional.empty();
    }

    final long prevRowIdHighWatermark = readRowIdHighWaterMark(snapshot);
    // Use a one-element array here to allow for mutation within the lambda.
    final long[] newRowIdHighWatermark = {prevRowIdHighWatermark};

    dataActions.forEach(
        row -> {
          if (!row.isNullAt(ADD_FILE_ORDINAL)) {
            newRowIdHighWatermark[0] +=
                getNumRecords(AddFile.fromRow(row.getStruct(ADD_FILE_ORDINAL)));
          }
        });

    return (newRowIdHighWatermark[0] != prevRowIdHighWatermark)
        ? Optional.of(new RowTrackingMetadataDomain(newRowIdHighWatermark[0]).toDomainMetadata())
        : Optional.empty();
  }

  /**
   * Reads the current row ID high watermark from the snapshot, or returns a default value if
   * missing.
   *
   * @param snapshot the snapshot to read
   * @return the row ID high watermark
   */
  public static long readRowIdHighWaterMark(SnapshotImpl snapshot) {
    return RowTrackingMetadataDomain.fromSnapshot(snapshot)
        .map(RowTrackingMetadataDomain::getRowIdHighWaterMark)
        .orElse(RowTrackingMetadataDomain.MISSING_ROW_ID_HIGH_WATERMARK);
  }

  /**
   * Retrieves the number of records from the AddFile's statistics. We error out if statistics are
   * missing.
   *
   * @param addFile the AddFile action
   * @return the number of records
   */
  private static long getNumRecords(AddFile addFile) {
    return addFile
        .getStats()
        .map(DataFileStatistics::getNumRecords)
        .orElseThrow(DeltaErrors::rowIDAssignmentWithoutStats);
  }
}
