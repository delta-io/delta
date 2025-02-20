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
import io.delta.kernel.internal.actions.*;
import io.delta.kernel.internal.tablefeatures.TableFeatures;
import io.delta.kernel.utils.CloseableIterable;
import io.delta.kernel.utils.CloseableIterator;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/** A collection of helper methods for working with row tracking. */
public class RowTracking {
  private RowTracking() {
    // Empty constructor to prevent instantiation of this class
  }

  /**
   * Assigns or reassigns baseRowIds and defaultRowCommitVersions to {@link AddFile} actions in the
   * provided {@code dataActions}. This method should be invoked only when the 'rowTracking' feature
   * is supported and is used in two scenarios:
   *
   * <ol>
   *   <li>Initial Assignment: Assigns row tracking fields to AddFile actions during commit
   *       preparation before they are committed.
   *   <li>Conflict Resolution: Updates row tracking fields when a transaction conflict occurs.
   *       Since the losing transaction gets a new commit version and winning transactions may have
   *       increased the row ID high watermark, this method reassigns the fields for the losing
   *       transaction using the latest state from winning transactions before retrying the commit.
   * </ol>
   *
   * @param txnReadSnapshot the snapshot of the table that this transaction is reading from
   * @param txnProtocol the (updated, if any) protocol that will result from this txn
   * @param winningTxnRowIdHighWatermark the latest row ID high watermark from the winning
   *     transactions. Should be empty for initial assignment and present for conflict resolution.
   * @param prevCommitVersion the commit version used by this transaction in the previous commit
   *     attempt. Should be empty for initial assignment and present for conflict resolution.
   * @param currCommitVersion the transaction's (latest) commit version
   * @param txnDataActions a {@link CloseableIterable} of data actions this txn is trying to commit
   * @return a {@link CloseableIterable} of data actions with baseRowIds and
   *     defaultRowCommitVersions assigned or reassigned
   */
  public static CloseableIterable<Row> assignBaseRowIdAndDefaultRowCommitVersion(
      SnapshotImpl txnReadSnapshot,
      Protocol txnProtocol,
      Optional<Long> winningTxnRowIdHighWatermark,
      Optional<Long> prevCommitVersion,
      long currCommitVersion,
      CloseableIterable<Row> txnDataActions) {
    checkArgument(
        TableFeatures.isRowTrackingSupported(txnProtocol),
        "Base row ID and default row commit version are assigned "
            + "only when feature 'rowTracking' is supported.");

    return new CloseableIterable<Row>() {
      @Override
      public void close() throws IOException {
        txnDataActions.close();
      }

      @Override
      public CloseableIterator<Row> iterator() {
        // The row ID high watermark from the snapshot of the table that this transaction is reading
        // at. Any baseRowIds higher than this watermark are assigned by this transaction.
        final long prevRowIdHighWatermark = readRowIdHighWaterMark(txnReadSnapshot);

        // Used to track the current high watermark as we iterate through the data actions and
        // assign baseRowIds. Use an AtomicLong to allow for updating in the lambda.
        final AtomicLong currRowIdHighWatermark =
            new AtomicLong(winningTxnRowIdHighWatermark.orElse(prevRowIdHighWatermark));

        // The row ID high watermark must increase monotonically, so the winning transaction's high
        // watermark must be greater than or equal to the high watermark from the current
        // transaction's read snapshot.
        checkArgument(
            currRowIdHighWatermark.get() >= prevRowIdHighWatermark,
            "The current row ID high watermark must be greater than or equal to "
                + "the high watermark from the transaction's read snapshot");

        return txnDataActions
            .iterator()
            .map(
                row -> {
                  // Non-AddFile actions are returned unchanged
                  if (row.isNullAt(SingleAction.ADD_FILE_ORDINAL)) {
                    return row;
                  }

                  AddFile addFile = new AddFile(row.getStruct(SingleAction.ADD_FILE_ORDINAL));

                  // Assign a baseRowId if not present, or update it if previously assigned
                  // by this transaction
                  if (!addFile.getBaseRowId().isPresent()
                      || addFile.getBaseRowId().get() > prevRowIdHighWatermark) {
                    addFile = addFile.withNewBaseRowId(currRowIdHighWatermark.get() + 1L);
                    currRowIdHighWatermark.addAndGet(getNumRecordsOrThrow(addFile));
                  }

                  // Assign a defaultRowCommitVersion if not present, or update it if previously
                  // assigned by this transaction
                  if (!addFile.getDefaultRowCommitVersion().isPresent()
                      || addFile.getDefaultRowCommitVersion().get()
                          == prevCommitVersion.orElse(-1L)) {
                    addFile = addFile.withNewDefaultRowCommitVersion(currCommitVersion);
                  }

                  return SingleAction.createAddFileSingleAction(addFile.toRow());
                });
      }
    };
  }

  /**
   * Inserts or updates the {@link DomainMetadata} action reflecting the new row ID high watermark
   * when this transaction adds rows and pushed it higher.
   *
   * <p>This method should only be called when the 'rowTracking' feature is supported. Similar to
   * {@link #assignBaseRowIdAndDefaultRowCommitVersion}, it should be called during the initial row
   * ID assignment or conflict resolution to reflect the change to the row ID high watermark.
   *
   * @param txnReadSnapshot the snapshot of the table that this transaction is reading at
   * @param txnProtocol the (updated, if any) protocol that will result from this txn
   * @param winningTxnRowIdHighWatermark the latest row ID high watermark from the winning
   *     transaction. Should be empty for initial assignment and present for conflict resolution.
   * @param txnDataActions a {@link CloseableIterable} of data actions this txn is trying to commit
   * @param txnDomainMetadatas a list of domain metadata actions this txn is trying to commit
   * @return Updated list of domain metadata actions for commit
   */
  public static List<DomainMetadata> updateRowIdHighWatermarkIfNeeded(
      SnapshotImpl txnReadSnapshot,
      Protocol txnProtocol,
      Optional<Long> winningTxnRowIdHighWatermark,
      CloseableIterable<Row> txnDataActions,
      List<DomainMetadata> txnDomainMetadatas) {
    checkArgument(
        TableFeatures.isRowTrackingSupported(txnProtocol),
        "Row ID high watermark is updated only when feature 'rowTracking' is supported.");

    // Filter out existing row tracking domainMetadata action, if any
    List<DomainMetadata> nonRowTrackingDomainMetadatas =
        txnDomainMetadatas.stream()
            .filter(dm -> !dm.getDomain().equals(RowTrackingMetadataDomain.DOMAIN_NAME))
            .collect(Collectors.toList());

    // The row ID high watermark from the snapshot of the table that this transaction is reading at.
    // Any baseRowIds higher than this watermark are assigned by this transaction.
    final long prevRowIdHighWatermark = readRowIdHighWaterMark(txnReadSnapshot);

    // Tracks the new row ID high watermark as we iterate through data actions and counting new rows
    // added in this transaction.
    final AtomicLong currRowIdHighWatermark =
        new AtomicLong(winningTxnRowIdHighWatermark.orElse(prevRowIdHighWatermark));

    // The row ID high watermark must increase monotonically, so the winning transaction's high
    // watermark (if present) must be greater than or equal to the high watermark from the
    // current transaction's read snapshot.
    checkArgument(
        currRowIdHighWatermark.get() >= prevRowIdHighWatermark,
        "The current row ID high watermark must be greater than or equal to "
            + "the high watermark from the transaction's read snapshot");

    txnDataActions.forEach(
        row -> {
          if (!row.isNullAt(SingleAction.ADD_FILE_ORDINAL)) {
            AddFile addFile = new AddFile(row.getStruct(SingleAction.ADD_FILE_ORDINAL));
            if (!addFile.getBaseRowId().isPresent()
                || addFile.getBaseRowId().get() > prevRowIdHighWatermark) {
              currRowIdHighWatermark.addAndGet(getNumRecordsOrThrow(addFile));
            }
          }
        });

    if (currRowIdHighWatermark.get() != prevRowIdHighWatermark) {
      nonRowTrackingDomainMetadatas.add(
          new RowTrackingMetadataDomain(currRowIdHighWatermark.get()).toDomainMetadata());
    }

    return nonRowTrackingDomainMetadatas;
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
