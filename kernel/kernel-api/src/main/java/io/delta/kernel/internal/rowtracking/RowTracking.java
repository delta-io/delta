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
import io.delta.kernel.internal.TableConfig;
import io.delta.kernel.internal.actions.*;
import io.delta.kernel.internal.tablefeatures.TableFeatures;
import io.delta.kernel.types.FieldMetadata;
import io.delta.kernel.types.MetadataColumnSpec;
import io.delta.kernel.types.StructField;
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

  // Metadata keys for row tracking fields (following Delta protocol specification)
  private static final String ROW_TRACKING_METADATA_TYPE_KEY = "__metadata_type";
  private static final String METADATA_TYPE_CONSTANT = "constant";
  private static final String METADATA_TYPE_GENERATED = "generated";
  private static final String BASE_ROW_ID_METADATA_COL_ATTR_KEY = "__base_row_id_metadata_col";
  private static final String DEFAULT_ROW_COMMIT_VERSION_METADATA_COL_ATTR_KEY =
      "__default_row_version_metadata_col";
  private static final String ROW_ID_METADATA_COL_ATTR_KEY = "__row_id_metadata_col";
  private static final String ROW_COMMIT_VERSION_METADATA_COL_ATTR_KEY =
      "__row_commit_version_metadata_col";

  /**
   * Checks if the provided field is a row tracking column, i.e., either the row ID or the row
   * commit version column.
   *
   * @param field the field to check
   * @return true if the field is a row tracking column, false otherwise
   */
  public static boolean isRowTrackingColumn(StructField field) {
    return field.isMetadataColumn()
        && (field.getMetadataColumnSpec() == MetadataColumnSpec.ROW_ID
            || field.getMetadataColumnSpec() == MetadataColumnSpec.ROW_COMMIT_VERSION);
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
   * @param txnReadSnapshotOpt the snapshot of the table that this transaction is reading from
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
      Optional<SnapshotImpl> txnReadSnapshotOpt,
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
        final long prevRowIdHighWatermark = readRowIdHighWaterMark(txnReadSnapshotOpt);

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
   * @param txnReadSnapshotOpt the snapshot of the table that this transaction is reading at
   * @param txnProtocol the (updated, if any) protocol that will result from this txn
   * @param winningTxnRowIdHighWatermark the latest row ID high watermark from the winning
   *     transaction. Should be empty for initial assignment and present for conflict resolution.
   * @param txnDataActions a {@link CloseableIterable} of data actions this txn is trying to commit
   * @param txnDomainMetadatas a list of domain metadata actions this txn is trying to commit
   * @param providedRowIdHighWatermark Optional row ID high watermark explicitly provided by the
   *     transaction builder.
   * @return Updated list of domain metadata actions for commit
   */
  public static List<DomainMetadata> updateRowIdHighWatermarkIfNeeded(
      Optional<SnapshotImpl> txnReadSnapshotOpt,
      Protocol txnProtocol,
      Optional<Long> winningTxnRowIdHighWatermark,
      CloseableIterable<Row> txnDataActions,
      List<DomainMetadata> txnDomainMetadatas,
      Optional<Long> providedRowIdHighWatermark) {
    checkArgument(
        TableFeatures.isRowTrackingSupported(txnProtocol),
        "Row ID high watermark is updated only when feature 'rowTracking' is supported.");
    checkArgument(
        !(providedRowIdHighWatermark.isPresent() && winningTxnRowIdHighWatermark.isPresent()),
        "Conflict resolution is not allowed when an explicit row tracking high "
            + "watermark is provided. Please recommit.");

    // Filter out existing row tracking domainMetadata action, if any
    List<DomainMetadata> nonRowTrackingDomainMetadatas =
        txnDomainMetadatas.stream()
            .filter(dm -> !dm.getDomain().equals(RowTrackingMetadataDomain.DOMAIN_NAME))
            .collect(Collectors.toList());

    // The row ID high watermark from the snapshot of the table that this transaction is reading at.
    // Any baseRowIds higher than this watermark are assigned by this transaction.
    final long prevRowIdHighWatermark = readRowIdHighWaterMark(txnReadSnapshotOpt);

    // Tracks the new row ID high watermark as we iterate through data actions and counting new rows
    // added in this transaction.
    final AtomicLong currCalculatedRowIdHighWatermark =
        new AtomicLong(winningTxnRowIdHighWatermark.orElse(prevRowIdHighWatermark));

    // The row ID high watermark must increase monotonically, so the winning transaction's high
    // watermark (if present) must be greater than or equal to the high watermark from the
    // current transaction's read snapshot.
    checkArgument(
        currCalculatedRowIdHighWatermark.get() >= prevRowIdHighWatermark,
        "The current row ID high watermark must be greater than or equal to "
            + "the high watermark from the transaction's read snapshot");

    txnDataActions.forEach(
        row -> {
          if (!row.isNullAt(SingleAction.ADD_FILE_ORDINAL)) {
            AddFile addFile = new AddFile(row.getStruct(SingleAction.ADD_FILE_ORDINAL));
            if (!addFile.getBaseRowId().isPresent()
                || addFile.getBaseRowId().get() > prevRowIdHighWatermark) {
              currCalculatedRowIdHighWatermark.addAndGet(getNumRecordsOrThrow(addFile));
            }
          }
        });

    // If the txn builder has explicitly provided a row ID high watermark, we should use that value
    // instead of the one calculated from the current row ID high watermark and uncommitted data
    // actions. Validate that the provided value is greater than or equal to the calculated
    // watermark to
    // ensure consistency.
    providedRowIdHighWatermark.ifPresent(
        providedHighWatermark ->
            checkArgument(
                providedHighWatermark >= currCalculatedRowIdHighWatermark.get(),
                String.format(
                    "The provided row ID high watermark (%d) must be greater than or equal to "
                        + "the calculated row ID high watermark (%d) based on the transaction's "
                        + "data actions.",
                    providedHighWatermark, currCalculatedRowIdHighWatermark.get())));

    final long finalRowIdHighWatermark =
        providedRowIdHighWatermark.orElse(currCalculatedRowIdHighWatermark.get());

    if (finalRowIdHighWatermark != prevRowIdHighWatermark) {
      nonRowTrackingDomainMetadatas.add(
          new RowTrackingMetadataDomain(finalRowIdHighWatermark).toDomainMetadata());
    }

    return nonRowTrackingDomainMetadatas;
  }

  /**
   * Throws an exception if row tracking enablement is toggled between the old and the new metadata.
   */
  public static void throwIfRowTrackingToggled(Metadata oldMetadata, Metadata newMetadata) {
    boolean oldRowTrackingEnabledValue = TableConfig.ROW_TRACKING_ENABLED.fromMetadata(oldMetadata);
    boolean newRowTrackingEnabledValue = TableConfig.ROW_TRACKING_ENABLED.fromMetadata(newMetadata);
    if (oldRowTrackingEnabledValue != newRowTrackingEnabledValue) {
      throw DeltaErrors.cannotToggleRowTrackingOnExistingTable();
    }
  }

  /**
   * Reads the current row ID high watermark from the snapshot, or returns a default value if
   * missing.
   */
  private static long readRowIdHighWaterMark(Optional<SnapshotImpl> snapshotOpt) {
    return snapshotOpt
        .flatMap(RowTrackingMetadataDomain::fromSnapshot)
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

  /**
   * Check if row tracking is enabled for reading.
   *
   * @param protocol the protocol to check
   * @param metadata the metadata to check
   * @return true if row tracking is enabled
   */
  public static boolean isEnabled(Protocol protocol, Metadata metadata) {
    boolean isEnabled = TableConfig.ROW_TRACKING_ENABLED.fromMetadata(metadata);
    if (isEnabled && !TableFeatures.isRowTrackingSupported(protocol)) {
      throw new IllegalStateException(
          "Table property 'delta.enableRowTracking' is set on the table but this table version "
              + "doesn't support the 'rowTracking' table feature.");
    }
    return isEnabled;
  }

  /**
   * Create the base_row_id field for reading. This is a constant field physically stored in Parquet
   * files.
   *
   * @param protocol the protocol
   * @param metadata the metadata
   * @param nullable whether the field should be nullable
   * @return Optional containing the StructField if row tracking is enabled, empty otherwise
   */
  public static Optional<StructField> createBaseRowIdField(
      Protocol protocol, Metadata metadata, boolean nullable) {
    if (!isEnabled(protocol, metadata)) {
      return Optional.empty();
    }
    return Optional.of(
        new StructField(
            "base_row_id",
            io.delta.kernel.types.LongType.LONG,
            nullable,
            createConstantFieldMetadata(BASE_ROW_ID_METADATA_COL_ATTR_KEY)));
  }

  /**
   * Create the default_row_commit_version field for reading. This is a constant field physically
   * stored in Parquet files.
   *
   * @param protocol the protocol
   * @param metadata the metadata
   * @param nullable whether the field should be nullable
   * @return Optional containing the StructField if row tracking is enabled, empty otherwise
   */
  public static Optional<StructField> createDefaultRowCommitVersionField(
      Protocol protocol, Metadata metadata, boolean nullable) {
    if (!isEnabled(protocol, metadata)) {
      return Optional.empty();
    }
    return Optional.of(
        new StructField(
            "default_row_commit_version",
            io.delta.kernel.types.LongType.LONG,
            nullable,
            createConstantFieldMetadata(DEFAULT_ROW_COMMIT_VERSION_METADATA_COL_ATTR_KEY)));
  }

  /**
   * Create the row_id field for reading. This is a generated field computed at read time.
   *
   * @param protocol the protocol
   * @param metadata the metadata
   * @param nullable whether the field should be nullable
   * @return Optional containing the StructField if row tracking is enabled, empty otherwise
   */
  public static Optional<StructField> createRowIdField(
      Protocol protocol, Metadata metadata, boolean nullable) {
    if (!isEnabled(protocol, metadata)) {
      return Optional.empty();
    }
    return Optional.of(
        new StructField(
            "row_id",
            io.delta.kernel.types.LongType.LONG,
            nullable,
            createGeneratedFieldMetadata(ROW_ID_METADATA_COL_ATTR_KEY)));
  }

  /**
   * Create the row_commit_version field for reading. This is a generated field computed at read
   * time.
   *
   * @param protocol the protocol
   * @param metadata the metadata
   * @param nullable whether the field should be nullable
   * @return Optional containing the StructField if row tracking is enabled, empty otherwise
   */
  public static Optional<StructField> createRowCommitVersionField(
      Protocol protocol, Metadata metadata, boolean nullable) {
    if (!isEnabled(protocol, metadata)) {
      return Optional.empty();
    }
    return Optional.of(
        new StructField(
            "row_commit_version",
            io.delta.kernel.types.LongType.LONG,
            nullable,
            createGeneratedFieldMetadata(ROW_COMMIT_VERSION_METADATA_COL_ATTR_KEY)));
  }

  /**
   * Create the row tracking metadata struct fields for reading. This combines all row tracking
   * fields (both constant and generated) with proper Delta protocol metadata attributes.
   *
   * @param protocol the protocol
   * @param metadata the metadata
   * @param nullableConstantFields whether constant fields should be nullable
   * @param nullableGeneratedFields whether generated fields should be nullable
   * @return list of struct fields for row tracking metadata
   */
  public static List<StructField> createMetadataStructFields(
      Protocol protocol,
      Metadata metadata,
      boolean nullableConstantFields,
      boolean nullableGeneratedFields) {
    List<StructField> fields = new java.util.ArrayList<>();

    // Add base_row_id (constant field)
    createBaseRowIdField(protocol, metadata, nullableConstantFields).ifPresent(fields::add);

    // Add default_row_commit_version (constant field)
    createDefaultRowCommitVersionField(protocol, metadata, nullableConstantFields)
        .ifPresent(fields::add);

    // Add row_id (generated field)
    createRowIdField(protocol, metadata, nullableGeneratedFields).ifPresent(fields::add);

    // Add row_commit_version (generated field)
    createRowCommitVersionField(protocol, metadata, nullableGeneratedFields).ifPresent(fields::add);

    return fields;
  }

  /**
   * Create metadata for constant row tracking fields (physically stored in files).
   *
   * @param attrKey the specific attribute key for this field
   * @return FieldMetadata marking this as a constant field
   */
  private static FieldMetadata createConstantFieldMetadata(String attrKey) {
    return new FieldMetadata.Builder()
        .putString(ROW_TRACKING_METADATA_TYPE_KEY, METADATA_TYPE_CONSTANT)
        .putBoolean(attrKey, true)
        .build();
  }

  /**
   * Create metadata for generated row tracking fields (computed at read time).
   *
   * @param attrKey the specific attribute key for this field
   * @return FieldMetadata marking this as a generated field
   */
  private static FieldMetadata createGeneratedFieldMetadata(String attrKey) {
    return new FieldMetadata.Builder()
        .putString(ROW_TRACKING_METADATA_TYPE_KEY, METADATA_TYPE_GENERATED)
        .putBoolean(attrKey, true)
        .build();
  }
}
