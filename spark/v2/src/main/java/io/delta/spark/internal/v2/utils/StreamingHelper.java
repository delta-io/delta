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
package io.delta.spark.internal.v2.utils;

import static io.delta.kernel.internal.util.Preconditions.checkArgument;
import static io.delta.kernel.internal.util.Preconditions.checkState;

import io.delta.kernel.CommitActions;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.DeltaLogActionUtils;
import io.delta.kernel.internal.actions.AddFile;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.RemoveFile;
import io.delta.kernel.internal.commitrange.CommitRangeImpl;
import io.delta.kernel.internal.data.StructRow;
import io.delta.kernel.internal.util.Preconditions;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.spark.internal.v2.snapshot.DeltaSnapshotManager;
import java.io.IOException;
import java.util.*;
import org.apache.spark.annotation.Experimental;

/**
 * Helper class providing utilities for working with Delta table data in streaming scenarios.
 *
 * <p>This class provides static utility methods for extracting information from Delta table
 * batches, such as version numbers and data change actions.
 */
@Experimental
public class StreamingHelper {

  /**
   * Returns the index of the field with the given name in the schema of the batch. Throws an {@link
   * IllegalArgumentException} if the field is not found.
   */
  private static int getFieldIndex(ColumnarBatch batch, String fieldName) {
    int index = batch.getSchema().indexOf(fieldName);
    checkArgument(index >= 0, "Field '%s' not found in schema: %s", fieldName, batch.getSchema());
    return index;
  }

  /**
   * Get the version from a {@link ColumnarBatch} of Delta log actions. Assumes all rows in the
   * batch belong to the same commit version, so it reads the version from the first row (rowId=0).
   */
  public static long getVersion(ColumnarBatch batch) {
    int versionColIdx = getFieldIndex(batch, "version");
    return batch.getColumnVector(versionColIdx).getLong(0);
  }

  /**
   * Get AddFile action from a FilteredColumnarBatch at the specified row, if present.
   *
   * <p>This method respects the selection vector to filter out duplicate files that may appear when
   * stats re-collection (e.g., ANALYZE TABLE COMPUTE STATISTICS) re-adds files with updated stats.
   * The Kernel uses selection vectors to mark which rows (AddFiles) are logically valid.
   *
   * @param batch the FilteredColumnarBatch containing AddFile actions
   * @param rowId the row index to check
   * @return Optional containing the AddFile if present and selected, empty otherwise
   */
  public static Optional<AddFile> getAddFile(FilteredColumnarBatch batch, int rowId) {
    // Check selection vector first - rows may be filtered out when stats re-collection
    // re-adds files with updated stats
    Optional<ColumnVector> selectionVector = batch.getSelectionVector();
    boolean isFiltered =
        selectionVector.map(sv -> sv.isNullAt(rowId) || !sv.getBoolean(rowId)).orElse(false);
    if (isFiltered) {
      return Optional.empty();
    }

    return getAddFile(batch.getData(), rowId);
  }

  /** Get AddFile action from a ColumnarBatch at the specified row, if present. */
  private static Optional<AddFile> getAddFile(ColumnarBatch batch, int rowId) {
    int addIdx = getFieldIndex(batch, DeltaLogActionUtils.DeltaAction.ADD.colName);
    ColumnVector addVector = batch.getColumnVector(addIdx);
    if (addVector.isNullAt(rowId)) {
      return Optional.empty();
    }

    Row addFileRow = StructRow.fromStructVector(addVector, rowId);
    checkState(
        addFileRow != null,
        String.format("Failed to extract AddFile struct from batch at rowId=%d.", rowId));

    return Optional.of(new AddFile(addFileRow));
  }

  /** Get AddFile action from a batch at the specified row, if present and has dataChange=true. */
  public static Optional<AddFile> getAddFileWithDataChange(ColumnarBatch batch, int rowId) {
    return getAddFile(batch, rowId).filter(AddFile::getDataChange);
  }

  /**
   * Get RemoveFile action from a batch at the specified row, if present and has dataChange=true.
   */
  public static Optional<RemoveFile> getDataChangeRemove(ColumnarBatch batch, int rowId) {
    int removeIdx = getFieldIndex(batch, DeltaLogActionUtils.DeltaAction.REMOVE.colName);
    ColumnVector removeVector = batch.getColumnVector(removeIdx);
    if (removeVector.isNullAt(rowId)) {
      return Optional.empty();
    }

    Row removeFileRow = StructRow.fromStructVector(removeVector, rowId);
    checkState(
        removeFileRow != null,
        String.format("Failed to extract RemoveFile struct from batch at rowId=%d.", rowId));

    RemoveFile removeFile = new RemoveFile(removeFileRow);
    return removeFile.getDataChange() ? Optional.of(removeFile) : Optional.empty();
  }

  /** Get Metadata action from a batch at the specified row, if present. */
  public static Optional<Metadata> getMetadata(ColumnarBatch batch, int rowId) {
    int metadataIdx = getFieldIndex(batch, DeltaLogActionUtils.DeltaAction.METADATA.colName);
    ColumnVector metadataVector = batch.getColumnVector(metadataIdx);
    Metadata metadata = Metadata.fromColumnVector(metadataVector, rowId);

    return Optional.ofNullable(metadata);
  }

  /**
   * Gets commit-level actions from a commit range without requiring a snapshot at the exact start
   * version.
   *
   * <p>Returns an iterator over {@link CommitActions}, where each CommitActions represents a single
   * commit.
   *
   * <p>This method is "unsafe" because it bypasses the standard {@code
   * CommitRange.getCommitActions()} API which requires a snapshot at the exact start version for
   * protocol validation.
   *
   * @param engine the Delta engine
   * @param commitRange the commit range to read actions from
   * @param tablePath the path to the Delta table
   * @param actionSet the set of actions to read (e.g., ADD, REMOVE)
   * @return an iterator over {@link CommitActions}, one per commit version
   */
  public static CloseableIterator<CommitActions> getCommitActionsFromRangeUnsafe(
      Engine engine,
      CommitRangeImpl commitRange,
      String tablePath,
      Set<DeltaLogActionUtils.DeltaAction> actionSet) {
    return DeltaLogActionUtils.getActionsFromCommitFilesWithProtocolValidation(
        engine, tablePath, commitRange.getDeltaFiles(), actionSet);
  }

  /**
   * Collects metadata actions from a commit range, mapping each version to its metadata.
   *
   * <p>This method is "unsafe" because it uses {@code getActionsFromRangeUnsafe()} which bypasses
   * the standard snapshot requirement for protocol validation.
   *
   * <p>Returns a map preserving version order (via LinkedHashMap) where each version maps to its
   * metadata action. Throws an exception if multiple metadata actions are found in the same commit.
   *
   * @param startVersion the starting version (inclusive) of the commit range
   * @param endVersionOpt optional ending version (exclusive) of the commit range
   * @param snapshotManager the Delta snapshot manager
   * @param engine the Delta engine
   * @param tablePath the path to the Delta table
   * @return a map from version number to metadata action, in version order
   */
  public static Map<Long, Metadata> collectMetadataActionsFromRangeUnsafe(
      long startVersion,
      Optional<Long> endVersionOpt,
      DeltaSnapshotManager snapshotManager,
      Engine engine,
      String tablePath) {
    CommitRangeImpl commitRange =
        (CommitRangeImpl) snapshotManager.getTableChanges(engine, startVersion, endVersionOpt);
    // LinkedHashMap to preserve insertion order
    Map<Long, Metadata> versionToMetadata = new LinkedHashMap<>();

    try (CloseableIterator<CommitActions> commitsIter =
        getCommitActionsFromRangeUnsafe(
            engine, commitRange, tablePath, Set.of(DeltaLogActionUtils.DeltaAction.METADATA))) {
      while (commitsIter.hasNext()) {
        try (CommitActions commit = commitsIter.next()) {
          long version = commit.getVersion();
          try (CloseableIterator<ColumnarBatch> actionsIter = commit.getActions()) {
            while (actionsIter.hasNext()) {
              ColumnarBatch batch = actionsIter.next();
              int numRows = batch.getSize();
              for (int rowId = 0; rowId < numRows; rowId++) {
                Optional<Metadata> metadataOpt = StreamingHelper.getMetadata(batch, rowId);
                if (metadataOpt.isPresent()) {
                  Metadata existing = versionToMetadata.putIfAbsent(version, metadataOpt.get());
                  Preconditions.checkArgument(
                      existing == null,
                      "Should not encounter two metadata actions in the same commit of version %d",
                      version);
                }
              }
            }
          } catch (IOException e) {
            throw new RuntimeException("Failed to process commit at version " + version, e);
          }
        }
      }
    } catch (RuntimeException e) {
      throw e; // Rethrow runtime exceptions directly
    } catch (Exception e) {
      // CommitActions.close() throws Exception
      throw new RuntimeException("Failed to process commits", e);
    }

    return versionToMetadata;
  }

  /** Private constructor to prevent instantiation of this utility class. */
  private StreamingHelper() {}
}
