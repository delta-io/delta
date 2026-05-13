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
import io.delta.kernel.internal.actions.CommitInfo;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.internal.actions.RemoveFile;
import io.delta.kernel.internal.commitrange.CommitRangeImpl;
import io.delta.kernel.internal.data.StructRow;
import io.delta.kernel.internal.util.Preconditions;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.spark.internal.v2.read.CDCDataFile;
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
  private static int getFieldIndex(ColumnarBatch columnarBatch, String fieldName) {
    int index = columnarBatch.getSchema().indexOf(fieldName);
    checkArgument(
        index >= 0, "Field '%s' not found in schema: %s", fieldName, columnarBatch.getSchema());
    return index;
  }

  /**
   * Get the version from a {@link ColumnarBatch} of Delta log actions. Assumes all rows in the
   * batch belong to the same commit version, so it reads the version from the first row (rowId=0).
   */
  public static long getVersion(ColumnarBatch columnarBatch) {
    int versionColIdx = getFieldIndex(columnarBatch, "version");
    return columnarBatch.getColumnVector(versionColIdx).getLong(0);
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

  /**
   * Get AddFile action from a ColumnarBatch at the specified row, if present.
   *
   * <p>Caller should ensure all rows are valid (e.g., not filtered by selection vector). For
   * FilteredColumnarBatch with selection vectors, use {@link #getAddFile(FilteredColumnarBatch,
   * int)} instead.
   */
  private static Optional<AddFile> getAddFile(ColumnarBatch columnarBatch, int rowId) {
    int addIdx = getFieldIndex(columnarBatch, DeltaLogActionUtils.DeltaAction.ADD.colName);
    ColumnVector addVector = columnarBatch.getColumnVector(addIdx);
    if (addVector.isNullAt(rowId)) {
      return Optional.empty();
    }

    Row addFileRow = StructRow.fromStructVector(addVector, rowId);
    checkState(
        addFileRow != null,
        String.format("Failed to extract AddFile struct from columnar batch at rowId=%d.", rowId));

    return Optional.of(new AddFile(addFileRow));
  }

  /** Get AddFile action from a batch at the specified row, if present and has dataChange=true. */
  public static Optional<AddFile> getAddFileWithDataChange(ColumnarBatch columnarBatch, int rowId) {
    return getAddFile(columnarBatch, rowId).filter(AddFile::getDataChange);
  }

  /**
   * Get RemoveFile action from a batch at the specified row, if present and has dataChange=true.
   */
  public static Optional<RemoveFile> getDataChangeRemove(ColumnarBatch columnarBatch, int rowId) {
    int removeIdx = getFieldIndex(columnarBatch, DeltaLogActionUtils.DeltaAction.REMOVE.colName);
    ColumnVector removeVector = columnarBatch.getColumnVector(removeIdx);
    if (removeVector.isNullAt(rowId)) {
      return Optional.empty();
    }

    Row removeFileRow = StructRow.fromStructVector(removeVector, rowId);
    checkState(
        removeFileRow != null,
        String.format(
            "Failed to extract RemoveFile struct from columnar batch at rowId=%d.", rowId));

    RemoveFile removeFile = new RemoveFile(removeFileRow);
    return removeFile.getDataChange() ? Optional.of(removeFile) : Optional.empty();
  }

  /** Get Metadata action from a batch at the specified row, if present. */
  public static Optional<Metadata> getMetadata(ColumnarBatch columnarBatch, int rowId) {
    int metadataIdx =
        getFieldIndex(columnarBatch, DeltaLogActionUtils.DeltaAction.METADATA.colName);
    ColumnVector metadataVector = columnarBatch.getColumnVector(metadataIdx);
    Metadata metadata = Metadata.fromColumnVector(metadataVector, rowId);

    return Optional.ofNullable(metadata);
  }

  /** Get Protocol action from a batch at the specified row, if present. */
  public static Optional<Protocol> getProtocol(ColumnarBatch batch, int rowId) {
    int protocolIdx = getFieldIndex(batch, DeltaLogActionUtils.DeltaAction.PROTOCOL.colName);
    ColumnVector protocolVector = batch.getColumnVector(protocolIdx);
    Protocol protocol = Protocol.fromColumnVector(protocolVector, rowId);

    return Optional.ofNullable(protocol);
  }

  /** Get CommitInfo action from a batch at the specified row, if present. */
  public static Optional<CommitInfo> getCommitInfo(ColumnarBatch columnarBatch, int rowId) {
    int commitInfoIdx =
        getFieldIndex(columnarBatch, DeltaLogActionUtils.DeltaAction.COMMITINFO.colName);
    ColumnVector commitInfoVector = columnarBatch.getColumnVector(commitInfoIdx);
    CommitInfo commitInfo = CommitInfo.fromColumnVector(commitInfoVector, rowId);

    return Optional.ofNullable(commitInfo);
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
   * Collects {@link Metadata} actions from commits in {@code [startVersion, endVersionOpt]}
   * (inclusive on both ends).
   *
   * <p><b>Returns:</b> a {@link LinkedHashMap} from commit version to its metadata action,
   * preserving ascending version order. Versions with no metadata action are omitted.
   *
   * <p><b>Throws:</b> {@link IllegalArgumentException} if a single commit contains more than one
   * metadata action; {@link RuntimeException} on underlying I/O errors.
   *
   * <p><b>Unsafe:</b> bypasses the snapshot-based protocol validation that {@link
   * io.delta.kernel.CommitRange#getCommitActions} would normally perform. Callers are responsible
   * for ensuring protocol compatibility.
   *
   * @param startVersion inclusive starting version of the commit range
   * @param endVersionOpt inclusive ending version, or empty to read through the latest
   * @param snapshotManager snapshot manager backing the table
   * @param engine Delta kernel engine
   * @param tablePath path to the Delta table
   */
  public static Map<Long, Metadata> collectMetadataActionsFromRangeUnsafe(
      long startVersion,
      Optional<Long> endVersionOpt,
      DeltaSnapshotManager snapshotManager,
      Engine engine,
      String tablePath) {
    return collectActionsFromRangeUnsafe(
        startVersion,
        endVersionOpt,
        snapshotManager,
        engine,
        tablePath,
        DeltaLogActionUtils.DeltaAction.METADATA,
        StreamingHelper::getMetadata);
  }

  /**
   * Collects {@link Protocol} actions from commits in {@code [startVersion, endVersionOpt]}
   * (inclusive on both ends).
   *
   * <p><b>Returns:</b> a {@link LinkedHashMap} from commit version to its protocol action,
   * preserving ascending version order. Versions with no protocol action are omitted.
   *
   * <p><b>Throws:</b> {@link IllegalArgumentException} if a single commit contains more than one
   * protocol action; {@link RuntimeException} on underlying I/O errors.
   *
   * <p><b>Unsafe:</b> bypasses the snapshot-based protocol validation that {@link
   * io.delta.kernel.CommitRange#getCommitActions} would normally perform. Callers are responsible
   * for ensuring protocol compatibility.
   *
   * @param startVersion inclusive starting version of the commit range
   * @param endVersionOpt inclusive ending version, or empty to read through the latest
   * @param snapshotManager snapshot manager backing the table
   * @param engine Delta kernel engine
   * @param tablePath path to the Delta table
   */
  public static Map<Long, Protocol> collectProtocolActionsFromRangeUnsafe(
      long startVersion,
      Optional<Long> endVersionOpt,
      DeltaSnapshotManager snapshotManager,
      Engine engine,
      String tablePath) {
    return collectActionsFromRangeUnsafe(
        startVersion,
        endVersionOpt,
        snapshotManager,
        engine,
        tablePath,
        DeltaLogActionUtils.DeltaAction.PROTOCOL,
        StreamingHelper::getProtocol);
  }

  /** Extracts an action of type {@code T} from a single row of a {@link ColumnarBatch}. */
  @FunctionalInterface
  private interface RowExtractor<T> {
    Optional<T> extract(ColumnarBatch batch, int rowId);
  }

  /**
   * Shared implementation for {@link #collectMetadataActionsFromRangeUnsafe} and {@link
   * #collectProtocolActionsFromRangeUnsafe}: walks the commit range filtered to {@code actionType},
   * applies {@code extractor} per row, and rejects commits with more than one matching action.
   */
  private static <T> Map<Long, T> collectActionsFromRangeUnsafe(
      long startVersion,
      Optional<Long> endVersionOpt,
      DeltaSnapshotManager snapshotManager,
      Engine engine,
      String tablePath,
      DeltaLogActionUtils.DeltaAction actionType,
      RowExtractor<T> extractor) {
    CommitRangeImpl commitRange =
        (CommitRangeImpl) snapshotManager.getTableChanges(engine, startVersion, endVersionOpt);
    // LinkedHashMap to preserve insertion order
    Map<Long, T> versionToAction = new LinkedHashMap<>();

    try (CloseableIterator<CommitActions> commitsIter =
        getCommitActionsFromRangeUnsafe(engine, commitRange, tablePath, Set.of(actionType))) {
      while (commitsIter.hasNext()) {
        try (CommitActions commit = commitsIter.next()) {
          long version = commit.getVersion();
          try (CloseableIterator<ColumnarBatch> actionsIter = commit.getActions()) {
            while (actionsIter.hasNext()) {
              ColumnarBatch batch = actionsIter.next();
              int numRows = batch.getSize();
              for (int rowId = 0; rowId < numRows; rowId++) {
                Optional<T> actionOpt = extractor.extract(batch, rowId);
                if (actionOpt.isPresent()) {
                  T existing = versionToAction.putIfAbsent(version, actionOpt.get());
                  Preconditions.checkArgument(
                      existing == null,
                      "Should not encounter two %s actions in the same commit of version %d",
                      actionType.colName,
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
    return versionToAction;
  }

  /** Get explicit CDC file (AddCDCFile) from a batch at the specified row, if present. */
  public static Optional<CDCDataFile> getCDCFile(
      ColumnarBatch batch, int rowId, long commitTimestamp) {
    int cdcIdx = getFieldIndex(batch, DeltaLogActionUtils.DeltaAction.CDC.colName);
    ColumnVector cdcVector = batch.getColumnVector(cdcIdx);
    if (cdcVector.isNullAt(rowId)) {
      return Optional.empty();
    }

    Row cdcRow = StructRow.fromStructVector(cdcVector, rowId);
    checkState(
        cdcRow != null,
        String.format("Failed to extract CDC struct from batch at rowId=%d.", rowId));

    return Optional.of(CDCDataFile.fromAddCDCFile(cdcRow, commitTimestamp));
  }

  /** Private constructor to prevent instantiation of this utility class. */
  private StreamingHelper() {}
}
