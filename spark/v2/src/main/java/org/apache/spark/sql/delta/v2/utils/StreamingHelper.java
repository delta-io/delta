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
package org.apache.spark.sql.delta.v2.utils;

import static io.delta.kernel.internal.util.Preconditions.checkArgument;
import static io.delta.kernel.internal.util.Preconditions.checkState;

import io.delta.kernel.CommitActions;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.DeltaLogActionUtils;
import io.delta.kernel.internal.actions.CommitInfo;
import io.delta.kernel.internal.actions.RemoveFile;
import io.delta.kernel.internal.commitrange.CommitRangeImpl;
import io.delta.kernel.internal.data.StructRow;
import io.delta.kernel.internal.util.Preconditions;
import java.util.*;
import org.apache.spark.annotation.Experimental;
import org.apache.spark.sql.delta.DeltaSingleCommit;
import org.apache.spark.sql.delta.actions.Action;
import org.apache.spark.sql.delta.actions.AddFile;
import org.apache.spark.sql.delta.actions.Metadata;
import org.apache.spark.sql.delta.actions.Protocol;
import org.apache.spark.sql.delta.storage.ClosableIterator;
import org.apache.spark.sql.delta.v2.interop.DeltaV2QueryContext;
import org.apache.spark.sql.delta.v2.kernel.KernelActionUtils$;
import org.apache.spark.sql.delta.v2.read.CDCDataFile;
import org.apache.spark.sql.delta.v2.tablemanager.DeltaV2TableManager;

/**
 * Helper class providing utilities for working with Delta table data in streaming scenarios.
 *
 * <p>This class provides static utility methods for extracting information from Delta table
 * batches, such as version numbers and data change actions.
 */
@Experimental
public class StreamingHelper {

  @FunctionalInterface
  private interface ActionFactory<T> {
    T apply(ColumnVector actionVector, int rowId);
  }

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
   * Extracts an action from the requested field and row using the given action factory.
   *
   * <p>Field lookup and vector retrieval happen before the action factory is invoked.
   */
  private static <T> Optional<T> getAction(
      ColumnarBatch columnarBatch,
      int rowId,
      DeltaLogActionUtils.DeltaAction deltaAction,
      ActionFactory<T> actionFactory) {
    int actionIdx = getFieldIndex(columnarBatch, deltaAction.colName);
    ColumnVector actionVector = columnarBatch.getColumnVector(actionIdx);
    T action = actionFactory.apply(actionVector, rowId);
    return Optional.ofNullable(action);
  }

  /**
   * Get the version from a {@link ColumnarBatch} of Delta log actions. Assumes all rows in the
   * batch belong to the same commit version, so it reads the version from the first row (rowId=0).
   */
  public static long getVersion(ColumnarBatch columnarBatch) {
    int versionColIdx = getFieldIndex(columnarBatch, "version");
    return columnarBatch.getColumnVector(versionColIdx).getLong(0);
  }

  /** Get AddFile action from a ColumnarBatch at the specified row, if present. */
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

    return Optional.of(
        KernelActionUtils$.MODULE$.addFileFromKernel(
            new io.delta.kernel.internal.actions.AddFile(addFileRow)));
  }

  /** Get AddFile action from a batch at the specified row, if present and has dataChange=true. */
  public static Optional<AddFile> getAddFileWithDataChange(ColumnarBatch columnarBatch, int rowId) {
    return getAddFile(columnarBatch, rowId).filter(AddFile::dataChange);
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
  public static Optional<io.delta.kernel.internal.actions.Metadata> getMetadata(
      ColumnarBatch columnarBatch, int rowId) {
    return getAction(
        columnarBatch,
        rowId,
        DeltaLogActionUtils.DeltaAction.METADATA,
        io.delta.kernel.internal.actions.Metadata::fromColumnVector);
  }

  /** Get Protocol action from a batch at the specified row, if present. */
  public static Optional<io.delta.kernel.internal.actions.Protocol> getProtocol(
      ColumnarBatch batch, int rowId) {
    return getAction(
        batch,
        rowId,
        DeltaLogActionUtils.DeltaAction.PROTOCOL,
        io.delta.kernel.internal.actions.Protocol::fromColumnVector);
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
  // TODO: callers can switch to CommitRange.getCommitActions(engine, actionSet) — the
  // no-Snapshot overload added alongside this comment makes this helper obsolete.
  // The Kernel iterator type is fully qualified because this package also defines a
  // CloseableIterator: Scala 2.12 resolves a same-package class before an explicit import when it
  // reads this signature, so Scala callers would see the wrong return type.
  public static io.delta.kernel.utils.CloseableIterator<CommitActions>
      getCommitActionsFromRangeUnsafe(
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
   * <p>Reads commit JSON through the table manager's LogStore using files listed by Kernel.
   *
   * @param startVersion inclusive starting version of the commit range
   * @param endVersionOpt inclusive ending version, or empty to read through the latest
   * @param tableManager table manager backing the table
   * @param originalStreamingQueryContext request-scoped catalog inputs used to load the commit
   *     range
   * @param failOnDataLoss whether to fail when commits in the range are missing
   */
  public static Map<Long, Metadata> collectMetadataActions(
      long startVersion,
      Optional<Long> endVersionOpt,
      DeltaV2TableManager tableManager,
      DeltaV2QueryContext originalStreamingQueryContext,
      boolean failOnDataLoss) {
    return collectActions(
        startVersion,
        endVersionOpt,
        tableManager,
        originalStreamingQueryContext,
        failOnDataLoss,
        DeltaLogActionUtils.DeltaAction.METADATA,
        Metadata.class);
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
   * <p>Reads commit JSON through the table manager's LogStore using files listed by Kernel.
   *
   * @param startVersion inclusive starting version of the commit range
   * @param endVersionOpt inclusive ending version, or empty to read through the latest
   * @param tableManager table manager backing the table
   * @param originalStreamingQueryContext request-scoped catalog inputs used to load the commit
   *     range
   * @param failOnDataLoss whether to fail when commits in the range are missing
   */
  public static Map<Long, Protocol> collectProtocolActions(
      long startVersion,
      Optional<Long> endVersionOpt,
      DeltaV2TableManager tableManager,
      DeltaV2QueryContext originalStreamingQueryContext,
      boolean failOnDataLoss) {
    return collectActions(
        startVersion,
        endVersionOpt,
        tableManager,
        originalStreamingQueryContext,
        failOnDataLoss,
        DeltaLogActionUtils.DeltaAction.PROTOCOL,
        Protocol.class);
  }

  /** Walks the commit files and rejects commits with more than one matching action. */
  private static <T extends Action> Map<Long, T> collectActions(
      long startVersion,
      Optional<Long> endVersionOpt,
      DeltaV2TableManager tableManager,
      DeltaV2QueryContext originalStreamingQueryContext,
      boolean failOnDataLoss,
      DeltaLogActionUtils.DeltaAction actionType,
      Class<T> actionClass) {
    // LinkedHashMap to preserve insertion order
    Map<Long, T> versionToAction = new LinkedHashMap<>();
    try (ClosableIterator<DeltaSingleCommit> commitsIter =
        tableManager.getSingleCommitIterator(
            startVersion,
            ScalaUtils.toScalaOption(endVersionOpt),
            originalStreamingQueryContext,
            failOnDataLoss)) {
      while (commitsIter.hasNext()) {
        DeltaSingleCommit commit = commitsIter.next();
        long version = commit.version();
        try (ClosableIterator<Action> actionsIter =
            commit.getActionsIterator(/* maxInMemoryFileSizeBytes= */ 0L)) {
          while (actionsIter.hasNext()) {
            Action action = actionsIter.next();
            if (actionClass.isInstance(action)) {
              T existing = versionToAction.putIfAbsent(version, actionClass.cast(action));
              Preconditions.checkArgument(
                  existing == null,
                  "Should not encounter two %s actions in the same commit of version %d",
                  actionType.colName,
                  version);
            }
          }
        }
      }
    } catch (RuntimeException e) {
      throw e; // Rethrow runtime exceptions directly
    } catch (Exception e) {
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
