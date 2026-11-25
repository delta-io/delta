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
import io.delta.kernel.internal.actions.RemoveFile;
import io.delta.kernel.internal.commitrange.CommitRangeImpl;
import io.delta.kernel.internal.data.StructRow;
import java.util.*;
import org.apache.spark.annotation.Experimental;
import org.apache.spark.sql.delta.actions.AddFile;
import org.apache.spark.sql.delta.v2.kernel.KernelActionUtils$;

/**
 * Helper class providing utilities for working with Delta table data in streaming scenarios.
 *
 * <p>This class provides static utility methods for extracting information from Delta table
 * batches, such as metadata and data change actions.
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

  /** Private constructor to prevent instantiation of this utility class. */
  private StreamingHelper() {}
}
