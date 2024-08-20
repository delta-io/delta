/*
 * Copyright (2023) The Delta Lake Project Authors.
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
package io.delta.kernel.internal.replay;

import static io.delta.kernel.internal.DeltaErrors.wrapEngineException;
import static io.delta.kernel.internal.replay.LogReplay.ADD_FILE_DV_ORDINAL;
import static io.delta.kernel.internal.replay.LogReplay.ADD_FILE_ORDINAL;
import static io.delta.kernel.internal.replay.LogReplay.ADD_FILE_PATH_ORDINAL;
import static io.delta.kernel.internal.replay.LogReplay.REMOVE_FILE_DV_ORDINAL;
import static io.delta.kernel.internal.replay.LogReplay.REMOVE_FILE_ORDINAL;
import static io.delta.kernel.internal.replay.LogReplay.REMOVE_FILE_PATH_ORDINAL;
import static io.delta.kernel.internal.replay.LogReplayUtils.pathToUri;
import static io.delta.kernel.internal.replay.LogReplayUtils.prepareSelectionVectorBuffer;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.expressions.ExpressionEvaluator;
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.internal.InternalScanFileUtils;
import io.delta.kernel.internal.actions.DeletionVectorDescriptor;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.replay.LogReplayUtils.UniqueFileActionTuple;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.types.StringType;
import io.delta.kernel.utils.CloseableIterator;
import java.io.IOException;
import java.net.URI;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class takes an iterator of ({@link ColumnarBatch}, isFromCheckpoint), where the columnar
 * data inside the columnar batch represents has top level columns "add" and "remove", and produces
 * an iterator of {@link FilteredColumnarBatch} with only the "add" column and with a selection
 * vector indicating which AddFiles are still active in the table (have not been tombstoned).
 */
public class ActiveAddFilesIterator implements CloseableIterator<FilteredColumnarBatch> {
  private static final Logger logger = LoggerFactory.getLogger(ActiveAddFilesIterator.class);

  private final Engine engine;
  private final Path tableRoot;

  private final CloseableIterator<ActionWrapper> iter;

  private final Set<UniqueFileActionTuple> tombstonesFromJson;
  private final Set<UniqueFileActionTuple> addFilesFromJson;

  private Optional<FilteredColumnarBatch> next;
  /**
   * This buffer is reused across batches to keep the memory allocations minimal. It is resized as
   * required and the array entries are reset between batches.
   */
  private boolean[] selectionVectorBuffer;

  private ExpressionEvaluator tableRootVectorGenerator;
  private boolean closed;

  /**
   * Metrics capturing the state reconstruction log replay. These counters are updated as the
   * iterator is consumed and printed when the iterator is closed.
   */
  private LogReplayMetrics metrics = new LogReplayMetrics();

  ActiveAddFilesIterator(Engine engine, CloseableIterator<ActionWrapper> iter, Path tableRoot) {
    this.engine = engine;
    this.tableRoot = tableRoot;
    this.iter = iter;
    this.tombstonesFromJson = new HashSet<>();
    this.addFilesFromJson = new HashSet<>();
    this.next = Optional.empty();
  }

  @Override
  public boolean hasNext() {
    if (closed) {
      throw new IllegalStateException("Can't call `hasNext` on a closed iterator.");
    }
    if (!next.isPresent()) {
      prepareNext();
    }
    return next.isPresent();
  }

  @Override
  public FilteredColumnarBatch next() {
    if (closed) {
      throw new IllegalStateException("Can't call `next` on a closed iterator.");
    }
    if (!hasNext()) {
      throw new NoSuchElementException();
    }

    // By the definition of `hasNext`, we know that `next` is non-empty

    final FilteredColumnarBatch ret = next.get();
    next = Optional.empty();
    return ret;
  }

  @Override
  public void close() throws IOException {
    closed = true;
    Utils.closeCloseables(iter);

    // Log the metrics of the log replay of actions that are consumed so far. If the iterator
    // is closed before consuming all the actions, the metrics will be partial.
    logger.info("Active add file finding log replay metrics: {}", metrics);
  }

  /**
   * Grabs the next FileDataReadResult from `iter` and updates the value of `next`.
   *
   * <p>Internally, implements the following algorithm: 1. read all the RemoveFiles in the next
   * ColumnarBatch to update the `tombstonesFromJson` set 2. read all the AddFiles in that same
   * ColumnarBatch, unselecting ones that have already been removed or returned by updating a
   * selection vector 3. produces a DataReadResult by dropping that RemoveFile column from the
   * ColumnarBatch and using that selection vector
   *
   * <p>Note that, according to the Delta protocol, "a valid [Delta] version is restricted to
   * contain at most one file action of the same type (i.e. add/remove) for any one combination of
   * path and dvId". This means that step 2 could actually come before 1 - there's no temporal
   * dependency between them.
   *
   * <p>Ensures that - `next` is non-empty if there is a next result - `next` is empty if there is
   * no next result
   */
  private void prepareNext() {
    if (next.isPresent()) {
      return; // already have a next result
    }
    if (!iter.hasNext()) {
      return; // no next result, and no batches to read
    }

    final ActionWrapper _next = iter.next();
    final ColumnarBatch addRemoveColumnarBatch = _next.getColumnarBatch();
    final boolean isFromCheckpoint = _next.isFromCheckpoint();

    // Step 1: Update `tombstonesFromJson` with all the RemoveFiles in this columnar batch, if
    //         and only if this batch is not from a checkpoint.
    //
    //         There's no reason to put a RemoveFile from a checkpoint into `tombstonesFromJson`
    //         since, when we generate a checkpoint, any corresponding AddFile would have
    //         been excluded already
    if (!isFromCheckpoint) {
      final ColumnVector removesVector =
          addRemoveColumnarBatch.getColumnVector(REMOVE_FILE_ORDINAL);
      for (int rowId = 0; rowId < removesVector.getSize(); rowId++) {
        if (removesVector.isNullAt(rowId)) {
          continue;
        }

        // Note: this row doesn't represent the complete RemoveFile schema. It only contains
        //       the fields we need for this replay.
        final String path = getRemoveFilePath(removesVector, rowId);
        final URI pathAsUri = pathToUri(path);
        final Optional<String> dvId =
            Optional.ofNullable(getRemoveFileDV(removesVector, rowId))
                .map(DeletionVectorDescriptor::getUniqueId);
        final UniqueFileActionTuple key = new UniqueFileActionTuple(pathAsUri, dvId);
        tombstonesFromJson.add(key);
        metrics.incNumTombstonesSeen();
      }
    }

    // Step 2: Iterate over all the AddFiles in this columnar batch in order to build up the
    //         selection vector. We unselect an AddFile when it was removed by a RemoveFile
    final ColumnVector addsVector = addRemoveColumnarBatch.getColumnVector(ADD_FILE_ORDINAL);
    selectionVectorBuffer =
        prepareSelectionVectorBuffer(selectionVectorBuffer, addsVector.getSize());
    boolean atLeastOneUnselected = false;

    for (int rowId = 0; rowId < addsVector.getSize(); rowId++) {
      if (addsVector.isNullAt(rowId)) {
        atLeastOneUnselected = true;
        continue; // selectionVector will be `false` at rowId by default
      }

      metrics.incNumAddFilesSeen();
      if (!isFromCheckpoint) {
        metrics.incNumAddFilesSeenFromDeltaFiles();
      }

      final String path = getAddFilePath(addsVector, rowId);
      final URI pathAsUri = pathToUri(path);
      final Optional<String> dvId =
          Optional.ofNullable(getAddFileDV(addsVector, rowId))
              .map(DeletionVectorDescriptor::getUniqueId);
      final UniqueFileActionTuple key = new UniqueFileActionTuple(pathAsUri, dvId);
      final boolean alreadyDeleted = tombstonesFromJson.contains(key);
      final boolean alreadyReturned = addFilesFromJson.contains(key);

      boolean doSelect = false;

      if (!alreadyReturned) {
        // Note: No AddFile will appear twice in a checkpoint, so we only need
        //       non-checkpoint AddFiles in the set. When stats are recomputed the same
        //       AddFile is added with stats without remove it first.
        if (!isFromCheckpoint) {
          addFilesFromJson.add(key);
        }

        if (!alreadyDeleted) {
          doSelect = true;
          selectionVectorBuffer[rowId] = true;
          metrics.incNumActiveAddFiles();
        }
      } else {
        metrics.incNumDuplicateAddFiles();
      }

      if (!doSelect) {
        atLeastOneUnselected = true;
      }
    }

    // Step 3: Drop the RemoveFile column and use the selection vector to build a new
    //         FilteredColumnarBatch
    ColumnarBatch scanAddFiles = addRemoveColumnarBatch.withDeletedColumnAt(1);

    // Step 4: TODO: remove this step. This is a temporary requirement until the path
    //         in `add` is converted to absolute path.
    final ColumnarBatch finalScanAddFiles = scanAddFiles;
    if (tableRootVectorGenerator == null) {
      tableRootVectorGenerator =
          wrapEngineException(
              () ->
                  engine
                      .getExpressionHandler()
                      .getEvaluator(
                          finalScanAddFiles.getSchema(),
                          Literal.ofString(tableRoot.toUri().toString(), "UTF8_BINARY"),
                          StringType.STRING),
              "Get the expression evaluator for the table root");
    }
    ColumnVector tableRootVector =
        wrapEngineException(
            () -> tableRootVectorGenerator.eval(finalScanAddFiles),
            "Evaluating the table root expression");
    scanAddFiles =
        scanAddFiles.withNewColumn(
            1, InternalScanFileUtils.TABLE_ROOT_STRUCT_FIELD, tableRootVector);

    Optional<ColumnVector> selectionColumnVector = Optional.empty();
    if (atLeastOneUnselected) {
      selectionColumnVector =
          Optional.of(
              wrapEngineException(
                  () ->
                      engine
                          .getExpressionHandler()
                          .createSelectionVector(selectionVectorBuffer, 0, addsVector.getSize()),
                  "Create selection vector for selected scan files"));
    }
    next = Optional.of(new FilteredColumnarBatch(scanAddFiles, selectionColumnVector));
  }

  public static String getAddFilePath(ColumnVector addFileVector, int rowId) {
    return addFileVector.getChild(ADD_FILE_PATH_ORDINAL).getString(rowId);
  }

  public static DeletionVectorDescriptor getAddFileDV(ColumnVector addFileVector, int rowId) {
    return DeletionVectorDescriptor.fromColumnVector(
        addFileVector.getChild(ADD_FILE_DV_ORDINAL), rowId);
  }

  public static String getRemoveFilePath(ColumnVector removeFileVector, int rowId) {
    return removeFileVector.getChild(REMOVE_FILE_PATH_ORDINAL).getString(rowId);
  }

  public static DeletionVectorDescriptor getRemoveFileDV(ColumnVector removeFileVector, int rowId) {
    return DeletionVectorDescriptor.fromColumnVector(
        removeFileVector.getChild(REMOVE_FILE_DV_ORDINAL), rowId);
  }

  /**
   * Returns the metrics for the log replay. Currently used in tests only. Caution: The metrics
   * should be fetched only after the iterator is closed, to avoid reading incomplete metrics.
   */
  public LogReplayMetrics getMetrics() {
    return metrics;
  }
}
