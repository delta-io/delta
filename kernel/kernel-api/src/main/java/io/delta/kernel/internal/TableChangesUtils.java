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

package io.delta.kernel.internal;

import static io.delta.kernel.internal.DeltaErrors.wrapEngineException;

import io.delta.kernel.CommitActions;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.expressions.ExpressionEvaluator;
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.internal.tablefeatures.TableFeatures;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;

/** Utility class for table changes operations. */
public class TableChangesUtils {

  /** Column name for the version metadata column added to getActions results. */
  public static final String VERSION_COLUMN_NAME = "version";

  /** Column name for the timestamp metadata column added to getActions results. */
  public static final String TIMESTAMP_COLUMN_NAME = "timestamp";

  private TableChangesUtils() {}

  /**
   * Validates protocol and drops protocol/commitInfo columns if not requested.
   *
   * @param batch the batch to process
   * @param tablePath the table path for error messages
   * @param shouldDropProtocolColumn whether to drop the protocol column
   * @param shouldDropCommitInfoColumn whether to drop the commitInfo column
   * @return the processed batch
   */
  public static ColumnarBatch validateProtocolAndDropInternalColumns(
      ColumnarBatch batch,
      String tablePath,
      boolean shouldDropProtocolColumn,
      boolean shouldDropCommitInfoColumn) {

    // Validate protocol if present in the batch.
    // The protocol column may not be present if the caller filtered it out from the readSchema,
    // or if the commit file contains no protocol actions.
    int protocolIdx = batch.getSchema().indexOf("protocol");
    if (protocolIdx >= 0) {
      ColumnVector protocolVector = batch.getColumnVector(protocolIdx);
      for (int rowId = 0; rowId < protocolVector.getSize(); rowId++) {
        if (!protocolVector.isNullAt(rowId)) {
          Protocol protocol = Protocol.fromColumnVector(protocolVector, rowId);
          TableFeatures.validateKernelCanReadTheTable(protocol, tablePath);
        }
      }
    }

    // Drop columns if not requested
    ColumnarBatch result = batch;
    if (shouldDropProtocolColumn && protocolIdx >= 0) {
      result = result.withDeletedColumnAt(protocolIdx);
    }

    int commitInfoIdx = result.getSchema().indexOf("commitInfo");
    if (shouldDropCommitInfoColumn && commitInfoIdx >= 0) {
      result = result.withDeletedColumnAt(commitInfoIdx);
    }

    return result;
  }

  /**
   * Adds version and timestamp columns to a columnar batch.
   *
   * <p>The version and timestamp columns are added as the first two columns in the batch.
   *
   * @param engine the engine for expression evaluation
   * @param batch the original batch
   * @param version the version value to add
   * @param timestamp the timestamp value to add
   * @return a new batch with version and timestamp columns prepended
   */
  public static ColumnarBatch addVersionAndTimestampColumns(
      Engine engine, ColumnarBatch batch, long version, long timestamp) {
    StructType schemaForEval = batch.getSchema();

    ExpressionEvaluator commitVersionGenerator =
        wrapEngineException(
            () ->
                engine
                    .getExpressionHandler()
                    .getEvaluator(schemaForEval, Literal.ofLong(version), LongType.LONG),
            "Get the expression evaluator for the commit version");

    ExpressionEvaluator commitTimestampGenerator =
        wrapEngineException(
            () ->
                engine
                    .getExpressionHandler()
                    .getEvaluator(schemaForEval, Literal.ofLong(timestamp), LongType.LONG),
            "Get the expression evaluator for the commit timestamp");

    ColumnVector commitVersionVector =
        wrapEngineException(
            () -> commitVersionGenerator.eval(batch), "Evaluating the commit version expression");

    ColumnVector commitTimestampVector =
        wrapEngineException(
            () -> commitTimestampGenerator.eval(batch),
            "Evaluating the commit timestamp expression");

    return batch
        .withNewColumn(
            0, new StructField(VERSION_COLUMN_NAME, LongType.LONG, false), commitVersionVector)
        .withNewColumn(
            1, new StructField(TIMESTAMP_COLUMN_NAME, LongType.LONG, false), commitTimestampVector);
  }

  /**
   * Flattens an iterator of CommitActions into an iterator of ColumnarBatch, adding version and
   * timestamp columns to each batch.
   * @param engine the engine for expression evaluation
   * @param commits the iterator of CommitActions to flatten
   * @return an iterator of ColumnarBatch with version and timestamp columns added
   */
  public static CloseableIterator<ColumnarBatch> flattenCommitsAndAddMetadata(
      Engine engine, CloseableIterator<CommitActions> commits) {
    CloseableIterator<CloseableIterator<ColumnarBatch>> nestedIterator =
        commits.map(
            commit -> {
              long version = commit.getVersion();
              long timestamp = commit.getTimestamp();
              CloseableIterator<ColumnarBatch> actions = commit.getActions();

              // Map each batch to add version and timestamp columns
              return actions.map(
                  batch -> addVersionAndTimestampColumns(engine, batch, version, timestamp));
            });

    return Utils.flatMap(nestedIterator);
  }
}
