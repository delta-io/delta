/*
 * Copyright (2026) The Delta Lake Project Authors.
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
package io.delta.spark.internal.v2.read.cdc;

import io.delta.spark.internal.v2.utils.CloseableIterator;
import java.io.Serializable;
import javax.annotation.Nullable;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.execution.datasources.PartitionedFile;
import org.apache.spark.sql.execution.vectorized.ConstantColumnVector;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.apache.spark.unsafe.types.UTF8String;
import scala.Function1;
import scala.collection.Iterator;
import scala.collection.immutable.Map;
import scala.runtime.AbstractFunction1;

/**
 * Wraps a Parquet reader function to null-coalesce CDC columns with per-file constants.
 *
 * <p>The Parquet reader includes CDC columns ({@code _change_type}, {@code _commit_version}, {@code
 * _commit_timestamp}) in its read schema. For files that don't contain these columns (inferred CDC
 * from AddFile/RemoveFile), Parquet fills them with null via schema evolution. This decorator
 * replaces those nulls with per-file constants from {@link
 * PartitionedFile#otherConstantMetadataColumnValues()}.
 *
 * <p>For explicit CDC files (AddCDCFile), {@code _change_type} comes from Parquet data and is
 * preserved; only {@code _commit_version} and {@code _commit_timestamp} are injected.
 *
 * <p>Follows the same decorator pattern as {@link
 * io.delta.spark.internal.v2.read.deletionvector.DeletionVectorReadFunction}.
 */
public class CDCReadFunction extends AbstractFunction1<PartitionedFile, Iterator<InternalRow>>
    implements Serializable {

  private static final long serialVersionUID = 1L;

  private final Function1<PartitionedFile, Iterator<InternalRow>> baseReadFunc;
  private final CDCSchemaContext cdcSchemaContext;
  private final boolean isVectorizedReader;

  private CDCReadFunction(
      Function1<PartitionedFile, Iterator<InternalRow>> baseReadFunc,
      CDCSchemaContext cdcSchemaContext,
      boolean isVectorizedReader) {
    this.baseReadFunc = baseReadFunc;
    this.cdcSchemaContext = cdcSchemaContext;
    this.isVectorizedReader = isVectorizedReader;
  }

  @Override
  public Iterator<InternalRow> apply(PartitionedFile file) {
    // Extract CDC constants from PartitionedFile metadata
    Map<String, Object> constants = file.otherConstantMetadataColumnValues();
    @Nullable
    String changeType =
        constants.contains(CDCSchemaContext.CDC_TYPE_COLUMN)
            ? (String) constants.apply(CDCSchemaContext.CDC_TYPE_COLUMN)
            : null;
    long commitVersion = (Long) constants.apply(CDCSchemaContext.CDC_COMMIT_VERSION);
    long commitTimestampMicros = (Long) constants.apply(CDCSchemaContext.CDC_COMMIT_TIMESTAMP);

    if (isVectorizedReader) {
      return applyBatch(file, changeType, commitVersion, commitTimestampMicros);
    } else {
      return applyRow(file, changeType, commitVersion, commitTimestampMicros);
    }
  }

  /** Row-based: wrap each InternalRow to reorder and null-coalesce CDC columns. */
  private Iterator<InternalRow> applyRow(
      PartitionedFile file,
      @Nullable String changeType,
      long commitVersion,
      long commitTimestampMicros) {
    int[] outputToInternal = cdcSchemaContext.getOutputToInternalMapping();
    int tableColCount = cdcSchemaContext.getTableColCount();
    int changeTypeInternalIdx = cdcSchemaContext.getChangeTypeInternalIndex();
    UTF8String changeTypeUtf8 = changeType != null ? UTF8String.fromString(changeType) : null;

    return CloseableIterator.wrap(baseReadFunc.apply(file))
        .mapCloseable(
            row ->
                (InternalRow)
                    new CDCCoalesceRow(
                        row,
                        outputToInternal,
                        tableColCount,
                        changeTypeInternalIdx,
                        changeTypeUtf8,
                        commitVersion,
                        commitTimestampMicros));
  }

  /** Vectorized: reorder and replace CDC ColumnVectors in each batch. */
  @SuppressWarnings("unchecked")
  private Iterator<InternalRow> applyBatch(
      PartitionedFile file,
      @Nullable String changeType,
      long commitVersion,
      long commitTimestampMicros) {
    int[] outputToInternal = cdcSchemaContext.getOutputToInternalMapping();
    int tableColCount = cdcSchemaContext.getTableColCount();
    int changeTypeInternalIdx = cdcSchemaContext.getChangeTypeInternalIndex();

    Iterator<Object> baseIterator = (Iterator<Object>) (Iterator<?>) baseReadFunc.apply(file);
    return (Iterator<InternalRow>)
        (Iterator<?>)
            CloseableIterator.wrap(baseIterator)
                .mapCloseable(
                    item -> {
                      if (item instanceof ColumnarBatch) {
                        return reorderColumnarBatch(
                            (ColumnarBatch) item,
                            outputToInternal,
                            tableColCount,
                            changeTypeInternalIdx,
                            changeType,
                            commitVersion,
                            commitTimestampMicros);
                      }
                      throw new IllegalStateException(
                          "Expected ColumnarBatch when vectorized reader is enabled, but got: "
                              + item.getClass());
                    });
  }

  /**
   * Reorder a ColumnarBatch from internal layout [readDataSchema, CDC, partition] to output layout
   * [tableSchema, CDC] and replace CDC columns with constants.
   */
  private static ColumnarBatch reorderColumnarBatch(
      ColumnarBatch batch,
      int[] outputToInternal,
      int tableColCount,
      int changeTypeInternalIdx,
      @Nullable String changeType,
      long commitVersion,
      long commitTimestampMicros) {
    int numRows = batch.numRows();
    ColumnVector[] columns = new ColumnVector[outputToInternal.length];

    // Table columns: remap from internal batch positions
    for (int outIdx = 0; outIdx < tableColCount; outIdx++) {
      columns[outIdx] = batch.column(outputToInternal[outIdx]);
    }
    // CDC columns: constants (or original _change_type for explicit CDC)
    if (changeType != null) {
      columns[tableColCount] = createConstantStringVector(changeType, numRows);
    } else {
      columns[tableColCount] = batch.column(changeTypeInternalIdx);
    }
    columns[tableColCount + 1] = createConstantLongVector(commitVersion, numRows);
    columns[tableColCount + 2] = createConstantTimestampVector(commitTimestampMicros, numRows);

    return new ColumnarBatch(columns, numRows);
  }

  /** Create a constant string ColumnVector using ConstantColumnVector. */
  private static ConstantColumnVector createConstantStringVector(String value, int numRows) {
    ConstantColumnVector vector = new ConstantColumnVector(numRows, DataTypes.StringType);
    vector.setUtf8String(UTF8String.fromString(value));
    return vector;
  }

  /** Create a constant long ColumnVector using ConstantColumnVector. */
  private static ConstantColumnVector createConstantLongVector(long value, int numRows) {
    ConstantColumnVector vector = new ConstantColumnVector(numRows, DataTypes.LongType);
    vector.setLong(value);
    return vector;
  }

  /** Create a constant timestamp ColumnVector (micros stored as long, typed as TimestampType). */
  private static ConstantColumnVector createConstantTimestampVector(long micros, int numRows) {
    ConstantColumnVector vector = new ConstantColumnVector(numRows, DataTypes.TimestampType);
    vector.setLong(micros);
    return vector;
  }

  /** Factory method to wrap a reader function with CDC null-coalesce. */
  public static CDCReadFunction wrap(
      Function1<PartitionedFile, Iterator<InternalRow>> baseReadFunc,
      CDCSchemaContext cdcSchemaContext,
      boolean isVectorizedReader) {
    return new CDCReadFunction(baseReadFunc, cdcSchemaContext, isVectorizedReader);
  }
}
