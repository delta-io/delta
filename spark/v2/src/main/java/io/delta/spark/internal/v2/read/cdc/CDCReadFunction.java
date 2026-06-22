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
import io.delta.spark.internal.v2.utils.ConstantColumnVectors;
import java.io.Serializable;
import javax.annotation.Nullable;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.ProjectingInternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.expressions.JoinedRow;
import org.apache.spark.sql.execution.datasources.PartitionedFile;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.apache.spark.unsafe.types.UTF8String;
import scala.Function1;
import scala.collection.Iterator;
import scala.collection.immutable.Map;
import scala.collection.immutable.Seq;
import scala.runtime.AbstractFunction1;

/**
 * Decorates a Parquet reader to inject CDC metadata columns ({@code _change_type}, {@code
 * _commit_version}, {@code _commit_timestamp}) for write-time CDF reads.
 */
public class CDCReadFunction extends AbstractFunction1<PartitionedFile, Iterator<InternalRow>>
    implements Serializable {

  private static final long serialVersionUID = 1L;
  private static final int CDC_COL_COUNT = 3;

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
    // Null for AddCDCFile: _change_type comes from the Parquet data per-row.
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

  /**
   * Row-based: project table columns, inject CDC constants, join into output order.
   *
   * <p>Output row order: {@code [readDataSchema, partition, _change_type, _commit_version,
   * _commit_timestamp]}.
   */
  private Iterator<InternalRow> applyRow(
      PartitionedFile file,
      @Nullable String changeType,
      long commitVersion,
      long commitTimestampMicros) {
    int changeTypeInternalIdx = cdcSchemaContext.getChangeTypeInternalIndex();
    @Nullable UTF8String changeTypeUtf8 = UTF8String.fromString(changeType);

    ProjectingInternalRow dataProjection =
        ProjectingInternalRow.apply(
            /* schema= */ cdcSchemaContext.getDataAndPartitionSchema(),
            /* ordinals= */ cdcSchemaContext.getDataAndPartitionOrdinals());
    GenericInternalRow cdcRow = new GenericInternalRow(3);
    cdcRow.setLong(CDCSchemaContext.COMMIT_VERSION_IDX, commitVersion);
    cdcRow.setLong(CDCSchemaContext.COMMIT_TIMESTAMP_IDX, commitTimestampMicros);
    JoinedRow joined = new JoinedRow();

    return CloseableIterator.wrap(baseReadFunc.apply(file))
        .mapCloseable(
            row -> {
              dataProjection.project(row);
              cdcRow.update(
                  CDCSchemaContext.CHANGE_TYPE_IDX,
                  changeTypeUtf8 != null
                      ? changeTypeUtf8
                      : row.getUTF8String(changeTypeInternalIdx));
              return (InternalRow) joined.apply(dataProjection, cdcRow);
            });
  }

  /** Vectorized: reorder and replace CDC ColumnVectors in each batch. */
  @SuppressWarnings("unchecked")
  private Iterator<InternalRow> applyBatch(
      PartitionedFile file,
      @Nullable String changeType,
      long commitVersion,
      long commitTimestampMicros) {
    Seq<Object> dataAndPartitionOrdinals = cdcSchemaContext.getDataAndPartitionOrdinals();
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
                            dataAndPartitionOrdinals,
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
   * [readDataSchema, partition, CDC] and replace CDC columns with constants.
   */
  private static ColumnarBatch reorderColumnarBatch(
      ColumnarBatch batch,
      Seq<Object> dataAndPartitionOrdinals,
      int changeTypeInternalIdx,
      @Nullable String changeType,
      long commitVersion,
      long commitTimestampMicros) {
    int numRows = batch.numRows();
    int nonCdcColCount = dataAndPartitionOrdinals.size();
    ColumnVector[] columns = new ColumnVector[nonCdcColCount + CDC_COL_COUNT];

    // Data + partition columns: remap from internal batch positions.
    for (int outIdx = 0; outIdx < nonCdcColCount; outIdx++) {
      columns[outIdx] = batch.column((Integer) dataAndPartitionOrdinals.apply(outIdx));
    }
    // CDC columns: constants (or original _change_type for explicit CDC).
    // The replaced CDC ColumnVectors from the original batch (null-filled by schema evolution)
    // are not explicitly closed here - their lifecycle is managed by the base Spark vectorized
    // reader that owns the original batch.
    int cdcStart = nonCdcColCount;
    if (changeType != null) {
      columns[cdcStart + CDCSchemaContext.CHANGE_TYPE_IDX] =
          ConstantColumnVectors.ofUtf8String(changeType, numRows);
    } else {
      columns[cdcStart + CDCSchemaContext.CHANGE_TYPE_IDX] = batch.column(changeTypeInternalIdx);
    }
    columns[cdcStart + CDCSchemaContext.COMMIT_VERSION_IDX] =
        ConstantColumnVectors.ofLong(commitVersion, numRows);
    columns[cdcStart + CDCSchemaContext.COMMIT_TIMESTAMP_IDX] =
        ConstantColumnVectors.ofTimestampMicros(commitTimestampMicros, numRows);

    return new ColumnarBatch(columns, numRows);
  }

  /** Factory method to wrap a reader function with CDC null-coalesce. */
  public static CDCReadFunction wrap(
      Function1<PartitionedFile, Iterator<InternalRow>> baseReadFunc,
      CDCSchemaContext cdcSchemaContext,
      boolean isVectorizedReader) {
    return new CDCReadFunction(baseReadFunc, cdcSchemaContext, isVectorizedReader);
  }
}
