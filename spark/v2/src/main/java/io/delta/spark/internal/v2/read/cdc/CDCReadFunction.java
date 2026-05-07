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
import java.util.OptionalInt;
import javax.annotation.Nullable;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.execution.datasources.PartitionedFile;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.apache.spark.unsafe.types.UTF8String;
import scala.Function1;
import scala.collection.Iterator;
import scala.collection.immutable.Map;
import scala.runtime.AbstractFunction1;

/**
 * Decorates a Parquet reader to fill in CDC metadata columns ({@code _change_type}, {@code
 * _commit_version}, {@code _commit_timestamp}) at their positions in {@code readDataSchema} using
 * per-file constants from {@link PartitionedFile#otherConstantMetadataColumnValues()}.
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
    Map<String, Object> constants = file.otherConstantMetadataColumnValues();
    // Null for AddCDCFile: _change_type comes from the parquet data per-row.
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
   * Row path: build a new {@link GenericInternalRow} per input row, copying non-CDC values from the
   * input and substituting per-file constants at CDC positions.
   */
  private Iterator<InternalRow> applyRow(
      PartitionedFile file,
      @Nullable String changeType,
      long commitVersion,
      long commitTimestampMicros) {
    final OptionalInt changeTypeIdxOpt = cdcSchemaContext.changeTypeIndex();
    final OptionalInt commitVersionIdxOpt = cdcSchemaContext.commitVersionIndex();
    final OptionalInt commitTimestampIdxOpt = cdcSchemaContext.commitTimestampIndex();
    @Nullable final UTF8String changeTypeUtf8 = UTF8String.fromString(changeType);
    final StructField[] fields = cdcSchemaContext.getFullRowSchema().fields();

    return CloseableIterator.wrap(baseReadFunc.apply(file))
        .mapCloseable(
            row -> {
              int n = row.numFields();
              Object[] values = new Object[n];
              for (int i = 0; i < n; i++) {
                values[i] = row.isNullAt(i) ? null : row.get(i, fields[i].dataType());
              }
              if (changeTypeIdxOpt.isPresent()) {
                int idx = changeTypeIdxOpt.getAsInt();
                // _change_type: null in row only for AddFile/RemoveFile (use constant). For
                // AddCDCFile, the parquet file has the value; preserve what's there.
                if (changeTypeUtf8 != null && row.isNullAt(idx)) {
                  values[idx] = changeTypeUtf8;
                }
              }
              if (commitVersionIdxOpt.isPresent()) {
                values[commitVersionIdxOpt.getAsInt()] = commitVersion;
              }
              if (commitTimestampIdxOpt.isPresent()) {
                values[commitTimestampIdxOpt.getAsInt()] = commitTimestampMicros;
              }
              return new GenericInternalRow(values);
            });
  }

  /**
   * Vectorized path: replace CDC ColumnVectors at their positions in the batch. {@code
   * _change_type} keeps the original parquet column when it has values (AddCDCFile); other CDC
   * columns become {@link ConstantColumnVectors}.
   */
  @SuppressWarnings("unchecked")
  private Iterator<InternalRow> applyBatch(
      PartitionedFile file,
      @Nullable String changeType,
      long commitVersion,
      long commitTimestampMicros) {
    final OptionalInt changeTypeIdxOpt = cdcSchemaContext.changeTypeIndex();
    final OptionalInt commitVersionIdxOpt = cdcSchemaContext.commitVersionIndex();
    final OptionalInt commitTimestampIdxOpt = cdcSchemaContext.commitTimestampIndex();

    Iterator<Object> baseIterator = (Iterator<Object>) (Iterator<?>) baseReadFunc.apply(file);
    return (Iterator<InternalRow>)
        (Iterator<?>)
            CloseableIterator.wrap(baseIterator)
                .mapCloseable(
                    item -> {
                      if (item instanceof ColumnarBatch) {
                        return substituteCDCColumns(
                            (ColumnarBatch) item,
                            changeTypeIdxOpt,
                            commitVersionIdxOpt,
                            commitTimestampIdxOpt,
                            changeType,
                            commitVersion,
                            commitTimestampMicros);
                      }
                      throw new IllegalStateException(
                          "Expected ColumnarBatch when vectorized reader is enabled, but got: "
                              + item.getClass());
                    });
  }

  private static ColumnarBatch substituteCDCColumns(
      ColumnarBatch batch,
      OptionalInt changeTypeIdxOpt,
      OptionalInt commitVersionIdxOpt,
      OptionalInt commitTimestampIdxOpt,
      @Nullable String changeType,
      long commitVersion,
      long commitTimestampMicros) {
    int numRows = batch.numRows();
    int numCols = batch.numCols();
    ColumnVector[] columns = new ColumnVector[numCols];
    for (int i = 0; i < numCols; i++) {
      columns[i] = batch.column(i);
    }
    if (changeTypeIdxOpt.isPresent() && changeType != null) {
      // AddFile/RemoveFile: replace the null-filled column with a constant. AddCDCFile
      // (changeType == null) keeps the original parquet column.
      columns[changeTypeIdxOpt.getAsInt()] =
          ConstantColumnVectors.ofUtf8String(changeType, numRows);
    }
    if (commitVersionIdxOpt.isPresent()) {
      columns[commitVersionIdxOpt.getAsInt()] =
          ConstantColumnVectors.ofLong(commitVersion, numRows);
    }
    if (commitTimestampIdxOpt.isPresent()) {
      columns[commitTimestampIdxOpt.getAsInt()] =
          ConstantColumnVectors.ofTimestampMicros(commitTimestampMicros, numRows);
    }
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
