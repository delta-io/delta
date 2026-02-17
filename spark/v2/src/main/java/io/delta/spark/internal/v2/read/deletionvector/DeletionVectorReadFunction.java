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
package io.delta.spark.internal.v2.read.deletionvector;

import io.delta.spark.internal.v2.utils.CloseableIterator;
import java.io.Serializable;
import java.util.Arrays;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.ProjectingInternalRow;
import org.apache.spark.sql.execution.datasources.PartitionedFile;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import scala.Function1;
import scala.collection.Iterator;
import scala.runtime.AbstractFunction1;

/**
 * Wraps a Parquet reader function to apply deletion vector filtering.
 *
 * <p>This function:
 *
 * <ol>
 *   <li>Reads rows from the base Parquet reader (which includes the is_row_deleted column)
 *   <li>Filters out deleted rows (where is_row_deleted != 0)
 *   <li>Projects out the is_row_deleted column from the output
 * </ol>
 *
 * <p>The returned iterator implements {@link java.io.Closeable} to ensure proper resource cleanup
 * of the underlying Parquet reader, even when the iterator is not fully consumed.
 */
public class DeletionVectorReadFunction
    extends AbstractFunction1<PartitionedFile, Iterator<InternalRow>> implements Serializable {

  private static final long serialVersionUID = 1L;

  /** Byte value in the DV column indicating the row is NOT deleted (row should be kept). */
  private static final byte ROW_NOT_DELETED = 0;

  private final Function1<PartitionedFile, Iterator<InternalRow>> baseReadFunc;
  private final DeletionVectorSchemaContext dvSchemaContext;

  private DeletionVectorReadFunction(
      Function1<PartitionedFile, Iterator<InternalRow>> baseReadFunc,
      DeletionVectorSchemaContext dvSchemaContext) {
    this.baseReadFunc = baseReadFunc;
    this.dvSchemaContext = dvSchemaContext;
  }

  @Override
  @SuppressWarnings("unchecked")
  public Iterator<InternalRow> apply(PartitionedFile file) {
    int dvColumnIndex = dvSchemaContext.getDvColumnIndex();
    // Use pre-computed ordinals from DeletionVectorSchemaContext.
    ProjectingInternalRow projection =
        ProjectingInternalRow.apply(
            dvSchemaContext.getOutputSchema(), dvSchemaContext.getOutputColumnOrdinals());

    // Filter: skip deleted rows (noop for vectorized - batch filtering done in map)
    // Map: project row / filter batch
    //
    // Double casts are needed due to Scala Iterator's invariant type parameter:
    //   Inner: (Iterator<Object>) (Iterator<?>) - erase InternalRow to Object, because in
    //     vectorized mode Spark passes ColumnarBatch via type erasure.
    //   Outer: (Iterator<InternalRow>) (Iterator<?>) - erase Object back to InternalRow to
    //     match the method's return type.
    // Wrap in CloseableIterator to preserve close() through filter/map when partially consumed.
    return (Iterator<InternalRow>)
        (Iterator<?>)
            CloseableIterator.wrap((Iterator<Object>) (Iterator<?>) baseReadFunc.apply(file))
                .filterCloseable(
                    item -> {
                      if (item instanceof InternalRow) {
                        // Row-based: filter deleted rows
                        return ((InternalRow) item).getByte(dvColumnIndex) == ROW_NOT_DELETED;
                      }
                      // Vectorized: noop (batch filtering done in map)
                      return true;
                    })
                .mapCloseable(
                    item -> {
                      if (item instanceof ColumnarBatch) {
                        return filterAndProjectBatch((ColumnarBatch) item, dvColumnIndex);
                      } else {
                        // Row-based: project out DV column
                        projection.project((InternalRow) item);
                        return projection;
                      }
                    });
  }

  /** Filter active rows and project out the DV column from a ColumnarBatch. */
  private static ColumnarBatch filterAndProjectBatch(ColumnarBatch batch, int dvColumnIndex) {
    int[] activeRows = findActiveRows(batch, dvColumnIndex);
    ColumnVector[] filteredColumns = buildFilteredColumns(batch, dvColumnIndex, activeRows);
    return new ColumnarBatch(filteredColumns, activeRows.length);
  }

  /** Build projected output columns by dropping the DV column and applying active row mapping. */
  private static ColumnVector[] buildFilteredColumns(
      ColumnarBatch batch, int dvColumnIndex, int[] activeRows) {
    ColumnVector[] filteredColumns = new ColumnVector[batch.numCols() - 1];
    int outputIndex = 0;
    for (int inputIndex = 0; inputIndex < batch.numCols(); inputIndex++) {
      if (inputIndex == dvColumnIndex) {
        continue;
      }
      filteredColumns[outputIndex++] =
          new ColumnVectorWithFilter(batch.column(inputIndex), activeRows);
    }
    return filteredColumns;
  }

  /** Find indices of rows where DV column is 0 (not deleted). */
  private static int[] findActiveRows(ColumnarBatch batch, int dvColumnIndex) {
    ColumnVector dvColumn = batch.column(dvColumnIndex);
    int[] temp = new int[batch.numRows()];
    int count = 0;
    for (int i = 0; i < batch.numRows(); i++) {
      if (dvColumn.getByte(i) == ROW_NOT_DELETED) {
        temp[count++] = i;
      }
    }
    return Arrays.copyOf(temp, count);
  }

  /** Factory method to wrap a reader function with DV filtering. */
  public static DeletionVectorReadFunction wrap(
      Function1<PartitionedFile, Iterator<InternalRow>> baseReadFunc,
      DeletionVectorSchemaContext dvSchemaContext) {
    return new DeletionVectorReadFunction(baseReadFunc, dvSchemaContext);
  }
}
