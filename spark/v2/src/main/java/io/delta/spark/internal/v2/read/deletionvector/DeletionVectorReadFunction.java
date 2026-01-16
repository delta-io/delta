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
package io.delta.spark.internal.v2.read.deletionvector;

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
 * Wraps a base reader function to apply deletion vector filtering.
 *
 * <p>Supports both row-based and vectorized reading:
 *
 * <ul>
 *   <li>Row-based: Filters deleted rows and projects out DV column using ProjectingInternalRow
 *   <li>Vectorized: Uses ColumnVectorWithFilter to remap row indices without copying data
 * </ul>
 */
public class DeletionVectorReadFunction
    extends AbstractFunction1<PartitionedFile, Iterator<InternalRow>> implements Serializable {

  private static final long serialVersionUID = 1L;

  private final Function1<PartitionedFile, Iterator<InternalRow>> baseReadFunc;
  private final DvSchemaContext dvContext;

  private DeletionVectorReadFunction(
      Function1<PartitionedFile, Iterator<InternalRow>> baseReadFunc, DvSchemaContext dvContext) {
    this.baseReadFunc = baseReadFunc;
    this.dvContext = dvContext;
  }

  @Override
  public Iterator<InternalRow> apply(PartitionedFile file) {
    int dvColumnIndex = dvContext.getDvColumnIndex();
    int outputColumnCount = dvContext.getOutputSchema().fields().length;
    // Use pre-computed ordinals from DvSchemaContext
    ProjectingInternalRow projection =
        ProjectingInternalRow.apply(
            dvContext.getOutputSchema(), dvContext.getOutputColumnOrdinals());

    // Use Object as input type: Spark passes ColumnarBatch cast to InternalRow in vectorized mode
    @SuppressWarnings("unchecked")
    Iterator<Object> baseIterator = (Iterator<Object>) (Iterator<?>) baseReadFunc.apply(file);

    // Filter: skip deleted rows (noop for vectorized)
    // Map: project row / filter batch
    @SuppressWarnings("unchecked")
    Iterator<InternalRow> result =
        (Iterator<InternalRow>)
            (Iterator<?>)
                baseIterator
                    .filter(
                        new AbstractFunction1<Object, Object>() {
                          @Override
                          public Object apply(Object item) {
                            if (item instanceof InternalRow) {
                              // Row-based: filter deleted rows
                              return ((InternalRow) item).getByte(dvColumnIndex) == 0;
                            }
                            // Vectorized: noop (batch filtering done in map)
                            return true;
                          }
                        })
                    .map(
                        new AbstractFunction1<Object, Object>() {
                          @Override
                          public Object apply(Object item) {
                            if (item instanceof ColumnarBatch) {
                              return filterBatch(
                                  (ColumnarBatch) item, dvColumnIndex, outputColumnCount);
                            } else {
                              // Row-based: project out DV column
                              projection.project((InternalRow) item);
                              return projection;
                            }
                          }
                        });
    return result;
  }

  /** Filter a ColumnarBatch by building row ID mapping for live rows. */
  private static ColumnarBatch filterBatch(
      ColumnarBatch batch, int dvColumnIndex, int outputColumnCount) {
    int[] liveRows = findLiveRows(batch, dvColumnIndex);
    // Build filtered column vectors (excluding DV column)
    ColumnVector[] filteredVectors = new ColumnVector[outputColumnCount];
    int outIdx = 0;
    for (int i = 0; i < batch.numCols(); i++) {
      if (i != dvColumnIndex) {
        filteredVectors[outIdx++] = new ColumnVectorWithFilter(batch.column(i), liveRows);
      }
    }
    return new ColumnarBatch(filteredVectors, liveRows.length);
  }

  /** Find indices of rows where DV column is 0 (not deleted). */
  private static int[] findLiveRows(ColumnarBatch batch, int dvColumnIndex) {
    ColumnVector dvColumn = batch.column(dvColumnIndex);
    int[] temp = new int[batch.numRows()];
    int count = 0;
    for (int i = 0; i < batch.numRows(); i++) {
      if (dvColumn.getByte(i) == 0) {
        temp[count++] = i;
      }
    }
    return Arrays.copyOf(temp, count);
  }

  /** Factory method to wrap a reader function with DV filtering. */
  public static DeletionVectorReadFunction wrap(
      Function1<PartitionedFile, Iterator<InternalRow>> baseReadFunc, DvSchemaContext dvContext) {
    return new DeletionVectorReadFunction(baseReadFunc, dvContext);
  }
}
