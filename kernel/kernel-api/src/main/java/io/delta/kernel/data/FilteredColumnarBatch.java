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
package io.delta.kernel.data;

import io.delta.kernel.annotation.Evolving;
import io.delta.kernel.internal.data.ColumnarBatchRow;
import io.delta.kernel.utils.CloseableIterator;
import java.util.NoSuchElementException;
import java.util.Optional;

/**
 * Represents a filtered version of {@link ColumnarBatch}. Contains original {@link ColumnarBatch}
 * with an optional selection vector to select only a subset of rows for the original columnar
 * batch.
 *
 * <p>The selection vector is of type boolean and has the same size as the data in the corresponding
 * {@link ColumnarBatch}. For each row index, a value of true in the selection vector indicates the
 * row at the same index in the data {@link ColumnarBatch} is valid; a value of false indicates the
 * row should be ignored. If there is no selection vector then all the rows are valid.
 *
 * @since 3.0.0
 */
@Evolving
public class FilteredColumnarBatch {
  private final ColumnarBatch data;
  private final Optional<ColumnVector> selectionVector;
  private final Optional<String> filePath;
  // Optional cached count of true rows in the selection vector
  private Optional<Integer> numSelectedRows;

  public FilteredColumnarBatch(ColumnarBatch data, Optional<ColumnVector> selectionVector) {
    this(data, selectionVector, Optional.empty(), Optional.empty());
  }

  public FilteredColumnarBatch(
      ColumnarBatch data,
      Optional<ColumnVector> selectionVector,
      Optional<String> filePath,
      Optional<Integer> numSelectedRows) {
    this.data = data;
    this.selectionVector = selectionVector;
    this.numSelectedRows = numSelectedRows;
    this.filePath = filePath;
  }

  /**
   * Return the data as {@link ColumnarBatch}. Not all rows in the data are valid for this result.
   * An optional <i>selectionVector</i> determines which rows are selected. If there is no selection
   * vector that means all rows in this columnar batch are valid for this result.
   *
   * @return all the data read from the file
   */
  public ColumnarBatch getData() {
    return data;
  }

  public Optional<String> getFilePath() {
    return filePath;
  }

  /**
   * Optional selection vector containing one entry for each row in <i>data</i> indicating whether a
   * row is selected or not selected. If there is no selection vector then all the rows are valid.
   *
   * @return an optional {@link ColumnVector} indicating which rows are valid
   */
  public Optional<ColumnVector> getSelectionVector() {
    return selectionVector;
  }

  /**
   * Iterator of rows that survived the filter.
   *
   * @return Closeable iterator of rows that survived the filter. It is responsibility of the caller
   *     to the close the iterator.
   */
  public CloseableIterator<Row> getRows() {
    if (!selectionVector.isPresent()) {
      return data.getRows();
    }

    return new CloseableIterator<Row>() {
      private int rowId = 0;
      private int maxRowId = data.getSize();
      private int nextRowId = -1;

      @Override
      public boolean hasNext() {
        for (; rowId < maxRowId && nextRowId == -1; rowId++) {
          boolean isSelected =
              !selectionVector.get().isNullAt(rowId) && selectionVector.get().getBoolean(rowId);
          if (isSelected) {
            nextRowId = rowId;
            rowId++;
            break;
          }
        }
        return nextRowId != -1;
      }

      @Override
      public Row next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        Row row = new ColumnarBatchRow(data, nextRowId);
        nextRowId = -1;
        return row;
      }

      @Override
      public void close() {}
    };
  }

  /** @return get the total number of selected rows */
  public int getCachedNumSelectedRows() {
    if (numSelectedRows.isPresent()) {
      return numSelectedRows.get();
    }
    if (!selectionVector.isPresent()) {
      return data.getSize(); // all rows are selected
    }

    throw new IllegalStateException("Number of selected Rows is not available!");
    /** Alternative: calculate numSelectedRows based on selection vector */
    /*
    int trueRowCount = 0;
    for (int rowId = 0; rowId < data.getSize(); rowId++) {
      if (selectionVector.get().isNullAt(rowId)) {
        boolean isSelected =
            !selectionVector.get().isNullAt(rowId) && selectionVector.get().getBoolean(rowId);
        if (isSelected) trueRowCount++;
      }
    }
    numSelectedRows = Optional.of(trueRowCount);
    return trueRowCount; */
  }
}
