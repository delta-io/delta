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
  private final Optional<Integer> preComputedNumSelectedRows;

  public FilteredColumnarBatch(ColumnarBatch data, Optional<ColumnVector> selectionVector) {
    this(data, selectionVector, null, null);
  }

  public FilteredColumnarBatch(
      ColumnarBatch data,
      Optional<ColumnVector> selectionVector,
      String filePath,
      Integer numSelectedRows) {
    this.data = data;
    this.selectionVector = selectionVector;
    this.filePath = Optional.ofNullable(filePath);
    assert selectionVector.isPresent()
            || numSelectedRows == null
            || numSelectedRows == data.getSize()
        : "Invalid precomputedNumSelectedRows: must be null or equal to batch size when selectionVector is empty.";
    this.preComputedNumSelectedRows = Optional.ofNullable(numSelectedRows);
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

  /**
   * @return an {@link Optional} containing the pre-computed number of selected rows, if available.
   *     <p>If present, this value was computed ahead of time and can be used without incurring any
   *     additional cost. This occurs in two cases:
   *     <ul>
   *       <li>When the selection vector is absent, which implies that all rows are selected — in
   *           this case, the number of selected rows is equal to the batch size.
   *       <li>When the number of selected rows was explicitly pre-computed and passed in.
   *     </ul>
   *     <p>If empty, the caller must compute the number of selected rows manually from the
   *     selection vector.
   */
  public Optional<Integer> getPreComputedNumSelectedRows() {
    if (!selectionVector.isPresent()) {
      return Optional.of(data.getSize());
    }
    return preComputedNumSelectedRows;
  }
}
