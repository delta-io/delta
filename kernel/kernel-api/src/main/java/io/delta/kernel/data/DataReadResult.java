package io.delta.kernel.data;

import java.util.Optional;

/**
 * Data read from Delta table file. Data is in {@link ColumnarBatch} format with an optional
 * selection vector to select only a subset of rows for this columnar batch.
 *
 * The selection vector is of type boolean and has the same size as the data in the corresponding
 * {@link ColumnarBatch}. For each row index, a value of true in the selection vector indicates
 * the row at the same index in the data {@link ColumnarBatch} is valid; a value of false
 * indicates the row should be ignored. If there is no selection vector then all the rows are valid.
 */
public class DataReadResult
{
    private final ColumnarBatch data;
    private final Optional<ColumnVector> selectionVector;

    public DataReadResult(ColumnarBatch data, Optional<ColumnVector> selectionVector)
    {
        this.data = data;
        this.selectionVector = selectionVector;
    }

    /**
     * Return the data as {@link ColumnarBatch}. Not all rows in the data are valid for this result.
     * An optional <i>selectionVector</i> determines which rows are selected. If there is no
     * selection vector that means all rows in this columnar batch are valid for this result.
     * @return all the data read from the file
     */
    public ColumnarBatch getData() {
        return data;
    }

    /**
     * Optional selection vector containing one entry for each row in <i>data</i> indicating whether
     * a row is selected or not selected. If there is no selection vector then all the rows are
     * valid.
     * @return an optional {@link ColumnVector} indicating which rows are valid
     */
    public Optional<ColumnVector> getSelectionVector()
    {
        return selectionVector;
    }

    /**
     * Helper method to rewrite the <i>data</i> in this result by removing the rows that are not
     * selected.
     * @return A {@link ColumnarBatch} with only the selected rows according to the
     *         {@link #getSelectionVector()}
     */
    public ColumnarBatch rewriteWithoutSelectionVector() {
        return data;
        // TODO: implement removing deleted rows.
    }
}
