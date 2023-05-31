package io.delta.kernel.data;

import java.util.Optional;

/**
 * Data read from Delta table file. Data is in {@link ColumnarBatch} format with
 * an additional selection vector to select only a subset of rows for this columnar batch.
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
     * @return
     */
    public ColumnarBatch getData() {
        return data;
    }

    /**
     * Optional selection vector containing one entry for each row in <i>data</i> indicating whether
     * a row is selected or not selected.
     * @return {@link ColumnVector}
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
