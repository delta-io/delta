package io.delta.kernel.data;

import io.delta.kernel.internal.ColumnarBatchRow;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;

/**
 * Represents zero or more rows of records with same schema type.
 */
public interface ColumnarBatch {
    /**
     * @return the schema of the data in this batch.
     */
    StructType getSchema();

    /**
     * Return the {@link ColumnVector} for the given ordinal in the columnar batch. If the ordinal
     * is not valid throws error (TODO:
     * @param ordinal
     * @return
     */
    ColumnVector getColumnVector(int ordinal);

    /**
     * Number of records/rows in the columnar batch.
     * @return
     */
    int getSize();

    /**
     * Return a slice of the current batch.
     * @param start Starting record to include in the returned columnar batch
     * @param end Ending record (exclusive) to include in the returned columnar batch
     * @return
     */
    default ColumnarBatch slice(int start, int end) {
        throw new UnsupportedOperationException("Not yet implemented!");
    }

    /**
     * Get an interator to read the data row by rows
     * @return
     */
    default CloseableIterator<Row> getRows() {

        ColumnarBatch batch = this;

        return new CloseableIterator<Row>() {

            int rowId = 0;
            int maxRowId = getSize();

            @Override
            public boolean hasNext() {
                return rowId < maxRowId;
            }

            @Override
            public Row next() {
                Row row = new ColumnarBatchRow(batch, rowId);
                rowId += 1;
                return row;
            }

            @Override
            public void close() { }
        };
    }
}
