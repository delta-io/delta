package io.delta.kernel.data;

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
     * is not valid throws error.
     * @param ordinal the ordinal of the column to retrieve
     * @return the {@link ColumnVector} for the given ordinal in the columnar batch
     */
    ColumnVector getColumnVector(int ordinal);

    /**
     * @return the number of rows/records in the columnar batch
     */
    int getSize();

    /**
     * Return a slice of the current batch.
     *
     * @param start Starting record index to include in the returned columnar batch
     * @param end Ending record index (exclusive) to include in the returned columnar batch
     * @return a columnar batch containing the records between [start, end)
     */
    default ColumnarBatch slice(int start, int end) {
        throw new UnsupportedOperationException("Not yet implemented!");
    }

    /**
     * @return iterator of {@link Row}s in this batch
     */
    default CloseableIterator<Row> getRows() {
        // TODO needs io.delta.kernel.internal.ColumnarBatchRow
        throw new UnsupportedOperationException("Not yet implemented!");
    }
}
