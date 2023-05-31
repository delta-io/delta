package io.delta.kernel.data;


/**
 * Data read from a Delta table file. The scan file info is attached as {@link Row} object which
 * should be same as the one given by the Delta Kernel.
 */
public interface FileDataReadResult
{
    /**
     * Get the data read from the Parquet file.
     * @return Data in {@link ColumnarBatch} format.
     */
    ColumnarBatch getData();

    /**
     * Get the scan file {@link Row} info of the file from which the data is read. It should be
     * same as the row the Delta Kernel has passed to the connector to read the data from.
     * @return Data scan file info as {@link Row}
     */
    Row getScanFileRow();
}
