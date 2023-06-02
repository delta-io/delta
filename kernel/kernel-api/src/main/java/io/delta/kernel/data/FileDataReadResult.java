package io.delta.kernel.data;


/**
 * Data read from a Delta table file and the corresponding scan file information.
 */
public interface FileDataReadResult
{
    /**
     * Get the data read from the file.
     * @return Data in {@link ColumnarBatch} format.
     */
    ColumnarBatch getData();

    /**
     * Get the scan file information of the file from which the data is read as a {@link Row}. This
     * should be the same {@link Row} that Delta Kernel provided when reading/contextualizing
     * the file.
     * @return a scan file {@link Row}
     */
    Row getScanFileRow();
}
