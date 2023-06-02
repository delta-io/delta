package io.delta.kernel.client;

import java.io.IOException;

import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.FileDataReadResult;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;

/**
 * Provides Parquet file related functionalities to Delta Kernel. Connectors can leverage this
 * interface to provide their own custom implementation of Parquet data file functionalities to
 * Delta Kernel.
 */
public interface ParquetHandler
    extends FileHandler
{
    /**
     * Read the Parquet format files at the given locations and return the data as a
     * {@link ColumnarBatch} with the columns requested by {@code physicalSchema}.
     *
     * @param fileIter Iterator of {@link FileReadContext} objects to read data from.
     * @param physicalSchema Select list of columns to read from the Parquet file.
     * @return an iterator of {@link FileDataReadResult}s containing the data in columnar format
     *         and the corresponding scan file information. It is the responsibility of the caller
     *         to close the iterator. The data returned is in the same as the order of files given
     *         in <i>fileIter</i>.
     * @throws IOException if an error occurs during the read.
     */
    CloseableIterator<FileDataReadResult> readParquetFiles(
            CloseableIterator<FileReadContext> fileIter,
            StructType physicalSchema) throws IOException;
}
