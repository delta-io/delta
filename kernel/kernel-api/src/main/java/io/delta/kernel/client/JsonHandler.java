package io.delta.kernel.client;

import java.io.IOException;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.FileDataReadResult;
import io.delta.kernel.data.Row;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;

/**
 * Provides JSON handling functionality to Delta Kernel. Delta Kernel can use this client to
 * parse JSON strings into {@link io.delta.kernel.data.Row}. Connectors can leverage
 * this interface to provide their best implementation of the JSON parsing capability to
 * Delta Kernel.
 */
public interface JsonHandler
        extends FileHandler
{
    /**
     * Parse the given <i>json</i> strings and return the fields requested by {@code outputSchema}
     * as columns in a {@link ColumnarBatch}.
     *
     * @param jsonStringVector String {@link ColumnVector} of valid JSON strings.
     * @param outputSchema Schema of the data to return from the parsed JSON. If any requested
     *                     fields are missing in the JSON string, a <i>null</i> is returned for that
     *                     particular field in the returned {@link Row}. The type for each given
     *                     field is expected to match the type in the JSON.
     * @return a {@link ColumnarBatch} of schema {@code outputSchema} with one row for each entry
     *         in {@code jsonStringVector}
     */
    ColumnarBatch parseJson(ColumnVector jsonStringVector, StructType outputSchema);

    /**
     * Read and parse the JSON format file at given locations and return the data as a
     * {@link ColumnarBatch} with the columns requested by {@code physicalSchema}.
     *
     * @param fileIter Iterator of {@link FileReadContext} objects to read data from.
     * @param physicalSchema Select list of columns to read from the JSON file.
     * @return an iterator of {@link FileDataReadResult}s containing the data in columnar format
     *         and the corresponding scan file information. It is the responsibility of the caller
     *         to close the iterator. The data returned is in the same as the order of files given
     *         in <i>fileIter</i>.
     * @throws IOException if an error occurs during the read.
     */
    CloseableIterator<FileDataReadResult> readJsonFiles(
            CloseableIterator<FileReadContext> fileIter,
            StructType physicalSchema) throws IOException;
}
