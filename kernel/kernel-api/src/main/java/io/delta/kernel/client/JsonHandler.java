package io.delta.kernel.client;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.FileDataReadResult;
import io.delta.kernel.data.Row;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;

import java.io.IOException;

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
     * Parse the given <i>json</i> string and return the requested fields in given <i>outputSchema</i>
     * as a {@link Row}.
     *
     * @param jsonStringVector Valid JSON object in string format.
     * @param outputSchema Schema of the data to return from the parse JSON. If any requested fields
     *               are missing in the JSON string, a <i>null</i> is returned for that particular
     *               field in returned {@link Row}. The type for each given field is expected to
     *               match the type in JSON.
     * @return {@link ColumnarBatch} with one row for each entry is the input column vector with
     *         <i>outputSchema</i>
     */
    ColumnarBatch parseJson(ColumnVector jsonStringVector, StructType outputSchema);

    /**
     * Read the JSON format file at given location and return the data.
     *
     * @param fileIter Iterator of {@link FileReadContext} objects to read data from.
     * @param physicalSchema Select list of columns to read from the JSON file.
     * @return an iterator of data in columnar format. It is the responsibility of the caller to
     *         close the iterator. The data returned is in the same as the order of files given in
     *         <i>fileIter</i>.
     * @throws IOException if an error occurs during the read.
     */
    CloseableIterator<FileDataReadResult> readJsonFiles(
            CloseableIterator<FileReadContext> fileIter,
            StructType physicalSchema) throws IOException;
}
