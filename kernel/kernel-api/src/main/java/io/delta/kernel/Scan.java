package io.delta.kernel;

import io.delta.kernel.client.FileReadContext;
import io.delta.kernel.client.ParquetHandler;
import io.delta.kernel.client.TableClient;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.DataReadResult;
import io.delta.kernel.data.FileDataReadResult;
import io.delta.kernel.data.Row;
import io.delta.kernel.expressions.Expression;
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.fs.FileStatus;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.Tuple2;
import io.delta.kernel.utils.Utils;

import java.io.IOException;
import java.util.Optional;

/**
 * An object representing a scan of a Delta table.
 */
public interface Scan {
    /**
     * Get an iterator of data files to scan.
     *
     * @param tableClient {@link TableClient} instance to use in Delta Kernel.
     * @return data in {@link ColumnarBatch} format. Each row correspond to one scan file.
     */
    CloseableIterator<ColumnarBatch> getScanFiles(TableClient tableClient);

    /**
     * Get the remaining filter the Delta Kernel can not guarantee the data returned by it
     * satisfies the filter. This filter is used by Delta Kernel to do data skipping whenever
     * possible.
     * @return Remaining filter as {@link Expression}.
     */
    Expression getRemainingFilter();

    /**
     * Get the scan state associate with the current scan. This state is common to all survived
     * files.
     *
     * @param tableClient {@link TableClient} instance to use in Delta Kernel.
     * @return Scan state in {@link Row} format.
     */
    Row getScanState(TableClient tableClient);

    /**
     * Get the data from a given scan files with using the connector provider {@link TableClient}.
     *
     * @param tableClient Connector provided {@link TableClient} implementation.
     * @param scanState Scan state returned by {@link Scan#getScanState(TableClient)}
     * @param scanFileRowIter an iterator of {@link Row}s. Each {@link Row} represents one scan file
     *                        from the {@link ColumnarBatch} returned by
     *                        {@link Scan#getScanFiles(TableClient)}
     * @param filter An optional filter that can be used for data skipping while reading the
     *               scan files.
     * @return Data read from the scan file as an iterator of {@link ColumnarBatch}es and
     *         {@link ColumnVector} pairs. The {@link ColumnVector} represents a selection vector
     *         of type boolean that has the same size as the data {@link ColumnarBatch}. A value
     *         of true at a given index indicates the row with the same index from the data
     *         {@link ColumnarBatch} should be considered and a value of false at a given index
     *         indicates the row with the same index in data {@link ColumnarBatch} should be
     *         ignored. It is the responsibility of the caller to close the iterator.
     *
     * @throws IOException when error occurs reading the data.
     */
    static CloseableIterator<DataReadResult> readData(
            TableClient tableClient,
            Row scanState,
            CloseableIterator<Row> scanFileRowIter,
            Optional<Expression> filter) throws IOException {

        StructType readSchema = Utils.getPhysicalSchema(scanState);

        ParquetHandler parquetHandler = tableClient.getParquetHandler();

        CloseableIterator<FileReadContext> filesReadContextsIter =
                parquetHandler.contextualizeFileReads(
                        scanFileRowIter,
                        Literal.TRUE);

        CloseableIterator<FileDataReadResult> data =
                parquetHandler.readParquetFiles(filesReadContextsIter, readSchema);

        // TODO: Attach the selection vector associated with the file
        return data.map(fileDataReadResult ->
                new DataReadResult(
                        fileDataReadResult.getData(),
                        Optional.empty()
                )
        );
    }
}
