/*
 * Copyright (2023) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.delta.kernel;

import java.io.IOException;
import java.util.Optional;

import io.delta.kernel.client.FileReadContext;
import io.delta.kernel.client.ParquetHandler;
import io.delta.kernel.client.TableClient;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.DataReadResult;
import io.delta.kernel.data.FileDataReadResult;
import io.delta.kernel.data.Row;
import io.delta.kernel.expressions.Expression;
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.Utils;

/**
 * Represents a scan of a Delta table.
 */
public interface Scan {
    /**
     * Get an iterator of data files to scan.
     *
     * @param tableClient {@link TableClient} instance to use in Delta Kernel.
     * @return iterator of {@link ColumnarBatch}s where each row in each batch corresponds to one
     *         scan file
     */
    CloseableIterator<ColumnarBatch> getScanFiles(TableClient tableClient);

    /**
     * Get the remaining filter that is not guaranteed to be satisfied for the data Delta Kernel
     * returns. This filter is used by Delta Kernel to do data skipping when possible.
     *
     * @return the remaining filter as an {@link Expression}.
     */
    Expression getRemainingFilter();

    /**
     * Get the scan state associated with the current scan. This state is common across all
     * files in the scan to be read.
     *
     * @param tableClient {@link TableClient} instance to use in Delta Kernel.
     * @return Scan state in {@link Row} format.
     */
    Row getScanState(TableClient tableClient);

    /**
     * Get the data from the given scan files using the connector provided {@link TableClient}.
     *
     * @param tableClient Connector provided {@link TableClient} implementation.
     * @param scanState Scan state returned by {@link Scan#getScanState(TableClient)}
     * @param scanFileRowIter an iterator of {@link Row}s. Each {@link Row} represents one scan file
     *                        from the {@link ColumnarBatch} returned by
     *                        {@link Scan#getScanFiles(TableClient)}
     * @param filter An optional filter that can be used for data skipping while reading the
     *               scan files.
     * @return Data read from the input scan files as an iterator of {@link DataReadResult}s. Each
     *         {@link DataReadResult} instance contains the data read and an optional selection
     *         vector that indicates data rows as valid or invalid. It is the responsibility of the
     *         caller to close this iterator.
     * @throws IOException when error occurs while reading the data.
     */
    static CloseableIterator<DataReadResult> readData(
            TableClient tableClient,
            Row scanState,
            CloseableIterator<Row> scanFileRowIter,
            Optional<Expression> filter) throws IOException {

        StructType readSchema = Utils.getPhysicalSchema(tableClient, scanState);

        ParquetHandler parquetHandler = tableClient.getParquetHandler();

        CloseableIterator<FileReadContext> filesReadContextsIter =
                parquetHandler.contextualizeFileReads(
                        scanFileRowIter,
                        filter.orElse(Literal.TRUE));

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
