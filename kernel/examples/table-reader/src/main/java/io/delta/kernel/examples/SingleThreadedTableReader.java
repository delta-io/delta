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
package io.delta.kernel.examples;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import org.apache.commons.cli.CommandLine;

import io.delta.kernel.*;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;

import io.delta.kernel.internal.InternalScanFileUtils;
import io.delta.kernel.internal.data.ScanStateRow;
import static io.delta.kernel.internal.util.Utils.singletonCloseableIterator;

/**
 * Single threaded Delta Lake table reader using the Delta Kernel APIs.
 *
 * <p>
 * Usage: java io.delta.kernel.examples.SingleThreadedTableReader [-c <arg>] [-l <arg>] -t <arg>
 * <p>
 * -c,--columns <arg>   Comma separated list of columns to read from the
 * table. Ex. --columns=id,name,address
 * -l,--limit <arg>     Maximum number of rows to read from the table
 * (default 20).
 * -t,--table <arg>     Fully qualified table path
 * </p>
 */
public class SingleThreadedTableReader
    extends BaseTableReader {
    public SingleThreadedTableReader(String tablePath) {
        super(tablePath);
    }

    @Override
    public int show(int limit, Optional<List<String>> columnsOpt, Optional<Predicate> predicate)
        throws TableNotFoundException, IOException {
        Table table = Table.forPath(tableClient, tablePath);
        Snapshot snapshot = table.getLatestSnapshot(tableClient);
        StructType readSchema = pruneSchema(snapshot.getSchema(tableClient), columnsOpt);

        ScanBuilder scanBuilder = snapshot.getScanBuilder(tableClient)
            .withReadSchema(tableClient, readSchema);

        if (predicate.isPresent()) {
            scanBuilder = scanBuilder.withFilter(tableClient, predicate.get());
        }

        return readData(readSchema, scanBuilder.build(), limit);
    }

    public static void main(String[] args)
        throws Exception {
        CommandLine commandLine = parseArgs(baseOptions(), args);

        String tablePath = commandLine.getOptionValue("table");
        int limit = parseInt(commandLine, "limit", DEFAULT_LIMIT);
        Optional<List<String>> columns = parseColumnList(commandLine, "columns");

        new SingleThreadedTableReader(tablePath)
            .show(limit, columns, Optional.empty());
    }

    /**
     * Utility method to read and print the data from the given {@code snapshot}.
     *
     * @param readSchema  Subset of columns to read from the snapshot.
     * @param scan        Table scan object
     * @param maxRowCount Not a hard limit but use this limit to stop reading more columnar batches
     *                    once the already read columnar batches have at least these many rows.
     * @return Number of rows read.
     * @throws Exception
     */
    private int readData(StructType readSchema, Scan scan, int maxRowCount) throws IOException {
        printSchema(readSchema);

        Row scanState = scan.getScanState(tableClient);
        CloseableIterator<FilteredColumnarBatch> scanFileIter = scan.getScanFiles(tableClient);

        int readRecordCount = 0;
        try {
            while (scanFileIter.hasNext()) {
                FilteredColumnarBatch scanFilesBatch = scanFileIter.next();
                try (CloseableIterator<Row> scanFileRows = scanFilesBatch.getRows()) {
                    while (scanFileRows.hasNext()) {
                        Row scanFileRow = scanFileRows.next();
                        FileStatus fileStatus =
                            InternalScanFileUtils.getAddFileStatus(scanFileRow);
                        StructType physicalReadSchema =
                            scan.getPhysicalDataReadSchema(tableClient, scanState, scanFileRow);
                        CloseableIterator<ColumnarBatch> physicalDataIter =
                            tableClient.getParquetHandler().readParquetFiles(
                                singletonCloseableIterator(fileStatus),
                                physicalReadSchema,
                                Optional.empty());
                        try (
                            CloseableIterator<FilteredColumnarBatch> transformedData =
                                Scan.transformPhysicalData(
                                    tableClient,
                                    scanState,
                                    scanFileRow,
                                    physicalDataIter)) {
                            while (transformedData.hasNext()) {
                                FilteredColumnarBatch filteredData = transformedData.next();
                                readRecordCount +=
                                    printData(filteredData, maxRowCount - readRecordCount);
                                if (readRecordCount >= maxRowCount) {
                                    return readRecordCount;
                                }
                            }
                        }
                    }
                }
            }
        } finally {
            scanFileIter.close();
        }

        return readRecordCount;
    }
}
