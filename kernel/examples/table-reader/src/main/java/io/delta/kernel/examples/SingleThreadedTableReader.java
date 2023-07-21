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

import io.delta.kernel.Scan;
import io.delta.kernel.Snapshot;
import io.delta.kernel.Table;
import io.delta.kernel.TableNotFoundException;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.DataReadResult;
import io.delta.kernel.data.Row;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;

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
    extends BaseTableReader
{
    public SingleThreadedTableReader(String tablePath)
    {
        super(tablePath);
    }

    public static void main(String[] args)
        throws Exception
    {
        CommandLine commandLine = parseArgs(baseOptions(), args);

        String tablePath = commandLine.getOptionValue("table");
        int limit = parseInt(commandLine, "limit", DEFAULT_LIMIT);
        Optional<List<String>> columns = parseColumnList(commandLine, "columns");

        new SingleThreadedTableReader(tablePath)
            .show(limit, columns);
    }

    @Override
    public void show(int limit, Optional<List<String>> columnsOpt)
        throws TableNotFoundException, IOException
    {
        Table table = Table.forPath(tablePath);
        Snapshot snapshot = table.getLatestSnapshot(tableClient);
        StructType readSchema = pruneSchema(snapshot.getSchema(tableClient), columnsOpt);

        readSnapshot(readSchema, snapshot, limit);
    }

    /**
     * Utility method to read and print the data from the given {@code snapshot}.
     *
     * @param readSchema Subset of columns to read from the snapshot.
     * @param snapshot Table snapshot object
     * @param maxRowCount Not a hard limit but use this limit to stop reading more columnar batches
     * once the already read columnar batches have at least these many rows.
     * @return
     * @throws Exception
     */
    private void readSnapshot(
        StructType readSchema,
        Snapshot snapshot,
        int maxRowCount) throws IOException
    {
        Scan scan = snapshot.getScanBuilder(tableClient)
            .withReadSchema(tableClient, readSchema)
            .build();

        printSchema(readSchema);

        Row scanState = scan.getScanState(tableClient);
        CloseableIterator<ColumnarBatch> scanFileIter = scan.getScanFiles(tableClient);

        int readRecordCount = 0;
        try {
            while (scanFileIter.hasNext()) {
                try (CloseableIterator<DataReadResult> data =
                    Scan.readData(
                        tableClient,
                        scanState,
                        scanFileIter.next().getRows(),
                        Optional.empty())) {
                    while (data.hasNext()) {
                        DataReadResult dataReadResult = data.next();
                        readRecordCount += printData(dataReadResult, maxRowCount - readRecordCount);
                        if (readRecordCount >= maxRowCount) {
                            return;
                        }
                    }
                }
            }
        }
        finally {
            scanFileIter.close();
        }
    }
}
