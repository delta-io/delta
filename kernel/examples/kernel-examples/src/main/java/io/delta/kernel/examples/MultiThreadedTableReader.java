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

import java.util.List;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import io.delta.kernel.*;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.examples.utils.RowSerDe;
import io.delta.kernel.exceptions.TableNotFoundException;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;

import static io.delta.kernel.examples.utils.Utils.parseArgs;

import io.delta.kernel.internal.InternalScanFileUtils;
import io.delta.kernel.internal.data.ScanStateRow;
import io.delta.kernel.internal.util.Utils;
import static io.delta.kernel.internal.util.Utils.singletonCloseableIterator;

/**
 * Multi-threaded Delta Lake table reader using the Delta Kernel APIs. It illustrates
 * how to use the scan files rows received from the Delta Kernel in distributed engine.
 * <p>
 * For this example serialization and deserialization is not needed as the work generator and
 * work executors share the same memory, but it illustrates an example of how Delta Kernel can
 * work in a distributed query engine. High level steps are:
 * - The query engine asks the Delta Kernel APIs for scan file and scan state rows at the driver
 * (or equivalent) node
 * - The query engine serializes the scan file and scan state at the driver node
 * - The driver sends the serialized bytes to remote worker node(s)
 * - Worker nodes deserialize the scan file and scan state rows from the serialized bytes
 * - Worker nodes read the data from given scan file(s) and scan state using the Delta Kernel APIs.
 *
 * <p>
 * Usage:
 * java io.delta.kernel.examples.SingleThreadedTableReader [-c <arg>][-l <arg>] [-p <arg>] -t <arg>
 * -c,--columns <arg>       Comma separated list of columns to read from the
 * table. Ex. --columns=id,name,address
 * -l,--limit <arg>         Maximum number of rows to read from the table (default 20).
 * -p,--parallelism <arg>   Number of parallel readers to use (default 3).
 * -t,--table <arg>         Fully qualified table path
 * </p>
 */
public class MultiThreadedTableReader
    extends BaseTableReader {
    private static final int DEFAULT_NUM_THREADS = 3;

    private final int numThreads;

    public MultiThreadedTableReader(int numThreads, String tablePath) {
        super(tablePath);
        this.numThreads = numThreads;
    }

    public int show(int limit, Optional<List<String>> columnsOpt, Optional<Predicate> predicate)
        throws TableNotFoundException {
        Table table = Table.forPath(engine, tablePath);
        Snapshot snapshot = table.getLatestSnapshot(engine);
        StructType readSchema = pruneSchema(snapshot.getSchema(engine), columnsOpt);

        ScanBuilder scanBuilder = snapshot.getScanBuilder(engine)
            .withReadSchema(engine, readSchema);

        if (predicate.isPresent()) {
            scanBuilder = scanBuilder.withFilter(engine, predicate.get());
        }

        return new Reader(limit)
            .readData(readSchema, scanBuilder.build());
    }

    public static void main(String[] args)
        throws Exception {
        Options cliOptions = baseOptions().addOption(
            Option.builder()
                .option("p")
                .longOpt("parallelism")
                .hasArg()
                .desc("Number of parallel readers to use (default 3).")
                .type(Number.class)
                .build());
        CommandLine commandLine = parseArgs(cliOptions, args);

        String tablePath = commandLine.getOptionValue("table");
        int limit = parseInt(commandLine, "limit", DEFAULT_LIMIT);
        int numThreads = parseInt(commandLine, "parallelism", DEFAULT_NUM_THREADS);
        Optional<List<String>> columns = parseColumnList(commandLine, "columns");

        new MultiThreadedTableReader(numThreads, tablePath)
            .show(limit, columns, Optional.empty());
    }

    /**
     * Work unit representing the scan state and scan file in serialized format.
     */
    private static class ScanFile {
        /**
         * Special instance of the {@link ScanFile} to indicate to the worker that there are no
         * more scan files to scan and stop the worker thread.
         */
        private static final ScanFile POISON_PILL = new ScanFile("", "");

        final String stateJson;
        final String fileJson;

        ScanFile(Row scanStateRow, Row scanFileRow) {
            this.stateJson = RowSerDe.serializeRowToJson(scanStateRow);
            this.fileJson = RowSerDe.serializeRowToJson(scanFileRow);
        }

        ScanFile(String stateJson, String fileJson) {
            this.stateJson = stateJson;
            this.fileJson = fileJson;
        }

        /**
         * Get the deserialized scan state as {@link Row} object
         */
        Row getScanRow(Engine engine) {
            return RowSerDe.deserializeRowFromJson(engine, stateJson);
        }

        /**
         * Get the deserialized scan file as {@link Row} object
         */
        Row getScanFileRow(Engine engine) {
            return RowSerDe.deserializeRowFromJson(engine, fileJson);
        }
    }

    private class Reader {
        private final int limit;
        private final AtomicBoolean stopSignal = new AtomicBoolean(false);
        private final CountDownLatch countDownLatch = new CountDownLatch(numThreads);
        private final ExecutorService executorService =
            Executors.newFixedThreadPool(numThreads + 1);
        private final BlockingQueue<ScanFile> workQueue = new ArrayBlockingQueue<>(20);

        private int readRecordCount; // Number of rows read so far, synchronized with `this` object
        private AtomicReference<Exception> error = new AtomicReference<>();

        Reader(int limit) {
            this.limit = limit;
        }

        /**
         * Read the data from the given {@code snapshot}.
         *
         * @param readSchema Subset of columns to read from the snapshot.
         * @param scan Scan object to read data from.
         * @return Number of rows read
         */
        int readData(StructType readSchema, Scan scan) {
            printSchema(readSchema);
            try {
                executorService.submit(workGenerator(scan));
                for (int i = 0; i < numThreads; i++) {
                    executorService.submit(workConsumer(i));
                }

                countDownLatch.await();
            } catch (InterruptedException ie) {
                System.out.println("Interrupted exiting now..");
                throw new RuntimeException(ie);
            } finally {
                stopSignal.set(true);
                executorService.shutdownNow();
                if (error.get() != null) {
                    throw new RuntimeException(error.get());
                }
            }

            return readRecordCount;
        }

        private Runnable workGenerator(Scan scan) {
            return (() -> {
                Row scanStateRow = scan.getScanState(engine);
                try(CloseableIterator<FilteredColumnarBatch> scanFileIter =
                    scan.getScanFiles(engine)) {

                    while (scanFileIter.hasNext() && !stopSignal.get()) {
                        try (CloseableIterator<Row> scanFileRows = scanFileIter.next().getRows()) {
                            while (scanFileRows.hasNext() && !stopSignal.get()) {
                                workQueue.put(new ScanFile(scanStateRow, scanFileRows.next()));
                            }
                        }
                    }

                    for (int i = 0; i < numThreads; i++) {
                        // poison pill for each worker threads to stop the work.
                        workQueue.put(ScanFile.POISON_PILL);
                    }
                } catch (InterruptedException ie) {
                    System.out.print("Work generator is interrupted");
                } catch (Exception e) {
                    error.compareAndSet(null /* expected */, e);
                    throw new RuntimeException(e);
                }
            });
        }

        private Runnable workConsumer(int workerId) {
            return (() -> {
                try {
                    ScanFile work = workQueue.take();
                    if (work == ScanFile.POISON_PILL) {
                        return; // exit as there are no more work units
                    }
                    Row scanState = work.getScanRow(engine);
                    Row scanFile = work.getScanFileRow(engine);
                    FileStatus fileStatus =
                        InternalScanFileUtils.getAddFileStatus(scanFile);
                    StructType physicalReadSchema =
                        ScanStateRow.getPhysicalDataReadSchema(engine, scanState);

                    CloseableIterator<ColumnarBatch> physicalDataIter =
                        engine.getParquetHandler().readParquetFiles(
                            singletonCloseableIterator(fileStatus),
                            physicalReadSchema,
                            Optional.empty());

                    try (
                        CloseableIterator<FilteredColumnarBatch> dataIter =
                            Scan.transformPhysicalData(
                                engine,
                                scanState,
                                scanFile,
                                physicalDataIter)) {
                        while (dataIter.hasNext()) {
                            if (printDataBatch(dataIter.next())) {
                                // Have enough records, exit now.
                                break;
                            }
                        }
                    }
                } catch (InterruptedException ie) {
                    System.out.printf("Worker %d is interrupted." + workerId);
                } catch (Exception e) {
                    error.compareAndSet(null /* expected */, e);
                    throw new RuntimeException(e);
                } finally {
                    countDownLatch.countDown();
                }
            });
        }

        /**
         * Returns true when sufficient amount of rows are received
         */
        private boolean printDataBatch(FilteredColumnarBatch data) {
            synchronized (this) {
                if (readRecordCount >= limit) {
                    return true;
                }
                readRecordCount += printData(data, limit - readRecordCount);
                return readRecordCount >= limit;
            }
        }
    }
}
