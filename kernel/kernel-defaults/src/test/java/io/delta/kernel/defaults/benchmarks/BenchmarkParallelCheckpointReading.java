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

package io.delta.kernel.defaults.benchmarks;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import static java.util.concurrent.Executors.newFixedThreadPool;

import org.apache.hadoop.conf.Configuration;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import io.delta.kernel.*;
import io.delta.kernel.client.ParquetHandler;
import io.delta.kernel.client.TableClient;
import io.delta.kernel.data.*;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;

import io.delta.kernel.internal.util.Utils;

import io.delta.kernel.defaults.client.DefaultParquetHandler;
import io.delta.kernel.defaults.client.DefaultTableClient;

import io.delta.kernel.defaults.internal.parquet.ParquetFileReader;

/**
 * Benchmark to measure the performance of reading multi-part checkpoint files, using a custom
 * ParquetHandler that reads the files in parallel. To run this benchmark (from delta repo root):
 * <ul>
 *     <li>Generate the test table by following the instructions at `testTablePath` member variable.
 *     </li>
 *     <li>
 *         <pre>{@code
 *          build/sbt sbt:delta> project kernelDefaults
 *          sbt:delta> set fork in run := true sbt:delta>
 *          sbt:delta> test:runMain \
 *            io.delta.kernel.defaults.benchmarks.BenchmarkParallelCheckpointReading.
 *         }</pre>
 *     </li>
 * </ul>
 * <p>
 * Sample benchmarks on a table with checkpoint (13) parts containing total of 1.3mil actions on
 * Macbook Pro M2 Max with table stored locally.
 * <pre>{@code
 * Benchmark  (parallelReaderCount)  Mode  Cnt Score Error  Units
 * benchmark                      0  avgt    5  1565.520 ±  20.551  ms/op
 * benchmark                      1  avgt    5  1064.850 ±  19.699  ms/op
 * benchmark                      2  avgt    5   785.918 ± 176.285  ms/op
 * benchmark                      4  avgt    5   729.487 ±  51.470  ms/op
 * benchmark                     10  avgt    5   693.757 ±  41.252  ms/op
 * benchmark                     20  avgt    5   702.656 ±  19.145  ms/op
 * }</pre>
 */
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3)
@Fork(1)
public class BenchmarkParallelCheckpointReading {

    /**
     * Following are the steps to generate a simple large table with multi-part checkpoint files
     * <pre>{@code
     * bin/spark-shell --packages io.delta:delta-spark_2.12:3.1.0 \
     *   --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
     *   --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
     *   --conf "spark.databricks.delta.checkpoint.partSize=100000"
     *
     * # Within the Spark shell, run the following commands
     * scala> spark.range(0, 100000) .withColumn("pCol", 'id % 100000) .repartition(10)
     *   .write.format("delta") .partitionBy("pCol") .mode("append")
     *   .save("~/test-tables/large-table")
     *
     * # Repeat the above steps for each of the next ranges # 100000 to 200000, 200000 to 300000 etc
     * until enough log entries are reached.
     *
     * # Then create a checkpoint
     * # This step create a multi-part checkpoint with each checkpoint containing 100K records.
     * scala> import org.apache.spark.sql.delta.DeltaLog
     * scala> DeltaLog.forTable(spark, "~/test-tables/large-table").checkpoint()
     * }</pre>
     */
    public static final String testTablePath = "<TODO> fill the path here";

    @State(Scope.Benchmark)
    public static class BenchmarkData {
        // Variations of number of threads to read the multi-part checkpoint files
        // When thread count is 0, we read using the current default parquet handler implementation
        // In all other cases we use the custom parallel parquet handler implementation defined
        // in this benchmark
        @Param({"0", "1", "2", "4", "10", "20"})
        private int parallelReaderCount = 0;
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void benchmark(BenchmarkData benchmarkData, Blackhole blackhole) throws Exception {
        TableClient tableClient = createTableClient(benchmarkData.parallelReaderCount);
        Table table = Table.forPath(tableClient, testTablePath);

        Snapshot snapshot = table.getLatestSnapshot(tableClient);
        ScanBuilder scanBuilder = snapshot.getScanBuilder(tableClient);
        Scan scan = scanBuilder.build();

        // Scan state is not used, but get it so that we simulate the real use case.
        Row row = scan.getScanState(tableClient);
        blackhole.consume(row); // To avoid dead code elimination by the JIT compiler
        long fileSize = 0;
        try (CloseableIterator<FilteredColumnarBatch> batchIter = scan.getScanFiles(tableClient)) {
            while (batchIter.hasNext()) {
                FilteredColumnarBatch batch = batchIter.next();
                try (CloseableIterator<Row> rowIter = batch.getRows()) {
                    while (rowIter.hasNext()) {
                        Row r = rowIter.next();
                        long size = r.getStruct(0).getLong(2);
                        fileSize += size;
                    }
                }
            }
        }

        // Consume the result to avoid dead code elimination by the JIT compiler
        blackhole.consume(fileSize);
    }

    public static void main(String[] args) throws Exception {
        org.openjdk.jmh.Main.main(args);
    }

    private static TableClient createTableClient(int numberOfParallelThreads) {
        Configuration hadoopConf = new Configuration();
        if (numberOfParallelThreads <= 0) {
            return DefaultTableClient.create(hadoopConf);
        }

        return new DefaultTableClient(hadoopConf) {
            @Override
            public ParquetHandler getParquetHandler() {
                return new ParallelParquetHandler(hadoopConf, numberOfParallelThreads);
            }
        };
    }

    /**
     * Custom implementation of {@link ParquetHandler} to read the Parquet files in parallel. Reason
     * for this not being in the {@link DefaultParquetHandler} is that this implementation keeps the
     * contents of the Parquet files in memory, which is not suitable for default implementation
     * without a proper design that allows limits on the memory usage. If the parallel reading of
     * checkpoint becomes a common in connectors, we can look at adding the functionality in the
     * default implementation.
     */
    static class ParallelParquetHandler extends DefaultParquetHandler {
        private final Configuration hadoopConf;
        private final int numberOfParallelThreads;

        ParallelParquetHandler(Configuration hadoopConf, int numberOfParallelThreads) {
            super(hadoopConf);
            this.hadoopConf = hadoopConf;
            this.numberOfParallelThreads = numberOfParallelThreads;
        }

        @Override
        public CloseableIterator<ColumnarBatch> readParquetFiles(
                CloseableIterator<FileStatus> fileIter,
                StructType physicalSchema,
                Optional<Predicate> predicate) throws IOException {
            return new CloseableIterator<ColumnarBatch>() {
                // Executor service will be closed as part of the returned `CloseableIterator`'s
                // close method.
                private final ExecutorService executorService =
                        newFixedThreadPool(numberOfParallelThreads);
                private Iterator<Future<List<ColumnarBatch>>> futuresIter;
                private Iterator<ColumnarBatch> currentBatchIter;

                @Override
                public void close() throws IOException {
                    Utils.closeCloseables(fileIter, () -> executorService.shutdown());
                }

                @Override
                public boolean hasNext() {
                    submitReadRequestsIfNotDone();
                    if (currentBatchIter != null && currentBatchIter.hasNext()) {
                        return true;
                    }

                    if (futuresIter.hasNext()) {
                        try {
                            currentBatchIter = futuresIter.next().get().iterator();
                            return hasNext();
                        } catch (InterruptedException | ExecutionException e) {
                            throw new RuntimeException(e);
                        }
                    }
                    return false;
                }

                @Override
                public ColumnarBatch next() {
                    return currentBatchIter.next();
                }

                private void submitReadRequestsIfNotDone() {
                    if (futuresIter != null) {
                        return;
                    }
                    List<Future<List<ColumnarBatch>>> futures = new ArrayList<>();
                    while (fileIter.hasNext()) {
                        String path = fileIter.next().getPath();
                        futures.add(
                                executorService.submit(
                                        () -> parquetFileReader(path, physicalSchema)));
                    }
                    futuresIter = futures.iterator();
                }
            };
        }

        List<ColumnarBatch> parquetFileReader(String filePath, StructType readSchema) {
            ParquetFileReader reader = new ParquetFileReader(hadoopConf);
            try (CloseableIterator<ColumnarBatch> batchIter = reader.read(filePath, readSchema)) {
                List<ColumnarBatch> batches = new ArrayList<>();
                while (batchIter.hasNext()) {
                    batches.add(batchIter.next());
                }
                return batches;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
