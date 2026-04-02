/*
 *  Copyright (2026) The Delta Lake Project Authors.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package io.delta.flink;

import io.delta.flink.kernel.FlinkParquetHandler;
import io.delta.flink.sink.DeltaWriterTask;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.defaults.engine.DefaultParquetHandler;
import io.delta.kernel.defaults.engine.fileio.FileIO;
import io.delta.kernel.defaults.engine.hadoopio.HadoopFileIO;
import io.delta.kernel.defaults.internal.data.DefaultColumnarBatch;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.types.DoubleType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.DataFileStatus;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.local.LocalDataOutputStream;
import org.apache.flink.formats.parquet.ParquetWriterFactory;
import org.apache.flink.formats.parquet.row.ParquetRowDataBuilder;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.hadoop.conf.Configuration;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

/**
 * Compares Parquet write throughput across three paths, including per-iteration
 * FilteredColumnarBatch construction cost:
 *
 * <ol>
 *   <li>{@code flinkParquetWriter} — Pure Flink BulkWriter (baseline)
 *   <li>{@code kernelParquetWriter} — Kernel's DefaultParquetHandler
 *   <li>{@code flinkKernelParquetHandler} — FlinkParquetHandler (Flink writer via Kernel adapter)
 * </ol>
 *
 * All benchmarks write 1,000,000 rows of schema (int, string(32), long, double) to local temp
 * directories. The FilteredColumnarBatch is rebuilt from the RowData list in each iteration.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 5)
@Measurement(iterations = 5, time = 5)
@Fork(1)
public class ParquetWriterBenchmark {

  private static final int NUM_RECORDS = 1_000_000;
  private static final int STRING_LENGTH = 32;

  private static final StructType KERNEL_SCHEMA =
      new StructType()
          .add("id", IntegerType.INTEGER)
          .add("name", StringType.STRING)
          .add("ts", LongType.LONG)
          .add("value", DoubleType.DOUBLE);

  private static final RowType FLINK_ROW_TYPE =
      RowType.of(
          new org.apache.flink.table.types.logical.IntType(),
          new VarCharType(VarCharType.MAX_LENGTH),
          new BigIntType(),
          new org.apache.flink.table.types.logical.DoubleType());

  private RowData[] preGeneratedRows;
  private FileIO localFileIO;

  @Setup(Level.Trial)
  public void setup() {
    Configuration conf = new Configuration();
    conf.set("fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem");
    localFileIO = new HadoopFileIO(conf);

    preGeneratedRows = new RowData[NUM_RECORDS];
    ThreadLocalRandom rng = ThreadLocalRandom.current();
    for (int i = 0; i < NUM_RECORDS; i++) {
      preGeneratedRows[i] =
          GenericRowData.of(
              i,
              StringData.fromString(randomString(rng, STRING_LENGTH)),
              rng.nextLong(),
              rng.nextDouble());
    }
  }

  // -----------------------------------------------------------------------
  //  Benchmark 1: Pure Flink Parquet Writer (baseline)
  // -----------------------------------------------------------------------

  @Benchmark
  public void flinkParquetWriter(Blackhole bh) throws Exception {
    Path tempDir = Files.createTempDirectory("flink-parquet-bench-");
    try {
      File outFile = tempDir.resolve("data.parquet").toFile();

      ParquetWriterFactory<RowData> writerFactory =
          ParquetRowDataBuilder.createWriterFactory(FLINK_ROW_TYPE, new Configuration(), true);

      try (FSDataOutputStream outputStream = new LocalDataOutputStream(outFile)) {
        BulkWriter<RowData> writer = writerFactory.create(outputStream);
        for (int i = 0; i < NUM_RECORDS; i++) {
          writer.addElement(preGeneratedRows[i]);
        }
        writer.finish();
      }
      bh.consume(outFile.length());
    } finally {
      deleteDir(tempDir);
    }
  }

  // -----------------------------------------------------------------------
  //  Benchmark 2: Kernel DefaultParquetHandler
  // -----------------------------------------------------------------------

  @Benchmark
  public void kernelParquetWriter(Blackhole bh) throws Exception {
    Path tempDir = Files.createTempDirectory("kernel-parquet-bench-");
    try {
      FilteredColumnarBatch batch = rowDataToColumnarBatch(preGeneratedRows, KERNEL_SCHEMA);

      DefaultParquetHandler handler = new DefaultParquetHandler(localFileIO);
      CloseableIterator<FilteredColumnarBatch> dataIter =
          Utils.singletonCloseableIterator(batch);

      long fileCount = 0;
      try (CloseableIterator<DataFileStatus> results =
          handler.writeParquetFiles(
              tempDir.toAbsolutePath().toString(), dataIter, Collections.emptyList())) {
        while (results.hasNext()) {
          bh.consume(results.next());
          fileCount++;
        }
      }
      bh.consume(fileCount);
    } finally {
      deleteDir(tempDir);
    }
  }

  // -----------------------------------------------------------------------
  //  Helpers
  // -----------------------------------------------------------------------

  private static FilteredColumnarBatch rowDataToColumnarBatch(
      RowData[] rows, StructType schema) {
    int numColumns = schema.length();
    ColumnVector[] columnVectors = new ColumnVector[numColumns];
    DeltaWriterTask.RowAccess rowAccess =
        new DeltaWriterTask.RowDataListAccess(Arrays.asList(rows));
    for (int colIdx = 0; colIdx < numColumns; colIdx++) {
      columnVectors[colIdx] =
          new DeltaWriterTask.RowDataColumnVectorView(
              rowAccess, colIdx, schema.at(colIdx).getDataType());
    }
    return new FilteredColumnarBatch(
        new DefaultColumnarBatch(rows.length, schema, columnVectors), Optional.empty());
  }

  private static final String CHARS = "abcdefghijklmnopqrstuvwxyz0123456789";

  private static String randomString(ThreadLocalRandom rng, int length) {
    char[] buf = new char[length];
    for (int i = 0; i < length; i++) {
      buf[i] = CHARS.charAt(rng.nextInt(CHARS.length()));
    }
    return new String(buf);
  }

  private static void deleteDir(Path dir) {
    try {
      Files.walk(dir)
          .sorted(Comparator.reverseOrder())
          .map(Path::toFile)
          .forEach(File::delete);
    } catch (IOException ignored) {
    }
  }
}
