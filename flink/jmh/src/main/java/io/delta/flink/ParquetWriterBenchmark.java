/*
 * Copyright (2026) The Delta Lake Project Authors.
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

package io.delta.flink;

import io.delta.flink.sink.DeltaSinkConf;
import io.delta.flink.sink.DeltaWriterTask;
import io.delta.flink.table.HadoopTable;
import io.delta.kernel.types.DoubleType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
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
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.hadoop.conf.Configuration;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

/**
 * Compares Parquet write throughput between Flink's generic ParquetRowDataBuilder writer and Delta
 * Kernel Engine's Parquet writer.
 *
 * <p>Both benchmarks write 1,000,000 rows of schema (int, string(32), long, double) to local temp
 * directories, measuring raw file-writing time.
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
  private RowDataSerializer serializer;

  @Setup(Level.Trial)
  public void setup() {
    serializer = new RowDataSerializer(FLINK_ROW_TYPE);
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
  //  Flink Generic Parquet Writer
  // -----------------------------------------------------------------------

//  @Benchmark
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
  //  Delta Kernel Engine Parquet Writer
  // -----------------------------------------------------------------------

  @Benchmark
  public void kernelParquetWriter(Blackhole bh) throws Exception {
    Path tempDir = Files.createTempDirectory("kernel-parquet-bench-");
    System.out.println(tempDir);
    try {
      HadoopTable table = new HadoopTable(tempDir.toUri(), Map.of(), KERNEL_SCHEMA, List.of());
      table.open();

      DeltaWriterTask task =
          new DeltaWriterTask(
              UUID.randomUUID().toString(),
              0,
              0,
              table,
              new DeltaSinkConf(KERNEL_SCHEMA, Map.of("file_rolling.size", "-1")),
              Map.of());
      EmptyContext context = new EmptyContext();

      for (int i = 0; i < NUM_RECORDS; i++) {
        task.write(preGeneratedRows[i], context);
      }
      var results = task.complete();
      bh.consume(results.size());

      table.close();
    } finally {
//      deleteDir(tempDir);
    }
  }

  // -----------------------------------------------------------------------
  //  Helpers
  // -----------------------------------------------------------------------

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
