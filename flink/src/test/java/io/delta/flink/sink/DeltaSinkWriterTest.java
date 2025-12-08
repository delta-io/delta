/*
 *  Copyright (2021) The Delta Lake Project Authors.
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

package io.delta.flink.sink;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.delta.flink.TestHelper;
import io.delta.flink.table.HadoopTable;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.types.*;
import java.lang.management.ManagementFactory;
import java.net.URI;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** JUnit 6 test suite for {@link DeltaSinkWriter}. */
class DeltaSinkWriterTest extends TestHelper {

  @Test
  void testWriteToEmptyTableWithNoPartition() {
    withTempDir(
        dir -> {
          String tablePath = dir.getAbsolutePath();
          StructType schema =
              new StructType().add("id", IntegerType.INTEGER).add("part", StringType.STRING);

          HadoopTable table =
              new HadoopTable(
                  URI.create(tablePath), Collections.emptyMap(), schema, Collections.emptyList());
          table.open();

          DeltaSinkWriter sinkWriter =
              new DeltaSinkWriter.Builder()
                  .withJobId("test-job")
                  .withSubtaskId(0)
                  .withAttemptNumber(1)
                  .withDeltaTable(table)
                  .withConf(new DeltaSinkConf(schema, Collections.emptyMap()))
                  .withMetricGroup(UnregisteredMetricsGroup.createSinkWriterMetricGroup())
                  .build();

          for (int i = 0; i < 20; i++) {
            sinkWriter.write(
                GenericRowData.of(i, StringData.fromString("p" + (i % 3))),
                new TestSinkWriterContext(i * 100, i * 100));
          }
          Collection<DeltaWriterResult> results = sinkWriter.prepareCommit();
          // One partition
          assertEquals(1, results.size());
          // Each partition has one action
          results.forEach(
              result -> {
                assertEquals(1, result.getDeltaActions().size());
                assertEquals(1900, result.getContext().getHighWatermark());
                assertEquals(0, result.getContext().getLowWatermark());
              });
        });
  }

  @Test
  void testWriteToEmptyTableUsingMultiplePartitions() {
    withTempDir(
        dir -> {
          String tablePath = dir.getAbsolutePath();
          StructType schema =
              new StructType().add("id", IntegerType.INTEGER).add("part", StringType.STRING);

          HadoopTable table =
              new HadoopTable(
                  URI.create(tablePath), Collections.emptyMap(), schema, List.of("part"));
          table.open();

          DeltaSinkWriter sinkWriter =
              new DeltaSinkWriter.Builder()
                  .withJobId("test-job")
                  .withSubtaskId(0)
                  .withAttemptNumber(1)
                  .withDeltaTable(table)
                  .withConf(new DeltaSinkConf(schema, Collections.emptyMap()))
                  .withMetricGroup(UnregisteredMetricsGroup.createSinkWriterMetricGroup())
                  .build();

          int rowCount = 20;
          int numPartitions = 3;
          for (int i = 0; i < rowCount; i++) {
            sinkWriter.write(
                GenericRowData.of(i, StringData.fromString("p" + (i % numPartitions))),
                new TestSinkWriterContext(i * 100, i * 100));
          }
          Collection<DeltaWriterResult> results = sinkWriter.prepareCommit();
          Map<Long, Long> expectedWatermarks =
              IntStream.range(0, numPartitions)
                  .boxed()
                  .collect(
                      Collectors.toMap(
                          i -> i * 100L,
                          i -> {
                            int overflow = (i < rowCount % numPartitions) ? 0 : 1;
                            int hwm = (rowCount / numPartitions - overflow) * numPartitions + i;
                            return hwm * 100L;
                          }));

          assertEquals(numPartitions, results.size());
          // Each partition has one action
          int idx = 0;
          for (DeltaWriterResult result : results) {
            assertEquals(1, result.getDeltaActions().size());
            assertEquals(
                expectedWatermarks.get(result.getContext().getLowWatermark()),
                result.getContext().getHighWatermark());
            idx++;
          }
        });
  }

  @Test
  void testWriteToExistingTableUsingMultiplePartitions() {
    withTempDir(
        dir -> {
          String tablePath = dir.getAbsolutePath();
          DefaultEngine engine = DefaultEngine.create(new Configuration());
          StructType schema =
              new StructType().add("id", IntegerType.INTEGER).add("part", StringType.STRING);

          // Create a non-empty table
          createNonEmptyTable(engine, tablePath, schema, List.of("part"), Collections.emptyMap());
          HadoopTable table =
              new HadoopTable(
                  URI.create(tablePath), Collections.emptyMap(), schema, List.of("part"));
          table.open();

          DeltaSinkWriter sinkWriter =
              new DeltaSinkWriter.Builder()
                  .withDeltaTable(table)
                  .withJobId("test-job")
                  .withSubtaskId(0)
                  .withAttemptNumber(1)
                  .withConf(new DeltaSinkConf(schema, Collections.emptyMap()))
                  .withMetricGroup(UnregisteredMetricsGroup.createSinkWriterMetricGroup())
                  .build();

          int rowCount = 20;
          int numPartitions = 3;
          for (int i = 0; i < rowCount; i++) {
            sinkWriter.write(
                GenericRowData.of(i, StringData.fromString("p" + (i % numPartitions))),
                new TestSinkWriterContext(i * 100, i * 100));
          }
          Collection<DeltaWriterResult> results = sinkWriter.prepareCommit();
          Map<Long, Long> expectedWatermarks =
              IntStream.range(0, numPartitions)
                  .boxed()
                  .collect(
                      Collectors.toMap(
                          i -> i * 100L,
                          i -> {
                            int overflow = (i < rowCount % numPartitions) ? 0 : 1;
                            int hwm = (rowCount / numPartitions - overflow) * numPartitions + i;
                            return hwm * 100L;
                          }));
          assertEquals(numPartitions, results.size());
          // Each partition has one action
          int idx = 0;
          for (DeltaWriterResult result : results) {
            assertEquals(1, result.getDeltaActions().size());
            assertEquals(
                expectedWatermarks.get(result.getContext().getLowWatermark()),
                result.getContext().getHighWatermark());
            idx++;
          }
        });
  }

  @Test
  @Disabled("memory is stable on large amount of partitions")
  void testMemoryIsStableOnLargeAmountOfPartitions() {
    withTempDir(
        dir -> {
          String tablePath = dir.getAbsolutePath();
          StructType schema =
              new StructType().add("id", IntegerType.INTEGER).add("part", IntegerType.INTEGER);

          HadoopTable table =
              new HadoopTable(
                  URI.create(tablePath), Collections.emptyMap(), schema, List.of("part"));
          table.open();

          DeltaSinkWriter sinkWriter =
              new DeltaSinkWriter.Builder()
                  .withJobId("test-job")
                  .withSubtaskId(0)
                  .withAttemptNumber(1)
                  .withDeltaTable(table)
                  .withConf(new DeltaSinkConf(schema, Collections.emptyMap()))
                  .withMetricGroup(UnregisteredMetricsGroup.createSinkWriterMetricGroup())
                  .build();

          Logger logger = LoggerFactory.getLogger(DeltaSinkWriter.class);
          var threadPool = Executors.newFixedThreadPool(2);
          threadPool.submit(
              () -> {
                var mxBean = ManagementFactory.getMemoryMXBean();
                int counter = 0;
                while (!Thread.currentThread().isInterrupted()) {
                  try {
                    Thread.sleep(5000);
                    counter += 1;
                    if (counter % 100 == 0) {
                      System.gc();
                    }
                    var usage = mxBean.getHeapMemoryUsage();
                    logger.info("{}, {}", usage.getUsed(), usage.getMax());
                  } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                  }
                }
              });

          int rowCount = 2000000;
          long startTime = System.currentTimeMillis();
          long lastBegin = -1L;
          for (int i = 0; i < rowCount; i++) {
            long begin = System.currentTimeMillis() - startTime;
            if (lastBegin / 300000 != begin / 300000) {
              logger.info("Clearing up buffer");
              sinkWriter.prepareCommit();
            }
            lastBegin = begin;

            sinkWriter.write(
                GenericRowData.of(i, random.nextInt(1000000)),
                new TestSinkWriterContext(i * 100, i * 100));
          }
        });
  }
}
