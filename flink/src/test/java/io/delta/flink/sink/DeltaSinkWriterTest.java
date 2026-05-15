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

package io.delta.flink.sink;

import static io.delta.kernel.internal.util.Utils.singletonCloseableIterator;
import static io.delta.kernel.internal.util.Utils.toCloseableIterator;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.delta.flink.TestHelper;
import io.delta.flink.table.HadoopTable;
import io.delta.kernel.Scan;
import io.delta.kernel.Snapshot;
import io.delta.kernel.TableManager;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.InternalScanFileUtils;
import io.delta.kernel.internal.data.ScanStateRow;
import io.delta.kernel.types.*;
import io.delta.kernel.utils.CloseableIterable;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;
import java.lang.management.ManagementFactory;
import java.net.URI;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.types.RowKind;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** JUnit test suite for {@link DeltaSinkWriter}. */
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

  // ---------------------------------------------------------------------------
  // Upsert-mode tests: verify the RowKind routing in DeltaSinkWriter.write and
  // the wiring between the writer and its MergeStrategy at prepareCommit time.
  //
  // These tests assert on the *logical* row content of the table after committing
  // the actions produced by prepareCommit. We deliberately do NOT inspect the
  // physical action shape (AddFile / RemoveFile / deletion vectors) so the same
  // expectations remain valid when the merge strategy switches from Copy-on-Write
  // to Merge-on-Read.
  // ---------------------------------------------------------------------------

  @Test
  void testUpsertInsertOnEmptyTable() throws Exception {
    withTempDir(
        dir -> {
          StructType schema =
              new StructType().add("id", IntegerType.INTEGER).add("part", StringType.STRING);
          HadoopTable table =
              new HadoopTable(
                  URI.create(dir.getAbsolutePath()),
                  Collections.emptyMap(),
                  schema,
                  Collections.emptyList());
          table.open();

          DeltaSinkWriter sinkWriter = newSinkWriter(table, upsertConf(schema, new int[] {0}));

          GenericRowData row = GenericRowData.of(5, StringData.fromString("p0"));
          row.setRowKind(RowKind.INSERT);
          sinkWriter.write(row, new TestSinkWriterContext(0, 0));

          commitResults(table, sinkWriter.prepareCommit());

          List<List<Object>> rows = readAllRows(dir.getAbsolutePath(), schema);
          assertEquals(List.of(List.of(5, "p0")), rows);
        });
  }

  @Test
  void testUpsertUpdateAfterReplacesPreImage() throws Exception {
    withTempDir(
        dir -> {
          StructType schema =
              new StructType().add("id", IntegerType.INTEGER).add("part", StringType.STRING);
          HadoopTable table =
              new HadoopTable(
                  URI.create(dir.getAbsolutePath()),
                  Collections.emptyMap(),
                  schema,
                  Collections.emptyList());
          table.open();

          prePopulate(
              table,
              schema,
              List.of(
                  GenericRowData.of(1, StringData.fromString("old")),
                  GenericRowData.of(5, StringData.fromString("old")),
                  GenericRowData.of(9, StringData.fromString("old"))));

          DeltaSinkWriter sinkWriter = newSinkWriter(table, upsertConf(schema, new int[] {0}));

          GenericRowData row = GenericRowData.of(5, StringData.fromString("new"));
          row.setRowKind(RowKind.UPDATE_AFTER);
          sinkWriter.write(row, new TestSinkWriterContext(0, 0));

          commitResults(table, sinkWriter.prepareCommit());

          // id=5 now carries the new image; id=1 and id=9 are untouched.
          List<List<Object>> rows = sortById(readAllRows(dir.getAbsolutePath(), schema));
          assertEquals(List.of(List.of(1, "old"), List.of(5, "new"), List.of(9, "old")), rows);
        });
  }

  @Test
  void testUpsertDeleteRemovesPreImage() throws Exception {
    withTempDir(
        dir -> {
          StructType schema =
              new StructType().add("id", IntegerType.INTEGER).add("part", StringType.STRING);
          HadoopTable table =
              new HadoopTable(
                  URI.create(dir.getAbsolutePath()),
                  Collections.emptyMap(),
                  schema,
                  Collections.emptyList());
          table.open();

          prePopulate(
              table,
              schema,
              List.of(
                  GenericRowData.of(1, StringData.fromString("a")),
                  GenericRowData.of(7, StringData.fromString("b")),
                  GenericRowData.of(9, StringData.fromString("c"))));

          DeltaSinkWriter sinkWriter = newSinkWriter(table, upsertConf(schema, new int[] {0}));

          GenericRowData row = GenericRowData.of(7, StringData.fromString("b"));
          row.setRowKind(RowKind.DELETE);
          sinkWriter.write(row, new TestSinkWriterContext(0, 0));

          commitResults(table, sinkWriter.prepareCommit());

          List<List<Object>> rows = sortById(readAllRows(dir.getAbsolutePath(), schema));
          assertEquals(List.of(List.of(1, "a"), List.of(9, "c")), rows);
        });
  }

  @Test
  void testUpsertUpdateBeforeIsDropped() throws Exception {
    withTempDir(
        dir -> {
          StructType schema =
              new StructType().add("id", IntegerType.INTEGER).add("part", StringType.STRING);
          HadoopTable table =
              new HadoopTable(
                  URI.create(dir.getAbsolutePath()),
                  Collections.emptyMap(),
                  schema,
                  Collections.emptyList());
          table.open();

          prePopulate(table, schema, List.of(GenericRowData.of(11, StringData.fromString("kept"))));

          DeltaSinkWriter sinkWriter = newSinkWriter(table, upsertConf(schema, new int[] {0}));

          GenericRowData row = GenericRowData.of(11, StringData.fromString("dropped"));
          row.setRowKind(RowKind.UPDATE_BEFORE);
          sinkWriter.write(row, new TestSinkWriterContext(0, 0));

          // UPDATE_BEFORE produces no work for the writer or the merge strategy. The
          // pre-existing row must remain in the table untouched.
          commitResults(table, sinkWriter.prepareCommit());

          List<List<Object>> rows = readAllRows(dir.getAbsolutePath(), schema);
          assertEquals(List.of(List.of(11, "kept")), rows);
        });
  }

  @Test
  void testAppendModeUpdateAfterThrows() {
    withTempDir(
        dir -> {
          StructType schema =
              new StructType().add("id", IntegerType.INTEGER).add("part", StringType.STRING);
          HadoopTable table =
              new HadoopTable(
                  URI.create(dir.getAbsolutePath()),
                  Collections.emptyMap(),
                  schema,
                  Collections.emptyList());
          table.open();

          DeltaSinkWriter sinkWriter =
              newSinkWriter(table, new DeltaSinkConf(schema, Collections.emptyMap()));

          GenericRowData row = GenericRowData.of(1, StringData.fromString("p0"));
          row.setRowKind(RowKind.UPDATE_AFTER);
          assertThrows(
              IllegalStateException.class,
              () -> sinkWriter.write(row, new TestSinkWriterContext(0, 0)));
        });
  }

  @Test
  void testAppendModeDeleteThrows() {
    withTempDir(
        dir -> {
          StructType schema =
              new StructType().add("id", IntegerType.INTEGER).add("part", StringType.STRING);
          HadoopTable table =
              new HadoopTable(
                  URI.create(dir.getAbsolutePath()),
                  Collections.emptyMap(),
                  schema,
                  Collections.emptyList());
          table.open();

          DeltaSinkWriter sinkWriter =
              newSinkWriter(table, new DeltaSinkConf(schema, Collections.emptyMap()));

          GenericRowData row = GenericRowData.of(1, StringData.fromString("p0"));
          row.setRowKind(RowKind.DELETE);
          assertThrows(
              IllegalStateException.class,
              () -> sinkWriter.write(row, new TestSinkWriterContext(0, 0)));
        });
  }

  // ---------------------------------------------------------------------------
  // Test-only helpers
  // ---------------------------------------------------------------------------

  /** Build an upsert-mode {@link DeltaSinkConf} for the given schema and PK ordinals. */
  private static DeltaSinkConf upsertConf(StructType schema, int[] pkOrdinals) {
    Map<String, String> opts = new HashMap<>();
    opts.put("write.mode", "upsert");
    opts.put(
        "primary_key",
        Arrays.stream(pkOrdinals).mapToObj(Integer::toString).collect(Collectors.joining(",")));
    return new DeltaSinkConf(schema, opts);
  }

  /** Build a {@link DeltaSinkWriter} with the standard test wiring. */
  private static DeltaSinkWriter newSinkWriter(HadoopTable table, DeltaSinkConf conf) {
    return new DeltaSinkWriter.Builder()
        .withJobId("test-job")
        .withSubtaskId(0)
        .withAttemptNumber(1)
        .withDeltaTable(table)
        .withConf(conf)
        .withMetricGroup(UnregisteredMetricsGroup.createSinkWriterMetricGroup())
        .build();
  }

  /**
   * Write the given rows into {@code table} via a {@link DeltaWriterTask} and commit them. After
   * this returns, {@code table}'s snapshot reflects the new AddFile actions.
   */
  private static void prePopulate(HadoopTable table, StructType schema, List<GenericRowData> rows)
      throws Exception {
    DeltaSinkConf conf = new DeltaSinkConf(schema, Collections.emptyMap());
    DeltaWriterTask task =
        new DeltaWriterTask(
            "setup-job",
            /* subtaskId= */ 0,
            /* attemptNumber= */ 0,
            table,
            conf,
            Collections.emptyMap());
    for (GenericRowData row : rows) {
      task.write(row, new TestSinkWriterContext(0, 0));
    }
    commitActions(table, task.complete());
  }

  /** Commit the actions inside {@code results} into {@code table}. */
  private static void commitResults(HadoopTable table, Collection<DeltaWriterResult> results) {
    if (results.isEmpty()) {
      return;
    }
    commitActions(table, results);
  }

  private static final AtomicLong TXN_COUNTER = new AtomicLong();

  private static void commitActions(HadoopTable table, Collection<DeltaWriterResult> results) {
    List<Row> actions = new ArrayList<>();
    for (DeltaWriterResult r : results) {
      actions.addAll(r.getDeltaActions());
    }
    if (actions.isEmpty()) {
      return;
    }
    // Use a unique txnId per commit. Reusing the same (appId, txnId) pair makes Delta treat
    // the second commit as an idempotent duplicate and silently skip it, which would mask the
    // logical effect of the merge strategy from the assertions below.
    table.commit(
        CloseableIterable.inMemoryIterable(toCloseableIterator(actions.iterator())),
        "test-app",
        TXN_COUNTER.incrementAndGet(),
        Collections.emptyMap());
  }

  /**
   * Read all logical rows of the table at {@code tablePath} through the Kernel scan API. Each row
   * is returned as a {@code List<Object>} whose entries are the column values in schema order.
   *
   * <p>This reader honors any deletion vectors / logical row filtering that the strategy may have
   * applied, so the result reflects the table's logical view regardless of whether the merge ran as
   * Copy-on-Write or Merge-on-Read.
   */
  private static List<List<Object>> readAllRows(String tablePath, StructType schema)
      throws Exception {
    Engine engine = DefaultEngine.create(new Configuration());
    Snapshot snapshot = TableManager.loadSnapshot(tablePath).build(engine);
    Scan scan = snapshot.getScanBuilder().withReadSchema(schema).build();
    Row scanState = scan.getScanState(engine);
    StructType physicalReadSchema = ScanStateRow.getPhysicalDataReadSchema(scanState);

    List<List<Object>> rows = new ArrayList<>();
    try (CloseableIterator<FilteredColumnarBatch> scanFileIter = scan.getScanFiles(engine)) {
      while (scanFileIter.hasNext()) {
        FilteredColumnarBatch scanFilesBatch = scanFileIter.next();
        try (CloseableIterator<Row> scanFileRows = scanFilesBatch.getRows()) {
          while (scanFileRows.hasNext()) {
            Row scanFileRow = scanFileRows.next();
            FileStatus fileStatus = InternalScanFileUtils.getAddFileStatus(scanFileRow);
            try (CloseableIterator<FilteredColumnarBatch> transformedData =
                Scan.transformPhysicalData(
                    engine,
                    scanState,
                    scanFileRow,
                    engine
                        .getParquetHandler()
                        .readParquetFiles(
                            singletonCloseableIterator(fileStatus),
                            physicalReadSchema,
                            Optional.empty())
                        .map(res -> res.getData()))) {
              while (transformedData.hasNext()) {
                FilteredColumnarBatch batch = transformedData.next();
                try (CloseableIterator<Row> rowIter = batch.getRows()) {
                  while (rowIter.hasNext()) {
                    rows.add(rowToList(rowIter.next(), schema));
                  }
                }
              }
            }
          }
        }
      }
    }
    return rows;
  }

  private static List<Object> rowToList(Row row, StructType schema) {
    List<Object> cells = new ArrayList<>(schema.length());
    for (int i = 0; i < schema.length(); i++) {
      cells.add(row.isNullAt(i) ? null : Conversions.DeltaToJava.data(schema, row, i));
    }
    return cells;
  }

  /** Sort {@code rows} ascending by the first column (assumed integer-typed id). */
  private static List<List<Object>> sortById(List<List<Object>> rows) {
    rows.sort(Comparator.comparingInt(r -> (Integer) r.get(0)));
    return rows;
  }
}
