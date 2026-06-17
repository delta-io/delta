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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.delta.flink.TestHelper;
import io.delta.flink.sink.mergestrategy.ScanLocator;
import io.delta.flink.sink.mergestrategy.Upsert;
import io.delta.flink.table.DeltaTable;
import io.delta.flink.table.HadoopTable;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.internal.actions.AddFile;
import io.delta.kernel.internal.actions.SingleAction;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import java.io.File;
import java.net.URI;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiPredicate;
import java.util.stream.Collectors;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.junit.jupiter.api.Test;

/** JUnit test suite for {@link DeltaUpsertWriterTask}. */
class DeltaUpsertWriterTaskTest extends TestHelper {

  private static final StructType SCHEMA =
      new StructType().add("id", IntegerType.INTEGER).add("name", StringType.STRING);

  // ===========================================================================
  // In-memory behaviors: write / erase / replace, before any dump.
  // ===========================================================================

  @Test
  void testAppendDistinctPksProducesSingleFileInOrder() {
    withTempDir(
        dir -> {
          HadoopTable table = openTable(dir);
          DeltaUpsertWriterTask task = newTask(table, defaultConf());

          for (int i = 0; i < 5; i++) {
            boolean replaced = task.write(pk(i), row(i, "v" + i), new TestSinkWriterContext(0, 0));
            assertFalse(replaced, "first write of a PK never reports replacement");
          }

          List<DeltaWriterResult> results = task.complete();
          assertEquals(1, results.size());
          AddFile addFile = addFileOf(results.get(0));
          assertFalse(addFile.getDeletionVector().isPresent(), "no DV on a fresh file");

          List<Row> rows = readParquet(absolutePath(dir, addFile), SCHEMA);
          assertEquals(5, rows.size());
          for (int i = 0; i < 5; i++) {
            assertEquals(i, rows.get(i).getInt(0));
            assertEquals("v" + i, rows.get(i).getString(1));
          }
        });
  }

  @Test
  void testInBatchReplaceSamePkKeepsLatestImage() {
    withTempDir(
        dir -> {
          HadoopTable table = openTable(dir);
          DeltaUpsertWriterTask task = newTask(table, defaultConf());

          assertFalse(task.write(pk(5), row(5, "A"), new TestSinkWriterContext(0, 0)));
          // Second write for the same PK must report a replacement and overwrite in place.
          assertTrue(task.write(pk(5), row(5, "B"), new TestSinkWriterContext(0, 0)));

          List<DeltaWriterResult> results = task.complete();
          assertEquals(1, results.size());

          List<Row> rows = readParquet(absolutePath(dir, addFileOf(results.get(0))), SCHEMA);
          assertEquals(1, rows.size());
          assertEquals(5, rows.get(0).getInt(0));
          assertEquals("B", rows.get(0).getString(1));
        });
  }

  @Test
  void testCompositeKeysWithDelimiterInValueAreDistinct() {
    withTempDir(
        dir -> {
          HadoopTable table = openTable(dir);
          DeltaUpsertWriterTask task = newTask(table, defaultConf());

          // Two distinct composite PKs that a naive join(";") would collapse to "a;b;c".
          // The canonical key escapes the delimiter, so they must remain separate rows.
          List<Literal> pk1 = List.of(Literal.ofString("a;b"), Literal.ofString("c"));
          List<Literal> pk2 = List.of(Literal.ofString("a"), Literal.ofString("b;c"));

          assertFalse(task.write(pk1, row(1, "first"), new TestSinkWriterContext(0, 0)));
          assertFalse(task.write(pk2, row(2, "second"), new TestSinkWriterContext(0, 0)));

          List<DeltaWriterResult> results = task.complete();
          assertEquals(1, results.size());
          List<Row> rows = readParquet(absolutePath(dir, addFileOf(results.get(0))), SCHEMA);
          assertEquals(2, rows.size());
        });
  }

  @Test
  void testEraseInMemoryRowYieldsNoFile() {
    withTempDir(
        dir -> {
          HadoopTable table = openTable(dir);
          DeltaUpsertWriterTask task = newTask(table, defaultConf());

          task.write(pk(7), row(7, "x"), new TestSinkWriterContext(0, 0));
          assertTrue(task.erase(pk(7)), "erase of an in-batch row reports success");

          // Compaction wipes the only buffered row, so the override short-circuits and no
          // AddFile is produced.
          assertTrue(task.complete().isEmpty(), "no file when every buffered row was erased");
        });
  }

  @Test
  void testEraseMissingPkIsNoop() {
    withTempDir(
        dir -> {
          HadoopTable table = openTable(dir);
          DeltaUpsertWriterTask task = newTask(table, defaultConf());

          assertFalse(task.erase(pk(42)), "erase of an unknown PK returns false");
          assertTrue(task.complete().isEmpty());
        });
  }

  @Test
  void testEraseThenReWriteSamePk() {
    withTempDir(
        dir -> {
          HadoopTable table = openTable(dir);
          DeltaUpsertWriterTask task = newTask(table, defaultConf());

          task.write(pk(3), row(3, "before"), new TestSinkWriterContext(0, 0));
          task.erase(pk(3));
          // The slot left behind by erase is reused for the re-written row.
          task.write(pk(3), row(3, "after"), new TestSinkWriterContext(0, 0));

          List<DeltaWriterResult> results = task.complete();
          assertEquals(1, results.size());
          List<Row> rows = readParquet(absolutePath(dir, addFileOf(results.get(0))), SCHEMA);
          assertEquals(1, rows.size());
          assertEquals("after", rows.get(0).getString(1));
        });
  }

  @Test
  void testCompactionRemovesGapsInMiddle() {
    withTempDir(
        dir -> {
          HadoopTable table = openTable(dir);
          DeltaUpsertWriterTask task = newTask(table, defaultConf());

          task.write(pk(1), row(1, "A"), new TestSinkWriterContext(0, 0));
          task.write(pk(2), row(2, "B"), new TestSinkWriterContext(0, 0));
          task.write(pk(3), row(3, "C"), new TestSinkWriterContext(0, 0));
          // Punch a hole in the middle of the buffer and verify compaction closes it cleanly
          // (no nulls bleed through into Parquet, and the surviving order is A, C).
          task.erase(pk(2));

          List<DeltaWriterResult> results = task.complete();
          assertEquals(1, results.size());
          List<Row> rows = readParquet(absolutePath(dir, addFileOf(results.get(0))), SCHEMA);
          assertEquals(List.of(1, 3), List.of(rows.get(0).getInt(0), rows.get(1).getInt(0)));
        });
  }

  @Test
  void testCompleteOnEmptyTaskYieldsNothing() {
    withTempDir(
        dir -> {
          HadoopTable table = openTable(dir);
          DeltaUpsertWriterTask task = newTask(table, defaultConf());

          // No writes at all -- complete must not produce phantom AddFiles, and must not trip
          // the internal results-vs-positionsToRemove invariant.
          assertTrue(task.complete().isEmpty());
        });
  }

  // ===========================================================================
  // Cross-dump behaviors: position tracking and algorithm delegation when a PK
  // touches an already-rolled file. Tests use SpyUpsert — algorithm-agnostic.
  // ===========================================================================

  @Test
  void testAllRowsErasedAfterDumpDropsFileFromOutput() {
    withTempDir(
        dir -> {
          HadoopTable table = openTable(dir);
          SpyUpsert spy = new SpyUpsert();
          // rollAfter(2) flushes pk(0)+"A" and pk(1)+"B" into file 0 as soon as the second write
          // arrives. Both rows are then erased, so every row in the only staged file is deleted
          // and complete() must return an empty list (the fully-deleted file is dropped before
          // the algorithm is ever called).
          DeltaUpsertWriterTask task = newTask(table, rollAfter(2), spy);

          task.write(pk(0), row(0, "A"), new TestSinkWriterContext(0, 0));
          task.write(pk(1), row(1, "B"), new TestSinkWriterContext(0, 0));
          assertTrue(task.erase(pk(0)), "erase of a dumped row must return true");
          assertTrue(task.erase(pk(1)), "erase of a dumped row must return true");

          assertTrue(
              task.complete().isEmpty(),
              "fully-deleted staged file must be dropped from complete() output");
          assertTrue(
              spy.capturedPositions.isEmpty(),
              "fully-deleted file is dropped before reaching the algorithm");
        });
  }

  @Test
  void testRollingProducesMultipleFilesWithNoDelegation() {
    withTempDir(
        dir -> {
          HadoopTable table = openTable(dir);
          SpyUpsert spy = new SpyUpsert();
          DeltaUpsertWriterTask task = newTask(table, rollAfter(2), spy);

          for (int i = 0; i < 4; i++) {
            task.write(pk(i), row(i, "v" + i), new TestSinkWriterContext(0, 0));
          }

          List<DeltaWriterResult> results = task.complete();
          assertEquals(2, results.size(), "two rolls -> two AddFiles");
          assertTrue(
              spy.capturedPositions.isEmpty(), "no PK touched twice — algorithm never called");
          // Files are unchanged by the algorithm: first [0, 1], second [2, 3].
          assertEquals(
              List.of(0, 1),
              idsOf(readParquet(absolutePath(dir, addFileOf(results.get(0))), SCHEMA)));
          assertEquals(
              List.of(2, 3),
              idsOf(readParquet(absolutePath(dir, addFileOf(results.get(1))), SCHEMA)));
        });
  }

  @Test
  void testInBatchUpdateAcrossRollDelegatesPositionToAlgorithm() {
    withTempDir(
        dir -> {
          HadoopTable table = openTable(dir);
          SpyUpsert spy = new SpyUpsert();
          DeltaUpsertWriterTask task = newTask(table, rollAfter(2), spy);

          // PK 0 lands in file 0; PK 1 triggers the roll. Then PK 0 is updated — markAsRemoved
          // must locate the old row (position 0) in the just-dumped file and queue it.
          task.write(pk(0), row(0, "old"), new TestSinkWriterContext(0, 0));
          task.write(pk(1), row(1, "B"), new TestSinkWriterContext(0, 0));
          assertTrue(
              task.write(pk(0), row(0, "new"), new TestSinkWriterContext(0, 0)),
              "second write of PK 0 (now on disk in file 0) must report replacement");

          List<DeltaWriterResult> results = task.complete();
          assertEquals(2, results.size());

          // File 0: algorithm was called with position 0 (the stale pk(0) row).
          assertEquals(1, spy.capturedPositions.size());
          assertEquals(Set.of(0), spy.capturedPositions.get(0));

          // File 1: the updated image of PK 0 — no positions queued, returned unchanged.
          List<Row> file1Rows = readParquet(absolutePath(dir, addFileOf(results.get(1))), SCHEMA);
          assertEquals(1, file1Rows.size());
          assertEquals(0, file1Rows.get(0).getInt(0));
          assertEquals("new", file1Rows.get(0).getString(1));
        });
  }

  @Test
  void testEraseAcrossRollDelegatesPositionToAlgorithm() {
    withTempDir(
        dir -> {
          HadoopTable table = openTable(dir);
          SpyUpsert spy = new SpyUpsert();
          DeltaUpsertWriterTask task = newTask(table, rollAfter(2), spy);

          task.write(pk(0), row(0, "A"), new TestSinkWriterContext(0, 0));
          task.write(pk(1), row(1, "B"), new TestSinkWriterContext(0, 0));
          assertTrue(
              task.erase(pk(0)), "erase of a PK that has already been dumped must report success");

          // No writes follow the erase. complete() yields exactly one result for the modified file.
          List<DeltaWriterResult> results = task.complete();
          assertEquals(1, results.size());

          // Algorithm was called with position 0 (the erased pk(0) row in file 0).
          assertEquals(1, spy.capturedPositions.size());
          assertEquals(Set.of(0), spy.capturedPositions.get(0));
        });
  }

  @Test
  void testEraseUnknownPkAfterRollReturnsFalse() {
    withTempDir(
        dir -> {
          HadoopTable table = openTable(dir);
          SpyUpsert spy = new SpyUpsert();
          DeltaUpsertWriterTask task = newTask(table, rollAfter(2), spy);

          // Roll file 0 = [0, 1], then ask markAsRemoved about a PK that lives in *no* file.
          // The file-scan loop must walk every entry, find nothing, and return -1.
          task.write(pk(0), row(0, "A"), new TestSinkWriterContext(0, 0));
          task.write(pk(1), row(1, "B"), new TestSinkWriterContext(0, 0));
          assertFalse(task.erase(pk(42)), "erase of an unknown PK returns false even after rolls");

          List<DeltaWriterResult> results = task.complete();
          assertEquals(1, results.size());
          assertTrue(
              spy.capturedPositions.isEmpty(),
              "no positions queued — algorithm must not be called");
          // File is unchanged (algorithm not called): content is still [0, 1].
          assertEquals(
              List.of(0, 1),
              idsOf(readParquet(absolutePath(dir, addFileOf(results.get(0))), SCHEMA)));
        });
  }

  @Test
  void testRepeatedSamePkAcrossMultipleRollsCancelsFullyDeletedFiles() {
    withTempDir(
        dir -> {
          HadoopTable table = openTable(dir);
          SpyUpsert spy = new SpyUpsert();
          // rollAfter(1): every write produces its own file, so each re-write of the same PK
          // forces markAsRemoved to walk past the previously-removed entries and find the
          // *current* live copy in the latest file.
          DeltaUpsertWriterTask task = newTask(table, rollAfter(1), spy);

          task.write(pk(7), row(7, "v1"), new TestSinkWriterContext(0, 0));
          task.write(pk(7), row(7, "v2"), new TestSinkWriterContext(0, 0));
          task.write(pk(7), row(7, "v3"), new TestSinkWriterContext(0, 0));

          List<DeltaWriterResult> results = task.complete();
          assertEquals(1, results.size(), "two files are cancelled out");
          assertTrue(
              spy.capturedPositions.isEmpty(),
              "fully-deleted files are dropped before the algorithm is called");

          // Only file 2's row survives; its on-disk content is v3 (unchanged by algorithm).
          List<Row> file2Rows = readParquet(absolutePath(dir, addFileOf(results.get(0))), SCHEMA);
          assertEquals(1, file2Rows.size());
          assertEquals("v3", file2Rows.get(0).getString(1));
        });
  }

  @Test
  void testMultiplePositionsInOneDumpedFileDelegatedToAlgorithm() {
    withTempDir(
        dir -> {
          HadoopTable table = openTable(dir);
          SpyUpsert spy = new SpyUpsert();
          DeltaUpsertWriterTask task = newTask(table, rollAfter(4), spy);

          // File 0: rows 0..3 (PKs 0..3).
          for (int i = 0; i < 4; i++) {
            task.write(pk(i), row(i, "v" + i), new TestSinkWriterContext(0, 0));
          }
          // After the roll, touch two distinct rows in file 0: PK 0 via re-write, PK 2 via erase.
          task.write(pk(0), row(0, "v0-updated"), new TestSinkWriterContext(0, 0));
          task.erase(pk(2));

          List<DeltaWriterResult> results = task.complete();
          assertEquals(2, results.size());

          // File 0: algorithm was called with positions 0 (pk0) and 2 (pk2).
          assertEquals(1, spy.capturedPositions.size());
          assertEquals(Set.of(0, 2), spy.capturedPositions.get(0));

          // File 1: the updated image of PK 0 — no positions queued, returned unchanged.
          List<Row> file1Rows = readParquet(absolutePath(dir, addFileOf(results.get(1))), SCHEMA);
          assertEquals(1, file1Rows.size());
          assertEquals(0, file1Rows.get(0).getInt(0));
          assertEquals("v0-updated", file1Rows.get(0).getString(1));
        });
  }

  // ===========================================================================
  // Watermark propagation across both write paths (append + in-memory replace).
  // ===========================================================================

  @Test
  void testWatermarksTrackBothAppendAndInMemoryReplace() {
    withTempDir(
        dir -> {
          HadoopTable table = openTable(dir);
          DeltaUpsertWriterTask task = newTask(table, defaultConf());

          // First write goes through super.write (the append path). Second write replaces in
          // memory and must extend the watermark window from the override itself.
          task.write(pk(5), row(5, "early"), new TestSinkWriterContext(100, 100));
          task.write(pk(5), row(5, "late"), new TestSinkWriterContext(500, 500));

          List<DeltaWriterResult> results = task.complete();
          assertEquals(1, results.size());
          WriterResultContext ctx = results.get(0).getContext();
          assertEquals(100, ctx.getLowWatermark());
          assertEquals(500, ctx.getHighWatermark());
        });
  }

  // ===========================================================================
  // Helpers
  // ===========================================================================

  private HadoopTable openTable(File dir) {
    HadoopTable table =
        new HadoopTable(
            URI.create(dir.getAbsolutePath()),
            Collections.emptyMap(),
            SCHEMA,
            Collections.emptyList());
    table.open();
    return table;
  }

  private DeltaSinkConf defaultConf() {
    return new DeltaSinkConf(SCHEMA, Collections.emptyMap());
  }

  /** A {@link DeltaSinkConf} that forces a file roll after every {@code n} writes. */
  private DeltaSinkConf rollAfter(int n) {
    return new DeltaSinkConf(
        SCHEMA, Map.of("file_rolling.strategy", "count", "file_rolling.count", String.valueOf(n)));
  }

  private DeltaUpsertWriterTask newTask(HadoopTable table, DeltaSinkConf conf) {
    return newTask(table, conf, new SpyUpsert());
  }

  private DeltaUpsertWriterTask newTask(HadoopTable table, DeltaSinkConf conf, Upsert algorithm) {
    return new DeltaUpsertWriterTask(
        /* jobId= */ "test-job",
        /* subtaskId= */ 0,
        /* attemptNumber= */ 0,
        table,
        conf,
        Collections.emptyMap(),
        algorithm);
  }

  private static List<Literal> pk(int id) {
    return List.of(Literal.ofInt(id));
  }

  private static GenericRowData row(int id, String name) {
    return GenericRowData.of(id, StringData.fromString(name));
  }

  private static AddFile addFileOf(DeltaWriterResult result) {
    Row action = result.getDeltaActions().get(0);
    return new AddFile(action.getStruct(SingleAction.ADD_FILE_ORDINAL));
  }

  private static Path absolutePath(File dir, AddFile addFile) {
    return dir.toPath().resolve(addFile.getPath()).toAbsolutePath();
  }

  private static List<Integer> idsOf(List<Row> rows) {
    return rows.stream().map(r -> r.getInt(0)).collect(Collectors.toList());
  }

  /**
   * Algorithm-agnostic test double. Captures the positions passed to {@link #markRemovesOnStaged}
   * and returns the staged action unchanged — no file rewrite, no DV. This lets task-level tests
   * assert on *what* positions were queued without depending on any specific algorithm's output
   * format.
   */
  private static final class SpyUpsert extends Upsert {

    final List<Set<Integer>> capturedPositions = new ArrayList<>();

    SpyUpsert() {
      super(new ScanLocator());
    }

    @Override
    public Row markRemovesOnStaged(
        DeltaTable table, Row stagedAddFile, Set<Integer> removePositions) {
      capturedPositions.add(new HashSet<>(removePositions));
      return stagedAddFile;
    }

    @Override
    protected CloseableIterator<Row> deleteRecords(
        Row addFile, BiPredicate<ColumnarBatch, Integer> filter) {
      throw new UnsupportedOperationException("deleteRecords not called in task unit tests");
    }
  }
}
