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

package io.delta.flink.sink.mergestrategy;

import static io.delta.kernel.internal.util.Utils.toCloseableIterator;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.delta.flink.TestHelper;
import io.delta.flink.kernel.dv.BinDVAccess;
import io.delta.flink.sink.DeltaSinkConf;
import io.delta.flink.sink.DeltaWriterResult;
import io.delta.flink.sink.DeltaWriterTask;
import io.delta.flink.sink.TestSinkWriterContext;
import io.delta.flink.table.AbstractKernelTable;
import io.delta.flink.table.HadoopTable;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.expressions.AlwaysTrue;
import io.delta.kernel.internal.InternalScanFileUtils;
import io.delta.kernel.internal.actions.AddFile;
import io.delta.kernel.internal.actions.DeletionVectorDescriptor;
import io.delta.kernel.internal.actions.SingleAction;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterable;
import io.delta.kernel.utils.CloseableIterator;
import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.BiPredicate;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.junit.jupiter.api.Test;

/**
 * Per-file tests for {@link MoRUpsert#deleteRecords}. Mirrors the structure of {@link
 * CoWUpsertTest}: supply a scan-file row and a row-level filter directly, bypassing {@link
 * Upsert#merge} / {@link RowLocator}.
 */
class MoRUpsertTest extends TestHelper {

  private static final StructType SCHEMA =
      new StructType().add("id", IntegerType.INTEGER).add("name", StringType.STRING);

  /** Column ordinal of {@code id} in {@link #SCHEMA}. */
  private static final int ID_ORDINAL = 0;

  @Test
  void testAddsDvForFreshFile() {
    withTempDir(
        dir -> {
          HadoopTable table = openTable(dir);
          // 10 rows with ids 0..9, no pre-existing DV.
          Row scanFileRow = writeAndScan(table, 10);

          // Drop every other row -- expect 5 row positions in the DV.
          BiPredicate<ColumnarBatch, Integer> dropEvens = idMatches(i -> i % 2 == 0);
          List<Row> actions = runDeleteRecords(table, scanFileRow, dropEvens).toInMemoryList();

          assertEquals(2, actions.size(), "expected RemoveFile + AddFile");
          assertTrue(isRemoveAction(actions.get(0)));
          assertTrue(isAddAction(actions.get(1)));

          AddFile addFile = addFileOf(actions.get(1));
          AddFile sourceAddFile = sourceAddFile(scanFileRow);
          // MoR reuses the data file -- only the DV pointer changes.
          assertEquals(sourceAddFile.getPath(), addFile.getPath());
          assertEquals(sourceAddFile.getSize(), addFile.getSize());

          // The AddFile now carries a path-based on-disk DV with cardinality 5.
          DeletionVectorDescriptor dv =
              addFile.getDeletionVector().orElseThrow(() -> new AssertionError("expected a DV"));
          assertEquals(DeletionVectorDescriptor.PATH_DV_MARKER, dv.getStorageType());
          assertEquals(5L, dv.getCardinality());
          // pathOrInlineDv is the absolute URI of the .bin file.
          assertTrue(dv.getPathOrInlineDv().startsWith("file:"));
          assertTrue(dv.getPathOrInlineDv().endsWith(".bin"));
          assertTrue(dv.getOffset().isPresent());

          // DV file exists on disk and round-trips to the expected positions. We use the
          // table's URI-form path so getAbsolutePath produces a fully-qualified file:// URI.
          String dvPath = dv.getAbsolutePath(table.getTablePath().toString());
          assertTrue(new File(URI.create(dvPath)).exists(), "DV file missing at " + dvPath);
          assertArrayEquals(
              new long[] {0L, 2L, 4L, 6L, 8L},
              new BinDVAccess().read(table.getEngine(), dvPath).toArray());
        });
  }

  @Test
  void testNoMatchReturnsNoActions() {
    withTempDir(
        dir -> {
          HadoopTable table = openTable(dir);
          Row scanFileRow = writeAndScan(table, 5);

          // Locator over-reports: this candidate file actually has no matching rows.
          List<Row> actions =
              runDeleteRecords(table, scanFileRow, (batch, rowId) -> false).toInMemoryList();

          assertEquals(0, actions.size(), "no actions when nothing in the file matched");
        });
  }

  @Test
  void testDeletesEveryRow() {
    withTempDir(
        dir -> {
          HadoopTable table = openTable(dir);
          Row scanFileRow = writeAndScan(table, 6);

          List<Row> actions =
              runDeleteRecords(table, scanFileRow, (batch, rowId) -> true).toInMemoryList();

          assertEquals(2, actions.size());
          assertTrue(isRemoveAction(actions.get(0)));
          assertTrue(isAddAction(actions.get(1)));

          AddFile addFile = addFileOf(actions.get(1));
          DeletionVectorDescriptor dv =
              addFile.getDeletionVector().orElseThrow(() -> new AssertionError("expected a DV"));
          assertEquals(6L, dv.getCardinality());

          String dvPath = dv.getAbsolutePath(table.getTablePath().toString());
          assertArrayEquals(
              new long[] {0L, 1L, 2L, 3L, 4L, 5L},
              new BinDVAccess().read(table.getEngine(), dvPath).toArray());
        });
  }

  @Test
  void testTwoSequentialRunsMergeDvs() {
    withTempDir(
        dir -> {
          HadoopTable table = openTable(dir);
          Row scanFileRow = writeAndScan(table, 10);

          // First run: delete rows 0, 4 (id % 4 == 0).
          List<Row> firstActions =
              runDeleteRecords(table, scanFileRow, idMatches(i -> i % 4 == 0)).toInMemoryList();
          AddFile firstAdd = addFileOf(firstActions.get(1));
          DeletionVectorDescriptor firstDv = firstAdd.getDeletionVector().get();

          // Splice the new DV onto the scan-file row so the second run sees it as the "existing".
          Row scanRowWithDv = scanFileRowWithDv(scanFileRow, firstDv);

          // Second run: delete rows where id % 2 == 0 (i.e. 0, 2, 4, 6, 8). The union with the
          // existing DV {0, 4} is the same set, so the new bitmap is {0, 2, 4, 6, 8}, cardinality
          // 5.
          List<Row> secondActions =
              runDeleteRecords(table, scanRowWithDv, idMatches(i -> i % 2 == 0)).toInMemoryList();
          assertEquals(2, secondActions.size());

          AddFile secondAdd = addFileOf(secondActions.get(1));
          DeletionVectorDescriptor secondDv = secondAdd.getDeletionVector().get();
          assertEquals(5L, secondDv.getCardinality());
          // Distinct DV file from the first run -- DVs are immutable.
          assertFalse(
              firstDv.getPathOrInlineDv().equals(secondDv.getPathOrInlineDv()),
              "expected a fresh DV file on the second run");

          String dvPath = secondDv.getAbsolutePath(table.getTablePath().toString());
          assertArrayEquals(
              new long[] {0L, 2L, 4L, 6L, 8L},
              new BinDVAccess().read(table.getEngine(), dvPath).toArray());
        });
  }

  @Test
  void testNoNewMatchesOverExistingDvReturnsNoActions() {
    withTempDir(
        dir -> {
          HadoopTable table = openTable(dir);
          Row scanFileRow = writeAndScan(table, 10);

          // Establish an existing DV by running MoR once.
          List<Row> firstActions =
              runDeleteRecords(table, scanFileRow, idMatches(i -> i % 2 == 0)).toInMemoryList();
          DeletionVectorDescriptor firstDv =
              addFileOf(firstActions.get(1)).getDeletionVector().get();
          Row scanRowWithDv = scanFileRowWithDv(scanFileRow, firstDv);

          // Second run drops exactly the same rows -- bitmap is unchanged, so MoR should emit
          // nothing rather than churn a redundant DV file.
          List<Row> secondActions =
              runDeleteRecords(table, scanRowWithDv, idMatches(i -> i % 2 == 0)).toInMemoryList();
          assertTrue(secondActions.isEmpty(), "no actions when DV is unchanged");
        });
  }

  // -------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------

  /**
   * Test-only subclass of {@link MoRUpsert} that exposes the protected {@code deleteRecords} and
   * the inherited {@code table} field, the same way {@code ExposedCoWUpsert} does for the CoW
   * tests.
   */
  private static final class ExposedMoRUpsert extends MoRUpsert {
    ExposedMoRUpsert(AbstractKernelTable backingTable) {
      this.table = backingTable;
    }

    CloseableIterator<Row> deleteRecordsForTest(
        Row addFile, BiPredicate<ColumnarBatch, Integer> filter) {
      return deleteRecords(addFile, filter);
    }
  }

  private CloseableIterator<Row> runDeleteRecords(
      HadoopTable table, Row scanFileRow, BiPredicate<ColumnarBatch, Integer> filter) {
    return new ExposedMoRUpsert(table).deleteRecordsForTest(scanFileRow, filter);
  }

  /** Build a "matches if {@code id} satisfies {@code idTest}" filter on column 0. */
  private static BiPredicate<ColumnarBatch, Integer> idMatches(
      java.util.function.IntPredicate idTest) {
    return (batch, rowId) -> idTest.test(batch.getColumnVector(ID_ORDINAL).getInt(rowId));
  }

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

  /**
   * Write {@code numRows} rows {@code (i, "row-i")} via a {@link DeltaWriterTask}, commit the
   * resulting AddFile, and return the (single) scan-file row.
   */
  private Row writeAndScan(HadoopTable table, int numRows) {
    DeltaSinkConf conf = new DeltaSinkConf(SCHEMA, Collections.emptyMap());
    DeltaWriterTask task =
        new DeltaWriterTask(
            "test-job",
            /* subtaskId= */ 0,
            /* attemptNumber= */ 0,
            table,
            conf,
            Collections.emptyMap());
    try {
      for (int i = 0; i < numRows; i++) {
        task.write(
            GenericRowData.of(Integer.valueOf(i), StringData.fromString("row-" + i)),
            new TestSinkWriterContext(0, 0));
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    List<DeltaWriterResult> results;
    try {
      results = task.complete();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    List<Row> actions = new ArrayList<>();
    for (DeltaWriterResult r : results) {
      actions.addAll(r.getDeltaActions());
    }
    table.commit(
        CloseableIterable.inMemoryIterable(toCloseableIterator(actions.iterator())),
        "test-app",
        /* txnId= */ 0L,
        Collections.emptyMap());
    List<Row> all = table.scan(AlwaysTrue.ALWAYS_TRUE).toInMemoryList();
    assertEquals(1, all.size(), "expected exactly one scan file after commit");
    return all.get(0);
  }

  /**
   * Wrap a fresh scan-file row whose {@code add} struct carries the given {@link
   * DeletionVectorDescriptor}, so we can feed a "file with an existing DV" into MoR without going
   * through a second commit cycle.
   */
  private static Row scanFileRowWithDv(Row originalScanRow, DeletionVectorDescriptor dv) {
    AddFile source = sourceAddFile(originalScanRow);
    Row newAddRow =
        AddFile.createAddFileRow(
            SCHEMA,
            source.getPath(),
            source.getPartitionValues(),
            source.getSize(),
            source.getModificationTime(),
            source.getDataChange(),
            java.util.Optional.of(dv),
            source.getTags(),
            source.getBaseRowId(),
            source.getDefaultRowCommitVersion(),
            /* stats= */ java.util.Optional.empty());

    // Re-pack into a scan-file row. SCAN_FILE_SCHEMA has just "add" + "tableRoot" (StringType);
    // copy tableRoot verbatim, replace add.
    java.util.Map<Integer, Object> fields = new java.util.HashMap<>();
    fields.put(InternalScanFileUtils.ADD_FILE_ORDINAL, newAddRow);
    int tableRootOrdinal = InternalScanFileUtils.SCAN_FILE_SCHEMA.indexOf("tableRoot");
    if (tableRootOrdinal >= 0 && !originalScanRow.isNullAt(tableRootOrdinal)) {
      fields.put(tableRootOrdinal, originalScanRow.getString(tableRootOrdinal));
    }
    return new io.delta.kernel.internal.data.GenericRow(
        InternalScanFileUtils.SCAN_FILE_SCHEMA, fields);
  }

  private static AddFile sourceAddFile(Row scanFileRow) {
    return new AddFile(scanFileRow.getStruct(InternalScanFileUtils.ADD_FILE_ORDINAL));
  }

  private static AddFile addFileOf(Row singleAction) {
    return new AddFile(singleAction.getStruct(SingleAction.ADD_FILE_ORDINAL));
  }

  private static boolean isAddAction(Row action) {
    return !action.isNullAt(SingleAction.ADD_FILE_ORDINAL);
  }

  private static boolean isRemoveAction(Row action) {
    return !action.isNullAt(SingleAction.REMOVE_FILE_ORDINAL);
  }
}
