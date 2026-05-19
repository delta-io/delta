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
import static org.junit.jupiter.api.Assertions.*;

import io.delta.flink.TestHelper;
import io.delta.flink.sink.DeltaSinkConf;
import io.delta.flink.sink.DeltaWriterResult;
import io.delta.flink.sink.DeltaWriterTask;
import io.delta.flink.sink.TestSinkWriterContext;
import io.delta.flink.table.AbstractKernelTable;
import io.delta.flink.table.HadoopTable;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.MapValue;
import io.delta.kernel.data.Row;
import io.delta.kernel.expressions.AlwaysTrue;
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.internal.InternalScanFileUtils;
import io.delta.kernel.internal.actions.AddFile;
import io.delta.kernel.internal.actions.RemoveFile;
import io.delta.kernel.internal.actions.SingleAction;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterable;
import io.delta.kernel.utils.CloseableIterator;
import java.io.File;
import java.net.URI;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiPredicate;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.junit.jupiter.api.Test;

/**
 * Per-file tests for {@link CoWUpsert#deleteRecords}.
 *
 * <p>This suite exercises the file-rewrite half of the upsert pipeline in isolation — it supplies a
 * scan-file row and a row-level filter directly, bypassing {@link
 * io.delta.flink.sink.MergeStrategy#recordUpsert} / {@link
 * io.delta.flink.sink.MergeStrategy#recordDelete} / {@link Upsert#merge}. End-to-end tests of the
 * full merge pipeline (including {@link RowLocator} and the PK-driven filter construction) are out
 * of scope here.
 *
 * <p>{@link CoWUpsert#deleteRecords} is {@code protected}, so we wrap it in a tiny test-only
 * subclass ({@link ExposedCoWUpsert}) that exposes the method plus the {@code table} field.
 */
class CoWUpsertTest extends TestHelper {

  private static final StructType SCHEMA =
      new StructType().add("id", IntegerType.INTEGER).add("name", StringType.STRING);

  /** Column ordinal of {@code id} in {@link #SCHEMA}. */
  private static final int ID_ORDINAL = 0;

  // -------------------------------------------------------------------------
  // Tests
  // -------------------------------------------------------------------------

  @Test
  void testDropSomeRows() {
    withTempDir(
        dir -> {
          HadoopTable table = openTable(dir, SCHEMA, Collections.emptyList());
          Row scanFileRow = writeAndScan(table, SCHEMA, 10, Collections.emptyMap());

          // Drop rows whose id is even — 5 survivors expected.
          BiPredicate<ColumnarBatch, Integer> deleteEvens = idMatches(i -> i % 2 == 0);

          List<Row> actions = runDeleteRecords(table, scanFileRow, deleteEvens).toInMemoryList();

          assertEquals(2, actions.size(), "expected RemoveFile + AddFile");
          assertIsRemoveAction(actions.get(0));
          assertIsAddAction(actions.get(1));

          AddFile addFile = addFileOf(actions.get(1));
          Path newFile = dir.toPath().resolve(addFile.getPath()).toAbsolutePath();
          List<Row> survivors = readParquet(newFile, SCHEMA);
          assertEquals(5, survivors.size());
          for (Row r : survivors) {
            assertEquals(1, r.getInt(0) % 2, "even id leaked through filter");
          }

          // RemoveFile path matches source AddFile path (same relative form).
          RemoveFile removeFile =
              new RemoveFile(actions.get(0).getStruct(SingleAction.REMOVE_FILE_ORDINAL));
          assertEquals(sourceAddFile(scanFileRow).getPath(), removeFile.getPath());
        });
  }

  @Test
  void testDropAllRows() {
    withTempDir(
        dir -> {
          HadoopTable table = openTable(dir, SCHEMA, Collections.emptyList());
          Row scanFileRow = writeAndScan(table, SCHEMA, 10, Collections.emptyMap());

          List<Row> actions =
              runDeleteRecords(table, scanFileRow, /* drop all */ (b, r) -> true).toInMemoryList();

          // Exactly one RemoveFile is required (the source file must be retired).
          int removes = 0;
          int addsWithSurvivors = 0;
          for (Row action : actions) {
            if (!action.isNullAt(SingleAction.REMOVE_FILE_ORDINAL)) {
              removes++;
            }
            if (!action.isNullAt(SingleAction.ADD_FILE_ORDINAL)) {
              // Whether the writer emits an empty-file AddFile is implementation-defined; if it
              // does, the resulting Parquet file must be row-empty.
              AddFile addFile = addFileOf(action);
              Path newFile = dir.toPath().resolve(addFile.getPath()).toAbsolutePath();
              if (!readParquet(newFile, SCHEMA).isEmpty()) {
                addsWithSurvivors++;
              }
            }
          }
          assertEquals(1, removes, "expected exactly one RemoveFile");
          assertEquals(
              0, addsWithSurvivors, "no AddFile should contain any rows when all are deleted");
        });
  }

  @Test
  void testKeepAllRows() {
    withTempDir(
        dir -> {
          HadoopTable table = openTable(dir, SCHEMA, Collections.emptyList());
          final int numRows = 7;
          Row scanFileRow = writeAndScan(table, SCHEMA, numRows, Collections.emptyMap());

          List<Row> actions =
              runDeleteRecords(table, scanFileRow, /* keep all */ (b, r) -> false).toInMemoryList();

          assertEquals(2, actions.size(), "expected RemoveFile + AddFile");
          assertIsRemoveAction(actions.get(0));
          assertIsAddAction(actions.get(1));

          AddFile addFile = addFileOf(actions.get(1));
          Path newFile = dir.toPath().resolve(addFile.getPath()).toAbsolutePath();
          List<Row> survivors = readParquet(newFile, SCHEMA);
          assertEquals(numRows, survivors.size());
          for (int i = 0; i < numRows; i++) {
            assertEquals(i, survivors.get(i).getInt(0), "row order changed");
          }
        });
  }

  @Test
  void testPreservesPartitionValues() {
    withTempDir(
        dir -> {
          StructType partSchema =
              new StructType().add("id", IntegerType.INTEGER).add("part", StringType.STRING);
          HadoopTable table = openTable(dir, partSchema, List.of("part"));

          Map<String, Literal> partValues = Map.of("part", Literal.ofString("p0"));
          Row scanFileRow = writeAndScan(table, partSchema, 6, partValues);

          List<Row> actions =
              runDeleteRecords(table, scanFileRow, idMatches(i -> i < 3)).toInMemoryList();

          assertEquals(2, actions.size());
          AddFile addFile = addFileOf(actions.get(1));

          // Partition values round-trip onto the new AddFile.
          MapValue pm = addFile.getPartitionValues();
          assertEquals(1, pm.getSize());
          assertEquals("part", pm.getKeys().getString(0));
          assertEquals("p0", pm.getValues().getString(0));

          Path newFile = dir.toPath().resolve(addFile.getPath()).toAbsolutePath();
          List<Row> survivors = readParquet(newFile, partSchema);
          assertEquals(3, survivors.size());
          for (Row r : survivors) {
            assertTrue(r.getInt(0) >= 3);
          }
        });
  }

  @Test
  void testRewriteCoLocatedWithSource() {
    withTempDir(
        dir -> {
          StructType partSchema =
              new StructType().add("id", IntegerType.INTEGER).add("part", StringType.STRING);
          HadoopTable table = openTable(dir, partSchema, List.of("part"));

          Map<String, Literal> partValues = Map.of("part", Literal.ofString("p7"));
          Row scanFileRow = writeAndScan(table, partSchema, 4, partValues);

          List<Row> actions =
              runDeleteRecords(table, scanFileRow, idMatches(i -> i == 0)).toInMemoryList();

          assertEquals(2, actions.size());
          String sourcePath = sourceAddFile(scanFileRow).getPath();
          String newPath = addFileOf(actions.get(1)).getPath();

          // The actual co-location guarantee: rewrite shares the source's parent directory.
          // Note: the on-disk layout for Flink-Delta writes is NOT partition-based — files land
          // under <jobId>-<subtaskId>-<attemptNumber>/<uuid>.parquet and the partition is
          // tracked only in AddFile.partitionValues. So we don't assert anything about a
          // "part=p7/" prefix in the path; we just check the parent dirs agree.
          assertEquals(
              parentDir(sourcePath),
              parentDir(newPath),
              "rewrite must land in the same folder as the source");
        });
  }

  @Test
  void testCarriesOverStatsFromSourceAddFileToRemove() {
    withTempDir(
        dir -> {
          HadoopTable table = openTable(dir, SCHEMA, Collections.emptyList());
          Row scanFileRow = writeAndScan(table, SCHEMA, 10, Collections.emptyMap());

          List<Row> actions =
              runDeleteRecords(table, scanFileRow, /* keep all */ (b, r) -> false).toInMemoryList();

          // The RemoveFile constructed via AddFile.toRemoveFileRow carries over size and path;
          // verify the metadata wasn't silently dropped during the conversion.
          AddFile srcAdd = sourceAddFile(scanFileRow);
          RemoveFile removeFile =
              new RemoveFile(actions.get(0).getStruct(SingleAction.REMOVE_FILE_ORDINAL));

          // RemoveFile.getSize() is Optional<Long>; AddFile.getSize() is a plain long. Compare
          // the values (the Optional should always be present here because toRemoveFileRow
          // copies the source's size unconditionally).
          assertTrue(removeFile.getSize().isPresent(), "RemoveFile.size missing");
          assertEquals(srcAdd.getSize(), removeFile.getSize().get().longValue());
          assertEquals(srcAdd.getPath(), removeFile.getPath());
        });
  }

  @Test
  void testFilterReceivesEveryRow() {
    // Defensive: ensure the filter is invoked for every input row exactly once, in row order.
    withTempDir(
        dir -> {
          HadoopTable table = openTable(dir, SCHEMA, Collections.emptyList());
          final int numRows = 8;
          Row scanFileRow = writeAndScan(table, SCHEMA, numRows, Collections.emptyMap());

          List<Integer> idsSeen = new ArrayList<>();
          BiPredicate<ColumnarBatch, Integer> recording =
              (batch, rowId) -> {
                idsSeen.add(batch.getColumnVector(ID_ORDINAL).getInt(rowId));
                return false; // keep all
              };

          List<Row> actions = runDeleteRecords(table, scanFileRow, recording).toInMemoryList();
          // Force survivor materialization (writeParquet is lazy until the iterator is consumed).
          AddFile addFile = addFileOf(actions.get(1));
          readParquet(dir.toPath().resolve(addFile.getPath()).toAbsolutePath(), SCHEMA);

          // 8 ids observed, in ascending order matching how writeAndScan populated the file.
          assertEquals(numRows, idsSeen.size());
          for (int i = 0; i < numRows; i++) {
            assertEquals(Integer.valueOf(i), idsSeen.get(i));
          }
        });
  }

  @Test
  void testDropByCompositeKey() {
    // Exercises a multi-column primary-key delete. The filter reads two columns
    // (org_id, region) and removes the rows whose tuple is in a delete set — the same shape an
    // Upsert strategy uses when the PK has more than one column.
    withTempDir(
        dir -> {
          StructType schema =
              new StructType()
                  .add("org_id", IntegerType.INTEGER)
                  .add("region", StringType.STRING)
                  .add("payload", StringType.STRING);

          HadoopTable table = openTable(dir, schema, Collections.emptyList());

          // 6 rows; (org_id, region) is the composite PK.
          List<GenericRowData> rows =
              List.of(
                  GenericRowData.of(1, StringData.fromString("us"), StringData.fromString("p0")),
                  GenericRowData.of(1, StringData.fromString("eu"), StringData.fromString("p1")),
                  GenericRowData.of(2, StringData.fromString("us"), StringData.fromString("p2")),
                  GenericRowData.of(2, StringData.fromString("eu"), StringData.fromString("p3")),
                  GenericRowData.of(3, StringData.fromString("us"), StringData.fromString("p4")),
                  GenericRowData.of(3, StringData.fromString("eu"), StringData.fromString("p5")));
          Row scanFileRow = writeAndScanRows(table, schema, rows);

          // Delete two specific PKs: (1, "eu") and (3, "us"). 4 survivors expected.
          Set<List<Object>> deleteKeys = Set.of(List.of(1, "eu"), List.of(3, "us"));
          BiPredicate<ColumnarBatch, Integer> filter =
              (batch, rowId) -> {
                int orgId = batch.getColumnVector(0).getInt(rowId);
                String region = batch.getColumnVector(1).getString(rowId);
                return deleteKeys.contains(List.of(orgId, region));
              };

          List<Row> actions = runDeleteRecords(table, scanFileRow, filter).toInMemoryList();

          assertEquals(2, actions.size(), "expected RemoveFile + AddFile");
          assertIsRemoveAction(actions.get(0));
          assertIsAddAction(actions.get(1));

          AddFile addFile = addFileOf(actions.get(1));
          Path newFile = dir.toPath().resolve(addFile.getPath()).toAbsolutePath();
          List<Row> survivors = readParquet(newFile, schema);

          // 4 survivors, none matching any delete key.
          assertEquals(4, survivors.size(), "expected 4 surviving rows");
          for (Row r : survivors) {
            int orgId = r.getInt(0);
            String region = r.getString(1);
            assertFalse(
                deleteKeys.contains(List.of(orgId, region)),
                "row (" + orgId + ", " + region + ") should have been deleted");
          }

          // Order-preserving spot-check by payload: expected survivors in source order are
          // p0 (1,us), p2 (2,us), p3 (2,eu), p5 (3,eu).
          assertEquals("p0", survivors.get(0).getString(2));
          assertEquals("p2", survivors.get(1).getString(2));
          assertEquals("p3", survivors.get(2).getString(2));
          assertEquals("p5", survivors.get(3).getString(2));
        });
  }

  // -------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------

  /**
   * Test-only subclass of {@link CoWUpsert} that exposes the {@code protected} {@code
   * deleteRecords(...)} method and the inherited {@code table} field. Bypassing {@link
   * Upsert#merge} lets us hand the strategy a scan-file row and a synthetic filter directly,
   * isolating the file-rewrite behavior from the locator and PK-bookkeeping concerns.
   */
  private static final class ExposedCoWUpsert extends CoWUpsert {
    ExposedCoWUpsert(AbstractKernelTable backingTable) {
      this.table = backingTable;
    }

    CloseableIterator<Row> deleteRecordsForTest(
        Row addFile, BiPredicate<ColumnarBatch, Integer> filter) {
      return deleteRecords(addFile, filter);
    }
  }

  /** Build a "drop rows where {@code id} matches {@code idTest}" filter. */
  private static BiPredicate<ColumnarBatch, Integer> idMatches(
      java.util.function.IntPredicate idTest) {
    return (batch, rowId) -> idTest.test(batch.getColumnVector(ID_ORDINAL).getInt(rowId));
  }

  /** Open a fresh HadoopTable rooted at {@code dir} with the given schema and partition cols. */
  private HadoopTable openTable(File dir, StructType schema, List<String> partitionCols) {
    HadoopTable table =
        new HadoopTable(
            URI.create(dir.getAbsolutePath()),
            Collections.emptyMap(),
            schema,
            partitionCols.isEmpty() ? Collections.emptyList() : partitionCols);
    table.open();
    return table;
  }

  /**
   * Write {@code numRows} rows into {@code table} via a {@link DeltaWriterTask}, commit the
   * resulting AddFile actions, and return the scan-file row for the (single) data file produced.
   *
   * <p>Rows are populated as {@code (id=i, name="row-i")} for unpartitioned tables, or {@code
   * (id=i, part=<partition-value>)} when {@code partitionValues} is non-empty.
   */
  private Row writeAndScan(
      HadoopTable table, StructType schema, int numRows, Map<String, Literal> partitionValues) {
    DeltaSinkConf conf = new DeltaSinkConf(schema, Collections.emptyMap());
    DeltaWriterTask task =
        new DeltaWriterTask(
            "test-job", /* subtaskId= */ 0, /* attemptNumber= */ 0, table, conf, partitionValues);

    String partString =
        partitionValues.isEmpty()
            ? null
            : String.valueOf(partitionValues.values().iterator().next().getValue());

    for (int i = 0; i < numRows; i++) {
      Object[] cells = new Object[schema.length()];
      cells[0] = Integer.valueOf(i);
      cells[1] =
          partString != null
              ? StringData.fromString(partString)
              : StringData.fromString("row-" + i);
      try {
        task.write(GenericRowData.of(cells), new TestSinkWriterContext(0, 0));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    return commitAndScan(table, task);
  }

  /**
   * Variant of {@link #writeAndScan} for callers that have already built the rows themselves —
   * useful for schemas with more than two columns or non-trivial row payloads.
   *
   * <p>The DeltaWriterTask is created with empty partition values (i.e. unpartitioned), so this
   * helper is only suitable for unpartitioned tables.
   */
  private Row writeAndScanRows(HadoopTable table, StructType schema, List<GenericRowData> rows) {
    DeltaSinkConf conf = new DeltaSinkConf(schema, Collections.emptyMap());
    DeltaWriterTask task =
        new DeltaWriterTask(
            "test-job", /* subtaskId= */ 0, /* attemptNumber= */ 0, table, conf, Map.of());
    try {
      for (GenericRowData row : rows) {
        task.write(row, new TestSinkWriterContext(0, 0));
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return commitAndScan(table, task);
  }

  /**
   * Shared tail of {@link #writeAndScan} and {@link #writeAndScanRows}: finalize the writer task,
   * commit the produced AddFile actions to the table, and return the (single) scan-file row.
   */
  private Row commitAndScan(HadoopTable table, DeltaWriterTask task) {
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

    // Materialize scan-file rows so they outlive the scan iterator. We don't wrap in
    // try-with-resources here: toInMemoryList() closes the iterator internally, and an explicit
    // double-close triggers an IllegalStateException inside Kernel's ScanImpl close path
    // (it calls hasNext() on an already-closed underlying iterator).
    List<Row> all = table.scan(AlwaysTrue.ALWAYS_TRUE).toInMemoryList();
    assertEquals(1, all.size(), "expected exactly one scan file from commitAndScan");
    return all.get(0);
  }

  /**
   * Invoke {@link CoWUpsert#deleteRecords} for the given scan-file row and filter, going through
   * the test-only subclass that exposes the protected method.
   */
  private CloseableIterator<Row> runDeleteRecords(
      HadoopTable table, Row scanFileRow, BiPredicate<ColumnarBatch, Integer> filter) {
    return new ExposedCoWUpsert(table).deleteRecordsForTest(scanFileRow, filter);
  }

  private static AddFile addFileOf(Row singleAction) {
    return new AddFile(singleAction.getStruct(SingleAction.ADD_FILE_ORDINAL));
  }

  /** Unwrap the {@code add} struct from a scan-file row, using the public ordinal constant. */
  private static AddFile sourceAddFile(Row scanFileRow) {
    return new AddFile(scanFileRow.getStruct(InternalScanFileUtils.ADD_FILE_ORDINAL));
  }

  private static String parentDir(String relativePath) {
    int slash = relativePath.lastIndexOf('/');
    return slash >= 0 ? relativePath.substring(0, slash) : "";
  }

  private static void assertIsRemoveAction(Row action) {
    assertFalse(
        action.isNullAt(SingleAction.REMOVE_FILE_ORDINAL),
        "expected the action to carry a RemoveFile");
    assertTrue(
        action.isNullAt(SingleAction.ADD_FILE_ORDINAL),
        "did not expect the action to also carry an AddFile");
  }

  private static void assertIsAddAction(Row action) {
    assertFalse(
        action.isNullAt(SingleAction.ADD_FILE_ORDINAL), "expected the action to carry an AddFile");
    assertTrue(
        action.isNullAt(SingleAction.REMOVE_FILE_ORDINAL),
        "did not expect the action to also carry a RemoveFile");
  }
}
