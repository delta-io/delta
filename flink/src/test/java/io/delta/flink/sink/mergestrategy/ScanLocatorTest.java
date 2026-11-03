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
import io.delta.flink.table.HadoopTable;
import io.delta.kernel.data.Row;
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.internal.InternalScanFileUtils;
import io.delta.kernel.internal.actions.AddFile;
import io.delta.kernel.utils.CloseableIterable;
import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.junit.jupiter.api.Test;

/** JUnit test suite for {@link ScanLocator}. */
class ScanLocatorTest extends TestHelper {

  private static final io.delta.kernel.types.StructType SCHEMA =
      new io.delta.kernel.types.StructType()
          .add("id", io.delta.kernel.types.IntegerType.INTEGER)
          .add("name", io.delta.kernel.types.StringType.STRING);

  /** PK = single column "id" at ordinal 0. */
  private static final int[] SINGLE_PK = {0};

  // -------------------------------------------------------------------------
  // Tests
  // -------------------------------------------------------------------------

  @Test
  void testFindReturnsFileWhenPkExists() {
    withTempDir(
        dir -> {
          HadoopTable table = openTable(dir, SCHEMA, Collections.emptyList());
          writeRows(table, SCHEMA, 10, Collections.emptyMap());

          // Single PK 5 — exists in the file (id range 0..9). Expect the file to be returned.
          List<List<Literal>> pks = List.of(List.of(Literal.ofInt(5)));
          List<Row> files = new ScanLocator().find(table, SINGLE_PK, pks).toInMemoryList();

          assertEquals(1, files.size(), "expected exactly one candidate file");
          // The row should be a scan-file row carrying an `add` substruct.
          AddFile addFile =
              new AddFile(files.get(0).getStruct(InternalScanFileUtils.ADD_FILE_ORDINAL));
          assertNotNull(addFile.getPath(), "scan-file row should carry a non-null AddFile.path");
        });
  }

  @Test
  void testFindReturnsFileForMultiplePks() {
    withTempDir(
        dir -> {
          HadoopTable table = openTable(dir, SCHEMA, Collections.emptyList());
          writeRows(table, SCHEMA, 10, Collections.emptyMap());

          // Multiple PKs all exist in the (single) file.
          List<List<Literal>> pks =
              List.of(
                  List.of(Literal.ofInt(2)), List.of(Literal.ofInt(4)), List.of(Literal.ofInt(7)));
          List<Row> files = new ScanLocator().find(table, SINGLE_PK, pks).toInMemoryList();

          assertEquals(1, files.size(), "expected exactly one candidate file");
        });
  }

  @Test
  void testFindReturnsEmptyForEmptyPks() {
    // Empty pks list → ExpressionUtils.in returns AlwaysFalse → scan returns no files at all.
    withTempDir(
        dir -> {
          HadoopTable table = openTable(dir, SCHEMA, Collections.emptyList());
          writeRows(table, SCHEMA, 10, Collections.emptyMap());

          List<Row> files =
              new ScanLocator().find(table, SINGLE_PK, Collections.emptyList()).toInMemoryList();

          assertTrue(files.isEmpty(), "empty pks list should yield no candidate files");
        });
  }

  @Test
  void testFindEmptyTableReturnsEmpty() {
    withTempDir(
        dir -> {
          // Open the table without writing any data.
          HadoopTable table = openTable(dir, SCHEMA, Collections.emptyList());
          // We need to create the empty table first by going through the open() path; that's
          // already done by openTable(). No commits → no scan files.

          List<List<Literal>> pks = List.of(List.of(Literal.ofInt(5)));
          List<Row> files = new ScanLocator().find(table, SINGLE_PK, pks).toInMemoryList();

          assertTrue(files.isEmpty(), "no files have been committed; scan should return empty");
        });
  }

  @Test
  void testFindWithCompositeKey() {
    withTempDir(
        dir -> {
          io.delta.kernel.types.StructType compositeSchema =
              new io.delta.kernel.types.StructType()
                  .add("org_id", io.delta.kernel.types.IntegerType.INTEGER)
                  .add("region", io.delta.kernel.types.StringType.STRING)
                  .add("payload", io.delta.kernel.types.StringType.STRING);

          HadoopTable table = openTable(dir, compositeSchema, Collections.emptyList());

          List<GenericRowData> rows =
              List.of(
                  GenericRowData.of(1, StringData.fromString("us"), StringData.fromString("p0")),
                  GenericRowData.of(2, StringData.fromString("eu"), StringData.fromString("p1")),
                  GenericRowData.of(3, StringData.fromString("us"), StringData.fromString("p2")));
          writeRowData(table, compositeSchema, rows, Collections.emptyMap());

          // (org_id, region) → ordinals (0, 1). Find tuples (1, "us") and (3, "us").
          int[] compositePk = {0, 1};
          List<List<Literal>> pks =
              List.of(
                  List.of(Literal.ofInt(1), Literal.ofString("us")),
                  List.of(Literal.ofInt(3), Literal.ofString("us")));

          List<Row> files = new ScanLocator().find(table, compositePk, pks).toInMemoryList();

          assertEquals(1, files.size(), "expected the single file containing matching tuples");
        });
  }

  @Test
  void testFindPreservesPkOrdinalOrder() {
    // ScanLocator maps int[] pkIndices → List<String> column names by indexing the schema's
    // fieldNames. Verify the order is preserved when the caller passes ordinals in reverse.
    withTempDir(
        dir -> {
          io.delta.kernel.types.StructType compositeSchema =
              new io.delta.kernel.types.StructType()
                  .add("a", io.delta.kernel.types.IntegerType.INTEGER)
                  .add("b", io.delta.kernel.types.IntegerType.INTEGER);
          HadoopTable table = openTable(dir, compositeSchema, Collections.emptyList());

          // Write rows (a=1, b=10), (a=2, b=20), (a=3, b=30).
          List<GenericRowData> rows =
              List.of(GenericRowData.of(1, 10), GenericRowData.of(2, 20), GenericRowData.of(3, 30));
          writeRowData(table, compositeSchema, rows, Collections.emptyMap());

          // Pass ordinals in (b, a) order — the value tuples must be (b-value, a-value).
          int[] reversedPk = {1, 0};
          List<List<Literal>> pks = List.of(List.of(Literal.ofInt(10), Literal.ofInt(1)));

          List<Row> files = new ScanLocator().find(table, reversedPk, pks).toInMemoryList();
          assertEquals(1, files.size(), "expected the file containing (a=1, b=10)");
        });
  }

  // -------------------------------------------------------------------------
  // Helpers (kept consistent with CoWUpsertTest)
  // -------------------------------------------------------------------------

  /** Open a HadoopTable at {@code dir} with the given schema/partition cols. */
  private HadoopTable openTable(
      File dir, io.delta.kernel.types.StructType schema, List<String> partitionCols) {
    HadoopTable table =
        new HadoopTable(
            URI.create(dir.getAbsolutePath()),
            Collections.emptyMap(),
            schema,
            partitionCols.isEmpty() ? Collections.emptyList() : partitionCols);
    table.open();
    return table;
  }

  /** Write {@code numRows} rows {@code (id=i, name="row-i")} via a DeltaWriterTask and commit. */
  private void writeRows(
      HadoopTable table,
      io.delta.kernel.types.StructType schema,
      int numRows,
      Map<String, Literal> partitionValues) {
    DeltaSinkConf conf = new DeltaSinkConf(schema, Collections.emptyMap());
    DeltaWriterTask task = new DeltaWriterTask("test-job", 0, 0, table, conf, partitionValues);
    for (int i = 0; i < numRows; i++) {
      try {
        task.write(
            GenericRowData.of(i, StringData.fromString("row-" + i)),
            new TestSinkWriterContext(0, 0));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    commit(table, task);
  }

  /**
   * Variant that takes pre-built rows — for schemas that don't fit the {@code (int, string)} mold.
   */
  private void writeRowData(
      HadoopTable table,
      io.delta.kernel.types.StructType schema,
      List<GenericRowData> rows,
      Map<String, Literal> partitionValues) {
    DeltaSinkConf conf = new DeltaSinkConf(schema, Collections.emptyMap());
    DeltaWriterTask task = new DeltaWriterTask("test-job", 0, 0, table, conf, partitionValues);
    try {
      for (GenericRowData row : rows) {
        task.write(row, new TestSinkWriterContext(0, 0));
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    commit(table, task);
  }

  private void commit(HadoopTable table, DeltaWriterTask task) {
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
  }
}
