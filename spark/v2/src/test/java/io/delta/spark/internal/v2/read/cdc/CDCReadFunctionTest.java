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
package io.delta.spark.internal.v2.read.cdc;

import static io.delta.spark.internal.v2.InternalRowTestUtils.*;
import static org.junit.jupiter.api.Assertions.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.spark.paths.SparkPath;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.execution.datasources.PartitionedFile;
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.junit.jupiter.api.Test;
import scala.Function1;
import scala.collection.Iterator;
import scala.jdk.javaapi.CollectionConverters;

public class CDCReadFunctionTest {

  private static final StructType DATA_SCHEMA = new StructType().add("id", DataTypes.IntegerType);
  private static final StructType PARTITION_SCHEMA =
      new StructType().add("country", DataTypes.StringType);

  private static final long COMMIT_VERSION = 5L;
  private static final long COMMIT_TIMESTAMP_MICROS = 1700000000000000L;

  // ===== Row-based tests =====

  @Test
  public void testInferredCDC_rowPath_nullCoalescesAllCDCColumns() {
    // Internal: [id, _change_type(null), _commit_version(null), _commit_timestamp(null), country]
    // Output:   [id, country, _change_type, _commit_version, _commit_timestamp]
    List<InternalRow> inputRows =
        List.of(row(1, null, null, null, "US"), row(2, null, null, null, "UK"));

    CDCSchemaContext context = new CDCSchemaContext(DATA_SCHEMA, PARTITION_SCHEMA);
    PartitionedFile file =
        createCDCPartitionedFile("insert", COMMIT_VERSION, COMMIT_TIMESTAMP_MICROS);

    Function1<PartitionedFile, Iterator<InternalRow>> baseReader = mockReader(inputRows);
    CDCReadFunction readFunc = CDCReadFunction.wrap(baseReader, context, false);

    List<InternalRow> result = collectRows(readFunc.apply(file));

    assertRowsEquals(
        result,
        List.of(
            row(1, "US", "insert", COMMIT_VERSION, COMMIT_TIMESTAMP_MICROS),
            row(2, "UK", "insert", COMMIT_VERSION, COMMIT_TIMESTAMP_MICROS)));
  }

  @Test
  public void testExplicitCDC_rowPath_preservesChangeTypeFromParquet() {
    // Explicit CDC (AddCDCFile): _change_type comes from Parquet, should be preserved.
    // Only _commit_version and _commit_timestamp are injected.
    List<InternalRow> inputRows =
        List.of(
            row(1, "update_preimage", null, null, "US"),
            row(2, "update_postimage", null, null, "UK"));

    CDCSchemaContext context = new CDCSchemaContext(DATA_SCHEMA, PARTITION_SCHEMA);
    PartitionedFile file =
        createExplicitCDCPartitionedFile(COMMIT_VERSION, COMMIT_TIMESTAMP_MICROS);

    Function1<PartitionedFile, Iterator<InternalRow>> baseReader = mockReader(inputRows);
    CDCReadFunction readFunc = CDCReadFunction.wrap(baseReader, context, false);

    List<InternalRow> result = collectRows(readFunc.apply(file));

    assertRowsEquals(
        result,
        List.of(
            row(1, "US", "update_preimage", COMMIT_VERSION, COMMIT_TIMESTAMP_MICROS),
            row(2, "UK", "update_postimage", COMMIT_VERSION, COMMIT_TIMESTAMP_MICROS)));
  }

  @Test
  public void testDeleteCDC_rowPath_injectsDeleteChangeType() {
    // RemoveFile -> delete: _change_type = "delete"
    List<InternalRow> inputRows = List.of(row(1, null, null, null, "US"));

    CDCSchemaContext context = new CDCSchemaContext(DATA_SCHEMA, PARTITION_SCHEMA);
    PartitionedFile file =
        createCDCPartitionedFile("delete", COMMIT_VERSION, COMMIT_TIMESTAMP_MICROS);

    Function1<PartitionedFile, Iterator<InternalRow>> baseReader = mockReader(inputRows);
    CDCReadFunction readFunc = CDCReadFunction.wrap(baseReader, context, false);

    List<InternalRow> result = collectRows(readFunc.apply(file));

    assertRowsEquals(
        result, List.of(row(1, "US", "delete", COMMIT_VERSION, COMMIT_TIMESTAMP_MICROS)));
  }

  // ===== Vectorized tests =====

  @Test
  public void testInferredCDC_vectorizedPath_replacesColumnVectors() {
    // 3-row batch: [id, _change_type(null), _commit_version(null), _commit_timestamp(null)]
    int numRows = 3;
    WritableColumnVector idCol = new OnHeapColumnVector(numRows, DataTypes.IntegerType);
    WritableColumnVector changeTypeCol = new OnHeapColumnVector(numRows, DataTypes.StringType);
    WritableColumnVector commitVersionCol = new OnHeapColumnVector(numRows, DataTypes.LongType);
    WritableColumnVector commitTimestampCol = new OnHeapColumnVector(numRows, DataTypes.LongType);

    for (int i = 0; i < numRows; i++) {
      idCol.putInt(i, i + 1);
      changeTypeCol.putNull(i);
      commitVersionCol.putNull(i);
      commitTimestampCol.putNull(i);
    }

    ColumnarBatch batch =
        new ColumnarBatch(
            new ColumnVector[] {idCol, changeTypeCol, commitVersionCol, commitTimestampCol},
            numRows);

    CDCSchemaContext context = new CDCSchemaContext(DATA_SCHEMA, new StructType());
    PartitionedFile file =
        createCDCPartitionedFile("insert", COMMIT_VERSION, COMMIT_TIMESTAMP_MICROS);

    CDCReadFunction readFunc = CDCReadFunction.wrap(mockBatchReader(List.of(batch)), context, true);

    List<ColumnarBatch> result = collectBatches(readFunc.apply(file));

    assertEquals(1, result.size());
    ColumnarBatch resultBatch = result.get(0);
    assertEquals(numRows, resultBatch.numRows());
    assertEquals(4, resultBatch.numCols());

    // Check id column preserved
    assertEquals(1, resultBatch.column(0).getInt(0));
    assertEquals(2, resultBatch.column(0).getInt(1));

    // Check _change_type replaced with "insert"
    assertEquals("insert", resultBatch.column(1).getUTF8String(0).toString());
    assertEquals("insert", resultBatch.column(1).getUTF8String(1).toString());

    // Check _commit_version replaced for all rows
    for (int i = 0; i < numRows; i++) {
      assertEquals(COMMIT_VERSION, resultBatch.column(2).getLong(i));
      assertEquals(COMMIT_TIMESTAMP_MICROS, resultBatch.column(3).getLong(i));
    }
  }

  @Test
  public void testInferredCDC_vectorizedPath_reordersPartitionColumns() {
    // Internal: [id(0), _change_type(1), _commit_version(2), _commit_timestamp(3), country(4)]
    // Output:   [id(0), country(1), _change_type(2), _commit_version(3), _commit_timestamp(4)]
    int numRows = 2;
    WritableColumnVector idCol = new OnHeapColumnVector(numRows, DataTypes.IntegerType);
    WritableColumnVector changeTypeCol = new OnHeapColumnVector(numRows, DataTypes.StringType);
    WritableColumnVector commitVersionCol = new OnHeapColumnVector(numRows, DataTypes.LongType);
    WritableColumnVector commitTimestampCol = new OnHeapColumnVector(numRows, DataTypes.LongType);
    WritableColumnVector countryCol = new OnHeapColumnVector(numRows, DataTypes.StringType);

    idCol.putInt(0, 42);
    idCol.putInt(1, 43);
    changeTypeCol.putNull(0);
    changeTypeCol.putNull(1);
    commitVersionCol.putNull(0);
    commitVersionCol.putNull(1);
    commitTimestampCol.putNull(0);
    commitTimestampCol.putNull(1);
    countryCol.putByteArray(0, "US".getBytes());
    countryCol.putByteArray(1, "UK".getBytes());

    ColumnarBatch batch =
        new ColumnarBatch(
            new ColumnVector[] {
              idCol, changeTypeCol, commitVersionCol, commitTimestampCol, countryCol
            },
            numRows);

    CDCSchemaContext context = new CDCSchemaContext(DATA_SCHEMA, PARTITION_SCHEMA);
    PartitionedFile file =
        createCDCPartitionedFile("insert", COMMIT_VERSION, COMMIT_TIMESTAMP_MICROS);

    CDCReadFunction readFunc = CDCReadFunction.wrap(mockBatchReader(List.of(batch)), context, true);

    List<ColumnarBatch> result = collectBatches(readFunc.apply(file));

    assertEquals(1, result.size());
    ColumnarBatch resultBatch = result.get(0);
    assertEquals(numRows, resultBatch.numRows());
    assertEquals(5, resultBatch.numCols());

    assertEquals(42, resultBatch.column(0).getInt(0));
    assertEquals(43, resultBatch.column(0).getInt(1));
    // country remapped from internal[4] to output[1]
    assertEquals("US", resultBatch.column(1).getUTF8String(0).toString());
    assertEquals("UK", resultBatch.column(1).getUTF8String(1).toString());
    for (int i = 0; i < numRows; i++) {
      assertEquals("insert", resultBatch.column(2).getUTF8String(i).toString());
      assertEquals(COMMIT_VERSION, resultBatch.column(3).getLong(i));
      assertEquals(COMMIT_TIMESTAMP_MICROS, resultBatch.column(4).getLong(i));
    }
  }

  @Test
  public void testInferredCDC_vectorizedPath_handlesMultipleBatches() {
    // Two batches, each with different rows, should both be processed correctly with the
    // same per-file CDC constants applied.
    ColumnarBatch batch1 = makeInferredCdcBatch(new int[] {10, 20});
    ColumnarBatch batch2 = makeInferredCdcBatch(new int[] {30, 40, 50});

    CDCSchemaContext context = new CDCSchemaContext(DATA_SCHEMA, new StructType());
    PartitionedFile file =
        createCDCPartitionedFile("insert", COMMIT_VERSION, COMMIT_TIMESTAMP_MICROS);

    CDCReadFunction readFunc =
        CDCReadFunction.wrap(mockBatchReader(List.of(batch1, batch2)), context, true);

    List<ColumnarBatch> result = collectBatches(readFunc.apply(file));

    assertEquals(2, result.size());

    ColumnarBatch out1 = result.get(0);
    assertEquals(2, out1.numRows());
    assertEquals(10, out1.column(0).getInt(0));
    assertEquals(20, out1.column(0).getInt(1));
    for (int i = 0; i < out1.numRows(); i++) {
      assertEquals("insert", out1.column(1).getUTF8String(i).toString());
      assertEquals(COMMIT_VERSION, out1.column(2).getLong(i));
      assertEquals(COMMIT_TIMESTAMP_MICROS, out1.column(3).getLong(i));
    }

    ColumnarBatch out2 = result.get(1);
    assertEquals(3, out2.numRows());
    assertEquals(30, out2.column(0).getInt(0));
    assertEquals(40, out2.column(0).getInt(1));
    assertEquals(50, out2.column(0).getInt(2));
    for (int i = 0; i < out2.numRows(); i++) {
      assertEquals("insert", out2.column(1).getUTF8String(i).toString());
      assertEquals(COMMIT_VERSION, out2.column(2).getLong(i));
      assertEquals(COMMIT_TIMESTAMP_MICROS, out2.column(3).getLong(i));
    }
  }

  @Test
  public void testExplicitCDC_vectorizedPath_preservesChangeTypeVector() {
    // Explicit CDC (AddCDCFile): _change_type vector preserved from Parquet, not replaced.
    int numRows = 2;
    WritableColumnVector idCol = new OnHeapColumnVector(numRows, DataTypes.IntegerType);
    WritableColumnVector changeTypeCol = new OnHeapColumnVector(numRows, DataTypes.StringType);
    WritableColumnVector commitVersionCol = new OnHeapColumnVector(numRows, DataTypes.LongType);
    WritableColumnVector commitTimestampCol = new OnHeapColumnVector(numRows, DataTypes.LongType);

    idCol.putInt(0, 1);
    idCol.putInt(1, 2);
    changeTypeCol.putByteArray(0, "update_preimage".getBytes());
    changeTypeCol.putByteArray(1, "update_postimage".getBytes());
    commitVersionCol.putNull(0);
    commitVersionCol.putNull(1);
    commitTimestampCol.putNull(0);
    commitTimestampCol.putNull(1);

    ColumnarBatch batch =
        new ColumnarBatch(
            new ColumnVector[] {idCol, changeTypeCol, commitVersionCol, commitTimestampCol},
            numRows);

    CDCSchemaContext context = new CDCSchemaContext(DATA_SCHEMA, new StructType());
    PartitionedFile file =
        createExplicitCDCPartitionedFile(COMMIT_VERSION, COMMIT_TIMESTAMP_MICROS);

    CDCReadFunction readFunc = CDCReadFunction.wrap(mockBatchReader(List.of(batch)), context, true);

    List<ColumnarBatch> result = collectBatches(readFunc.apply(file));

    assertEquals(1, result.size());
    ColumnarBatch resultBatch = result.get(0);

    // _change_type should be the ORIGINAL vector (not replaced)
    assertEquals("update_preimage", resultBatch.column(1).getUTF8String(0).toString());
    assertEquals("update_postimage", resultBatch.column(1).getUTF8String(1).toString());

    // _commit_version and _commit_timestamp replaced with constants for all rows
    for (int i = 0; i < numRows; i++) {
      assertEquals(COMMIT_VERSION, resultBatch.column(2).getLong(i));
      assertEquals(COMMIT_TIMESTAMP_MICROS, resultBatch.column(3).getLong(i));
    }
  }

  // ===== Helpers =====

  /**
   * Build a ColumnarBatch shaped like an inferred-CDC parquet batch (no partition column): {@code
   * [id, _change_type(null), _commit_version(null), _commit_timestamp(null)]}.
   */
  private static ColumnarBatch makeInferredCdcBatch(int[] ids) {
    int numRows = ids.length;
    WritableColumnVector idCol = new OnHeapColumnVector(numRows, DataTypes.IntegerType);
    WritableColumnVector changeTypeCol = new OnHeapColumnVector(numRows, DataTypes.StringType);
    WritableColumnVector commitVersionCol = new OnHeapColumnVector(numRows, DataTypes.LongType);
    WritableColumnVector commitTimestampCol = new OnHeapColumnVector(numRows, DataTypes.LongType);
    for (int i = 0; i < numRows; i++) {
      idCol.putInt(i, ids[i]);
      changeTypeCol.putNull(i);
      commitVersionCol.putNull(i);
      commitTimestampCol.putNull(i);
    }
    return new ColumnarBatch(
        new ColumnVector[] {idCol, changeTypeCol, commitVersionCol, commitTimestampCol}, numRows);
  }

  /** Create a PartitionedFile for inferred CDC with _change_type constant. */
  private static PartitionedFile createCDCPartitionedFile(
      String changeType, long commitVersion, long commitTimestampMicros) {
    Map<String, Object> constants = new HashMap<>();
    constants.put(CDCSchemaContext.CDC_TYPE_COLUMN, changeType);
    constants.put(CDCSchemaContext.CDC_COMMIT_VERSION, commitVersion);
    constants.put(CDCSchemaContext.CDC_COMMIT_TIMESTAMP, commitTimestampMicros);
    return createPartitionedFile(constants);
  }

  /** Create a PartitionedFile for explicit CDC (no _change_type constant). */
  private static PartitionedFile createExplicitCDCPartitionedFile(
      long commitVersion, long commitTimestampMicros) {
    Map<String, Object> constants = new HashMap<>();
    constants.put(CDCSchemaContext.CDC_COMMIT_VERSION, commitVersion);
    constants.put(CDCSchemaContext.CDC_COMMIT_TIMESTAMP, commitTimestampMicros);
    return createPartitionedFile(constants);
  }

  private static PartitionedFile createPartitionedFile(Map<String, Object> constants) {
    scala.collection.immutable.Map<String, Object> scalaConstants =
        scala.collection.immutable.Map$.MODULE$.from(CollectionConverters.asScala(constants));
    return new PartitionedFile(
        InternalRow.empty(),
        SparkPath.fromPathString("/tmp/test.parquet"),
        /* start= */ 0L,
        /* length= */ 100L,
        /* locations= */ new String[0],
        /* modificationTime= */ 0L,
        /* fileSize= */ 100L,
        scalaConstants);
  }
}
