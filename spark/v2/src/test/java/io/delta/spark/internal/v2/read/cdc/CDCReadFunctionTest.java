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

  private static final long COMMIT_VERSION = 5L;
  private static final long COMMIT_TIMESTAMP_MICROS = 1700000000000000L;

  // ===== Row-based tests =====

  @Test
  public void testInferredCDC_rowPath_nullCoalescesAllCDCColumns() {
    // Inferred CDC (AddFile -> insert): all 3 CDC columns are null from Parquet,
    // should be replaced with constants.
    // Schema: [id, _change_type(null), _commit_version(null), _commit_timestamp(null)]
    List<InternalRow> inputRows = List.of(row(1, null, null, null), row(2, null, null, null));

    CDCSchemaContext context = new CDCSchemaContext(DATA_SCHEMA, new StructType(), DATA_SCHEMA);
    PartitionedFile file =
        createCDCPartitionedFile("insert", COMMIT_VERSION, COMMIT_TIMESTAMP_MICROS);

    Function1<PartitionedFile, Iterator<InternalRow>> baseReader = mockReader(inputRows);
    CDCReadFunction readFunc = CDCReadFunction.wrap(baseReader, context, false);

    List<InternalRow> result = collectRows(readFunc.apply(file));

    assertEquals(2, result.size());
    // Row 0: id=1, _change_type="insert", _commit_version=5, _commit_timestamp=...
    assertEquals(1, result.get(0).getInt(0));
    assertEquals("insert", result.get(0).getUTF8String(1).toString());
    assertEquals(COMMIT_VERSION, result.get(0).getLong(2));
    assertEquals(COMMIT_TIMESTAMP_MICROS, result.get(0).getLong(3));
    // Row 1
    assertEquals(2, result.get(1).getInt(0));
    assertEquals("insert", result.get(1).getUTF8String(1).toString());
  }

  @Test
  public void testExplicitCDC_rowPath_preservesChangeTypeFromParquet() {
    // Explicit CDC (AddCDCFile): _change_type comes from Parquet, should be preserved.
    // Only _commit_version and _commit_timestamp are injected.
    List<InternalRow> inputRows =
        List.of(row(1, "update_preimage", null, null), row(2, "update_postimage", null, null));

    CDCSchemaContext context = new CDCSchemaContext(DATA_SCHEMA, new StructType(), DATA_SCHEMA);
    // No _change_type constant for explicit CDC
    PartitionedFile file =
        createExplicitCDCPartitionedFile(COMMIT_VERSION, COMMIT_TIMESTAMP_MICROS);

    Function1<PartitionedFile, Iterator<InternalRow>> baseReader = mockReader(inputRows);
    CDCReadFunction readFunc = CDCReadFunction.wrap(baseReader, context, false);

    List<InternalRow> result = collectRows(readFunc.apply(file));

    assertEquals(2, result.size());
    // _change_type preserved from Parquet data
    assertEquals("update_preimage", result.get(0).getUTF8String(1).toString());
    assertEquals("update_postimage", result.get(1).getUTF8String(1).toString());
    // _commit_version and _commit_timestamp injected
    assertEquals(COMMIT_VERSION, result.get(0).getLong(2));
    assertEquals(COMMIT_TIMESTAMP_MICROS, result.get(0).getLong(3));
  }

  @Test
  public void testDeleteCDC_rowPath_injectsDeleteChangeType() {
    // RemoveFile -> delete: _change_type = "delete"
    List<InternalRow> inputRows = List.of(row(1, null, null, null));

    CDCSchemaContext context = new CDCSchemaContext(DATA_SCHEMA, new StructType(), DATA_SCHEMA);
    PartitionedFile file =
        createCDCPartitionedFile("delete", COMMIT_VERSION, COMMIT_TIMESTAMP_MICROS);

    Function1<PartitionedFile, Iterator<InternalRow>> baseReader = mockReader(inputRows);
    CDCReadFunction readFunc = CDCReadFunction.wrap(baseReader, context, false);

    List<InternalRow> result = collectRows(readFunc.apply(file));

    assertEquals(1, result.size());
    assertEquals("delete", result.get(0).getUTF8String(1).toString());
  }

  @Test
  public void testInferredCDC_rowPath_reordersPartitionColumns() {
    // Table has interleaved data + partition columns: [id (data), country (partition)].
    // Internal batch layout: [id, _change_type, _commit_version, _commit_timestamp, country]
    // Output layout:         [id, country, _change_type, _commit_version, _commit_timestamp]
    // This exercises the non-trivial ordinal mapping where partition columns jump over CDC.
    StructType partitionSchema = new StructType().add("country", DataTypes.StringType);
    StructType tableSchema =
        new StructType().add("id", DataTypes.IntegerType).add("country", DataTypes.StringType);

    // Internal row: [id=42, _change_type=null, _commit_version=null, _commit_timestamp=null,
    //                country="US"]
    List<InternalRow> inputRows = List.of(row(42, null, null, null, "US"));

    CDCSchemaContext context = new CDCSchemaContext(DATA_SCHEMA, partitionSchema, tableSchema);
    PartitionedFile file =
        createCDCPartitionedFile("insert", COMMIT_VERSION, COMMIT_TIMESTAMP_MICROS);

    CDCReadFunction readFunc = CDCReadFunction.wrap(mockReader(inputRows), context, false);
    List<InternalRow> result = collectRows(readFunc.apply(file));

    assertEquals(1, result.size());
    InternalRow out = result.get(0);
    // output[0] = id (from internal[0])
    assertEquals(42, out.getInt(0));
    // output[1] = country (from internal[4], NOT internal[1])
    assertEquals("US", out.getUTF8String(1).toString());
    // output[2] = _change_type (constant)
    assertEquals("insert", out.getUTF8String(2).toString());
    // output[3] = _commit_version (constant)
    assertEquals(COMMIT_VERSION, out.getLong(3));
    // output[4] = _commit_timestamp (constant)
    assertEquals(COMMIT_TIMESTAMP_MICROS, out.getLong(4));
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

    CDCSchemaContext context = new CDCSchemaContext(DATA_SCHEMA, new StructType(), DATA_SCHEMA);
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

    // Check _commit_version replaced
    assertEquals(COMMIT_VERSION, resultBatch.column(2).getLong(0));

    // Check _commit_timestamp replaced
    assertEquals(COMMIT_TIMESTAMP_MICROS, resultBatch.column(3).getLong(0));
  }

  @Test
  public void testExplicitCDC_vectorizedPath_preservesChangeTypeVector() {
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

    CDCSchemaContext context = new CDCSchemaContext(DATA_SCHEMA, new StructType(), DATA_SCHEMA);
    PartitionedFile file =
        createExplicitCDCPartitionedFile(COMMIT_VERSION, COMMIT_TIMESTAMP_MICROS);

    CDCReadFunction readFunc = CDCReadFunction.wrap(mockBatchReader(List.of(batch)), context, true);

    List<ColumnarBatch> result = collectBatches(readFunc.apply(file));

    assertEquals(1, result.size());
    ColumnarBatch resultBatch = result.get(0);

    // _change_type should be the ORIGINAL vector (not replaced)
    assertEquals("update_preimage", resultBatch.column(1).getUTF8String(0).toString());
    assertEquals("update_postimage", resultBatch.column(1).getUTF8String(1).toString());

    // _commit_version and _commit_timestamp replaced with constants
    assertEquals(COMMIT_VERSION, resultBatch.column(2).getLong(0));
    assertEquals(COMMIT_TIMESTAMP_MICROS, resultBatch.column(3).getLong(0));
  }

  // ===== Helpers =====

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
