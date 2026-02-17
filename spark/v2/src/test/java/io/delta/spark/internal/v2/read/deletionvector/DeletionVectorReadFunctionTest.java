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
package io.delta.spark.internal.v2.read.deletionvector;

import static io.delta.spark.internal.v2.InternalRowTestUtils.*;
import static org.junit.jupiter.api.Assertions.*;

import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.junit.jupiter.api.Test;

public class DeletionVectorReadFunctionTest {

  private static final StructType DATA_SCHEMA =
      new StructType().add("id", DataTypes.IntegerType).add("name", DataTypes.StringType);
  private static final StructType PARTITION_SCHEMA = new StructType();

  // ===== Row-based tests =====

  @Test
  public void testFilterDeletedRowsAndProjectRemovesDvColumn() {
    // Input: 3 rows, middle one is deleted.
    List<InternalRow> inputRows =
        List.of(
            row(1, "alice", (byte) 0), // Not deleted.
            row(2, "bob", (byte) 1), // Deleted.
            row(3, "charlie", (byte) 0)); // Not deleted.

    DeletionVectorSchemaContext context =
        new DeletionVectorSchemaContext(DATA_SCHEMA, PARTITION_SCHEMA);
    DeletionVectorReadFunction readFunc =
        DeletionVectorReadFunction.wrap(mockReader(inputRows), context);

    List<InternalRow> result = collectRows(readFunc.apply(/* file= */ null));

    // Verify filtered and projected output (DV column removed, deleted row filtered).
    assertRowsEquals(result, List.of(row(1, "alice"), row(3, "charlie")));
  }

  @Test
  public void testAllRowsDeleted() {
    List<InternalRow> inputRows =
        List.of(row(1, "alice", (byte) 1), row(2, "bob", (byte) 1)); // All deleted.

    DeletionVectorSchemaContext context =
        new DeletionVectorSchemaContext(DATA_SCHEMA, PARTITION_SCHEMA);
    DeletionVectorReadFunction readFunc =
        DeletionVectorReadFunction.wrap(mockReader(inputRows), context);

    List<InternalRow> result = collectRows(readFunc.apply(/* file= */ null));

    assertRowsEquals(result, List.of());
  }

  @Test
  public void testNoRowsDeleted() {
    List<InternalRow> inputRows =
        List.of(row(1, "alice", (byte) 0), row(2, "bob", (byte) 0), row(3, "charlie", (byte) 0));

    DeletionVectorSchemaContext context =
        new DeletionVectorSchemaContext(DATA_SCHEMA, PARTITION_SCHEMA);
    DeletionVectorReadFunction readFunc =
        DeletionVectorReadFunction.wrap(mockReader(inputRows), context);

    List<InternalRow> result = collectRows(readFunc.apply(/* file= */ null));

    assertRowsEquals(result, List.of(row(1, "alice"), row(2, "bob"), row(3, "charlie")));
  }

  // ===== ColumnarBatch (vectorized) tests =====

  @Test
  public void testBatchFilterDeletedRowsAndProjectRemovesDvColumn() {
    // 3 rows: row 1 deleted, rows 0 and 2 kept.
    ColumnarBatch inputBatch = createBatch(new int[] {1, 2, 3}, new byte[] {0, 1, 0});
    List<ColumnarBatch> result = runBatchRead(inputBatch);

    assertEquals(1, result.size());
    // Filtered: row 0 -> original 0 (id=1), row 1 -> original 2 (id=3)
    assertBatchRows(result.get(0), new int[] {1, 3}, new String[] {"name_0", "name_2"});
  }

  @Test
  public void testBatchAllRowsDeleted() {
    ColumnarBatch inputBatch = createBatch(new int[] {1, 2}, new byte[] {1, 1});
    List<ColumnarBatch> result = runBatchRead(inputBatch);

    assertEquals(1, result.size());
    assertBatchRows(result.get(0), new int[] {}, new String[] {});
  }

  @Test
  public void testBatchNoRowsDeleted() {
    ColumnarBatch inputBatch = createBatch(new int[] {1, 2, 3}, new byte[] {0, 0, 0});
    List<ColumnarBatch> result = runBatchRead(inputBatch);

    assertEquals(1, result.size());
    assertBatchRows(
        result.get(0), new int[] {1, 2, 3}, new String[] {"name_0", "name_1", "name_2"});
  }

  @Test
  public void testBatchMultipleBatchesWithEmptyInBetween() {
    ColumnarBatch allDeleted = createBatch(new int[] {1, 2, 3}, new byte[] {1, 1, 1});
    ColumnarBatch mixed = createBatch(new int[] {4, 5, 6}, new byte[] {0, 1, 0});
    ColumnarBatch allLive = createBatch(new int[] {7}, new byte[] {0});

    List<ColumnarBatch> result = runBatchRead(allDeleted, mixed, allLive);

    assertEquals(3, result.size());
    assertBatchRows(result.get(0), new int[] {}, new String[] {});
    assertBatchRows(result.get(1), new int[] {4, 6}, new String[] {"name_0", "name_2"});
    assertBatchRows(result.get(2), new int[] {7}, new String[] {"name_0"});
  }

  private List<ColumnarBatch> runBatchRead(ColumnarBatch... inputBatches) {
    DeletionVectorSchemaContext context =
        new DeletionVectorSchemaContext(DATA_SCHEMA, PARTITION_SCHEMA);
    DeletionVectorReadFunction readFunc =
        DeletionVectorReadFunction.wrap(mockBatchReader(List.of(inputBatches)), context);
    return collectBatches(readFunc.apply(/* file= */ null));
  }

  private static void assertBatchRows(
      ColumnarBatch batch, int[] expectedIds, String[] expectedNames) {
    assertEquals(expectedIds.length, expectedNames.length, "Expected id/name lengths must match");
    assertEquals(expectedIds.length, batch.numRows(), "Unexpected number of filtered rows");
    for (int i = 0; i < expectedIds.length; i++) {
      assertEquals(expectedIds[i], batch.column(0).getInt(i));
      assertEquals(expectedNames[i], batch.column(1).getUTF8String(i).toString());
    }
  }

  /**
   * Creates a ColumnarBatch with columns [id (int), name (string), is_row_deleted (byte)].
   *
   * <p>Name values are auto-generated as "name_0", "name_1", etc.
   */
  private static ColumnarBatch createBatch(int[] ids, byte[] deletionVector) {
    int numRows = ids.length;
    WritableColumnVector idCol = new OnHeapColumnVector(numRows, DataTypes.IntegerType);
    WritableColumnVector nameCol = new OnHeapColumnVector(numRows, DataTypes.StringType);
    WritableColumnVector dvCol = new OnHeapColumnVector(numRows, DataTypes.ByteType);

    for (int i = 0; i < numRows; i++) {
      idCol.putInt(i, ids[i]);
      nameCol.putByteArray(i, ("name_" + i).getBytes(StandardCharsets.UTF_8));
      dvCol.putByte(i, deletionVector[i]);
    }

    return new ColumnarBatch(new ColumnVector[] {idCol, nameCol, dvCol}, numRows);
  }
}
