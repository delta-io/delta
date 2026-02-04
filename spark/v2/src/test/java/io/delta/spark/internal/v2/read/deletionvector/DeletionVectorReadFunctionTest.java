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

import java.util.List;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

public class DeletionVectorReadFunctionTest {

  private static final StructType DATA_SCHEMA =
      new StructType().add("id", DataTypes.IntegerType).add("name", DataTypes.StringType);
  private static final StructType PARTITION_SCHEMA = new StructType();

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

    List<InternalRow> result = collectRows(readFunc.apply(null));

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

    List<InternalRow> result = collectRows(readFunc.apply(null));

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

    List<InternalRow> result = collectRows(readFunc.apply(null));

    assertRowsEquals(result, List.of(row(1, "alice"), row(2, "bob"), row(3, "charlie")));
  }
}
