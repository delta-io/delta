/*
 * Copyright (2025) The Delta Lake Project Authors.
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

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.execution.datasources.PartitionedFile;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.jupiter.api.Test;
import scala.Function1;
import scala.collection.Iterator;
import scala.jdk.javaapi.CollectionConverters;
import scala.runtime.AbstractFunction1;

public class DeletionVectorReadFunctionTest {

  private static final StructType DATA_SCHEMA =
      new StructType().add("id", DataTypes.IntegerType).add("name", DataTypes.StringType);

  private static final StructType PARTITION_SCHEMA = new StructType();

  /** Create a mock row with id, name, and isRowDeleted flag. */
  private static InternalRow createRow(int id, String name, byte isDeleted) {
    return new GenericInternalRow(new Object[] {id, UTF8String.fromString(name), isDeleted});
  }

  /** Create a mock base reader that returns the given rows. */
  private static Function1<PartitionedFile, Iterator<InternalRow>> mockReader(
      List<InternalRow> rows) {
    return new AbstractFunction1<PartitionedFile, Iterator<InternalRow>>() {
      @Override
      public Iterator<InternalRow> apply(PartitionedFile file) {
        return CollectionConverters.asScala(rows.iterator());
      }
    };
  }

  /** Collect all rows from iterator into a list. */
  private static List<InternalRow> collectRows(Iterator<InternalRow> iter) {
    List<InternalRow> result = new ArrayList<>();
    while (iter.hasNext()) {
      InternalRow row = iter.next();
      // Copy the row since ProjectingInternalRow reuses the same instance
      result.add(row.copy());
    }
    return result;
  }

  @Test
  public void testFilterDeletedRowsAndProjectRemovesDvColumn() {
    // Input: 3 rows, middle one is deleted
    List<InternalRow> inputRows = new ArrayList<>();
    inputRows.add(createRow(1, "alice", (byte) 0)); // not deleted
    inputRows.add(createRow(2, "bob", (byte) 1)); // deleted
    inputRows.add(createRow(3, "charlie", (byte) 0)); // not deleted

    DvSchemaContext context = new DvSchemaContext(DATA_SCHEMA, PARTITION_SCHEMA);
    DeletionVectorReadFunction readFunc =
        DeletionVectorReadFunction.wrap(mockReader(inputRows), context);

    List<InternalRow> result = collectRows(readFunc.apply(null));

    // Should have 2 rows (deleted row filtered out)
    assertEquals(2, result.size());
    assertEquals(1, result.get(0).getInt(0));
    assertEquals(3, result.get(1).getInt(0));

    // Output should have 2 columns (id, name) - DV column removed
    InternalRow outputRow = result.get(0);
    assertEquals(2, outputRow.numFields());
    assertEquals(1, outputRow.getInt(0));
    assertEquals("alice", outputRow.getString(1));
  }

  @Test
  public void testAllRowsDeleted() {
    // Input: all rows deleted
    List<InternalRow> inputRows = new ArrayList<>();
    inputRows.add(createRow(1, "alice", (byte) 1));
    inputRows.add(createRow(2, "bob", (byte) 1));

    DvSchemaContext context = new DvSchemaContext(DATA_SCHEMA, PARTITION_SCHEMA);
    DeletionVectorReadFunction readFunc =
        DeletionVectorReadFunction.wrap(mockReader(inputRows), context);

    List<InternalRow> result = collectRows(readFunc.apply(null));

    // Should be empty
    assertEquals(0, result.size());
  }

  @Test
  public void testNoRowsDeleted() {
    // Input: no rows deleted
    List<InternalRow> inputRows = new ArrayList<>();
    inputRows.add(createRow(1, "alice", (byte) 0));
    inputRows.add(createRow(2, "bob", (byte) 0));
    inputRows.add(createRow(3, "charlie", (byte) 0));

    DvSchemaContext context = new DvSchemaContext(DATA_SCHEMA, PARTITION_SCHEMA);
    DeletionVectorReadFunction readFunc =
        DeletionVectorReadFunction.wrap(mockReader(inputRows), context);

    List<InternalRow> result = collectRows(readFunc.apply(null));

    // All 3 rows should be present
    assertEquals(3, result.size());
  }
}
