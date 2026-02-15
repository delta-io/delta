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

import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.junit.jupiter.api.Test;

public class ColumnVectorWithFilterTest {

  @Test
  void testIntegerFiltering() {
    try (WritableColumnVector delegate = new OnHeapColumnVector(5, DataTypes.IntegerType)) {
      for (int i = 0; i < 5; i++) {
        delegate.putInt(i, (i + 1) * 10); // [10, 20, 30, 40, 50]
      }
      ColumnVectorWithFilter mappedVector = new ColumnVectorWithFilter(delegate, new int[] {1, 3});

      assertEquals(20, mappedVector.getInt(0));
      assertEquals(40, mappedVector.getInt(1));
    }
  }

  @Test
  void testLongFiltering() {
    try (WritableColumnVector delegate = new OnHeapColumnVector(4, DataTypes.LongType)) {
      for (int i = 0; i < 4; i++) {
        delegate.putLong(i, (i + 1) * 100L);
      }
      ColumnVectorWithFilter mappedVector = new ColumnVectorWithFilter(delegate, new int[] {0, 2});

      assertEquals(100L, mappedVector.getLong(0));
      assertEquals(300L, mappedVector.getLong(1));
    }
  }

  @Test
  void testDoubleFiltering() {
    try (WritableColumnVector delegate = new OnHeapColumnVector(3, DataTypes.DoubleType)) {
      delegate.putDouble(0, 1.1);
      delegate.putDouble(1, 2.2);
      delegate.putDouble(2, 3.3);
      ColumnVectorWithFilter mappedVector = new ColumnVectorWithFilter(delegate, new int[] {0, 2});

      assertEquals(1.1, mappedVector.getDouble(0), 0.001);
      assertEquals(3.3, mappedVector.getDouble(1), 0.001);
    }
  }

  @Test
  void testBooleanFiltering() {
    try (WritableColumnVector delegate = new OnHeapColumnVector(4, DataTypes.BooleanType)) {
      delegate.putBoolean(0, true);
      delegate.putBoolean(1, false);
      delegate.putBoolean(2, true);
      delegate.putBoolean(3, false);
      ColumnVectorWithFilter mappedVector = new ColumnVectorWithFilter(delegate, new int[] {1, 2});

      assertFalse(mappedVector.getBoolean(0));
      assertTrue(mappedVector.getBoolean(1));
    }
  }

  @Test
  void testStringFiltering() {
    try (WritableColumnVector delegate = new OnHeapColumnVector(3, DataTypes.StringType)) {
      delegate.putByteArray(0, "alice".getBytes());
      delegate.putByteArray(1, "bob".getBytes());
      delegate.putByteArray(2, "charlie".getBytes());
      ColumnVectorWithFilter mappedVector = new ColumnVectorWithFilter(delegate, new int[] {2});

      assertEquals("charlie", mappedVector.getUTF8String(0).toString());
    }
  }

  @Test
  void testStructGetChildPropagatesMapping() {
    StructType structType =
        new StructType().add("id", DataTypes.IntegerType).add("name", DataTypes.StringType);
    try (WritableColumnVector delegate = new OnHeapColumnVector(3, structType)) {
      WritableColumnVector idChild = (WritableColumnVector) delegate.getChild(0);
      for (int i = 0; i < 3; i++) {
        idChild.putInt(i, i + 1); // [1, 2, 3]
      }
      WritableColumnVector nameChild = (WritableColumnVector) delegate.getChild(1);
      nameChild.putByteArray(0, "a".getBytes());
      nameChild.putByteArray(1, "b".getBytes());
      nameChild.putByteArray(2, "c".getBytes());

      ColumnVectorWithFilter mappedVector = new ColumnVectorWithFilter(delegate, new int[] {0, 2});

      // getChild should return ColumnVectorWithFilter with the same rowIdMapping
      ColumnVector mappedIdColumn = mappedVector.getChild(0);
      ColumnVector mappedNameColumn = mappedVector.getChild(1);

      // filtered: row 0 -> original 0, row 1 -> original 2
      assertEquals(1, mappedIdColumn.getInt(0));
      assertEquals(3, mappedIdColumn.getInt(1));
      assertEquals("a", mappedNameColumn.getUTF8String(0).toString());
      assertEquals("c", mappedNameColumn.getUTF8String(1).toString());
    }
  }

  @Test
  void testColumnVectorWithNull() {
    try (WritableColumnVector delegate = new OnHeapColumnVector(4, DataTypes.IntegerType)) {
      // original rows: [10, null, 30, null]
      delegate.putInt(0, 10);
      delegate.putNull(1);
      delegate.putInt(2, 30);
      delegate.putNull(3);
      // selected rows (in order): [3, 0, 1] => [null, 10, null]
      ColumnVectorWithFilter mappedVector =
          new ColumnVectorWithFilter(delegate, new int[] {3, 0, 1});

      assertTrue(mappedVector.isNullAt(0));
      assertFalse(mappedVector.isNullAt(1));
      assertEquals(10, mappedVector.getInt(1));
      assertTrue(mappedVector.isNullAt(2));
    }
  }

  @Test
  void testEmptySelectionMapping() {
    try (WritableColumnVector delegate = new OnHeapColumnVector(3, DataTypes.IntegerType)) {
      delegate.putInt(0, 10);
      delegate.putInt(1, 20);
      delegate.putInt(2, 30);

      ColumnVectorWithFilter noRowsSelected = new ColumnVectorWithFilter(delegate, new int[] {});
      assertEquals(DataTypes.IntegerType, noRowsSelected.dataType());
    }
  }

  @Test
  void testIdentitySelectionMapping() {
    try (WritableColumnVector delegate = new OnHeapColumnVector(3, DataTypes.IntegerType)) {
      delegate.putInt(0, 10);
      delegate.putInt(1, 20);
      delegate.putInt(2, 30);

      // Identity mapping -> all select
      ColumnVectorWithFilter allRowsSelected =
          new ColumnVectorWithFilter(delegate, new int[] {0, 1, 2});
      assertEquals(10, allRowsSelected.getInt(0));
      assertEquals(20, allRowsSelected.getInt(1));
      assertEquals(30, allRowsSelected.getInt(2));
    }
  }
}
