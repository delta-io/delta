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
      ColumnVectorWithFilter filtered = new ColumnVectorWithFilter(delegate, new int[] {1, 3});

      assertEquals(20, filtered.getInt(0));
      assertEquals(40, filtered.getInt(1));
    }
  }

  @Test
  void testLongFiltering() {
    try (WritableColumnVector delegate = new OnHeapColumnVector(4, DataTypes.LongType)) {
      for (int i = 0; i < 4; i++) {
        delegate.putLong(i, (i + 1) * 100L);
      }
      ColumnVectorWithFilter filtered = new ColumnVectorWithFilter(delegate, new int[] {0, 2});

      assertEquals(100L, filtered.getLong(0));
      assertEquals(300L, filtered.getLong(1));
    }
  }

  @Test
  void testDoubleFiltering() {
    try (WritableColumnVector delegate = new OnHeapColumnVector(3, DataTypes.DoubleType)) {
      delegate.putDouble(0, 1.1);
      delegate.putDouble(1, 2.2);
      delegate.putDouble(2, 3.3);
      ColumnVectorWithFilter filtered = new ColumnVectorWithFilter(delegate, new int[] {0, 2});

      assertEquals(1.1, filtered.getDouble(0), 0.001);
      assertEquals(3.3, filtered.getDouble(1), 0.001);
    }
  }

  @Test
  void testBooleanFiltering() {
    try (WritableColumnVector delegate = new OnHeapColumnVector(4, DataTypes.BooleanType)) {
      delegate.putBoolean(0, true);
      delegate.putBoolean(1, false);
      delegate.putBoolean(2, true);
      delegate.putBoolean(3, false);
      ColumnVectorWithFilter filtered = new ColumnVectorWithFilter(delegate, new int[] {1, 2});

      assertFalse(filtered.getBoolean(0));
      assertTrue(filtered.getBoolean(1));
    }
  }

  @Test
  void testStringFiltering() {
    try (WritableColumnVector delegate = new OnHeapColumnVector(3, DataTypes.StringType)) {
      delegate.putByteArray(0, "alice".getBytes());
      delegate.putByteArray(1, "bob".getBytes());
      delegate.putByteArray(2, "charlie".getBytes());
      ColumnVectorWithFilter filtered = new ColumnVectorWithFilter(delegate, new int[] {2});

      assertEquals("charlie", filtered.getUTF8String(0).toString());
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

      ColumnVectorWithFilter filtered = new ColumnVectorWithFilter(delegate, new int[] {0, 2});

      // getChild should return ColumnVectorWithFilter with the same rowIdMapping
      ColumnVector filteredId = filtered.getChild(0);
      ColumnVector filteredName = filtered.getChild(1);
      assertTrue(filteredId instanceof ColumnVectorWithFilter);
      assertTrue(filteredName instanceof ColumnVectorWithFilter);

      // filtered: row 0 -> original 0, row 1 -> original 2
      assertEquals(1, filteredId.getInt(0));
      assertEquals(3, filteredId.getInt(1));
      assertEquals("a", filteredName.getUTF8String(0).toString());
      assertEquals("c", filteredName.getUTF8String(1).toString());
    }
  }

  @Test
  void testNullHandling() {
    try (WritableColumnVector delegate = new OnHeapColumnVector(4, DataTypes.IntegerType)) {
      delegate.putInt(0, 10);
      delegate.putNull(1);
      delegate.putInt(2, 30);
      delegate.putNull(3);
      ColumnVectorWithFilter filtered =
          new ColumnVectorWithFilter(delegate, new int[] {0, 1, 2, 3});

      assertFalse(filtered.isNullAt(0));
      assertTrue(filtered.isNullAt(1));
      assertFalse(filtered.isNullAt(2));
      assertTrue(filtered.isNullAt(3));
    }
  }

  @Test
  void testEmptyAndIdentityMapping() {
    try (WritableColumnVector delegate = new OnHeapColumnVector(3, DataTypes.IntegerType)) {
      delegate.putInt(0, 10);
      delegate.putInt(1, 20);
      delegate.putInt(2, 30);

      // Empty mapping
      ColumnVectorWithFilter empty = new ColumnVectorWithFilter(delegate, new int[] {});
      assertEquals(DataTypes.IntegerType, empty.dataType());

      // Identity mapping
      ColumnVectorWithFilter identity = new ColumnVectorWithFilter(delegate, new int[] {0, 1, 2});
      assertEquals(10, identity.getInt(0));
      assertEquals(20, identity.getInt(1));
      assertEquals(30, identity.getInt(2));
    }
  }
}
