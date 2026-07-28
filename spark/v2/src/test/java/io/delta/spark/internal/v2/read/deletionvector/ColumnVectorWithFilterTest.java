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

import static org.junit.jupiter.api.Assertions.*;

import java.util.function.IntFunction;
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarArray;
import org.apache.spark.unsafe.types.CalendarInterval;
import org.apache.spark.unsafe.types.VariantVal;
import org.junit.jupiter.api.Test;

public class ColumnVectorWithFilterTest {

  @Test
  void testIntegerColumnVector() {
    try (WritableColumnVector delegate = new OnHeapColumnVector(5, DataTypes.IntegerType)) {
      for (int i = 0; i < 5; i++) {
        delegate.putInt(i, (i + 1) * 10); // [10, 20, 30, 40, 50]
      }
      ColumnVectorWithFilter mappedVector = new ColumnVectorWithFilter(delegate, new int[] {1, 3});

      assertMappedValues(mappedVector, mappedVector::getInt, new Integer[] {20, 40});
    }
  }

  @Test
  void testLongColumnVector() {
    try (WritableColumnVector delegate = new OnHeapColumnVector(4, DataTypes.LongType)) {
      for (int i = 0; i < 4; i++) {
        delegate.putLong(i, (i + 1) * 100L);
      }
      ColumnVectorWithFilter mappedVector = new ColumnVectorWithFilter(delegate, new int[] {0, 2});

      assertMappedValues(mappedVector, mappedVector::getLong, new Long[] {100L, 300L});
    }
  }

  @Test
  void testDoubleColumnVector() {
    try (WritableColumnVector delegate = new OnHeapColumnVector(3, DataTypes.DoubleType)) {
      delegate.putDouble(0, 1.1);
      delegate.putDouble(1, 2.2);
      delegate.putDouble(2, 3.3);
      ColumnVectorWithFilter mappedVector = new ColumnVectorWithFilter(delegate, new int[] {0, 2});

      assertMappedValues(mappedVector, mappedVector::getDouble, new Double[] {1.1, 3.3});
    }
  }

  @Test
  void testBooleanColumnVector() {
    try (WritableColumnVector delegate = new OnHeapColumnVector(4, DataTypes.BooleanType)) {
      delegate.putBoolean(0, true);
      delegate.putBoolean(1, false);
      delegate.putBoolean(2, true);
      delegate.putBoolean(3, false);
      ColumnVectorWithFilter mappedVector = new ColumnVectorWithFilter(delegate, new int[] {1, 2});

      assertMappedValues(mappedVector, mappedVector::getBoolean, new Boolean[] {false, true});
    }
  }

  @Test
  void testStringColumnVector() {
    try (WritableColumnVector delegate = new OnHeapColumnVector(3, DataTypes.StringType)) {
      delegate.putByteArray(0, "alice".getBytes());
      delegate.putByteArray(1, "bob".getBytes());
      delegate.putByteArray(2, "charlie".getBytes());
      ColumnVectorWithFilter mappedVector = new ColumnVectorWithFilter(delegate, new int[] {2});

      assertMappedValues(
          mappedVector, i -> mappedVector.getUTF8String(i).toString(), new String[] {"charlie"});
    }
  }

  @Test
  void testStructColumnVector() {
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

      ColumnVector mappedIdColumn = mappedVector.getChild(0);
      ColumnVector mappedNameColumn = mappedVector.getChild(1);

      assertMappedValues(mappedIdColumn, mappedIdColumn::getInt, new Integer[] {1, 3});
      assertMappedValues(
          mappedNameColumn,
          i -> mappedNameColumn.getUTF8String(i).toString(),
          new String[] {"a", "c"});
    }
  }

  @Test
  void testIntervalColumnVector() {
    try (WritableColumnVector delegate =
        new OnHeapColumnVector(5, DataTypes.CalendarIntervalType)) {
      for (int i = 0; i < 5; i++) {
        delegate.putInterval(i, new CalendarInterval(i, i * 10, i * 100L));
      }
      ColumnVectorWithFilter mappedVector = new ColumnVectorWithFilter(delegate, new int[] {1, 3});

      CalendarInterval row0 = mappedVector.getInterval(0);
      assertNotNull(row0);
      assertEquals(1, row0.months);
      assertEquals(10, row0.days);
      assertEquals(100L, row0.microseconds);

      CalendarInterval row1 = mappedVector.getInterval(1);
      assertNotNull(row1);
      assertEquals(3, row1.months);
      assertEquals(30, row1.days);
      assertEquals(300L, row1.microseconds);
    }
  }

  @Test
  void testVariantColumnVector() {
    try (WritableColumnVector delegate = new OnHeapColumnVector(5, DataTypes.VariantType)) {
      WritableColumnVector valueChild = (WritableColumnVector) delegate.getChild(0);
      WritableColumnVector metadataChild = (WritableColumnVector) delegate.getChild(1);
      for (int i = 0; i < 5; i++) {
        valueChild.putByteArray(i, ("value-" + i).getBytes());
        metadataChild.putByteArray(i, ("meta-" + i).getBytes());
      }
      ColumnVectorWithFilter mappedVector = new ColumnVectorWithFilter(delegate, new int[] {1, 3});

      VariantVal row0 = mappedVector.getVariant(0);
      assertNotNull(row0);
      assertArrayEquals("value-1".getBytes(), row0.getValue());
      assertArrayEquals("meta-1".getBytes(), row0.getMetadata());

      VariantVal row1 = mappedVector.getVariant(1);
      assertNotNull(row1);
      assertArrayEquals("value-3".getBytes(), row1.getValue());
      assertArrayEquals("meta-3".getBytes(), row1.getMetadata());
    }
  }

  @Test
  void testArrayColumnVector() {
    try (WritableColumnVector delegate =
        new OnHeapColumnVector(3, DataTypes.createArrayType(DataTypes.IntegerType))) {
      // 3 rows: row 0 = [10, 20], row 1 = [30], row 2 = [40, 50, 60].
      // All elements live contiguously in the single int element child.
      WritableColumnVector elementChild = (WritableColumnVector) delegate.getChild(0);
      elementChild.reserve(6);
      int[] flatElements = {10, 20, 30, 40, 50, 60};
      for (int i = 0; i < flatElements.length; i++) {
        elementChild.putInt(i, flatElements[i]);
      }
      delegate.putArray(0, 0, 2);
      delegate.putArray(1, 2, 1);
      delegate.putArray(2, 3, 3);

      // Selected rows: [0, 2] -> survivors are row 0 ([10, 20]) and row 2 ([40, 50, 60]).
      ColumnVectorWithFilter mappedVector = new ColumnVectorWithFilter(delegate, new int[] {0, 2});

      ColumnarArray mappedRow0 = mappedVector.getArray(0);
      assertEquals(2, mappedRow0.numElements());
      assertEquals(10, mappedRow0.getInt(0));
      assertEquals(20, mappedRow0.getInt(1));

      ColumnarArray mappedRow1 = mappedVector.getArray(1);
      assertEquals(3, mappedRow1.numElements());
      assertEquals(40, mappedRow1.getInt(0));
      assertEquals(50, mappedRow1.getInt(1));
      assertEquals(60, mappedRow1.getInt(2));
    }
  }

  @Test
  void testNullColumnVector() {
    try (WritableColumnVector delegate = new OnHeapColumnVector(4, DataTypes.IntegerType)) {
      // original rows: [10, null, 30, null]
      delegate.putInt(0, 10);
      delegate.putNull(1);
      delegate.putInt(2, 30);
      delegate.putNull(3);
      // selected rows (in order): [3, 0, 1] => [null, 10, null]
      ColumnVectorWithFilter mappedVector =
          new ColumnVectorWithFilter(delegate, new int[] {3, 0, 1});

      assertMappedValues(mappedVector, mappedVector::getInt, new Integer[] {null, 10, null});
    }
  }

  @Test
  void testAllNullColumnVector() {
    try (WritableColumnVector delegate = new OnHeapColumnVector(3, DataTypes.IntegerType)) {
      delegate.putNull(0);
      delegate.putNull(1);
      delegate.putNull(2);
      ColumnVectorWithFilter mappedVector = new ColumnVectorWithFilter(delegate, new int[] {2, 0});

      assertMappedValues(mappedVector, mappedVector::getInt, new Integer[] {null, null});
    }
  }

  @Test
  void testNoneSelectColumnVector() {
    try (WritableColumnVector delegate = new OnHeapColumnVector(3, DataTypes.IntegerType)) {
      delegate.putInt(0, 10);
      delegate.putInt(1, 20);
      delegate.putInt(2, 30);

      ColumnVectorWithFilter noRowsSelected = new ColumnVectorWithFilter(delegate, new int[] {});
      assertEquals(DataTypes.IntegerType, noRowsSelected.dataType());
    }
  }

  @Test
  void testAllSelectColumnVector() {
    try (WritableColumnVector delegate = new OnHeapColumnVector(3, DataTypes.IntegerType)) {
      delegate.putInt(0, 10);
      delegate.putInt(1, 20);
      delegate.putInt(2, 30);

      // all select
      ColumnVectorWithFilter allRowsSelected =
          new ColumnVectorWithFilter(delegate, new int[] {0, 1, 2});
      assertMappedValues(allRowsSelected, allRowsSelected::getInt, new Integer[] {10, 20, 30});
    }
  }

  private static <T> void assertMappedValues(
      ColumnVector vector, IntFunction<T> getter, T[] expected) {
    for (int i = 0; i < expected.length; i++) {
      if (expected[i] == null) {
        assertTrue(vector.isNullAt(i), "Expected null at mapped row " + i);
      } else {
        assertFalse(vector.isNullAt(i), "Expected non-null at mapped row " + i);
        assertEquals(expected[i], getter.apply(i), "Mismatch at mapped row " + i);
      }
    }
  }
}
