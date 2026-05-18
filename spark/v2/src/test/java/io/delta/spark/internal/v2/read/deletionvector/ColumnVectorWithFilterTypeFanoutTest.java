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
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.VariantType;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarArray;
import org.apache.spark.sql.vectorized.ColumnarMap;
import org.apache.spark.unsafe.types.CalendarInterval;
import org.apache.spark.unsafe.types.VariantVal;
import org.junit.jupiter.api.Test;

/**
 * Type-fanout coverage for {@link ColumnVectorWithFilter}.
 *
 * <p>Companion to {@link ColumnVectorWithFilterTest}, which covers Int/Long/Double/Boolean/String
 * /Struct/Null. Each test here exercises a distinct type-sensitive code path on the wrapper:
 *
 * <ul>
 *   <li>Primitive get methods: getByte, getShort, getFloat
 *   <li>Date/Timestamp/TimestampNTZ — distinct DataTypes that share int/long storage paths
 *   <li>Decimal in three storage modes (int / long / byte[])
 *   <li>Binary
 *   <li>Top-level ARRAY/MAP via getArray/getMap
 *   <li>Top-level VARIANT via getVariant() — exercises getChild() through the public contract
 *   <li>Top-level CalendarInterval via getInterval() — exercises getChild() through the public
 *       contract on a non-Struct top-level type
 *   <li>Nested STRUCT&lt;ARRAY&lt;INT&gt;&gt; — getChild() returns a wrapper whose dataType() is
 *       ArrayType; exercises the non-Struct branch in getChild()
 *   <li>Nested ARRAY&lt;STRUCT&lt;INT,STRING&gt;&gt; — top-level array whose elements are structs
 * </ul>
 */
public class ColumnVectorWithFilterTypeFanoutTest {

  // ----------------------------------------------------------------------
  // Primitive types not covered by the original test file.
  // ----------------------------------------------------------------------

  @Test
  void testByteColumnVector() {
    try (WritableColumnVector delegate = new OnHeapColumnVector(4, DataTypes.ByteType)) {
      delegate.putByte(0, (byte) 1);
      delegate.putByte(1, (byte) 2);
      delegate.putByte(2, (byte) 3);
      delegate.putByte(3, (byte) 4);
      ColumnVectorWithFilter mapped = new ColumnVectorWithFilter(delegate, new int[] {3, 0});

      assertMappedValues(mapped, mapped::getByte, new Byte[] {(byte) 4, (byte) 1});
    }
  }

  @Test
  void testShortColumnVector() {
    try (WritableColumnVector delegate = new OnHeapColumnVector(3, DataTypes.ShortType)) {
      delegate.putShort(0, (short) 100);
      delegate.putShort(1, (short) 200);
      delegate.putShort(2, (short) 300);
      ColumnVectorWithFilter mapped = new ColumnVectorWithFilter(delegate, new int[] {2, 1});

      assertMappedValues(mapped, mapped::getShort, new Short[] {(short) 300, (short) 200});
    }
  }

  @Test
  void testFloatColumnVector() {
    try (WritableColumnVector delegate = new OnHeapColumnVector(3, DataTypes.FloatType)) {
      delegate.putFloat(0, 1.5f);
      delegate.putFloat(1, 2.5f);
      delegate.putFloat(2, 3.5f);
      ColumnVectorWithFilter mapped = new ColumnVectorWithFilter(delegate, new int[] {0, 2});

      assertMappedValues(mapped, mapped::getFloat, new Float[] {1.5f, 3.5f});
    }
  }

  // ----------------------------------------------------------------------
  // Logical types backed by int/long storage. Each is a distinct DataType and
  // verifies the wrapper preserves DataType identity (super(delegate.dataType())).
  // ----------------------------------------------------------------------

  @Test
  void testDateColumnVector() {
    // DateType stores days-since-epoch as int.
    try (WritableColumnVector delegate = new OnHeapColumnVector(3, DataTypes.DateType)) {
      delegate.putInt(0, 18000); // 2019-04-14
      delegate.putInt(1, 19000); // 2022-01-08
      delegate.putInt(2, 20000); // 2024-10-04
      ColumnVectorWithFilter mapped = new ColumnVectorWithFilter(delegate, new int[] {2, 0});

      assertEquals(DataTypes.DateType, mapped.dataType());
      assertMappedValues(mapped, mapped::getInt, new Integer[] {20000, 18000});
    }
  }

  @Test
  void testTimestampColumnVector() {
    // TimestampType stores micros-since-epoch as long.
    try (WritableColumnVector delegate = new OnHeapColumnVector(3, DataTypes.TimestampType)) {
      delegate.putLong(0, 1_700_000_000_000_000L);
      delegate.putLong(1, 1_700_000_001_000_000L);
      delegate.putLong(2, 1_700_000_002_000_000L);
      ColumnVectorWithFilter mapped = new ColumnVectorWithFilter(delegate, new int[] {1});

      assertEquals(DataTypes.TimestampType, mapped.dataType());
      assertMappedValues(mapped, mapped::getLong, new Long[] {1_700_000_001_000_000L});
    }
  }

  @Test
  void testTimestampNtzColumnVector() {
    try (WritableColumnVector delegate = new OnHeapColumnVector(3, DataTypes.TimestampNTZType)) {
      delegate.putLong(0, 100L);
      delegate.putLong(1, 200L);
      delegate.putLong(2, 300L);
      ColumnVectorWithFilter mapped = new ColumnVectorWithFilter(delegate, new int[] {2, 1, 0});

      assertEquals(DataTypes.TimestampNTZType, mapped.dataType());
      assertMappedValues(mapped, mapped::getLong, new Long[] {300L, 200L, 100L});
    }
  }

  // ----------------------------------------------------------------------
  // Decimal in all three storage modes.
  // ----------------------------------------------------------------------

  @Test
  void testDecimalShortColumnVector() {
    // precision <= MAX_INT_DIGITS (9) — int-backed.
    DecimalType dt = DataTypes.createDecimalType(5, 2);
    try (WritableColumnVector delegate = new OnHeapColumnVector(3, dt)) {
      delegate.putDecimal(0, Decimal.apply("123.45"), dt.precision());
      delegate.putDecimal(1, Decimal.apply("999.99"), dt.precision());
      delegate.putDecimal(2, Decimal.apply("0.01"), dt.precision());
      ColumnVectorWithFilter mapped = new ColumnVectorWithFilter(delegate, new int[] {2, 0});

      assertMappedValues(
          mapped,
          i -> mapped.getDecimal(i, dt.precision(), dt.scale()).toString(),
          new String[] {"0.01", "123.45"});
    }
  }

  @Test
  void testDecimalLongColumnVector() {
    // MAX_INT_DIGITS < precision <= MAX_LONG_DIGITS (18) — long-backed.
    DecimalType dt = DataTypes.createDecimalType(15, 4);
    try (WritableColumnVector delegate = new OnHeapColumnVector(3, dt)) {
      delegate.putDecimal(0, Decimal.apply("12345.6789"), dt.precision());
      delegate.putDecimal(1, Decimal.apply("99999999999.9999"), dt.precision());
      delegate.putDecimal(2, Decimal.apply("0.0001"), dt.precision());
      ColumnVectorWithFilter mapped = new ColumnVectorWithFilter(delegate, new int[] {1, 2});

      assertMappedValues(
          mapped,
          i -> mapped.getDecimal(i, dt.precision(), dt.scale()).toString(),
          new String[] {"99999999999.9999", "0.0001"});
    }
  }

  @Test
  void testDecimalByteArrayColumnVector() {
    // precision > MAX_LONG_DIGITS (18) — byte[] backed.
    DecimalType dt = DataTypes.createDecimalType(30, 6);
    try (WritableColumnVector delegate = new OnHeapColumnVector(3, dt)) {
      delegate.putDecimal(0, Decimal.apply("123456789012345678.123456"), dt.precision());
      delegate.putDecimal(1, Decimal.apply("0.000001"), dt.precision());
      delegate.putDecimal(2, Decimal.apply("999999999999999999.999999"), dt.precision());
      ColumnVectorWithFilter mapped = new ColumnVectorWithFilter(delegate, new int[] {0, 2});

      assertMappedValues(
          mapped,
          i -> mapped.getDecimal(i, dt.precision(), dt.scale()).toString(),
          new String[] {"123456789012345678.123456", "999999999999999999.999999"});
    }
  }

  // ----------------------------------------------------------------------
  // Binary.
  // ----------------------------------------------------------------------

  @Test
  void testBinaryColumnVector() {
    try (WritableColumnVector delegate = new OnHeapColumnVector(3, DataTypes.BinaryType)) {
      delegate.putByteArray(0, new byte[] {1, 2, 3});
      delegate.putByteArray(1, new byte[] {4, 5});
      delegate.putByteArray(2, new byte[] {6});
      ColumnVectorWithFilter mapped = new ColumnVectorWithFilter(delegate, new int[] {2, 0});

      assertFalse(mapped.isNullAt(0));
      assertArrayEquals(new byte[] {6}, mapped.getBinary(0));
      assertFalse(mapped.isNullAt(1));
      assertArrayEquals(new byte[] {1, 2, 3}, mapped.getBinary(1));
    }
  }

  // ----------------------------------------------------------------------
  // Top-level complex types. These exercise getArray/getMap forwarders.
  // ----------------------------------------------------------------------

  @Test
  void testTopLevelArrayColumnVector() {
    DataType arrayType = DataTypes.createArrayType(DataTypes.IntegerType);
    try (WritableColumnVector delegate = new OnHeapColumnVector(3, arrayType)) {
      // row 0: [10, 20]
      ((WritableColumnVector) delegate.arrayData()).appendInt(10);
      ((WritableColumnVector) delegate.arrayData()).appendInt(20);
      delegate.putArray(0, 0, 2);
      // row 1: [30]
      ((WritableColumnVector) delegate.arrayData()).appendInt(30);
      delegate.putArray(1, 2, 1);
      // row 2: []
      delegate.putArray(2, 3, 0);

      ColumnVectorWithFilter mapped = new ColumnVectorWithFilter(delegate, new int[] {2, 0});
      assertEquals(arrayType, mapped.dataType());

      ColumnarArray a0 = mapped.getArray(0);
      assertEquals(0, a0.numElements());
      ColumnarArray a1 = mapped.getArray(1);
      assertEquals(2, a1.numElements());
      assertEquals(10, a1.getInt(0));
      assertEquals(20, a1.getInt(1));
    }
  }

  @Test
  void testTopLevelMapColumnVector() {
    DataType mapType = DataTypes.createMapType(DataTypes.StringType, DataTypes.IntegerType);
    try (WritableColumnVector delegate = new OnHeapColumnVector(2, mapType)) {
      WritableColumnVector keys = (WritableColumnVector) delegate.getChild(0);
      WritableColumnVector vals = (WritableColumnVector) delegate.getChild(1);
      // row 0: {"a"->1, "b"->2}
      keys.appendByteArray("a".getBytes(), 0, 1);
      keys.appendByteArray("b".getBytes(), 0, 1);
      vals.appendInt(1);
      vals.appendInt(2);
      delegate.putArray(0, 0, 2);
      // row 1: {"c"->3}
      keys.appendByteArray("c".getBytes(), 0, 1);
      vals.appendInt(3);
      delegate.putArray(1, 2, 1);

      ColumnVectorWithFilter mapped = new ColumnVectorWithFilter(delegate, new int[] {1, 0});
      assertEquals(mapType, mapped.dataType());

      ColumnarMap m0 = mapped.getMap(0);
      assertEquals(1, m0.numElements());
      assertEquals("c", m0.keyArray().getUTF8String(0).toString());
      assertEquals(3, m0.valueArray().getInt(0));

      ColumnarMap m1 = mapped.getMap(1);
      assertEquals(2, m1.numElements());
      assertEquals("a", m1.keyArray().getUTF8String(0).toString());
      assertEquals(1, m1.valueArray().getInt(0));
      assertEquals("b", m1.keyArray().getUTF8String(1).toString());
      assertEquals(2, m1.valueArray().getInt(1));
    }
  }

  // ----------------------------------------------------------------------
  // Top-level VARIANT — exercises getChild() through getVariant() public API.
  // VariantType is non-Struct but has 2 binary children. Pre-fix code crashed
  // here with ClassCastException: VariantType cannot be cast to StructType.
  // ----------------------------------------------------------------------

  @Test
  void testTopLevelVariantColumnVector() {
    VariantType variantType = (VariantType) DataTypes.VariantType;
    try (WritableColumnVector delegate = new OnHeapColumnVector(3, variantType)) {
      WritableColumnVector valueChild = (WritableColumnVector) delegate.getChild(0);
      WritableColumnVector metadataChild = (WritableColumnVector) delegate.getChild(1);
      // Three rows. The actual byte payload doesn't need to be valid Variant
      // for this test — we only verify the wrapper plumbs getChild correctly.
      for (int i = 0; i < 3; i++) {
        valueChild.putByteArray(i, new byte[] {(byte) (10 + i)});
        metadataChild.putByteArray(i, new byte[] {(byte) (100 + i)});
      }

      ColumnVectorWithFilter mapped = new ColumnVectorWithFilter(delegate, new int[] {2, 0});
      assertEquals(variantType, mapped.dataType());

      VariantVal v0 = mapped.getVariant(0);
      assertNotNull(v0);
      assertArrayEquals(new byte[] {12}, v0.getValue());
      assertArrayEquals(new byte[] {102}, v0.getMetadata());

      VariantVal v1 = mapped.getVariant(1);
      assertNotNull(v1);
      assertArrayEquals(new byte[] {10}, v1.getValue());
      assertArrayEquals(new byte[] {100}, v1.getMetadata());
    }
  }

  // ----------------------------------------------------------------------
  // Top-level CalendarInterval — exercises getChild() through getInterval().
  // CalendarIntervalType is non-Struct with 3 children. Pre-fix code crashed
  // with ClassCastException: CalendarIntervalType cannot be cast to StructType.
  // ----------------------------------------------------------------------

  @Test
  void testTopLevelCalendarIntervalColumnVector() {
    try (WritableColumnVector delegate =
        new OnHeapColumnVector(3, DataTypes.CalendarIntervalType)) {
      delegate.putInterval(0, new CalendarInterval(1, 2, 3L));
      delegate.putInterval(1, new CalendarInterval(4, 5, 6L));
      delegate.putInterval(2, new CalendarInterval(7, 8, 9L));

      ColumnVectorWithFilter mapped = new ColumnVectorWithFilter(delegate, new int[] {1, 2});

      CalendarInterval i0 = mapped.getInterval(0);
      assertEquals(4, i0.months);
      assertEquals(5, i0.days);
      assertEquals(6L, i0.microseconds);

      CalendarInterval i1 = mapped.getInterval(1);
      assertEquals(7, i1.months);
      assertEquals(8, i1.days);
      assertEquals(9L, i1.microseconds);
    }
  }

  // ----------------------------------------------------------------------
  // Nested combos.
  // ----------------------------------------------------------------------

  @Test
  void testStructWithArrayFieldColumnVector() {
    // STRUCT<id:INT, tags:ARRAY<INT>>. Calling getChild(1) returns a wrapper whose
    // dataType() is ArrayType; that wrapper's getArray() must still work, and
    // calling getChild() on it must NOT crash with ClassCastException (this is
    // the inner-non-Struct branch added by PR #6578).
    DataType arrayType = DataTypes.createArrayType(DataTypes.IntegerType);
    StructType structType =
        new StructType().add("id", DataTypes.IntegerType).add("tags", arrayType);
    try (WritableColumnVector delegate = new OnHeapColumnVector(3, structType)) {
      WritableColumnVector idChild = (WritableColumnVector) delegate.getChild(0);
      WritableColumnVector tagsChild = (WritableColumnVector) delegate.getChild(1);
      WritableColumnVector tagsData = (WritableColumnVector) tagsChild.arrayData();
      for (int i = 0; i < 3; i++) {
        idChild.putInt(i, i + 1);
      }
      // row 0: tags = [10, 20]
      tagsData.appendInt(10);
      tagsData.appendInt(20);
      tagsChild.putArray(0, 0, 2);
      // row 1: tags = [30]
      tagsData.appendInt(30);
      tagsChild.putArray(1, 2, 1);
      // row 2: tags = []
      tagsChild.putArray(2, 3, 0);

      ColumnVectorWithFilter mapped = new ColumnVectorWithFilter(delegate, new int[] {0, 2});
      ColumnVector idCol = mapped.getChild(0);
      ColumnVector tagsCol = mapped.getChild(1);

      assertEquals(arrayType, tagsCol.dataType());
      assertMappedValues(idCol, idCol::getInt, new Integer[] {1, 3});

      ColumnarArray tags0 = tagsCol.getArray(0);
      assertEquals(2, tags0.numElements());
      assertEquals(10, tags0.getInt(0));
      assertEquals(20, tags0.getInt(1));
      ColumnarArray tags1 = tagsCol.getArray(1);
      assertEquals(0, tags1.numElements());
    }
  }

  @Test
  void testStructWithMapFieldColumnVector() {
    // STRUCT<id:INT, m:MAP<STRING,INT>>. Same shape as above but for MapType.
    DataType mapType = DataTypes.createMapType(DataTypes.StringType, DataTypes.IntegerType);
    StructType structType = new StructType().add("id", DataTypes.IntegerType).add("m", mapType);
    try (WritableColumnVector delegate = new OnHeapColumnVector(2, structType)) {
      WritableColumnVector idChild = (WritableColumnVector) delegate.getChild(0);
      WritableColumnVector mChild = (WritableColumnVector) delegate.getChild(1);
      WritableColumnVector keys = (WritableColumnVector) mChild.getChild(0);
      WritableColumnVector vals = (WritableColumnVector) mChild.getChild(1);
      idChild.putInt(0, 1);
      idChild.putInt(1, 2);
      keys.appendByteArray("k0".getBytes(), 0, 2);
      vals.appendInt(100);
      mChild.putArray(0, 0, 1);
      keys.appendByteArray("k1".getBytes(), 0, 2);
      keys.appendByteArray("k2".getBytes(), 0, 2);
      vals.appendInt(200);
      vals.appendInt(300);
      mChild.putArray(1, 1, 2);

      ColumnVectorWithFilter mapped = new ColumnVectorWithFilter(delegate, new int[] {1, 0});
      ColumnVector mCol = mapped.getChild(1);
      assertEquals(mapType, mCol.dataType());

      ColumnarMap m0 = mCol.getMap(0);
      assertEquals(2, m0.numElements());
      assertEquals("k1", m0.keyArray().getUTF8String(0).toString());
      assertEquals(200, m0.valueArray().getInt(0));
    }
  }

  @Test
  void testStructWithVariantFieldColumnVector() {
    // STRUCT<id:INT, v:VARIANT>. Calling getChild(1) returns a wrapper whose
    // dataType() is VariantType. Pre-fix code would crash here when downstream
    // code calls getChild(0) / getChild(1) on the inner wrapper to assemble a
    // VariantVal.
    StructType structType =
        new StructType().add("id", DataTypes.IntegerType).add("v", DataTypes.VariantType);
    try (WritableColumnVector delegate = new OnHeapColumnVector(2, structType)) {
      WritableColumnVector idChild = (WritableColumnVector) delegate.getChild(0);
      WritableColumnVector vChild = (WritableColumnVector) delegate.getChild(1);
      idChild.putInt(0, 1);
      idChild.putInt(1, 2);
      ((WritableColumnVector) vChild.getChild(0)).putByteArray(0, new byte[] {1});
      ((WritableColumnVector) vChild.getChild(1)).putByteArray(0, new byte[] {2});
      ((WritableColumnVector) vChild.getChild(0)).putByteArray(1, new byte[] {3});
      ((WritableColumnVector) vChild.getChild(1)).putByteArray(1, new byte[] {4});

      ColumnVectorWithFilter mapped = new ColumnVectorWithFilter(delegate, new int[] {1, 0});
      ColumnVector vCol = mapped.getChild(1);

      // Calling getVariant on the inner wrapper must NOT crash.
      VariantVal v0 = vCol.getVariant(0);
      assertNotNull(v0);
      assertArrayEquals(new byte[] {3}, v0.getValue());
      assertArrayEquals(new byte[] {4}, v0.getMetadata());

      VariantVal v1 = vCol.getVariant(1);
      assertArrayEquals(new byte[] {1}, v1.getValue());
      assertArrayEquals(new byte[] {2}, v1.getMetadata());
    }
  }

  @Test
  void testArrayOfStructColumnVector() {
    // ARRAY<STRUCT<id:INT, name:STRING>>. ColumnarArray exposes elements via the
    // underlying data ColumnVector, which is itself a wrapper of the struct
    // child column. Verifies getArray() returns a ColumnarArray that reads from
    // the struct's children correctly under the row-id mapping.
    StructType elementType =
        new StructType().add("id", DataTypes.IntegerType).add("name", DataTypes.StringType);
    DataType arrayType = DataTypes.createArrayType(elementType);
    try (WritableColumnVector delegate = new OnHeapColumnVector(2, arrayType)) {
      WritableColumnVector elementData = (WritableColumnVector) delegate.arrayData();
      WritableColumnVector idChild = (WritableColumnVector) elementData.getChild(0);
      WritableColumnVector nameChild = (WritableColumnVector) elementData.getChild(1);
      // row 0: [{1,"a"},{2,"b"}]
      idChild.appendInt(1);
      idChild.appendInt(2);
      nameChild.appendByteArray("a".getBytes(), 0, 1);
      nameChild.appendByteArray("b".getBytes(), 0, 1);
      elementData.appendStruct(false);
      elementData.appendStruct(false);
      delegate.putArray(0, 0, 2);
      // row 1: [{3,"c"}]
      idChild.appendInt(3);
      nameChild.appendByteArray("c".getBytes(), 0, 1);
      elementData.appendStruct(false);
      delegate.putArray(1, 2, 1);

      ColumnVectorWithFilter mapped = new ColumnVectorWithFilter(delegate, new int[] {1, 0});
      assertEquals(arrayType, mapped.dataType());

      ColumnarArray a0 = mapped.getArray(0);
      assertEquals(1, a0.numElements());
      assertEquals(3, a0.getStruct(0, 2).getInt(0));
      assertEquals("c", a0.getStruct(0, 2).getUTF8String(1).toString());

      ColumnarArray a1 = mapped.getArray(1);
      assertEquals(2, a1.numElements());
      assertEquals(1, a1.getStruct(0, 2).getInt(0));
      assertEquals("a", a1.getStruct(0, 2).getUTF8String(1).toString());
      assertEquals(2, a1.getStruct(1, 2).getInt(0));
      assertEquals("b", a1.getStruct(1, 2).getUTF8String(1).toString());
    }
  }

  // ----------------------------------------------------------------------
  // All-null variants for the new types covered above.
  // ----------------------------------------------------------------------

  @Test
  void testAllNullDateColumnVector() {
    try (WritableColumnVector delegate = new OnHeapColumnVector(3, DataTypes.DateType)) {
      delegate.putNull(0);
      delegate.putNull(1);
      delegate.putNull(2);
      ColumnVectorWithFilter mapped = new ColumnVectorWithFilter(delegate, new int[] {2, 1});
      assertMappedValues(mapped, mapped::getInt, new Integer[] {null, null});
    }
  }

  @Test
  void testAllNullDecimalColumnVector() {
    DecimalType dt = DataTypes.createDecimalType(15, 4);
    try (WritableColumnVector delegate = new OnHeapColumnVector(3, dt)) {
      delegate.putNull(0);
      delegate.putNull(1);
      delegate.putNull(2);
      ColumnVectorWithFilter mapped = new ColumnVectorWithFilter(delegate, new int[] {0, 2});
      for (int i = 0; i < 2; i++) {
        assertTrue(mapped.isNullAt(i));
        assertNull(mapped.getDecimal(i, dt.precision(), dt.scale()));
      }
    }
  }

  @Test
  void testAllNullBinaryColumnVector() {
    try (WritableColumnVector delegate = new OnHeapColumnVector(2, DataTypes.BinaryType)) {
      delegate.putNull(0);
      delegate.putNull(1);
      ColumnVectorWithFilter mapped = new ColumnVectorWithFilter(delegate, new int[] {1, 0});
      assertTrue(mapped.isNullAt(0));
      assertTrue(mapped.isNullAt(1));
    }
  }

  @Test
  void testAllNullVariantColumnVector() {
    try (WritableColumnVector delegate = new OnHeapColumnVector(2, DataTypes.VariantType)) {
      delegate.appendStruct(true);
      delegate.appendStruct(true);
      ColumnVectorWithFilter mapped = new ColumnVectorWithFilter(delegate, new int[] {0, 1});
      assertNull(mapped.getVariant(0));
      assertNull(mapped.getVariant(1));
    }
  }

  @Test
  void testAllNullIntervalColumnVector() {
    try (WritableColumnVector delegate =
        new OnHeapColumnVector(2, DataTypes.CalendarIntervalType)) {
      delegate.putNull(0);
      delegate.putNull(1);
      ColumnVectorWithFilter mapped = new ColumnVectorWithFilter(delegate, new int[] {0, 1});
      assertNull(mapped.getInterval(0));
      assertNull(mapped.getInterval(1));
    }
  }

  // ----------------------------------------------------------------------
  // Helper duplicated from ColumnVectorWithFilterTest — kept private here so
  // the two test classes can evolve independently. Same shape as the original.
  // ----------------------------------------------------------------------

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
