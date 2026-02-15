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

import java.util.function.Consumer;
import java.util.stream.Stream;
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class ColumnVectorWithFilterTest {

  @ParameterizedTest(name = "{0}")
  @MethodSource("dataTypeFilteringCases")
  void testFilteringByType(
      String typeName,
      DataType dataType,
      int vectorSize,
      Consumer<WritableColumnVector> populate,
      int[] filterIndices,
      Consumer<ColumnVectorWithFilter> verify) {
    try (WritableColumnVector delegate = new OnHeapColumnVector(vectorSize, dataType)) {
      populate.accept(delegate);
      ColumnVectorWithFilter filtered = new ColumnVectorWithFilter(delegate, filterIndices);
      verify.accept(filtered);
    }
  }

  static Stream<Arguments> dataTypeFilteringCases() {
    return Stream.of(
        Arguments.of(
            "Integer",
            DataTypes.IntegerType,
            5,
            (Consumer<WritableColumnVector>)
                v -> {
                  for (int i = 0; i < 5; i++) v.putInt(i, (i + 1) * 10); // [10,20,30,40,50]
                },
            new int[] {1, 3},
            (Consumer<ColumnVectorWithFilter>)
                f -> {
                  assertEquals(20, f.getInt(0));
                  assertEquals(40, f.getInt(1));
                }),
        Arguments.of(
            "Long",
            DataTypes.LongType,
            4,
            (Consumer<WritableColumnVector>)
                v -> {
                  for (int i = 0; i < 4; i++) v.putLong(i, (i + 1) * 100L);
                },
            new int[] {0, 2},
            (Consumer<ColumnVectorWithFilter>)
                f -> {
                  assertEquals(100L, f.getLong(0));
                  assertEquals(300L, f.getLong(1));
                }),
        Arguments.of(
            "Double",
            DataTypes.DoubleType,
            3,
            (Consumer<WritableColumnVector>)
                v -> {
                  v.putDouble(0, 1.1);
                  v.putDouble(1, 2.2);
                  v.putDouble(2, 3.3);
                },
            new int[] {0, 2},
            (Consumer<ColumnVectorWithFilter>)
                f -> {
                  assertEquals(1.1, f.getDouble(0), 0.001);
                  assertEquals(3.3, f.getDouble(1), 0.001);
                }),
        Arguments.of(
            "Boolean",
            DataTypes.BooleanType,
            4,
            (Consumer<WritableColumnVector>)
                v -> {
                  v.putBoolean(0, true);
                  v.putBoolean(1, false);
                  v.putBoolean(2, true);
                  v.putBoolean(3, false);
                },
            new int[] {1, 2},
            (Consumer<ColumnVectorWithFilter>)
                f -> {
                  assertFalse(f.getBoolean(0));
                  assertTrue(f.getBoolean(1));
                }),
        Arguments.of(
            "String",
            DataTypes.StringType,
            3,
            (Consumer<WritableColumnVector>)
                v -> {
                  v.putByteArray(0, "alice".getBytes());
                  v.putByteArray(1, "bob".getBytes());
                  v.putByteArray(2, "charlie".getBytes());
                },
            new int[] {2},
            (Consumer<ColumnVectorWithFilter>)
                f -> {
                  assertEquals("charlie", f.getUTF8String(0).toString());
                }));
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
