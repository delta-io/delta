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

import java.util.Arrays;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnVector;
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
    StructType structType =
        new StructType()
            .add("id", DataTypes.IntegerType)
            .add("name", DataTypes.StringType);

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
                }),
        Arguments.of(
            "Struct (getChild propagates mapping)",
            structType,
            3,
            (Consumer<WritableColumnVector>)
                v -> {
                  // child(0) = id: [1, 2, 3]
                  WritableColumnVector idChild = (WritableColumnVector) v.getChild(0);
                  for (int i = 0; i < 3; i++) idChild.putInt(i, i + 1);
                  // child(1) = name: ["a", "b", "c"]
                  WritableColumnVector nameChild = (WritableColumnVector) v.getChild(1);
                  nameChild.putByteArray(0, "a".getBytes());
                  nameChild.putByteArray(1, "b".getBytes());
                  nameChild.putByteArray(2, "c".getBytes());
                },
            new int[] {0, 2},
            (Consumer<ColumnVectorWithFilter>)
                f -> {
                  // getChild should return ColumnVectorWithFilter with the same rowIdMapping
                  ColumnVector idChild = f.getChild(0);
                  ColumnVector nameChild = f.getChild(1);
                  assertTrue(idChild instanceof ColumnVectorWithFilter);
                  assertTrue(nameChild instanceof ColumnVectorWithFilter);
                  // filtered: row 0 -> original 0, row 1 -> original 2
                  assertEquals(1, idChild.getInt(0));
                  assertEquals(3, idChild.getInt(1));
                  assertEquals("a", nameChild.getUTF8String(0).toString());
                  assertEquals("c", nameChild.getUTF8String(1).toString());
                }),
        Arguments.of(
            "Null handling",
            DataTypes.IntegerType,
            4,
            (Consumer<WritableColumnVector>)
                v -> {
                  v.putInt(0, 10);
                  v.putNull(1);
                  v.putInt(2, 30);
                  v.putNull(3);
                },
            new int[] {0, 1, 2, 3},
            (Consumer<ColumnVectorWithFilter>)
                f -> {
                  assertFalse(f.isNullAt(0));
                  assertTrue(f.isNullAt(1));
                  assertFalse(f.isNullAt(2));
                  assertTrue(f.isNullAt(3));
                }),
        Arguments.of(
            "Empty mapping",
            DataTypes.IntegerType,
            3,
            (Consumer<WritableColumnVector>)
                v -> {
                  for (int i = 0; i < 3; i++) v.putInt(i, (i + 1) * 10);
                },
            new int[] {},
            (Consumer<ColumnVectorWithFilter>)
                f -> {
                  assertEquals(DataTypes.IntegerType, f.dataType());
                }),
        Arguments.of(
            "Identity mapping",
            DataTypes.IntegerType,
            3,
            (Consumer<WritableColumnVector>)
                v -> {
                  for (int i = 0; i < 3; i++) v.putInt(i, (i + 1) * 10);
                },
            new int[] {0, 1, 2},
            (Consumer<ColumnVectorWithFilter>)
                f -> {
                  assertEquals(10, f.getInt(0));
                  assertEquals(20, f.getInt(1));
                  assertEquals(30, f.getInt(2));
                }));
  }
}
