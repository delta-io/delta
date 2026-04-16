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
package io.delta.spark.internal.v2.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.spark.sql.execution.vectorized.ConstantColumnVector;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.jupiter.api.Test;

public class ConstantColumnVectorsTest {

  @Test
  void ofUtf8String_returnsConstantUtf8AcrossAllRows() {
    ConstantColumnVector v = ConstantColumnVectors.ofUtf8String("insert", 3);
    assertEquals(DataTypes.StringType, v.dataType());
    UTF8String expected = UTF8String.fromString("insert");
    assertEquals(expected, v.getUTF8String(0));
    assertEquals(expected, v.getUTF8String(1));
    assertEquals(expected, v.getUTF8String(2));
  }

  @Test
  void ofLong_returnsConstantLongAcrossAllRows() {
    ConstantColumnVector v = ConstantColumnVectors.ofLong(42L, 2);
    assertEquals(DataTypes.LongType, v.dataType());
    assertEquals(42L, v.getLong(0));
    assertEquals(42L, v.getLong(1));
  }

  @Test
  void ofTimestampMicros_storesMicrosAsLongWithTimestampType() {
    long micros = 1_700_000_000_000_000L;
    ConstantColumnVector v = ConstantColumnVectors.ofTimestampMicros(micros, 2);
    assertEquals(DataTypes.TimestampType, v.dataType());
    assertEquals(micros, v.getLong(0));
    assertEquals(micros, v.getLong(1));
  }
}
