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

import org.apache.spark.sql.execution.vectorized.ConstantColumnVector;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.unsafe.types.UTF8String;

/** Factories for {@link ConstantColumnVector} instances of common Spark SQL types. */
public final class ConstantColumnVectors {

  private ConstantColumnVectors() {}

  public static ConstantColumnVector ofUtf8String(String value, int numRows) {
    ConstantColumnVector vector = new ConstantColumnVector(numRows, DataTypes.StringType);
    vector.setUtf8String(UTF8String.fromString(value));
    return vector;
  }

  public static ConstantColumnVector ofLong(long value, int numRows) {
    ConstantColumnVector vector = new ConstantColumnVector(numRows, DataTypes.LongType);
    vector.setLong(value);
    return vector;
  }

  /** TimestampType is stored as long microseconds. */
  public static ConstantColumnVector ofTimestampMicros(long micros, int numRows) {
    ConstantColumnVector vector = new ConstantColumnVector(numRows, DataTypes.TimestampType);
    vector.setLong(micros);
    return vector;
  }
}
