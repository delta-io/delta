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

package io.delta.spark.dsv2.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.delta.kernel.types.ArrayType;
import io.delta.kernel.types.BinaryType;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.ByteType;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.DateType;
import io.delta.kernel.types.DecimalType;
import io.delta.kernel.types.DoubleType;
import io.delta.kernel.types.FloatType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.MapType;
import io.delta.kernel.types.ShortType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;
import io.delta.kernel.types.TimestampNTZType;
import io.delta.kernel.types.TimestampType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.junit.jupiter.api.Test;

public class SchemaUtilsTest {

  @Test
  public void testPrimitiveTypes() {
    checkConversion(DataTypes.StringType, StringType.STRING);
    checkConversion(DataTypes.BooleanType, BooleanType.BOOLEAN);
    checkConversion(DataTypes.IntegerType, IntegerType.INTEGER);
    checkConversion(DataTypes.LongType, LongType.LONG);
    checkConversion(DataTypes.BinaryType, BinaryType.BINARY);
    checkConversion(DataTypes.ByteType, ByteType.BYTE);
    checkConversion(DataTypes.DateType, DateType.DATE);
    checkConversion(DataTypes.createDecimalType(10, 2), new DecimalType(10, 2));
    checkConversion(DataTypes.DoubleType, DoubleType.DOUBLE);
    checkConversion(DataTypes.FloatType, FloatType.FLOAT);
    checkConversion(DataTypes.ShortType, ShortType.SHORT);
    checkConversion(DataTypes.TimestampType, TimestampType.TIMESTAMP);
    checkConversion(DataTypes.TimestampNTZType, TimestampNTZType.TIMESTAMP_NTZ);
  }

  @Test
  public void testArrayType() {
    checkConversion(
        DataTypes.createArrayType(DataTypes.IntegerType, true /* containsNull */),
        new ArrayType(IntegerType.INTEGER, true /* containsNull */));
    checkConversion(
        DataTypes.createArrayType(DataTypes.StringType, false /* containsNull */),
        new ArrayType(StringType.STRING, false /* containsNull */));
  }

  @Test
  public void testMapType() {
    checkConversion(
        DataTypes.createMapType(
            DataTypes.StringType, DataTypes.IntegerType, true /* valueContainsNull */),
        new MapType(StringType.STRING, IntegerType.INTEGER, true /* valueContainsNull */));
    checkConversion(
        DataTypes.createMapType(
            DataTypes.LongType, DataTypes.BooleanType, false /* valueContainsNull */),
        new MapType(LongType.LONG, BooleanType.BOOLEAN, false /* valueContainsNull */));
  }

  @Test
  public void testStructType() {
    org.apache.spark.sql.types.StructType sparkStruct =
        DataTypes.createStructType(
            new StructField[] {
              DataTypes.createStructField("a", DataTypes.IntegerType, true /* nullable */),
              DataTypes.createStructField("b", DataTypes.StringType, false /* nullable */)
            });
    StructType kernelStruct =
        new StructType()
            .add("a", IntegerType.INTEGER, true /* nullable */)
            .add("b", StringType.STRING, false /* nullable */);

    checkConversion(sparkStruct, kernelStruct);
  }

  @Test
  public void testNestedTypes() {
    org.apache.spark.sql.types.StructType sparkStruct =
        DataTypes.createStructType(
            new StructField[] {
              DataTypes.createStructField(
                  "a",
                  DataTypes.createArrayType(DataTypes.IntegerType, true /* containsNull */),
                  true /* nullable */),
              DataTypes.createStructField(
                  "b",
                  DataTypes.createMapType(
                      DataTypes.StringType, DataTypes.BooleanType, false /* valueContainsNull */),
                  false /* nullable */)
            });
    StructType kernelStruct =
        new StructType()
            .add(
                "a",
                new ArrayType(IntegerType.INTEGER, true /* containsNull */),
                true /* nullable */)
            .add(
                "b",
                new MapType(StringType.STRING, BooleanType.BOOLEAN, false /* valueContainsNull */),
                false /* nullable */);

    checkConversion(sparkStruct, kernelStruct);
  }

  private void checkConversion(
      org.apache.spark.sql.types.DataType sparkDataType, DataType kernelDataType) {
    DataType toKernel = SchemaUtils.convertSparkDataTypeToKernelDataType(sparkDataType);
    assertEquals(kernelDataType, toKernel);
    org.apache.spark.sql.types.DataType toSpark =
        SchemaUtils.convertKernelDataTypeToSparkDataType(kernelDataType);
    assertEquals(sparkDataType, toSpark);
  }
}
