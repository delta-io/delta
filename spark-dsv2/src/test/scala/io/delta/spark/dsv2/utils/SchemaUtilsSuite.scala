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
package io.delta.spark.dsv2.utils

import io.delta.kernel.types.{ArrayType => KernelArrayType, BinaryType => KernelBinaryType, BooleanType => KernelBooleanType, ByteType => KernelByteType, DataType => KernelDataType, DateType => KernelDateType, DecimalType => KernelDecimalType, DoubleType => KernelDoubleType, FloatType => KernelFloatType, IntegerType => KernelIntegerType, LongType => KernelLongType, MapType => KernelMapType, ShortType => KernelShortType, StringType => KernelStringType, StructType => KernelStructType, TimestampNTZType => KernelTimestampNTZType, TimestampType => KernelTimestampType}
import org.apache.spark.sql.types.{ArrayType => SparkArrayType, BinaryType => SparkBinaryType, BooleanType => SparkBooleanType, ByteType => SparkByteType, DataType => SparkDataType, DateType => SparkDateType, DecimalType => SparkDecimalType, DoubleType => SparkDoubleType, FloatType => SparkFloatType, IntegerType => SparkIntegerType, LongType => SparkLongType, MapType => SparkMapType, ShortType => SparkShortType, StringType => SparkStringType, StructField => SparkStructField, StructType => SparkStructType, TimestampNTZType => SparkTimestampNTZType, TimestampType => SparkTimestampType}
import org.scalatest.funsuite.AnyFunSuite

class SchemaUtilsSuite extends AnyFunSuite {

  private def checkConversion(
      sparkDataType: SparkDataType,
      kernelDataType: KernelDataType): Unit = {
    val toKernel = SchemaUtils.convertSparkDataTypeToKernelDataType(sparkDataType)
    assert(toKernel == kernelDataType)
    val toSpark = SchemaUtils.convertKernelDataTypeToSparkDataType(kernelDataType)
    assert(toSpark == sparkDataType)
  }

  test("primitive types") {
    checkConversion(SparkStringType, KernelStringType.STRING)
    checkConversion(SparkBooleanType, KernelBooleanType.BOOLEAN)
    checkConversion(SparkIntegerType, KernelIntegerType.INTEGER)
    checkConversion(SparkLongType, KernelLongType.LONG)
    checkConversion(SparkBinaryType, KernelBinaryType.BINARY)
    checkConversion(SparkByteType, KernelByteType.BYTE)
    checkConversion(SparkDateType, KernelDateType.DATE)
    checkConversion(SparkDecimalType(10, 2), new KernelDecimalType(10, 2))
    checkConversion(SparkDoubleType, KernelDoubleType.DOUBLE)
    checkConversion(SparkFloatType, KernelFloatType.FLOAT)
    checkConversion(SparkShortType, KernelShortType.SHORT)
    checkConversion(SparkTimestampType, KernelTimestampType.TIMESTAMP)
    checkConversion(SparkTimestampNTZType, KernelTimestampNTZType.TIMESTAMP_NTZ)
  }

  test("array type") {
    checkConversion(
      SparkArrayType(SparkIntegerType, true),
      new KernelArrayType(KernelIntegerType.INTEGER, true))
    checkConversion(
      SparkArrayType(SparkStringType, false),
      new KernelArrayType(KernelStringType.STRING, false))
  }

  test("map type") {
    checkConversion(
      SparkMapType(SparkStringType, SparkIntegerType, true),
      new KernelMapType(KernelStringType.STRING, KernelIntegerType.INTEGER, true))
    checkConversion(
      SparkMapType(SparkLongType, SparkBooleanType, false),
      new KernelMapType(KernelLongType.LONG, KernelBooleanType.BOOLEAN, false))
  }

  test("struct type") {
    val sparkStruct = SparkStructType(Seq(
      SparkStructField("a", SparkIntegerType, true),
      SparkStructField("b", SparkStringType, false)))
    val kernelStruct = new KernelStructType()
      .add("a", KernelIntegerType.INTEGER, true)
      .add("b", KernelStringType.STRING, false)

    checkConversion(sparkStruct, kernelStruct)
  }

  test("nested types") {
    val sparkStruct = SparkStructType(Seq(
      SparkStructField("a", SparkArrayType(SparkIntegerType, true), true),
      SparkStructField("b", SparkMapType(SparkStringType, SparkBooleanType, false), false)))
    val kernelStruct = new KernelStructType()
      .add("a", new KernelArrayType(KernelIntegerType.INTEGER, true), true)
      .add("b", new KernelMapType(KernelStringType.STRING, KernelBooleanType.BOOLEAN, false), false)

    checkConversion(sparkStruct, kernelStruct)
  }

  test("unsupported type") {
    val e1 = intercept[IllegalArgumentException] {
      SchemaUtils.convertSparkDataTypeToKernelDataType(null)
    }
    assert(e1.getMessage.contains("unsupported data type null"))

    val e2 = intercept[IllegalArgumentException] {
      SchemaUtils.convertKernelDataTypeToSparkDataType(null)
    }
    assert(e2.getMessage.contains("unsupported data type null"))
  }
} 