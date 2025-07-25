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

import scala.collection.JavaConverters._

import io.delta.kernel.types.{ArrayType => KernelArrayType, BinaryType => KernelBinaryType, BooleanType => KernelBooleanType, ByteType => KernelByteType, DataType => KernelDataType, DateType => KernelDateType, DecimalType => KernelDecimalType, DoubleType => KernelDoubleType, FloatType => KernelFloatType, IntegerType => KernelIntegerType, LongType => KernelLongType, MapType => KernelMapType, ShortType => KernelShortType, StringType => KernelStringType, StructField => KernelStructField, StructType => KernelStructType, TimestampNTZType => KernelTimestampNTZType, TimestampType => KernelTimestampType}

import org.apache.spark.sql.types.{ArrayType => SparkArrayType, BinaryType => SparkBinaryType, BooleanType => SparkBooleanType, ByteType => SparkByteType, DataType => SparkDataType, DateType => SparkDateType, DecimalType => SparkDecimalType, DoubleType => SparkDoubleType, FloatType => SparkFloatType, IntegerType => SparkIntegerType, LongType => SparkLongType, MapType => SparkMapType, ShortType => SparkShortType, StringType => SparkStringType, StructField => SparkStructField, StructType => SparkStructType, TimestampNTZType => SparkTimestampNTZType, TimestampType => SparkTimestampType}

object SchemaUtils {

  //////////////////////
  // Kernel --> Spark //
  //////////////////////

  def convertKernelSchemaToSparkSchema(kernelSchema: KernelStructType): SparkStructType = {
    SparkStructType(kernelSchema.fields().asScala.map(convertKernelFieldToSparkField).toSeq)
  }

  def convertKernelFieldToSparkField(kernelField: KernelStructField): SparkStructField = {
    SparkStructField(
      kernelField.getName,
      convertKernelDataTypeToSparkDataType(kernelField.getDataType),
      kernelField.isNullable)
  }

  def convertKernelDataTypeToSparkDataType(kernelDataType: KernelDataType): SparkDataType = {
    kernelDataType match {
      case _: KernelStringType => SparkStringType
      case _: KernelBooleanType => SparkBooleanType
      case _: KernelIntegerType => SparkIntegerType
      case _: KernelLongType => SparkLongType
      case _: KernelBinaryType => SparkBinaryType
      case _: KernelByteType => SparkByteType
      case _: KernelDateType => SparkDateType
      case kernelDecimal: KernelDecimalType =>
        SparkDecimalType(kernelDecimal.getPrecision, kernelDecimal.getScale)
      case _: KernelDoubleType => SparkDoubleType
      case _: KernelFloatType => SparkFloatType
      case _: KernelShortType => SparkShortType
      case _: KernelTimestampType => SparkTimestampType
      case _: KernelTimestampNTZType => SparkTimestampNTZType
      case kernelArray: KernelArrayType =>
        SparkArrayType(
          convertKernelDataTypeToSparkDataType(kernelArray.getElementType),
          kernelArray.containsNull())
      case kernelMap: KernelMapType =>
        SparkMapType(
          convertKernelDataTypeToSparkDataType(kernelMap.getKeyType),
          convertKernelDataTypeToSparkDataType(kernelMap.getValueType),
          kernelMap.isValueContainsNull)
      case kernelStruct: KernelStructType => convertKernelSchemaToSparkSchema(kernelStruct)
      case x => throw new IllegalArgumentException(s"unsupported data type $x")
    }
  }

  //////////////////////
  // Spark --> Kernel //
  //////////////////////

  def convertSparkSchemaToKernelSchema(sparkSchema: SparkStructType): KernelStructType = {
    new KernelStructType(
      sparkSchema.fields
        .map(convertSparkFieldToKernelField)
        .toList
        .asJava)
  }

  def convertSparkFieldToKernelField(sparkField: SparkStructField): KernelStructField = {
    new KernelStructField(
      sparkField.name,
      convertSparkDataTypeToKernelDataType(sparkField.dataType),
      sparkField.nullable)
  }

  def convertSparkDataTypeToKernelDataType(sparkDataType: SparkDataType): KernelDataType = {
    sparkDataType match {
      case SparkStringType => KernelStringType.STRING
      case SparkBooleanType => KernelBooleanType.BOOLEAN
      case SparkIntegerType => KernelIntegerType.INTEGER
      case SparkLongType => KernelLongType.LONG
      case SparkBinaryType => KernelBinaryType.BINARY
      case SparkByteType => KernelByteType.BYTE
      case SparkDateType => KernelDateType.DATE
      case sparkDecimal: SparkDecimalType =>
        new KernelDecimalType(sparkDecimal.precision, sparkDecimal.scale)
      case SparkDoubleType => KernelDoubleType.DOUBLE
      case SparkFloatType => KernelFloatType.FLOAT
      case SparkShortType => KernelShortType.SHORT
      case SparkTimestampType => KernelTimestampType.TIMESTAMP
      case SparkTimestampNTZType => KernelTimestampNTZType.TIMESTAMP_NTZ
      case sparkArray: SparkArrayType =>
        new KernelArrayType(
          convertSparkDataTypeToKernelDataType(sparkArray.elementType),
          sparkArray.containsNull)
      case sparkMap: SparkMapType =>
        new KernelMapType(
          convertSparkDataTypeToKernelDataType(sparkMap.keyType),
          convertSparkDataTypeToKernelDataType(sparkMap.valueType),
          sparkMap.valueContainsNull)
      case sparkStruct: SparkStructType => convertSparkSchemaToKernelSchema(sparkStruct)
      case x => throw new IllegalArgumentException(s"unsupported data type $x")
    }
  }
}
