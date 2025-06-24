/*
 * Copyright (2023) The Delta Lake Project Authors.
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
package io.delta.dsv2.utils

import scala.collection.JavaConverters._

import io.delta.kernel.types.{BooleanType => KernelBooleanType, DataType => KernelDataType, IntegerType => KernelIntegerType, LongType => KernelLongType, StringType => KernelStringType, StructField => KernelStructField, StructType => KernelStructType}

import org.apache.spark.sql.types.{BooleanType => SparkBooleanType, DataType => SparkDataType, IntegerType => SparkIntegerType, LongType => SparkLongType, StringType => SparkStringType, StructField => SparkStructField, StructType => SparkStructType}

object SchemaUtils {

  //////////////////////
  // Kernel --> Spark //
  //////////////////////

  def convertKernelSchemaToSparkSchema(kernelSchema: KernelStructType): SparkStructType = {
    SparkStructType(kernelSchema.fields().asScala.map { field =>
      SparkStructField(
        field.getName,
        convertKernelDataTypeToSparkDataType(field.getDataType),
        field.isNullable)
    })
  }

  def convertKernelDataTypeToSparkDataType(kernelDataType: KernelDataType): SparkDataType = {
    kernelDataType match {
      case _: KernelStringType => SparkStringType
      case _: KernelBooleanType => SparkBooleanType
      case _: KernelIntegerType => SparkIntegerType
      case _: KernelLongType => SparkLongType
      case x => throw new IllegalArgumentException(s"unsupported data type $x")
    }
  }

  //////////////////////
  // Spark --> Kernel //
  //////////////////////

  def convertSparkSchemaToKernelSchema(sparkSchema: SparkStructType): KernelStructType = {
    new KernelStructType(
      sparkSchema.fields
        .map { field =>
          new KernelStructField(
            field.name,
            convertSparkDataTypeToKernelDataType(field.dataType),
            field.nullable)
        }
        .toList
        .asJava)
  }

  def convertSparkDataTypeToKernelDataType(sparkDataType: SparkDataType): KernelDataType = {
    sparkDataType match {
      case SparkStringType => KernelStringType.STRING
      case SparkBooleanType => KernelBooleanType.BOOLEAN
      case SparkIntegerType => KernelIntegerType.INTEGER
      case SparkLongType => KernelLongType.LONG
      case x => throw new IllegalArgumentException(s"unsupported data type $x")
    }
  }

}
