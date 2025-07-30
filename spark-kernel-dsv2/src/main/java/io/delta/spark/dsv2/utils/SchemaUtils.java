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
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.delta.kernel.types.TimestampNTZType;
import io.delta.kernel.types.TimestampType;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;

public class SchemaUtils {

  //////////////////////
  // Kernel --> Spark //
  //////////////////////

  /** Converts a Delta Kernel schema to a Spark schema. */
  public static org.apache.spark.sql.types.StructType convertKernelSchemaToSparkSchema(
      StructType kernelSchema) {
    List<org.apache.spark.sql.types.StructField> fields = new ArrayList<>();

    for (StructField field : kernelSchema.fields()) {
      fields.add(
          new org.apache.spark.sql.types.StructField(
              field.getName(),
              convertKernelDataTypeToSparkDataType(field.getDataType()),
              field.isNullable(),
              Metadata.empty()));
    }

    return new org.apache.spark.sql.types.StructType(
        fields.toArray(new org.apache.spark.sql.types.StructField[0]));
  }

  /** Converts a Delta Kernel data type to a Spark data type. */
  public static org.apache.spark.sql.types.DataType convertKernelDataTypeToSparkDataType(
      DataType kernelDataType) {
    if (kernelDataType instanceof StringType) {
      return DataTypes.StringType;
    } else if (kernelDataType instanceof BooleanType) {
      return DataTypes.BooleanType;
    } else if (kernelDataType instanceof IntegerType) {
      return DataTypes.IntegerType;
    } else if (kernelDataType instanceof LongType) {
      return DataTypes.LongType;
    } else if (kernelDataType instanceof BinaryType) {
      return DataTypes.BinaryType;
    } else if (kernelDataType instanceof ByteType) {
      return DataTypes.ByteType;
    } else if (kernelDataType instanceof DateType) {
      return DataTypes.DateType;
    } else if (kernelDataType instanceof DecimalType) {
      DecimalType kernelDecimal = (DecimalType) kernelDataType;
      return DataTypes.createDecimalType(kernelDecimal.getPrecision(), kernelDecimal.getScale());
    } else if (kernelDataType instanceof DoubleType) {
      return DataTypes.DoubleType;
    } else if (kernelDataType instanceof FloatType) {
      return DataTypes.FloatType;
    } else if (kernelDataType instanceof ShortType) {
      return DataTypes.ShortType;
    } else if (kernelDataType instanceof TimestampType) {
      return DataTypes.TimestampType;
    } else if (kernelDataType instanceof TimestampNTZType) {
      return DataTypes.TimestampNTZType;
    } else if (kernelDataType instanceof ArrayType) {
      ArrayType kernelArray = (ArrayType) kernelDataType;
      return DataTypes.createArrayType(
          convertKernelDataTypeToSparkDataType(kernelArray.getElementType()),
          kernelArray.containsNull());
    } else if (kernelDataType instanceof MapType) {
      MapType kernelMap = (MapType) kernelDataType;
      return DataTypes.createMapType(
          convertKernelDataTypeToSparkDataType(kernelMap.getKeyType()),
          convertKernelDataTypeToSparkDataType(kernelMap.getValueType()),
          kernelMap.isValueContainsNull());
    } else if (kernelDataType instanceof StructType) {
      return convertKernelSchemaToSparkSchema((StructType) kernelDataType);
    } else {
      // TODO: add variant type, this requires upgrading spark version dependency to 4.0
      throw new IllegalArgumentException("unsupported data type " + kernelDataType);
    }
  }

  //////////////////////
  // Spark --> Kernel //
  //////////////////////

  /** Converts a Spark schema to a Delta Kernel schema. */
  public static StructType convertSparkSchemaToKernelSchema(
      org.apache.spark.sql.types.StructType sparkSchema) {
    List<StructField> kernelFields = new ArrayList<>();

    for (org.apache.spark.sql.types.StructField field : sparkSchema.fields()) {
      kernelFields.add(
          new StructField(
              field.name(),
              convertSparkDataTypeToKernelDataType(field.dataType()),
              field.nullable()));
    }

    return new StructType(kernelFields);
  }

  /** Converts a Spark data type to a Delta Kernel data type. */
  public static DataType convertSparkDataTypeToKernelDataType(
      org.apache.spark.sql.types.DataType sparkDataType) {
    if (sparkDataType == DataTypes.StringType) {
      return StringType.STRING;
    } else if (sparkDataType == DataTypes.BooleanType) {
      return BooleanType.BOOLEAN;
    } else if (sparkDataType == DataTypes.IntegerType) {
      return IntegerType.INTEGER;
    } else if (sparkDataType == DataTypes.LongType) {
      return LongType.LONG;
    } else if (sparkDataType == DataTypes.BinaryType) {
      return BinaryType.BINARY;
    } else if (sparkDataType == DataTypes.ByteType) {
      return ByteType.BYTE;
    } else if (sparkDataType == DataTypes.DateType) {
      return DateType.DATE;
    } else if (sparkDataType instanceof org.apache.spark.sql.types.DecimalType) {
      org.apache.spark.sql.types.DecimalType sparkDecimal =
          (org.apache.spark.sql.types.DecimalType) sparkDataType;
      return new DecimalType(sparkDecimal.precision(), sparkDecimal.scale());
    } else if (sparkDataType == DataTypes.DoubleType) {
      return DoubleType.DOUBLE;
    } else if (sparkDataType == DataTypes.FloatType) {
      return FloatType.FLOAT;
    } else if (sparkDataType == DataTypes.ShortType) {
      return ShortType.SHORT;
    } else if (sparkDataType == DataTypes.TimestampType) {
      return TimestampType.TIMESTAMP;
    } else if (sparkDataType == DataTypes.TimestampNTZType) {
      return TimestampNTZType.TIMESTAMP_NTZ;
    } else if (sparkDataType instanceof org.apache.spark.sql.types.ArrayType) {
      org.apache.spark.sql.types.ArrayType sparkArray =
          (org.apache.spark.sql.types.ArrayType) sparkDataType;
      return new ArrayType(
          convertSparkDataTypeToKernelDataType(sparkArray.elementType()),
          sparkArray.containsNull());
    } else if (sparkDataType instanceof org.apache.spark.sql.types.MapType) {
      org.apache.spark.sql.types.MapType sparkMap =
          (org.apache.spark.sql.types.MapType) sparkDataType;
      return new MapType(
          convertSparkDataTypeToKernelDataType(sparkMap.keyType()),
          convertSparkDataTypeToKernelDataType(sparkMap.valueType()),
          sparkMap.valueContainsNull());
    } else if (sparkDataType instanceof org.apache.spark.sql.types.StructType) {
      return convertSparkSchemaToKernelSchema(
          (org.apache.spark.sql.types.StructType) sparkDataType);
    } else {
      throw new IllegalArgumentException("unsupported data type " + sparkDataType);
    }
  }
}
