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
package io.delta.spark.internal.v2.utils;

import static java.util.Objects.requireNonNull;

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
import java.util.Arrays;
import java.util.List;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.MetadataBuilder;
import scala.jdk.CollectionConverters;

/** A utility class for converting between Delta Kernel and Spark schemas and data types. */
public class SchemaUtils {

  //////////////////////
  // Kernel --> Spark //
  //////////////////////

  /** Converts a Delta Kernel schema to a Spark schema. */
  public static org.apache.spark.sql.types.StructType convertKernelSchemaToSparkSchema(
      StructType kernelSchema) {
    requireNonNull(kernelSchema);
    List<org.apache.spark.sql.types.StructField> fields = new ArrayList<>();

    for (StructField field : kernelSchema.fields()) {
      fields.add(
          new org.apache.spark.sql.types.StructField(
              field.getName(),
              convertKernelDataTypeToSparkDataType(field.getDataType()),
              field.isNullable(),
              convertKernelFieldMetadataToSparkMetadata(field.getMetadata())));
    }

    return new org.apache.spark.sql.types.StructType(
        fields.toArray(new org.apache.spark.sql.types.StructField[0]));
  }

  /** Converts a Delta Kernel data type to a Spark data type. */
  public static org.apache.spark.sql.types.DataType convertKernelDataTypeToSparkDataType(
      DataType kernelDataType) {
    requireNonNull(kernelDataType);
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
    requireNonNull(sparkSchema);
    List<StructField> kernelFields = new ArrayList<>();

    for (org.apache.spark.sql.types.StructField field : sparkSchema.fields()) {
      kernelFields.add(
          new StructField(
              field.name(),
              convertSparkDataTypeToKernelDataType(field.dataType()),
              field.nullable(),
              convertSparkMetadataToKernelFieldMetadata(field.metadata())));
    }

    return new StructType(kernelFields);
  }

  /** Converts a Spark data type to a Delta Kernel data type. */
  public static DataType convertSparkDataTypeToKernelDataType(
      org.apache.spark.sql.types.DataType sparkDataType) {
    requireNonNull(sparkDataType);
    if (sparkDataType instanceof org.apache.spark.sql.types.StringType) {
      return StringType.STRING;
    } else if (sparkDataType instanceof org.apache.spark.sql.types.BooleanType) {
      return BooleanType.BOOLEAN;
    } else if (sparkDataType instanceof org.apache.spark.sql.types.IntegerType) {
      return IntegerType.INTEGER;
    } else if (sparkDataType instanceof org.apache.spark.sql.types.LongType) {
      return LongType.LONG;
    } else if (sparkDataType instanceof org.apache.spark.sql.types.BinaryType) {
      return BinaryType.BINARY;
    } else if (sparkDataType instanceof org.apache.spark.sql.types.ByteType) {
      return ByteType.BYTE;
    } else if (sparkDataType instanceof org.apache.spark.sql.types.DateType) {
      return DateType.DATE;
    } else if (sparkDataType instanceof org.apache.spark.sql.types.DecimalType) {
      org.apache.spark.sql.types.DecimalType sparkDecimal =
          (org.apache.spark.sql.types.DecimalType) sparkDataType;
      return new DecimalType(sparkDecimal.precision(), sparkDecimal.scale());
    } else if (sparkDataType instanceof org.apache.spark.sql.types.DoubleType) {
      return DoubleType.DOUBLE;
    } else if (sparkDataType instanceof org.apache.spark.sql.types.FloatType) {
      return FloatType.FLOAT;
    } else if (sparkDataType instanceof org.apache.spark.sql.types.ShortType) {
      return ShortType.SHORT;
    } else if (sparkDataType instanceof org.apache.spark.sql.types.TimestampType) {
      return TimestampType.TIMESTAMP;
    } else if (sparkDataType instanceof org.apache.spark.sql.types.TimestampNTZType) {
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

  ///////////////////////////////
  // Field Metadata Conversion //
  ///////////////////////////////

  /**
   * Converts Kernel FieldMetadata to Spark Metadata.
   *
   * @param kernelMetadata the Kernel FieldMetadata to convert
   * @return the equivalent Spark Metadata
   */
  public static Metadata convertKernelFieldMetadataToSparkMetadata(
      io.delta.kernel.types.FieldMetadata kernelMetadata) {
    requireNonNull(kernelMetadata);
    if (kernelMetadata.getEntries().isEmpty()) {
      return Metadata.empty();
    }
    MetadataBuilder builder = new MetadataBuilder();
    kernelMetadata
        .getEntries()
        .forEach(
            (key, value) -> {
              if (value instanceof Long) {
                builder.putLong(key, (Long) value);
              } else if (value instanceof Double) {
                builder.putDouble(key, (Double) value);
              } else if (value instanceof Boolean) {
                builder.putBoolean(key, (Boolean) value);
              } else if (value instanceof String) {
                builder.putString(key, (String) value);
              } else if (value instanceof io.delta.kernel.types.FieldMetadata) {
                builder.putMetadata(
                    key,
                    convertKernelFieldMetadataToSparkMetadata(
                        (io.delta.kernel.types.FieldMetadata) value));
              } else if (value instanceof Long[]) {
                builder.putLongArray(key, unboxLongArray((Long[]) value, key));
              } else if (value instanceof Double[]) {
                builder.putDoubleArray(key, unboxDoubleArray((Double[]) value, key));
              } else if (value instanceof Boolean[]) {
                builder.putBooleanArray(key, unboxBooleanArray((Boolean[]) value, key));
              } else if (value instanceof String[]) {
                builder.putStringArray(key, (String[]) value);
              } else if (value instanceof io.delta.kernel.types.FieldMetadata[]) {
                io.delta.kernel.types.FieldMetadata[] kernelMetadatas =
                    (io.delta.kernel.types.FieldMetadata[]) value;
                Metadata[] sparkMetadatas =
                    Arrays.stream(kernelMetadatas)
                        .map(SchemaUtils::convertKernelFieldMetadataToSparkMetadata)
                        .toArray(Metadata[]::new);
                builder.putMetadataArray(key, sparkMetadatas);
              } else if (value == null) {
                builder.putNull(key);
              } else {
                throw new UnsupportedOperationException(
                    "Unsupported metadata value type: " + value.getClass().getName());
              }
            });
    return builder.build();
  }

  /**
   * Converts Spark Metadata to Kernel FieldMetadata.
   *
   * @param sparkMetadata the Spark Metadata to convert
   * @return the equivalent Kernel FieldMetadata
   */
  public static io.delta.kernel.types.FieldMetadata convertSparkMetadataToKernelFieldMetadata(
      Metadata sparkMetadata) {
    requireNonNull(sparkMetadata);
    if (sparkMetadata.map().isEmpty()) {
      return io.delta.kernel.types.FieldMetadata.empty();
    }
    io.delta.kernel.types.FieldMetadata.Builder builder =
        io.delta.kernel.types.FieldMetadata.builder();

    CollectionConverters.MapHasAsJava(sparkMetadata.map())
        .asJava()
        .forEach(
            (key, value) -> {
              if (value instanceof Long) {
                builder.putLong(key, (Long) value);
              } else if (value instanceof Double) {
                builder.putDouble(key, (Double) value);
              } else if (value instanceof Boolean) {
                builder.putBoolean(key, (Boolean) value);
              } else if (value instanceof String) {
                builder.putString(key, (String) value);
              } else if (value instanceof Metadata) {
                builder.putFieldMetadata(
                    key, convertSparkMetadataToKernelFieldMetadata((Metadata) value));
              } else if (value instanceof long[]) {
                builder.putLongArray(
                    key, Arrays.stream((long[]) value).boxed().toArray(Long[]::new));
              } else if (value instanceof double[]) {
                builder.putDoubleArray(
                    key, Arrays.stream((double[]) value).boxed().toArray(Double[]::new));
              } else if (value instanceof boolean[]) {
                boolean[] valArray = (boolean[]) value;
                Boolean[] booleanArray = new Boolean[valArray.length];
                for (int i = 0; i < valArray.length; i++) {
                  booleanArray[i] = valArray[i];
                }
                builder.putBooleanArray(key, booleanArray);
              } else if (value instanceof String[]) {
                builder.putStringArray(key, (String[]) value);
              } else if (value instanceof Metadata[]) {
                Metadata[] sparkMetadatas = (Metadata[]) value;
                io.delta.kernel.types.FieldMetadata[] kernelMetadatas =
                    Arrays.stream(sparkMetadatas)
                        .map(SchemaUtils::convertSparkMetadataToKernelFieldMetadata)
                        .toArray(io.delta.kernel.types.FieldMetadata[]::new);
                builder.putFieldMetadataArray(key, kernelMetadatas);
              } else if (value == null) {
                builder.putNull(key);
              } else {
                throw new UnsupportedOperationException(
                    "Unsupported metadata value type: " + value.getClass().getName());
              }
            });
    return builder.build();
  }

  /**
   * Unboxes a Long[] to long[], checking for nulls.
   *
   * @throws NullPointerException if any element is null
   */
  private static long[] unboxLongArray(Long[] boxedArray, String key) {
    long[] primitiveArray = new long[boxedArray.length];
    for (int i = 0; i < boxedArray.length; i++) {
      requireNonNull(
          boxedArray[i],
          String.format("Null element at index %s in Long array for key '%s'", i, key));
      primitiveArray[i] = boxedArray[i];
    }
    return primitiveArray;
  }

  /**
   * Unboxes a Double[] to double[], checking for nulls.
   *
   * @throws NullPointerException if any element is null
   */
  private static double[] unboxDoubleArray(Double[] boxedArray, String key) {
    double[] primitiveArray = new double[boxedArray.length];
    for (int i = 0; i < boxedArray.length; i++) {
      requireNonNull(
          boxedArray[i],
          String.format("Null element at index %s in Double array for key '%s'", i, key));
      primitiveArray[i] = boxedArray[i];
    }
    return primitiveArray;
  }

  /**
   * Unboxes a Boolean[] to boolean[], checking for nulls.
   *
   * @throws NullPointerException if any element is null
   */
  private static boolean[] unboxBooleanArray(Boolean[] boxedArray, String key) {
    boolean[] primitiveArray = new boolean[boxedArray.length];
    for (int i = 0; i < boxedArray.length; i++) {
      requireNonNull(
          boxedArray[i],
          String.format("Null element at index %s in Boolean array for key '%s'", i, key));
      primitiveArray[i] = boxedArray[i];
    }
    return primitiveArray;
  }
}
