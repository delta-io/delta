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
package org.apache.spark.sql.delta.v2.utils;

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
import io.delta.kernel.types.VariantType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import org.apache.spark.sql.catalyst.util.CollationFactory;
import org.apache.spark.sql.errors.QueryCompilationErrors;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.unsafe.Platform;
import scala.jdk.CollectionConverters;

/** A utility class for converting between Delta Kernel and Spark schemas and data types. */
public class SchemaUtils {

  //////////////////////
  // Kernel --> Spark //
  //////////////////////

  /**
   * Returns the table schema described by {@code metadata}, parsing its schema string the same way
   * DSv1 does so that string collations recorded in the log are preserved.
   */
  public static org.apache.spark.sql.types.StructType convertKernelMetadataToSparkSchema(
      io.delta.kernel.internal.actions.Metadata metadata) {
    requireNonNull(metadata);
    return (org.apache.spark.sql.types.StructType)
        org.apache.spark.sql.types.DataType.fromJson(metadata.getSchemaString());
  }

  /**
   * Projects the union of {@code readDataSchema} and {@code partitionSchema} into DDL order. Fields
   * not present in {@code rawSchema} (e.g. {@code _metadata}) are appended at the tail in insertion
   * order.
   *
   * @throws NullPointerException if any argument is null
   */
  public static org.apache.spark.sql.types.StructType ddlOrderedOutputSchema(
      org.apache.spark.sql.types.StructType rawSchema,
      org.apache.spark.sql.types.StructType readDataSchema,
      org.apache.spark.sql.types.StructType partitionSchema) {
    requireNonNull(rawSchema, "rawSchema is null");
    requireNonNull(readDataSchema, "readDataSchema is null");
    requireNonNull(partitionSchema, "partitionSchema is null");
    if (partitionSchema.fields().length == 0) {
      return readDataSchema;
    }
    LinkedHashMap<String, org.apache.spark.sql.types.StructField> fieldsByName =
        new LinkedHashMap<>(readDataSchema.fields().length + partitionSchema.fields().length);
    for (org.apache.spark.sql.types.StructField f : readDataSchema.fields()) {
      fieldsByName.put(f.name(), f);
    }
    for (org.apache.spark.sql.types.StructField f : partitionSchema.fields()) {
      fieldsByName.put(f.name(), f);
    }
    List<org.apache.spark.sql.types.StructField> ordered = new ArrayList<>(fieldsByName.size());
    for (org.apache.spark.sql.types.StructField f : rawSchema.fields()) {
      org.apache.spark.sql.types.StructField projected = fieldsByName.remove(f.name());
      if (projected != null) {
        ordered.add(projected);
      }
    }
    // Append Spark-synthetic fields not in the persisted schema (e.g. _metadata).
    ordered.addAll(fieldsByName.values());
    return new org.apache.spark.sql.types.StructType(
        ordered.toArray(new org.apache.spark.sql.types.StructField[0]));
  }

  /**
   * Whether data written under {@code existingSchema} can be read using {@code endSchema}. Backed
   * by Delta's read-compatibility check, which accepts additive changes (new columns, relaxed
   * nullability) and ignores internal field metadata, unlike strict equality. Nullability
   * tightening is rejected because older data may contain nulls.
   */
  public static boolean isReadCompatible(
      org.apache.spark.sql.types.StructType existingSchema,
      org.apache.spark.sql.types.StructType endSchema) {
    requireNonNull(existingSchema, "existingSchema is null");
    requireNonNull(endSchema, "endSchema is null");
    org.apache.spark.sql.delta.schema.SchemaUtils$ delta =
        org.apache.spark.sql.delta.schema.SchemaUtils$.MODULE$;
    return delta.isReadCompatible(
        existingSchema,
        endSchema,
        /* forbidTightenNullability */ true,
        delta.isReadCompatible$default$4(),
        delta.isReadCompatible$default$5(),
        delta.isReadCompatible$default$6(),
        delta.isReadCompatible$default$7(),
        delta.isReadCompatible$default$8());
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
      org.apache.spark.sql.types.StringType sparkString =
          (org.apache.spark.sql.types.StringType) sparkDataType;
      if (sparkString.isUTF8BinaryCollation()) {
        return StringType.STRING;
      }
      return new StringType(
          CollationFactory.fetchCollation(sparkString.collationId())
              .identifier()
              .toStringWithoutVersion());
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
      // Kernel's MapType rejects collated keys with an unclassified error, so raise V1's instead.
      if (sparkMap.keyType() instanceof org.apache.spark.sql.types.StringType
          && !((org.apache.spark.sql.types.StringType) sparkMap.keyType())
              .isUTF8BinaryCollation()) {
        Platform.throwException(QueryCompilationErrors.collatedStringsInMapKeysNotSupportedError());
      }
      return new MapType(
          convertSparkDataTypeToKernelDataType(sparkMap.keyType()),
          convertSparkDataTypeToKernelDataType(sparkMap.valueType()),
          sparkMap.valueContainsNull());
    } else if (sparkDataType instanceof org.apache.spark.sql.types.StructType) {
      return convertSparkSchemaToKernelSchema(
          (org.apache.spark.sql.types.StructType) sparkDataType);
    } else if (sparkDataType instanceof org.apache.spark.sql.types.VariantType) {
      return VariantType.VARIANT;
    } else {
      throw new IllegalArgumentException("unsupported data type " + sparkDataType);
    }
  }

  ///////////////////////////////
  // Field Metadata Conversion //
  ///////////////////////////////

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
}
