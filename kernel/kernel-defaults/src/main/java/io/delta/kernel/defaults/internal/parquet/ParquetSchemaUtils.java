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
package io.delta.kernel.defaults.internal.parquet;

import static io.delta.kernel.internal.util.ColumnMapping.PARQUET_FIELD_NESTED_IDS_METADATA_KEY;
import static java.lang.String.format;
import static org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit.MICROS;
import static org.apache.parquet.schema.LogicalTypeAnnotation.decimalType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.timestampType;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.*;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;
import static org.apache.parquet.schema.Types.primitive;

import io.delta.kernel.internal.util.ColumnMapping;
import io.delta.kernel.types.*;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.parquet.schema.*;
import org.apache.parquet.schema.LogicalTypeAnnotation.DecimalLogicalTypeAnnotation;
import org.apache.parquet.schema.Type.Repetition;

/** Utility methods for Delta schema to Parquet schema conversion. */
class ParquetSchemaUtils {

  /**
   * Constants that help if a Decimal type can be stored as INT32 or INT64 based on the precision.
   * The maximum precision that can be stored in INT32 is 9 and in INT64 is 18. If the precision
   * exceeds these values, then the DecimalType is stored as FIXED_LEN_BYTE_ARRAY.
   */
  public static final int DECIMAL_MAX_DIGITS_IN_INT = 9;

  public static final int DECIMAL_MAX_DIGITS_IN_LONG = 18;

  /**
   * Maximum number of bytes required to store a decimal of a given precision as
   * FIXED_LEN_BYTE_ARRAY in Parquet.
   */
  public static final List<Integer> MAX_BYTES_PER_PRECISION;

  static {
    List<Integer> maxBytesPerPrecision = new ArrayList<>();
    for (int i = 0; i <= 38; i++) {
      int numBytes = 1;
      while (Math.pow(2.0, 8 * numBytes - 1) < Math.pow(10.0, i)) {
        numBytes += 1;
      }
      maxBytesPerPrecision.add(numBytes);
    }
    MAX_BYTES_PER_PRECISION = Collections.unmodifiableList(maxBytesPerPrecision);
  }

  private ParquetSchemaUtils() {}

  /**
   * Given the file schema in Parquet file and selected columns by Delta, return a subschema of the
   * file schema.
   *
   * @param fileSchema
   * @param deltaType
   * @return
   */
  static MessageType pruneSchema(
      GroupType fileSchema /* parquet */, StructType deltaType /* delta-kernel */) {
    return new MessageType("fileSchema", pruneFields(fileSchema, deltaType));
  }

  /**
   * Search for the Parquet type in {@code groupType} of subfield which is equivalent to given
   * {@code field}.
   *
   * @param groupType Parquet group type coming from the file schema.
   * @param field Sub field given as Delta Kernel's {@link StructField}
   * @return {@link Type} of the Parquet field. Returns {@code null}, if not found.
   */
  static Type findSubFieldType(
      GroupType groupType, StructField field, Map<Integer, Type> parquetFieldIdToTypeMap) {

    // First search by the field id. If not found, search by case-sensitive name. Finally
    // by the case-insensitive name.
    // For Delta readers, no need to use the nested field ids added as part of icebergCompatV2
    Optional<Integer> fieldId = getFieldId(field.getMetadata());
    if (fieldId.isPresent()) {
      Type subType = parquetFieldIdToTypeMap.get(fieldId.get());
      if (subType != null) {
        return subType;
      }
    }

    final String columnName = field.getName();
    if (groupType.containsField(columnName)) {
      return groupType.getType(columnName);
    }
    // Parquet is case-sensitive, but the engine that generated the parquet file may not be.
    // Check for direct match above but if no match found, try case-insensitive match.
    for (org.apache.parquet.schema.Type type : groupType.getFields()) {
      if (type.getName().equalsIgnoreCase(columnName)) {
        return type;
      }
    }

    return null;
  }

  /** Returns a map from field id to Parquet type for fields that have the field id set. */
  static Map<Integer, Type> getParquetFieldToTypeMap(GroupType parquetGroupType) {
    // Generate the field id to Parquet type map only if the read schema has field ids.
    return parquetGroupType.getFields().stream()
        .filter(subFieldType -> subFieldType.getId() != null)
        .collect(
            Collectors.toMap(
                subFieldType -> subFieldType.getId().intValue(),
                subFieldType -> subFieldType,
                (u, v) -> {
                  throw new IllegalStateException(
                      format(
                          "Parquet file contains multiple columns "
                              + "(%s, %s) with the same field id",
                          u, v));
                }));
  }

  /**
   * Convert the given Kernel schema to Parquet's schema
   *
   * @param structType Kernel schema object
   * @return {@link MessageType} representing the schema in Parquet format.
   */
  static MessageType toParquetSchema(StructType structType) {
    List<Type> types = new ArrayList<>();
    for (StructField structField : structType.fields()) {
      types.add(
          toParquetType(
              structField /* nearestAncestor with struct field */,
              structField.getName() /* relativePath to nearestAncestor */,
              structField.getDataType(),
              structField.getName(),
              structField.isNullable() ? OPTIONAL : REQUIRED,
              getFieldId(structField.getMetadata())));
    }
    return new MessageType("DefaultKernelSchema", types);
  }

  private static List<Type> pruneFields(GroupType type, StructType deltaDataType) {
    // prune fields including nested pruning like in pruneSchema
    final Map<Integer, Type> parquetFieldIdToTypeMap = getParquetFieldToTypeMap(type);

    return deltaDataType.fields().stream()
        .map(
            column -> {
              Type subType = findSubFieldType(type, column, parquetFieldIdToTypeMap);
              if (subType != null) {
                return prunedType(subType, column.getDataType());
              } else {
                return null;
              }
            })
        .filter(Objects::nonNull)
        .collect(Collectors.toList());
  }

  private static Type prunedType(Type type, DataType deltaType) {
    if (type instanceof GroupType && deltaType instanceof StructType) {
      GroupType groupType = (GroupType) type;
      StructType structType = (StructType) deltaType;
      return groupType.withNewFields(pruneFields(groupType, structType));
    } else {
      return type;
    }
  }

  /**
   * Converts a Delta type {@code dataType} to a Parquet type.
   *
   * @param nearestAncestor The nearest ancestor with a {@link StructField}. This ancestor
   *     represents the current node or any other node on the path to the current node from root of
   *     the schema.
   * @param relativePath The relative path to this element from {@code nearestAncestor}. For
   *     example, consider a column type {@code col1 STRUCT(a INT, b STRUCT(c INT, d ARRAY(INT)))}.
   *     The absolute path to the nested {@code element} field of the list is col1.b.d.element,
   *     while the relative path is d.element, i.e., relative to the nearest ancestor with a struct
   *     field.
   * @param dataType The Delta type to be converted to a Parquet type.
   * @param name The name of the field.
   * @param repetition The {@link Repetition} of the field.
   * @param fieldId The field ID of the field. If present, the field ID is added to the Parquet
   *     type.
   * @return The Parquet type representing the given Delta type.
   */
  private static Type toParquetType(
      StructField nearestAncestor,
      String relativePath,
      DataType dataType,
      String name,
      Repetition repetition,
      Optional<Integer> fieldId) {
    Type type;
    if (dataType instanceof BooleanType) {
      type = primitive(BOOLEAN, repetition).named(name);
    } else if (dataType instanceof ByteType
        || dataType instanceof ShortType
        || dataType instanceof IntegerType) {
      type = primitive(INT32, repetition).named(name);
    } else if (dataType instanceof LongType) {
      type = primitive(INT64, repetition).named(name);
    } else if (dataType instanceof FloatType) {
      type = primitive(FLOAT, repetition).named(name);
    } else if (dataType instanceof DoubleType) {
      type = primitive(DOUBLE, repetition).named(name);
    } else if (dataType instanceof DecimalType) {
      DecimalType decimalType = (DecimalType) dataType;
      int precision = decimalType.getPrecision();
      int scale = decimalType.getScale();
      // DecimalType constructor already has checks to make sure the precision and scale are
      // within the valid range. No need to check them again.

      DecimalLogicalTypeAnnotation decimalAnnotation = decimalType(scale, precision);
      if (precision <= DECIMAL_MAX_DIGITS_IN_INT) {
        type = primitive(INT32, repetition).as(decimalAnnotation).named(name);
      } else if (precision <= DECIMAL_MAX_DIGITS_IN_LONG) {
        type = primitive(INT64, repetition).as(decimalAnnotation).named(name);
      } else {
        type =
            primitive(FIXED_LEN_BYTE_ARRAY, repetition)
                .as(decimalAnnotation)
                .length(MAX_BYTES_PER_PRECISION.get(precision))
                .named(name);
      }
    } else if (dataType instanceof StringType) {
      type = primitive(BINARY, repetition).as(LogicalTypeAnnotation.stringType()).named(name);
    } else if (dataType instanceof BinaryType) {
      type = primitive(BINARY, repetition).named(name);
    } else if (dataType instanceof DateType) {
      type = primitive(INT32, repetition).as(LogicalTypeAnnotation.dateType()).named(name);
    } else if (dataType instanceof TimestampType) {
      // Kernel is by default going to write as INT64 with isAdjustedToUTC set to true
      // Delta-Spark writes as INT96 for legacy reasons (maintaining compatibility with
      // unknown consumers with very, very old versions of Parquet reader). Kernel is a new
      // project, and we are ok if it breaks readers (we use this opportunity to find such
      // readers and ask them to upgrade).
      type =
          primitive(INT64, repetition)
              .as(timestampType(true /* isAdjustedToUTC */, MICROS))
              .named(name);
    } else if (dataType instanceof TimestampNTZType) {
      // Write as INT64 with isAdjustedToUTC set to false
      type =
          primitive(INT64, repetition)
              .as(timestampType(false /* isAdjustedToUTC */, MICROS))
              .named(name);
    } else if (dataType instanceof ArrayType) {
      type =
          toParquetArrayType(nearestAncestor, relativePath, (ArrayType) dataType, name, repetition);
    } else if (dataType instanceof MapType) {
      type = toParquetMapType(nearestAncestor, relativePath, (MapType) dataType, name, repetition);
    } else if (dataType instanceof StructType) {
      type = toParquetStructType((StructType) dataType, name, repetition);
    } else {
      throw new UnsupportedOperationException(
          "Writing given type data to Parquet is not supported: " + dataType);
    }

    if (fieldId.isPresent()) {
      // Add field id to the type.
      type = type.withId(fieldId.get());
    }
    return type;
  }

  private static Type toParquetArrayType(
      StructField nearestAncestor,
      String relativePath,
      ArrayType arrayType,
      String name,
      Repetition rep) {
    // We will be supporting the 3-level array structure only. 2-level array structures are
    // a very old legacy versions of Parquet which Kernel doesn't support writing as.

    String elementRelativePath = relativePath + ".element";
    return Types.buildGroup(rep)
        .as(LogicalTypeAnnotation.listType())
        .addField(
            Types.repeatedGroup()
                .addField(
                    toParquetType(
                        nearestAncestor,
                        elementRelativePath,
                        arrayType.getElementType(),
                        "element", /* name */
                        arrayType.containsNull() ? OPTIONAL : REQUIRED,
                        getNestedFieldId(nearestAncestor, elementRelativePath)))
                .named("list"))
        .named(name);
  }

  private static Type toParquetMapType(
      StructField nearestAncestor,
      String relativePath,
      MapType mapType,
      String name,
      Repetition repetition) {
    // We will be supporting the 3-level map structure only. 2-level map structures are
    // a very old legacy versions of Parquet which Kernel doesn't support writing as.

    String keyRelativePath = relativePath + ".key";
    String valueRelativePath = relativePath + ".value";

    return Types.buildGroup(repetition)
        .as(LogicalTypeAnnotation.mapType())
        .addField(
            Types.repeatedGroup()
                .addField(
                    toParquetType(
                        nearestAncestor,
                        keyRelativePath,
                        mapType.getKeyType(),
                        "key", /* name */
                        REQUIRED, /* repetition */
                        getNestedFieldId(nearestAncestor, keyRelativePath)))
                .addField(
                    toParquetType(
                        nearestAncestor,
                        valueRelativePath,
                        mapType.getValueType(),
                        "value", /* name */
                        mapType.isValueContainsNull() ? OPTIONAL : REQUIRED,
                        getNestedFieldId(nearestAncestor, valueRelativePath)))
                .named("key_value"))
        .named(name);
  }

  private static Type toParquetStructType(
      StructType structType, String name, Repetition repetition) {
    List<Type> fields = new ArrayList<>();
    for (StructField field : structType.fields()) {
      fields.add(
          toParquetType(
              field, /* nearestAncestor with struct field */
              field.getName(), /* relativePath to nearestAncestor */
              field.getDataType(),
              field.getName(),
              field.isNullable() ? OPTIONAL : REQUIRED,
              getFieldId(field.getMetadata())));
    }
    return new GroupType(repetition, name, fields);
  }

  private static Optional<Integer> getFieldId(FieldMetadata fieldMetadata) {
    return getFieldId(fieldMetadata, ColumnMapping.PARQUET_FIELD_ID_KEY);
  }

  private static Optional<Integer> getNestedFieldId(
      StructField field, String nestedFieldRelativePath) {
    FieldMetadata nestedFieldIDMetadata =
        field.getMetadata().getMetadata(PARQUET_FIELD_NESTED_IDS_METADATA_KEY);
    if (nestedFieldIDMetadata != null) {
      return getFieldId(nestedFieldIDMetadata, nestedFieldRelativePath);
    }
    return Optional.empty();
  }

  private static Optional<Integer> getFieldId(FieldMetadata fieldMetadata, String fieldIdKey) {
    // Field id delta schema metadata is deserialized as long, but the range should always
    // be within integer range.
    return Optional.ofNullable(fieldMetadata.getLong(fieldIdKey)).map(Math::toIntExact);
  }
}
