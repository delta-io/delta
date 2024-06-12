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

import java.util.*;
import java.util.stream.Collectors;
import static java.lang.String.format;

import org.apache.parquet.schema.*;
import org.apache.parquet.schema.LogicalTypeAnnotation.*;
import org.apache.parquet.schema.Type.Repetition;
import static org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit.MICROS;
import static org.apache.parquet.schema.LogicalTypeAnnotation.decimalType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.timestampType;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.*;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;
import static org.apache.parquet.schema.Types.primitive;

import io.delta.kernel.types.*;

import io.delta.kernel.internal.util.ColumnMapping;
import static io.delta.kernel.internal.util.Preconditions.checkArgument;

/**
 * Utility methods for Delta schema to Parquet schema conversion.
 */
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

    private ParquetSchemaUtils() {
    }

    /**
     * Given the file schema in Parquet file and selected columns by Delta, return a subschema of
     * the file schema.
     *
     * @param fileSchema
     * @param deltaType
     * @return
     */
    static MessageType pruneSchema(
            GroupType fileSchema /* parquet */,
            StructType deltaType /* delta-kernel */) {
        boolean hasFieldIds = hasFieldIds(deltaType);
        return new MessageType("fileSchema", pruneFields(fileSchema, deltaType, hasFieldIds));
    }

    /**
     * Search for the Parquet type in {@code groupType} of subfield which is equivalent to given
     * {@code field}.
     *
     * @param groupType Parquet group type coming from the file schema.
     * @param field     Sub field given as Delta Kernel's {@link StructField}
     * @return {@link Type} of the Parquet field. Returns {@code null}, if not found.
     */
    static Type findSubFieldType(
            GroupType groupType,
            StructField field,
            Map<Integer, Type> parquetFieldIdToTypeMap) {

        // First search by the field id. If not found, search by case-sensitive name. Finally
        // by the case-insensitive name.
        if (hasFieldId(field.getMetadata())) {
            int deltaFieldId = getFieldId(field.getMetadata());
            Type subType = parquetFieldIdToTypeMap.get(deltaFieldId);
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

    /**
     * Returns a map from field id to Parquet type for fields that have the field id set.
     */
    static Map<Integer, Type> getParquetFieldToTypeMap(GroupType parquetGroupType) {
        // Generate the field id to Parquet type map only if the read schema has field ids.
        return parquetGroupType.getFields().stream()
                .filter(subFieldType -> subFieldType.getId() != null)
                .collect(Collectors.toMap(
                        subFieldType -> subFieldType.getId().intValue(),
                        subFieldType -> subFieldType,
                        (u, v) -> {
                            throw new IllegalStateException(
                                    format("Parquet file contains multiple columns " +
                                            "(%s, %s) with the same field id", u, v));
                        }));
    }

    /**
     * Convert the given Kernel schema to Parquet's schema
     *
     * @param structType Kernel schema object
     * @return {@link MessageType} representing the schema in Parquet format.
     */
    public static MessageType toParquetSchema(StructType structType) {
        List<Type> types = new ArrayList<>();
        for (StructField structField : structType.fields()) {
            types.add(toParquetType(
                    structField.getDataType(),
                    structField.getName(),
                    structField.isNullable() ? OPTIONAL : REQUIRED,
                    getFieldId(structField)));
        }
        return new MessageType("Default Kernel Schema", types);
    }

    /**
     * Convert the given Parquet data type to a Kernel data type.
     *
     * TODO(r.chen): Test this function.
     *
     * @param type Parquet type object
     * @return {@link DataType} representing the Parquet type in Kernel.
     */
    public static DataType toKernelType(Type type) {
        if (type.isPrimitive()) {
            PrimitiveType pt = type.asPrimitiveType();

            if (pt.getOriginalType() == OriginalType.DECIMAL) {
                DecimalLogicalTypeAnnotation dlta =
                        (DecimalLogicalTypeAnnotation) pt.getLogicalTypeAnnotation();
                return new DecimalType(dlta.getPrecision(), dlta.getScale());
            } else if (pt.getPrimitiveTypeName() == BOOLEAN) {
                return BooleanType.BOOLEAN;
            } else if (pt.getPrimitiveTypeName() == INT32) {
                if (pt.getOriginalType() == OriginalType.INT_8) {
                    return ByteType.BYTE;
                } else if (pt.getOriginalType() == OriginalType.INT_16) {
                    return ShortType.SHORT;
                } else if (pt.getLogicalTypeAnnotation() == LogicalTypeAnnotation.dateType()) {
                    return DateType.DATE;
                }
                return IntegerType.INTEGER;
            } else if (pt.getPrimitiveTypeName() == INT64) {
                if (pt.getOriginalType() == OriginalType.TIMESTAMP_MICROS) {
                    TimestampLogicalTypeAnnotation tlta =
                        (TimestampLogicalTypeAnnotation) pt.getLogicalTypeAnnotation();
                    return tlta.isAdjustedToUTC() ?
                        TimestampType.TIMESTAMP : TimestampNTZType.TIMESTAMP_NTZ;
                }
                return LongType.LONG;
            } else if (pt.getPrimitiveTypeName() == FLOAT) {
                return FloatType.FLOAT;
            } else if (pt.getPrimitiveTypeName() == DOUBLE) {
                return DoubleType.DOUBLE;
            } else if (pt.getPrimitiveTypeName() == BINARY) {
                if (pt.getLogicalTypeAnnotation() == LogicalTypeAnnotation.stringType()) {
                    return StringType.STRING;
                } else {
                    return BinaryType.BINARY;
                }
            } else {
                throw new UnsupportedOperationException(
                    "Converting the given Parquet data type to Kernel is not supported: " + type);
            }
        } else {
            if (type.getLogicalTypeAnnotation() == LogicalTypeAnnotation.listType()) {
                GroupType gt = (GroupType) type;
                Type childType = gt.getType(0);
                return new ArrayType(
                    toKernelType(childType), childType.getRepetition() == OPTIONAL);
            } else if (type.getLogicalTypeAnnotation() == LogicalTypeAnnotation.mapType()) {
                GroupType gt = (GroupType) type;
                Type keyType = gt.getType(0);
                Type valueType = gt.getType(1);
                return new MapType(
                    toKernelType(keyType),
                    toKernelType(valueType),
                    valueType.getRepetition() == OPTIONAL
                );
            } else {
                List<StructField> kernelFields = new ArrayList<>();
                GroupType gt = (GroupType) type;
                for (Type parquetType : gt.getFields()) {
                    FieldMetadata.Builder metadataBuilder = FieldMetadata.builder();
                    if (type.getId() != null) {
                        metadataBuilder.putLong(
                            ColumnMapping.PARQUET_FIELD_ID_KEY,
                            (long) (type.getId().intValue())
                        );
                    }
                    kernelFields.add(new StructField(
                        parquetType.getName(),
                        toKernelType(parquetType),
                        parquetType.getRepetition() == OPTIONAL,
                        metadataBuilder.build()
                    ));
                }
                return new StructType(kernelFields);
            }
        }
    }

    private static List<Type> pruneFields(
            GroupType type, StructType deltaDataType, boolean hasFieldIds) {
        // prune fields including nested pruning like in pruneSchema
        final Map<Integer, Type> parquetFieldIdToTypeMap = getParquetFieldToTypeMap(type);

        return deltaDataType.fields().stream()
                .map(column -> {
                    Type subType = findSubFieldType(type, column, parquetFieldIdToTypeMap);
                    if (subType != null) {
                        return prunedType(subType, column.getDataType(), hasFieldIds);
                    } else {
                        return null;
                    }
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    private static Type prunedType(Type type, DataType deltaType, boolean hasFieldIds) {
        if (type instanceof GroupType && deltaType instanceof StructType) {
            GroupType groupType = (GroupType) type;
            StructType structType = (StructType) deltaType;
            return groupType.withNewFields(pruneFields(groupType, structType, hasFieldIds));
        } else {
            return type;
        }
    }

    private static Type toParquetType(
            DataType dataType,
            String name,
            Repetition repetition,
            Optional<Integer> fieldId) {
        Type type;
        if (dataType instanceof BooleanType) {
            type = primitive(BOOLEAN, repetition).named(name);
        } else if (dataType instanceof ByteType ||
                dataType instanceof ShortType ||
                dataType instanceof IntegerType) {
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
                type = primitive(INT32, repetition)
                        .as(decimalAnnotation)
                        .named(name);
            } else if (precision <= DECIMAL_MAX_DIGITS_IN_LONG) {
                type = primitive(INT64, repetition)
                        .as(decimalAnnotation)
                        .named(name);
            } else {
                type = primitive(FIXED_LEN_BYTE_ARRAY, repetition)
                        .as(decimalAnnotation)
                        .length(MAX_BYTES_PER_PRECISION.get(precision))
                        .named(name);
            }
        } else if (dataType instanceof StringType) {
            type = primitive(BINARY, repetition)
                    .as(LogicalTypeAnnotation.stringType())
                    .named(name);
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
            type = primitive(INT64, repetition)
                    .as(timestampType(true /* isAdjustedToUTC */, MICROS))
                    .named(name);
        } else if (dataType instanceof TimestampNTZType) {
            // Write as INT64 with isAdjustedToUTC set to false
            type = primitive(INT64, repetition)
                    .as(timestampType(false /* isAdjustedToUTC */, MICROS))
                    .named(name);
        } else if (dataType instanceof ArrayType) {
            type = toParquetArrayType((ArrayType) dataType, name, repetition);
        } else if (dataType instanceof MapType) {
            type = toParquetMapType((MapType) dataType, name, repetition);
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

    private static Type toParquetArrayType(ArrayType arrayType, String name, Repetition rep) {
        // We will be supporting the 3-level array structure only. 2-level array structure will
        // be supported in the future.
        return Types
                .buildGroup(rep)
                .as(LogicalTypeAnnotation.listType())
                .addField(Types.repeatedGroup()
                        .addField(toParquetType(
                                arrayType.getElementType(),
                                "element", /* name */
                                arrayType.containsNull() ? OPTIONAL : REQUIRED,
                                Optional.empty()))
                        .named("list"))
                .named(name);
    }

    private static Type toParquetMapType(MapType mapType, String name, Repetition repetition) {
        // We will be supporting the 3-level array structure only. 2-level array structure will
        // be supported in the future.
        return Types.buildGroup(repetition)
                .as(LogicalTypeAnnotation.mapType())
                .addField(Types.repeatedGroup()
                        .addField(toParquetType(
                                mapType.getKeyType(),
                                "key", /* name */
                                REQUIRED, /* repetition */
                                Optional.empty()))
                        .addField(toParquetType(
                                mapType.getValueType(),
                                "value", /* name */
                                mapType.isValueContainsNull() ? OPTIONAL : REQUIRED,
                                Optional.empty()))
                        .named("key_value"))
                .named(name);
    }

    private static Type toParquetStructType(StructType structType, String name,
                                            Repetition repetition) {
        List<Type> fields = new ArrayList<>();
        for (StructField field : structType.fields()) {
            fields.add(toParquetType(
                    field.getDataType(),
                    field.getName(),
                    field.isNullable() ? OPTIONAL : REQUIRED,
                    getFieldId(field)));
        }
        return new GroupType(repetition, name, fields);
    }

    /**
     * Recursively checks whether the given data type has any Parquet field ids in it.
     */
    private static boolean hasFieldIds(DataType dataType) {
        if (dataType instanceof StructType) {
            StructType structType = (StructType) dataType;
            for (StructField field : structType.fields()) {
                if (hasFieldId(field.getMetadata()) || hasFieldIds(field.getDataType())) {
                    return true;
                }
            }
            return false;
        } else if (dataType instanceof ArrayType) {
            return hasFieldIds(((ArrayType) dataType).getElementType());
        } else if (dataType instanceof MapType) {
            MapType mapType = (MapType) dataType;
            return hasFieldIds(mapType.getKeyType()) || hasFieldIds(mapType.getValueType());
        }

        // Primitive types don't have metadata field. It will be checked as part of the
        // StructType check this primitive type is part of.
        return false;
    }

    private static boolean hasFieldId(FieldMetadata fieldMetadata) {
        return fieldMetadata.contains(ColumnMapping.PARQUET_FIELD_ID_KEY);
    }

    /**
     * Assumes the field id exists
     */
    private static int getFieldId(FieldMetadata fieldMetadata) {
        // Field id delta schema metadata is deserialized as long, but the range should always
        // be within integer range.
        Long fieldId = (Long) fieldMetadata.get(ColumnMapping.PARQUET_FIELD_ID_KEY);
        long fieldIdLong = fieldId.longValue();
        int fieldIdInt = (int) fieldIdLong;
        checkArgument(
                (long) fieldIdInt == fieldIdLong,
                "Field id out of range", fieldIdLong);
        return fieldIdInt;
    }

    private static Optional<Integer> getFieldId(StructField field) {
        if (hasFieldId(field.getMetadata())) {
            return Optional.of(getFieldId(field.getMetadata()));
        } else {
            return Optional.empty();
        }
    }
}
