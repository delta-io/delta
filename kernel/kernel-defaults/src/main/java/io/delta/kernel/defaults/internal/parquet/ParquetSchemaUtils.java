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

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

import io.delta.kernel.types.*;

import io.delta.kernel.internal.util.ColumnMapping;
import static io.delta.kernel.internal.util.Preconditions.checkArgument;

/**
 * Utility methods for Delta schema to Parquet schema conversion.
 */
class ParquetSchemaUtils {
    private ParquetSchemaUtils() {}
    /**
     * Given the file schema in Parquet file and selected columns by Delta, return
     * a subschema of the file schema.
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
     * Search for the Parquet type in {@code groupType} of subfield which is equivalent to
     * given {@code field}.
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
            .collect(
                Collectors.toMap(
                    subFieldType -> subFieldType.getId().intValue(),
                    subFieldType -> subFieldType,
                    (u, v) -> {
                        throw new IllegalStateException(String.format("Parquet file contains " +
                            "multiple columns (%s, %s) with the same field id", u, v));
                    }));
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

    private static Boolean hasFieldId(FieldMetadata fieldMetadata) {
        return fieldMetadata.contains(ColumnMapping.PARQUET_FIELD_ID_KEY);
    }

    /** Assumes the field id exists */
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
}
