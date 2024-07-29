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
package io.delta.kernel.internal.util;

import java.util.*;

import io.delta.kernel.types.*;

import io.delta.kernel.internal.actions.Metadata;

/**
 * Utilities related to the column mapping feature.
 */
public class ColumnMapping {
    private ColumnMapping() {}

    public static final String COLUMN_MAPPING_MODE_KEY = "delta.columnMapping.mode";
    public static final String COLUMN_MAPPING_MODE_NONE = "none";
    public static final String COLUMN_MAPPING_MODE_NAME = "name";
    public static final String COLUMN_MAPPING_MODE_ID = "id";
    public static final String COLUMN_MAPPING_PHYSICAL_NAME_KEY =
        "delta.columnMapping.physicalName";
    public static final String COLUMN_MAPPING_ID_KEY = "delta.columnMapping.id";

    public static final String PARQUET_FIELD_ID_KEY = "parquet.field.id";
    public static final String COLUMN_MAPPING_MAX_COLUMN_ID_KEY = "delta.columnMapping.maxColumnId";
    private static final Set<String> NAME_OR_ID = new HashSet<>(
            Arrays.asList(COLUMN_MAPPING_MODE_NAME, COLUMN_MAPPING_MODE_ID));

    /////////////////
    // Public APIs //
    /////////////////

    /**
     * Returns the column mapping mode from the given configuration.
     *
     * @param  configuration Configuration
     * @return Column mapping mode. One of ("none", "name", "id")
     */
    public static String getColumnMappingMode(Map<String, String> configuration) {
        return configuration.getOrDefault(
            COLUMN_MAPPING_MODE_KEY,
            COLUMN_MAPPING_MODE_NONE);
    }

    /**
     * Checks if the given column mapping mode in the given table metadata is supported. Throws on
     * unsupported modes.
     *
     * @param metadata Metadata of the table
     */
    public static void throwOnUnsupportedColumnMappingMode(Metadata metadata) {
        String columnMappingMode = getColumnMappingMode(metadata.getConfiguration());
        switch (columnMappingMode) {
            case COLUMN_MAPPING_MODE_NONE: // fall through
            case COLUMN_MAPPING_MODE_ID: // fall through
            case COLUMN_MAPPING_MODE_NAME:
                return;
            default:
                throw new UnsupportedOperationException(
                    "Unsupported column mapping mode: " + columnMappingMode);
        }
    }

    /**
     * Helper method that converts the logical schema (requested by the connector) to physical
     * schema of the data stored in data files based on the table's column mapping mode.
     *
     * @param logicalSchema     Logical schema of the scan
     * @param physicalSchema    Physical schema of the scan
     * @param columnMappingMode Column mapping mode
     */
    public static StructType convertToPhysicalSchema(
            StructType logicalSchema,
            StructType physicalSchema,
            String columnMappingMode) {
        switch (columnMappingMode) {
            case COLUMN_MAPPING_MODE_NONE:
                return logicalSchema;
            case COLUMN_MAPPING_MODE_ID: // fall through
            case COLUMN_MAPPING_MODE_NAME:
                boolean includeFieldIds = columnMappingMode.equals(COLUMN_MAPPING_MODE_ID);
                return convertToPhysicalSchema(logicalSchema, physicalSchema, includeFieldIds);
            default:
                throw new UnsupportedOperationException(
                    "Unsupported column mapping mode: " + columnMappingMode);
        }
    }

    /** Returns the physical name for a given {@link StructField} */
    public static String getPhysicalName(StructField field) {
        if (hasPhysicalName(field)) {
            // TODO: add method to FieldMetadata to extract this as String
            // instead of casting here
            return (String) field
                    .getMetadata()
                    .get(COLUMN_MAPPING_PHYSICAL_NAME_KEY);
        } else {
            return field.getName();
        }
    }

    public static void verifyColumnMappingChange(
            Map<String, String> oldConfig,
            Map<String, String> newConfig,
            boolean isNewTable) {
        String oldMappingMode = getColumnMappingMode(oldConfig);
        String newMappingMode = getColumnMappingMode(newConfig);

        Preconditions.checkArgument(isNewTable ||
                        allowMappingModeChange(oldMappingMode, newMappingMode),
                "Changing column mapping mode from '%s' to '%s' is not supported",
                oldMappingMode, newMappingMode);
    }

    public static boolean isColumnMappingModeEnabled(String columnMappingMode) {
        return NAME_OR_ID.contains(columnMappingMode);
    }

    public static Metadata updateColumnMappingMetadata(
            Metadata metadata,
            String columnMappingMode,
            boolean isNewTable) {
        switch (columnMappingMode) {
            case COLUMN_MAPPING_MODE_NONE:
                return metadata;
            case COLUMN_MAPPING_MODE_ID: // fall through
            case COLUMN_MAPPING_MODE_NAME:
                return assignColumnIdAndPhysicalName(metadata, isNewTable);
            default:
                throw new UnsupportedOperationException(
                        "Unsupported column mapping mode: " + columnMappingMode);
        }
    }

    ////////////////////////////
    // Private Helper Methods //
    ////////////////////////////

    /** Visible for testing */
    static int findMaxColumnId(StructType schema) {
        return schema.fields().stream()
                .filter(ColumnMapping::hasColumnId)
                .map(ColumnMapping::getColumnId)
                .max(Integer::compareTo).orElse(0);
    }

    /**
     * Utility method to convert the given logical schema to physical schema, recursively
     * converting sub-types in case of complex types. When {@code includeFieldId} is true,
     * converted physical schema will have field ids in the metadata.
     */
    private static StructType convertToPhysicalSchema(
            StructType logicalSchema,
            StructType physicalSchema,
            boolean includeFieldId) {
        StructType newSchema = new StructType();
        for (StructField logicalField : logicalSchema.fields()) {
            DataType logicalType = logicalField.getDataType();
            StructField physicalField = physicalSchema.get(logicalField.getName());
            DataType physicalType =
                convertToPhysicalType(
                    logicalType,
                    physicalField.getDataType(),
                    includeFieldId);
            String physicalName = (String) physicalField
                .getMetadata()
                .get(COLUMN_MAPPING_PHYSICAL_NAME_KEY);

            if (includeFieldId) {
                Long fieldId = (Long) physicalField
                    .getMetadata()
                    .get(COLUMN_MAPPING_ID_KEY);
                FieldMetadata fieldMetadata = FieldMetadata.builder()
                    .putLong(PARQUET_FIELD_ID_KEY, fieldId)
                    .build();

                newSchema = newSchema
                    .add(physicalName, physicalType, logicalField.isNullable(), fieldMetadata);
            } else {
                newSchema = newSchema.add(physicalName, physicalType, logicalField.isNullable());
            }
        }
        return newSchema;
    }

    private static DataType convertToPhysicalType(
            DataType logicalType,
            DataType physicalType,
            boolean includeFieldId) {
        if (logicalType instanceof StructType) {
            return convertToPhysicalSchema(
                (StructType) logicalType,
                (StructType) physicalType,
                includeFieldId);
        } else if (logicalType instanceof ArrayType) {
            ArrayType logicalArrayType = (ArrayType) logicalType;
            return new ArrayType(
                convertToPhysicalType(
                    logicalArrayType.getElementType(),
                    ((ArrayType) physicalType).getElementType(),
                    includeFieldId),
                logicalArrayType.containsNull());
        } else if (logicalType instanceof MapType) {
            MapType logicalMapType = (MapType) logicalType;
            MapType physicalMapType = (MapType) physicalType;
            return new MapType(
                convertToPhysicalType(
                    logicalMapType.getKeyType(),
                    physicalMapType.getKeyType(),
                    includeFieldId),
                convertToPhysicalType(
                    logicalMapType.getValueType(),
                    physicalMapType.getValueType(),
                    includeFieldId),
                logicalMapType.isValueContainsNull());
        }
        return logicalType;
    }

    private static boolean allowMappingModeChange(String oldMode, String newMode) {
        // only upgrade from none to name mapping is allowed
        return oldMode.equals(newMode) ||
                (COLUMN_MAPPING_MODE_NONE.equals(oldMode) &&
                        COLUMN_MAPPING_MODE_NAME.equals(newMode));
    }

    /**
     * For each column/field in a {@link Metadata}'s schema, assign an id using the current
     * maximum id as the basis and increment from there. Additionally, assign a physical name based
     * on a random UUID or re-use the old display name if the mapping mode is updated on an
     * existing table.
     * @param metadata The new metadata to assign ids and physical names to
     * @param isNewTable whether this is part of a commit that sets the mapping mode on a new table
     * @return {@link Metadata} with a new schema where ids and physical names have been assigned
     */
    private static Metadata assignColumnIdAndPhysicalName(Metadata metadata, boolean isNewTable) {
        StructType schema = metadata.getSchema();
        int maxColumnId = Math.max(
                Integer.parseInt(metadata.getConfiguration()
                        .getOrDefault(COLUMN_MAPPING_MAX_COLUMN_ID_KEY, "0")),
                findMaxColumnId(schema));
        StructType newSchema = new StructType();
        for (StructField field : schema.fields()) {
            StructField updatedField = field;

            if (!hasColumnId(field)) {
                maxColumnId++;
                updatedField = field.withNewMetadata(FieldMetadata.builder()
                        .fromMetadata(field.getMetadata())
                        .putLong(COLUMN_MAPPING_ID_KEY, maxColumnId)
                        .build());
            }
            if (!hasPhysicalName(field)) {
                // re-use old display names as physical names when a table is updated
                String physicalName = isNewTable ? "col-" + UUID.randomUUID() : field.getName();
                updatedField = updatedField.withNewMetadata(FieldMetadata.builder()
                        .fromMetadata(updatedField.getMetadata())
                        .putString(COLUMN_MAPPING_PHYSICAL_NAME_KEY, physicalName)
                        .build());
            }
            newSchema = newSchema.add(updatedField);
        }

        Map<String, String> config = new HashMap<>();
        config.put(COLUMN_MAPPING_MAX_COLUMN_ID_KEY, Integer.toString(maxColumnId));

        return metadata.withNewSchema(newSchema).withNewConfiguration(config);
    }

    private static boolean hasColumnId(StructField field) {
        return field.getMetadata().contains(COLUMN_MAPPING_ID_KEY);
    }

    private static boolean hasPhysicalName(StructField field) {
        return field.getMetadata().contains(COLUMN_MAPPING_PHYSICAL_NAME_KEY);
    }

    private static int getColumnId(StructField field) {
        // TODO: add a method to FieldMetadata to extract this as an int
        // instead of having to parse it here
        return Integer.parseInt(field.getMetadata().get(COLUMN_MAPPING_ID_KEY).toString());
    }
}
