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

import io.delta.kernel.exceptions.InvalidConfigurationValueException;
import io.delta.kernel.types.*;

import io.delta.kernel.internal.actions.Metadata;

/**
 * Utilities related to the column mapping feature.
 */
public class ColumnMapping {
    private ColumnMapping() {}

    public enum ColumnMappingMode {
        NONE("none"),
        ID("id"),
        NAME("name");

        public final String value;

        ColumnMappingMode(String value) {
            this.value = value;
        }

        public static ColumnMappingMode fromTableConfig(String modeString) {
            for (ColumnMappingMode mode : ColumnMappingMode.values()) {
                if (mode.value.equalsIgnoreCase(modeString)) {
                    return mode;
                }
            }
            throw new InvalidConfigurationValueException(
                    COLUMN_MAPPING_MODE_KEY,
                    modeString,
                    String.format("Needs to be one of: %s.",
                            Arrays.toString(ColumnMappingMode.values())));
        }

        @Override
        public String toString() {
            return this.value;
        }
    }

    public static final String COLUMN_MAPPING_MODE_KEY = "delta.columnMapping.mode";
    public static final String COLUMN_MAPPING_PHYSICAL_NAME_KEY =
        "delta.columnMapping.physicalName";
    public static final String COLUMN_MAPPING_ID_KEY = "delta.columnMapping.id";

    public static final String PARQUET_FIELD_ID_KEY = "parquet.field.id";
    public static final String COLUMN_MAPPING_MAX_COLUMN_ID_KEY = "delta.columnMapping.maxColumnId";

    /////////////////
    // Public APIs //
    /////////////////

    /**
     * Returns the column mapping mode from the given configuration.
     *
     * @param  configuration Configuration
     * @return Column mapping mode. One of ("none", "name", "id")
     */
    public static ColumnMappingMode getColumnMappingMode(Map<String, String> configuration) {
        return Optional.ofNullable(configuration.get(COLUMN_MAPPING_MODE_KEY))
                .map(ColumnMappingMode::fromTableConfig)
                .orElse(ColumnMappingMode.NONE);
    }

    /**
     * Checks if the given column mapping mode in the given table metadata is supported. Throws on
     * unsupported modes.
     *
     * @param metadata Metadata of the table
     */
    public static void throwOnUnsupportedColumnMappingMode(Metadata metadata) {
        getColumnMappingMode(metadata.getConfiguration());
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
            ColumnMappingMode columnMappingMode) {
        switch (columnMappingMode) {
            case NONE:
                return logicalSchema;
            case ID: // fall through
            case NAME:
                boolean includeFieldIds = columnMappingMode == ColumnMappingMode.ID;
                return convertToPhysicalSchema(logicalSchema, physicalSchema, includeFieldIds);
            default:
                throw new UnsupportedOperationException(
                    "Unsupported column mapping mode: " + columnMappingMode);
        }
    }

    /** Returns the physical name for a given {@link StructField} */
    public static String getPhysicalName(StructField field) {
        if (hasPhysicalName(field)) {
            return field.getMetadata().getString(COLUMN_MAPPING_PHYSICAL_NAME_KEY);
        } else {
            return field.getName();
        }
    }

    public static void verifyColumnMappingChange(
            Map<String, String> oldConfig,
            Map<String, String> newConfig,
            boolean isNewTable) {
        ColumnMappingMode oldMappingMode = getColumnMappingMode(oldConfig);
        ColumnMappingMode newMappingMode = getColumnMappingMode(newConfig);

        Preconditions.checkArgument(isNewTable ||
                        validModeChange(oldMappingMode, newMappingMode),
                "Changing column mapping mode from '%s' to '%s' is not supported",
                oldMappingMode, newMappingMode);
    }

    public static boolean isColumnMappingModeEnabled(ColumnMappingMode columnMappingMode) {
        return columnMappingMode == ColumnMappingMode.ID ||
                columnMappingMode == ColumnMappingMode.NAME;
    }

    public static Metadata updateColumnMappingMetadata(
            Metadata metadata,
            ColumnMappingMode columnMappingMode,
            boolean isNewTable) {
        switch (columnMappingMode) {
            case NONE:
                return metadata;
            case ID: // fall through
            case NAME:
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
            String physicalName = physicalField
                .getMetadata()
                .getString(COLUMN_MAPPING_PHYSICAL_NAME_KEY);

            if (includeFieldId) {
                Long fieldId = physicalField.getMetadata().getLong(COLUMN_MAPPING_ID_KEY);
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

    private static boolean validModeChange(ColumnMappingMode oldMode, ColumnMappingMode newMode) {
        // only upgrade from none to name mapping is allowed
        return oldMode.equals(newMode) ||
                (oldMode == ColumnMappingMode.NONE && newMode == ColumnMappingMode.NAME);
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
        return field.getMetadata().getLong(COLUMN_MAPPING_ID_KEY).intValue();
    }
}
