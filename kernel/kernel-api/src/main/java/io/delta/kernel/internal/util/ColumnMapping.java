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

import java.util.Map;

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

                newSchema =
                    newSchema.add(physicalName, physicalType, logicalField.isNullable(), fieldMetadata);
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
}
