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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import io.delta.kernel.exceptions.InvalidConfigurationValueException;
import io.delta.kernel.types.*;

import io.delta.kernel.internal.TableConfig;
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
    public static final String COLUMN_MAPPING_NESTED_IDS_KEY = "delta.columnMapping.nested.ids";

    public static final String PARQUET_FIELD_ID_KEY = "parquet.field.id";
    public static final String PARQUET_FIELD_NESTED_IDS_METADATA_KEY = "parquet.field.nested.ids";
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
        int maxColumnId = 0;
        for (StructField field : schema.fields()) {
            maxColumnId = Math.max(maxColumnId, findMaxColumnId(field, maxColumnId));
        }
        return maxColumnId;
    }

    private static int findMaxColumnId(StructField field, int maxColumnId) {
        if (hasColumnId(field)) {
            maxColumnId = Math.max(maxColumnId, getColumnId(field));

            if (hasNestedColumnIds(field)) {
                maxColumnId = Math.max(maxColumnId, getMaxNestedColumnId(field));
            }
        }

        if (field.getDataType() instanceof StructType) {
            StructType structType = (StructType) field.getDataType();
            for (StructField structField : structType.fields()) {
                maxColumnId = Math.max(maxColumnId, findMaxColumnId(structField, maxColumnId));
            }
            return maxColumnId;
        } else if (field.getDataType() instanceof ArrayType) {
            ArrayType arrayType = (ArrayType) field.getDataType();
            return Math.max(maxColumnId, findMaxColumnId(arrayType.getElementField(), maxColumnId));
        } else if (field.getDataType() instanceof MapType) {
            MapType mapType = (MapType) field.getDataType();
            return Math.max(
                    findMaxColumnId(mapType.getKeyField(), maxColumnId),
                    findMaxColumnId(mapType.getValueField(), maxColumnId));
        }
        return maxColumnId;
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
                FieldMetadata.Builder builder = FieldMetadata.builder()
                    .putLong(PARQUET_FIELD_ID_KEY, fieldId);

                // Nested field IDs for the 'element' and 'key'/'value' fields of Arrays/Maps
                // are written when Uniform with IcebergCompatV2 is enabled on a table
                if (hasNestedColumnIds(physicalField)) {
                    builder.putFieldMetadata(
                            PARQUET_FIELD_NESTED_IDS_METADATA_KEY,
                            getNestedColumnIds(physicalField));
                }

                newSchema = newSchema
                    .add(physicalName, physicalType, logicalField.isNullable(), builder.build());
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
        AtomicInteger maxColumnId = new AtomicInteger(
                Math.max(
                Integer.parseInt(metadata.getConfiguration()
                        .getOrDefault(COLUMN_MAPPING_MAX_COLUMN_ID_KEY, "0")),
                findMaxColumnId(schema)));
        StructType newSchema = new StructType();
        for (StructField field : schema.fields()) {
            newSchema = newSchema.add(
                    transformSchema(
                            assignColumnIdAndPhysicalName(field, maxColumnId, isNewTable),
                            maxColumnId,
                            isNewTable));
        }

        if (Boolean.parseBoolean(metadata.getConfiguration()
                .getOrDefault(TableConfig.ICEBERG_COMPAT_V2_ENABLED.getKey(), "false"))) {
            newSchema = rewriteFieldIdsForIceberg(newSchema, maxColumnId);
        }

        Map<String, String> config = new HashMap<>();
        config.put(COLUMN_MAPPING_MAX_COLUMN_ID_KEY, Integer.toString(maxColumnId.get()));

        return metadata.withNewSchema(newSchema).withNewConfiguration(config);
    }

    private static StructField transformSchema(
            StructField field, AtomicInteger maxColumnId, boolean isNewTable) {
        DataType dataType = field.getDataType();
        if (dataType instanceof StructType) {
            StructType type = (StructType) dataType;
            StructType schema = new StructType();
            for (StructField f : type.fields()) {
                schema = schema.add(
                        transformSchema(
                                assignColumnIdAndPhysicalName(f, maxColumnId, isNewTable),
                                maxColumnId,
                                isNewTable));
            }
            return new StructField(
                    field.getName(), schema, field.isNullable(), field.getMetadata());
        } else if (dataType instanceof ArrayType) {
            ArrayType type = (ArrayType) dataType;
            StructField elementField = transformSchema(
                    type.getElementField(), maxColumnId, isNewTable);
            return new StructField(
                    field.getName(),
                    new ArrayType(elementField),
                    field.isNullable(),
                    field.getMetadata());
        } else if (dataType instanceof MapType) {
            MapType type = (MapType) dataType;
            StructField key = transformSchema(type.getKeyField(), maxColumnId, isNewTable);
            StructField value = transformSchema(type.getValueField(), maxColumnId, isNewTable);
            return new StructField(
                    field.getName(),
                    new MapType(key, value),
                    field.isNullable(),
                    field.getMetadata());
        }
        return field;
    }

    private static StructField assignColumnIdAndPhysicalName(
            StructField field, AtomicInteger maxColumnId, boolean isNewTable) {
        if (!hasColumnId(field)) {
            field = field.withNewMetadata(FieldMetadata.builder()
                    .fromMetadata(field.getMetadata())
                    .putLong(COLUMN_MAPPING_ID_KEY, maxColumnId.incrementAndGet())
                    .build());
        }
        if (!hasPhysicalName(field)) {
            // re-use old display names as physical names when a table is updated
            String physicalName = isNewTable ? "col-" + UUID.randomUUID() : field.getName();
            field = field.withNewMetadata(FieldMetadata.builder()
                    .fromMetadata(field.getMetadata())
                    .putString(COLUMN_MAPPING_PHYSICAL_NAME_KEY, physicalName)
                    .build());
        }
        return field;
    }

    /**
     * Adds the nested field IDs required by Iceberg.
     *
     * In parquet, list-type columns have a nested, implicitly defined {@code element} field and
     * map-type columns have implicitly defined {@code key} and {@code value} fields. By default,
     * Spark does not write field IDs for these fields in the parquet files. However, Iceberg
     * requires these *nested* field IDs to be present. This method rewrites the specified
     * schema to add those nested field IDs.
     *
     * Nested field IDs are stored in a map as part of the metadata of the
     * *nearest* parent {@link StructField}.
     * For example, consider the following schema:
     *
     * col1 ARRAY(INT)
     * col2 MAP(INT, INT)
     * col3 STRUCT(a INT, b ARRAY(STRUCT(c INT, d MAP(INT, INT))))
     *
     * col1 is a list and so requires one nested field ID for the {@code element} field in parquet.
     * This nested field ID will be stored in a map that is part
     * of col1's {@link StructField#getMetadata()}.
     * The same applies to the nested field IDs for col2's implicit {@code key} and {@code value}
     * fields. col3 itself is a Struct, consisting of an integer field and a list field named 'b'.
     * The nested field ID for the list of 'b' is stored in b's {@link StructField#getMetadata()}.
     * Finally, the list type itself is again a struct consisting of an integer field
     * and a map field named 'd'.
     * The nested field IDs for the map of 'd' are stored in d's {@link StructField#getMetadata()}.
     *
     * @param schema  The schema to which nested field IDs should be added
     * @param startId The first field ID to use for the nested field IDs
     */
    private static StructType rewriteFieldIdsForIceberg(StructType schema, AtomicInteger startId) {
        StructType newSchema = new StructType();
        for (StructField field : schema.fields()) {
            StructField structField = transformSchema(startId, field, "");
            newSchema = newSchema.add(structField);
        }
        return newSchema;
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

    private static boolean hasNestedColumnIds(StructField field) {
        return field.getMetadata().contains(COLUMN_MAPPING_NESTED_IDS_KEY);
    }

    private static FieldMetadata getNestedColumnIds(StructField field) {
        return field.getMetadata().getMetadata(COLUMN_MAPPING_NESTED_IDS_KEY);
    }

    private static int getMaxNestedColumnId(StructField field) {
        return getNestedColumnIds(field).getEntries().values().stream()
                .filter(v -> v instanceof Long)
                .map(v -> (Long) v)
                .max(Long::compare)
                .orElse(0L)
                .intValue();
    }

    private static StructField transformSchema(
            AtomicInteger currentFieldId,
            StructField structField,
            String path) {
        DataType dataType = structField.getDataType();
        if (dataType instanceof StructType) {
            StructType type = (StructType) dataType;
            List<StructField> fields =
                    type.fields().stream()
                            .map(field -> {
                                StructField nestedField = transformSchema(
                                        currentFieldId,
                                        field,
                                        getPhysicalName(field));
                                return new StructField(
                                        nestedField.getName(),
                                        nestedField.getDataType(),
                                        nestedField.isNullable(),
                                        nestedField.getMetadata());
                            })
                            .collect(Collectors.toList());
            return new StructField(
                    structField.getName(),
                    new StructType(fields),
                    structField.isNullable(),
                    structField.getMetadata());
        } else if (dataType instanceof ArrayType) {
            ArrayType type = (ArrayType) dataType;
            path = "".equals(path) ? getPhysicalName(structField) : path;
            // update element type metadata and recurse into element type
            String elementPath = path + "." + type.getElementField().getName();
            structField = maybeUpdateFieldId(structField, elementPath, currentFieldId);
            StructField elementType = transformSchema(
                    currentFieldId, type.getElementField(), elementPath);
            return new StructField(
                    structField.getName(),
                    new ArrayType(elementType),
                    structField.isNullable(),
                    structField.getMetadata());
        } else if (dataType instanceof MapType) {
            MapType type = (MapType) dataType;
            // update key type metadata and recurse into key type
            path = "".equals(path) ? getPhysicalName(structField) : path;
            String keyPath = path + "." + type.getKeyField().getName();
            structField = maybeUpdateFieldId(structField, keyPath, currentFieldId);
            StructField key = transformSchema(currentFieldId, type.getKeyField(), keyPath);
            // update value type metadata and recurse into value type
            String valuePath = path + "." + type.getValueField().getName();
            structField = maybeUpdateFieldId(structField, valuePath, currentFieldId);
            StructField value = transformSchema(currentFieldId, type.getValueField(), valuePath);
            return new StructField(
                    structField.getName(),
                    new MapType(key, value),
                    structField.isNullable(),
                    structField.getMetadata());
        }

        return structField;
    }

    private static StructField maybeUpdateFieldId(
            StructField field, String key, AtomicInteger currentFieldId) {
        if (!field.getMetadata().contains(COLUMN_MAPPING_NESTED_IDS_KEY)) {
            FieldMetadata.Builder nestedIdsBuilder =
                    initNestedIdsMetadataBuilder(field);
            FieldMetadata metadata = FieldMetadata.builder()
                    .fromMetadata(field.getMetadata())
                    .putFieldMetadata(
                            COLUMN_MAPPING_NESTED_IDS_KEY,
                            nestedIdsBuilder.build())
                    .build();
            field = field.withNewMetadata(metadata);
        }

        FieldMetadata nestedMeta = field
                .getMetadata()
                .getMetadata(COLUMN_MAPPING_NESTED_IDS_KEY);
        if (!nestedMeta.contains(key)) {
            FieldMetadata newNestedMeta = FieldMetadata.builder()
                    .fromMetadata(nestedMeta)
                    .putLong(key, currentFieldId.incrementAndGet())
                    .build();
            return field.withNewMetadata(
                    FieldMetadata.builder()
                            .fromMetadata(field.getMetadata())
                            .putFieldMetadata(COLUMN_MAPPING_NESTED_IDS_KEY, newNestedMeta)
                            .build());
        }
        return field;
    }

    private static FieldMetadata.Builder initNestedIdsMetadataBuilder(StructField field) {
        if (hasNestedColumnIds(field)) {
            return FieldMetadata.builder().fromMetadata(getNestedColumnIds(field));
        }
        return FieldMetadata.builder();
    }
}
