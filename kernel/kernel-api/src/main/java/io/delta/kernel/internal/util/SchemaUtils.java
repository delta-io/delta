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
import java.util.stream.Collectors;

import io.delta.kernel.expressions.Literal;
import io.delta.kernel.types.*;

import io.delta.kernel.internal.DeltaErrors;
import static io.delta.kernel.internal.DeltaErrors.*;
import static io.delta.kernel.internal.util.Preconditions.checkArgument;

/**
 * Utility methods for schema related operations such as validating the schema has no duplicate
 * columns and the names contain only valid characters.
 */
public class SchemaUtils {

    private SchemaUtils() {
    }

    public static void validateSchemaEvolution(StructType oldSchema, StructType newSchema) {
        List<String> fieldNames = oldSchema.fieldNames();
        for (String fieldName : fieldNames) {
            if (newSchema.indexOf(fieldName) == -1) {
                throw unsupportedSchemaColumnDrop();
            }
        }
    }

    /**
     * Validate the schema. This method checks if the schema has no duplicate columns, the names
     * contain only valid characters and the data types are supported.
     *
     * @param schema                 the schema to validate
     * @param isColumnMappingEnabled whether column mapping is enabled. When column mapping is
     *                               enabled, the column names in the schema can contain special
     *                               characters that are allowed as column names in the Parquet
     *                               file
     * @throws IllegalArgumentException if the schema is invalid
     */
    public static void validateSchema(StructType schema, boolean isColumnMappingEnabled) {
        checkArgument(schema.length() > 0, "Schema should contain at least one column");

        List<String> flattenColNames = flattenNestedFieldNames(schema);

        // check there are no duplicate column names in the schema
        Set<String> uniqueColNames = flattenColNames.stream()
                .map(String::toLowerCase)
                .collect(Collectors.toSet());

        if (uniqueColNames.size() != flattenColNames.size()) {
            Set<String> uniqueCols = new HashSet<>();
            List<String> duplicateColumns = flattenColNames.stream()
                    .map(String::toLowerCase)
                    .filter(n -> !uniqueCols.add(n))
                    .collect(Collectors.toList());
            throw DeltaErrors.duplicateColumnsInSchema(schema, duplicateColumns);
        }

        // Check the column names are valid
        if (!isColumnMappingEnabled) {
            validParquetColumnNames(flattenColNames);
        } else {
            // when column mapping is enabled, just check the name contains no new line in it.
            flattenColNames.forEach(name -> {
                if (name.contains("\\n")) {
                    throw invalidColumnName(name, "\\n");
                }
            });
        }

        validateSupportedType(schema);
    }

    /**
     * Verify the partition columns exists in the table schema and a supported data type for a
     * partition column.
     *
     * @param schema
     * @param partitionCols
     */
    public static void validatePartitionColumns(StructType schema, List<String> partitionCols) {
        // partition columns are always the top-level columns
        Map<String, DataType> columnNameToType = schema.fields().stream()
                .collect(Collectors.toMap(
                        field -> field.getName().toLowerCase(Locale.ROOT),
                        StructField::getDataType));

        partitionCols.stream().forEach(partitionCol -> {
            DataType dataType = columnNameToType.get(partitionCol.toLowerCase(Locale.ROOT));
            checkArgument(
                    dataType != null,
                    "Partition column " + partitionCol + " not found in the schema");

            if (!(dataType instanceof BooleanType ||
                    dataType instanceof ByteType ||
                    dataType instanceof ShortType ||
                    dataType instanceof IntegerType ||
                    dataType instanceof LongType ||
                    dataType instanceof FloatType ||
                    dataType instanceof DoubleType ||
                    dataType instanceof DecimalType ||
                    dataType instanceof StringType ||
                    dataType instanceof BinaryType ||
                    dataType instanceof DateType ||
                    dataType instanceof TimestampType)) {
                throw unsupportedPartitionDataType(partitionCol, dataType);
            }
        });
    }

    /**
     * Delta expects partition column names to be same case preserving as the name in the schema.
     * E.g: Schema: (a INT, B STRING) and partition columns: (b). In this case we store the
     * schema as (a INT, B STRING) and partition columns as (B).
     *
     * This method expects the inputs are already validated (i.e. schema contains all the partition
     * columns).
     */
    public static List<String> casePreservingPartitionColNames(
            StructType tableSchema,
            List<String> partitionColumns) {
        Map<String, String> columnNameMap = new HashMap<>();
        tableSchema.fieldNames().forEach(colName ->
                columnNameMap.put(colName.toLowerCase(Locale.ROOT), colName));
        return partitionColumns.stream()
                .map(colName -> columnNameMap.get(colName.toLowerCase(Locale.ROOT)))
                .collect(Collectors.toList());
    }

    /**
     * Convert the partition column names in {@code partitionValues} map into the same case as the
     * column in the table metadata. Delta expects the partition column names to preserve the case
     * same as the table schema.
     *
     * @param partitionColNames List of partition columns in the table metadata. The names preserve
     *                          the case as given by the connector when the table is created.
     * @param partitionValues   Map of partition column name to partition value. Convert the
     *                          partition column name to be same case preserving name as its
     *                          equivalent column in the {@code partitionColName}. Column name
     *                          comparison is case-insensitive.
     * @return Rewritten {@code partitionValues} map with names case preserved.
     */
    public static Map<String, Literal> casePreservingPartitionColNames(
            List<String> partitionColNames,
            Map<String, Literal> partitionValues) {
        Map<String, String> partitionColNameMap = new HashMap<>();
        partitionColNames.forEach(colName ->
                partitionColNameMap.put(colName.toLowerCase(Locale.ROOT), colName));

        return partitionValues.entrySet().stream()
                .collect(Collectors.toMap(
                        entry -> partitionColNameMap.get(
                                entry.getKey().toLowerCase(Locale.ROOT)),
                        Map.Entry::getValue));
    }

    /**
     * Search (case-insensitive) for the given {@code colName} in the {@code schema} and return its
     * position in the {@code schema}.
     *
     * @param schema  {@link StructType}
     * @param colName Name of the column whose index is needed.
     * @return Valid index or -1 if not found.
     */
    public static int findColIndex(StructType schema, String colName) {
        for (int i = 0; i < schema.length(); i++) {
            if (schema.at(i).getName().equalsIgnoreCase(colName)) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Returns all column names in this schema as a flat list. For example, a schema like:
     * <pre>
     *   | - a
     *   | | - 1
     *   | | - 2
     *   | - b
     *   | - c
     *   | | - nest
     *   |   | - 3
     *   will get flattened to: "a", "a.1", "a.2", "b", "c", "c.nest", "c.nest.3"
     * </pre>
     */
    private static List<String> flattenNestedFieldNames(StructType schema) {
        List<String> fieldNames = new ArrayList<>();
        for (StructField field : schema.fields()) {
            String escapedName = escapeDots(field.getName());
            fieldNames.add(escapedName);
            fieldNames.addAll(
                    flattenNestedFieldNamesRecursive(escapedName, field.getDataType()));
        }
        return fieldNames;
    }

    private static List<String> flattenNestedFieldNamesRecursive(String prefix, DataType type) {
        List<String> fieldNames = new ArrayList<>();
        if (type instanceof StructType) {
            for (StructField field : ((StructType) type).fields()) {
                String escapedName = escapeDots(field.getName());
                fieldNames.add(prefix + "." + escapedName);
                fieldNames.addAll(flattenNestedFieldNamesRecursive(
                        prefix + "." + escapedName, field.getDataType()));
            }
        } else if (type instanceof ArrayType) {
            fieldNames.addAll(flattenNestedFieldNamesRecursive(
                    prefix + ".element",
                    ((ArrayType) type).getElementType()));
        } else if (type instanceof MapType) {
            MapType mapType = (MapType) type;
            fieldNames.addAll(
                    flattenNestedFieldNamesRecursive(prefix + ".key", mapType.getKeyType()));
            fieldNames.addAll(
                    flattenNestedFieldNamesRecursive(prefix + ".value",
                            mapType.getValueType()));
        }
        return fieldNames;
    }

    private static String escapeDots(String name) {
        return name.contains(".") ? "`" + name + "`" : name;
    }

    protected static void validParquetColumnNames(List<String> columnNames) {
        for (String name : columnNames) {
            // ,;{}()\n\t= and space are special characters in Parquet schema
            if (name.matches(".*[ ,;{}()\n\t=].*")) {
                throw invalidColumnName(name, "[ ,;{}()\\n\\t=]");
            }
        }
    }

    /**
     * Validate the supported data types. Once we start supporting additional types, take input the
     * protocol features and validate the schema.
     *
     * @param dataType the data type to validate
     */
    protected static void validateSupportedType(DataType dataType) {
        if (dataType instanceof BooleanType ||
                dataType instanceof ByteType ||
                dataType instanceof ShortType ||
                dataType instanceof IntegerType ||
                dataType instanceof LongType ||
                dataType instanceof FloatType ||
                dataType instanceof DoubleType ||
                dataType instanceof DecimalType ||
                dataType instanceof StringType ||
                dataType instanceof BinaryType ||
                dataType instanceof DateType ||
                dataType instanceof TimestampType) {
            // supported types
            return;
        } else if (dataType instanceof StructType) {
            ((StructType) dataType).fields()
                    .forEach(field -> validateSupportedType(field.getDataType()));
        } else if (dataType instanceof ArrayType) {
            validateSupportedType(((ArrayType) dataType).getElementType());
        } else if (dataType instanceof MapType) {
            validateSupportedType(((MapType) dataType).getKeyType());
            validateSupportedType(((MapType) dataType).getValueType());
        } else {
            throw unsupportedDataType(dataType);
        }
    }

}
