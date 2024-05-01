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
import static java.lang.String.format;

import io.delta.kernel.types.*;

import static io.delta.kernel.internal.util.Preconditions.checkArgument;

/**
 * Utility methods for schema related operations such as validating the schema has no duplicate
 * columns and the names contain only valid characters.
 */
public class SchemaUtils {

    private SchemaUtils() {
    }

    /**
     * Validate the schema. This method checks if the schema has no duplicate columns and the names
     * contain only valid characters.
     *
     * @param schema                 the schema to validate
     * @param isColumnMappingEnabled whether column mapping is enabled. When column mapping is
     *                               enabled, the column names in the schema can contain special
     *                               characters that are allowed as column names in the Parquet
     *                               file
     * @throws IllegalArgumentException if the schema is invalid
     */
    public static void validateSchema(StructType schema, boolean isColumnMappingEnabled) {
        List<String> flattenColNames = flattenNestedFieldNames(schema);

        // check there are no duplicate column names in the schema
        Set<String> uniqueColNames = flattenColNames.stream()
                .map(String::toLowerCase)
                .collect(Collectors.toSet());

        if (uniqueColNames.size() != flattenColNames.size()) {
            Set<String> uniqueCols = new HashSet<>();
            String duplicateColumns = flattenColNames.stream()
                    .map(String::toLowerCase)
                    .filter(n -> !uniqueCols.add(n))
                    .collect(Collectors.joining(", "));
            throw new IllegalArgumentException(
                    "Schema contains duplicate columns: " + duplicateColumns);
        }

        // Check the column names are valid
        if (!isColumnMappingEnabled) {
            validParquetColumnNames(flattenColNames);
        } else {
            // when column mapping is enabled, just check the name contains no new line in it.
            flattenColNames.forEach(name -> checkArgument(
                    !name.contains("\\n"),
                    format("Attribute name '%s' contains invalid new line characters.", name)));
        }
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
            fieldNames.addAll(flattenNestedFieldNamesRecursive(escapedName, field.getDataType()));
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
                    flattenNestedFieldNamesRecursive(prefix + ".value", mapType.getValueType()));
        }
        return fieldNames;
    }

    private static String escapeDots(String name) {
        return name.contains(".") ? "`" + name + "`" : name;
    }

    protected static void validParquetColumnNames(List<String> columnNames) {
        for (String name : columnNames) {
            // ,;{}()\n\t= and space are special characters in Parquet schema
            checkArgument(
                    !name.matches(".*[ ,;{}()\n\t=].*"),
                    format("Attribute name '%s' contains invalid character(s) among" +
                            " ' ,;{}()\\n\\t='.", name));
        }
    }
}
