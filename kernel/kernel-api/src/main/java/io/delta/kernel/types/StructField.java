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

package io.delta.kernel.types;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class StructField
{

    ////////////////////////////////////////////////////////////////////////////////
    // Static Fields / Methods
    ////////////////////////////////////////////////////////////////////////////////

    // TODO: for now we introduce isMetadataColumn as a field in the column metadata
    /**
     * Indicates a metadata column when present in the field metadata and the value is true
     */
    private static String IS_METADATA_COLUMN_KEY = "isMetadataColumn";

    /**
     * The name of a row index metadata column. When present this column must be populated with
     * row index of each row when reading from parquet.
     */
    public static String ROW_INDEX_COLUMN_NAME = "_metadata.row_index";
    public static StructField ROW_INDEX_COLUMN = new StructField(
            ROW_INDEX_COLUMN_NAME,
            LongType.INSTANCE,
            false,
            Collections.singletonMap(IS_METADATA_COLUMN_KEY, "true"));


    ////////////////////////////////////////////////////////////////////////////////
    // Instance Fields / Methods
    ////////////////////////////////////////////////////////////////////////////////

    private final String name;
    private final DataType dataType;
    private final boolean nullable;
    private final Map<String, String> metadata;

    public StructField(
        String name,
        DataType dataType,
        boolean nullable,
        Map<String, String> metadata)
    {
        this.name = name;
        this.dataType = dataType;
        this.nullable = nullable;
        this.metadata = metadata;
    }

    /**
     * @return the name of this field
     */
    public String getName()
    {
        return name;
    }

    /**
     * @return the data type of this field
     */
    public DataType getDataType()
    {
        return dataType;
    }

    /**
     * @return the metadata for this field
     */
    public Map<String, String> getMetadata()
    {
        return metadata;
    }

    /**
     * @return whether this field allows to have a {@code null} value.
     */
    public boolean isNullable()
    {
        return nullable;
    }

    public boolean isMetadataColumn() {
        return metadata.containsKey(IS_METADATA_COLUMN_KEY) &&
                Boolean.parseBoolean(metadata.get(IS_METADATA_COLUMN_KEY));
    }

    public boolean isDataColumn() {
        return !isMetadataColumn();
    }

    @Override
    public String toString()
    {
        return String.format("StructField(name=%s,type=%s,nullable=%s,metadata=%s)",
            name, dataType, nullable, "empty(fix - this)");
    }

    public String toJson()
    {
        String metadataAsJson = metadata.entrySet().stream()
            .map(e -> String.format("\"%s\" : \"%s\"", e.getKey(), e.getValue()))
            .collect(Collectors.joining(",\n"));

        return String.format(
            "{\n" +
                "  \"name\" : \"%s\",\n" +
                "  \"type\" : %s,\n" +
                "  \"nullable\" : %s, \n" +
                "  \"metadata\" : { %s }\n" +
                "}", name, dataType.toJson(), nullable, metadataAsJson);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        StructField that = (StructField) o;
        return nullable == that.nullable && name.equals(that.name) &&
            dataType.equals(that.dataType) &&
            metadata.equals(that.metadata);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, dataType, nullable, metadata);
    }
}
