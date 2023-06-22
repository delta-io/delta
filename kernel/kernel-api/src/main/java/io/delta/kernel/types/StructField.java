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

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class StructField
{

    ////////////////////////////////////////////////////////////////////////////////
    // Static Fields / Methods
    ////////////////////////////////////////////////////////////////////////////////

    /**
     * The name of a row index metadata column. When present this column must be populated with
     * row index of each row when reading from parquet.
     */
    public static String ROW_INDEX_COLUMN_NAME = "_metadata.row_index";
    public static StructField ROW_INDEX_COLUMN = new StructField(
            ROW_INDEX_COLUMN_NAME,
            LongType.INSTANCE,
            false,
            new HashMap<String, String>(),
            ColumnType.METADATA);

    ////////////////////////////////////////////////////////////////////////////////
    // Instance Fields / Methods
    ////////////////////////////////////////////////////////////////////////////////

    // TODO: for now we introduce column type as an enum. Revisit this decision and finalize API.
    /**
     * Supported column types. Columns are either data or metadata columns. Metadata columns need
     * to be populated by the file reader.
     */
    public enum ColumnType {
        DATA,
        METADATA
    }

    private final String name;
    private final DataType dataType;
    private final boolean nullable;
    private final Map<String, String> metadata;
    private final ColumnType columnType;

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
        this.columnType = ColumnType.DATA;
    }

    public StructField(
            String name,
            DataType dataType,
            boolean nullable,
            Map<String, String> metadata,
            ColumnType columnType) {
        this.name = name;
        this.dataType = dataType;
        this.nullable = nullable;
        this.metadata = metadata;
        this.columnType = columnType;
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

    public boolean isDataColumn() {
        return columnType == ColumnType.DATA;
    }

    public boolean isMetadataColumn() {
        return columnType == ColumnType.METADATA;
    }

    @Override
    public String toString()
    {
        return String.format("StructField(name=%s,type=%s,nullable=%s,metadata=%s,columnType=%s)",
                name, dataType, nullable, "empty(fix - this)", columnType.name());
    }

    // TODO: what about column type?
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
            metadata.equals(that.metadata) &&
            columnType.equals(that.columnType);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, dataType, nullable, metadata, columnType);
    }
}
