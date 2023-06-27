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
package io.delta.kernel.internal.actions;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import io.delta.kernel.client.TableClient;
import io.delta.kernel.data.Row;
import io.delta.kernel.types.ArrayType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.MapType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;

import io.delta.kernel.internal.types.TableSchemaSerDe;

import static io.delta.kernel.utils.Utils.requireNonNull;
import static java.util.Objects.requireNonNull;

public class Metadata implements Action
{
    public static Metadata fromRow(Row row, TableClient tableClient)
    {
        if (row == null) {
            return null;
        }

        final String schemaJson = requireNonNull(row, 4, "schemaString").getString(4);
        StructType schema = TableSchemaSerDe.fromJson(tableClient.getJsonHandler(), schemaJson);

        return new Metadata(
            requireNonNull(row, 0, "id").getString(0),
            Optional.ofNullable(row.isNullAt(1) ? null : row.getString(1)),
            Optional.ofNullable(row.isNullAt(2) ? null : row.getString(2)),
            Format.fromRow(requireNonNull(row, 0, "id").getStruct(3)),
            schemaJson,
            schema,
            row.getArray(5),
            Optional.ofNullable(row.isNullAt(6) ? null : row.getLong(6)),
            row.getMap(7)
        );
    }

    public static final StructType READ_SCHEMA = new StructType()
        .add("id", StringType.INSTANCE, false /* nullable */)
        .add("name", StringType.INSTANCE, true /* nullable */)
        .add("description", StringType.INSTANCE, true /* nullable */)
        .add("format", Format.READ_SCHEMA, false /* nullable */)
        .add("schemaString", StringType.INSTANCE, false /* nullable */)
        .add("partitionColumns",
            new ArrayType(StringType.INSTANCE, false /* contains null */),
            false /* nullable */)
        .add("createdTime", LongType.INSTANCE, true /* contains null */)
        .add("configuration",
            new MapType(StringType.INSTANCE, StringType.INSTANCE, false),
            false /* contains null */);

    private final String id;
    private final Optional<String> name;
    private final Optional<String> description;
    private final Format format;
    private final String schemaString;
    private final StructType schema;
    private final List<String> partitionColumns;
    private final Optional<Long> createdTime;
    private final Map<String, String> configuration;

    public Metadata(
        String id,
        Optional<String> name,
        Optional<String> description,
        Format format,
        String schemaString,
        StructType schema,
        List<String> partitionColumns,
        Optional<Long> createdTime,
        Map<String, String> configuration)
    {
        this.id = requireNonNull(id, "id is null");
        this.name = name;
        this.description = requireNonNull(description, "description is null");
        this.format = requireNonNull(format, "format is null");
        this.schemaString = requireNonNull(schemaString, "schemaString is null");
        this.schema = schema;
        this.partitionColumns =
            partitionColumns == null ? Collections.emptyList() : partitionColumns;
        this.createdTime = createdTime;
        this.configuration = configuration == null ? Collections.emptyMap() : configuration;
    }

    public String getSchemaString()
    {
        return schemaString;
    }

    public StructType getSchema()
    {
        return schema;
    }

    public List<String> getPartitionColumns()
    {
        return partitionColumns;
    }

    public String getId()
    {
        return id;
    }

    public Optional<String> getName()
    {
        return name;
    }

    public Optional<String> getDescription()
    {
        return description;
    }

    public Format getFormat()
    {
        return format;
    }

    public Optional<Long> getCreatedTime()
    {
        return createdTime;
    }

    public Map<String, String> getConfiguration()
    {
        return configuration;
    }
}
