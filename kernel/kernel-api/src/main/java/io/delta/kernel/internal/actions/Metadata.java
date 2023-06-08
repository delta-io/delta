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
import java.util.stream.Collectors;

import io.delta.kernel.client.TableClient;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.types.ArrayType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.MapType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.Utils;

public class Metadata implements Action {
    public static Metadata fromRow(Row row, TableClient tableClient) {
        if (row == null) return null;
        final Map<String, String> configuration = row.getMap(6);
        final String schemaJson = row.getString(4);
        final List<String> partitionColumns = row.getList(5);
        ColumnarBatch output = tableClient.getJsonHandler().parseJson(
                Utils.singletonColumnVector(schemaJson),
                StructType.READ_SCHEMA);
        StructType schema = StructType.fromRow(output.getRows().next());

        return new Metadata(schemaJson, schema, partitionColumns, configuration);
    }

    public static final StructType READ_SCHEMA = new StructType()
        .add("id", StringType.INSTANCE)
        .add("name", StringType.INSTANCE)
        .add("description", StringType.INSTANCE)
        .add("format", Format.READ_SCHEMA)
        .add("schemaString", StringType.INSTANCE)
        .add("partitionColumns", new ArrayType(StringType.INSTANCE, false /* contains null */))
        .add("configuration", new MapType(StringType.INSTANCE, StringType.INSTANCE, false))
        .add("createdTime", LongType.INSTANCE);


    private final String schemaString;
    private final StructType schema;
    private final List<String> partitionColumns;
    private final Map<String, String> configuration;

    public Metadata(
            String schemaString,
            StructType schema,
            List<String> partitionColumns,
            Map<String, String> configuration) {
        this.schemaString = schemaString;
        this.schema = schema;
        if (partitionColumns == null) {
            partitionColumns = Collections.emptyList();
        }
        this.partitionColumns = partitionColumns;
        this.configuration = configuration;
    }

    public String getSchemaString()
    {
        return schemaString;
    }

    public StructType getSchema() {
        return schema;
    }

    public List<String> getPartitionColumns() {
        return partitionColumns;
    }

    public StructType getPartitionSchema() {
        return new StructType(
            partitionColumns.stream().map(schema::get).collect(Collectors.toList())
        );
    }

    public Map<String, String> getConfiguration() {
        return configuration;
    }
}
