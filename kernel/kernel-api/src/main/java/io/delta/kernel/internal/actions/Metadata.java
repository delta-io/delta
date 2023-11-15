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
import java.util.Map;
import java.util.Optional;
import static java.util.Objects.requireNonNull;

import io.delta.kernel.client.TableClient;
import io.delta.kernel.data.ArrayValue;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.MapValue;
import io.delta.kernel.types.*;

import io.delta.kernel.internal.lang.Lazy;
import io.delta.kernel.internal.util.VectorUtils;
import static io.delta.kernel.internal.util.InternalUtils.requireNonNull;

public class Metadata {

    public static Metadata fromColumnVector(
            ColumnVector vector, int rowId, TableClient tableClient) {
        if (vector.isNullAt(rowId)) {
            return null;
        }

        final String schemaJson = requireNonNull(vector.getChild(4), rowId, "schemaString")
            .getString(rowId);
        StructType schema = tableClient.getJsonHandler().deserializeStructType(schemaJson);

        return new Metadata(
            requireNonNull(vector.getChild(0), rowId, "id").getString(rowId),
            Optional.ofNullable(vector.getChild(1).isNullAt(rowId) ? null :
                vector.getChild(1).getString(rowId)),
            Optional.ofNullable(vector.getChild(2).isNullAt(rowId) ? null :
                vector.getChild(2).getString(rowId)),
            Format.fromColumnVector(requireNonNull(vector.getChild(3), rowId, "format"), rowId),
            schemaJson,
            schema,
            vector.getChild(5).getArray(rowId),
            Optional.ofNullable(vector.getChild(6).isNullAt(rowId) ? null :
                vector.getChild(6).getLong(rowId)),
            vector.getChild(7).getMap(rowId)
        );
    }

    public static final StructType READ_SCHEMA = new StructType()
        .add("id", StringType.STRING, false /* nullable */)
        .add("name", StringType.STRING, true /* nullable */)
        .add("description", StringType.STRING, true /* nullable */)
        .add("format", Format.READ_SCHEMA, false /* nullable */)
        .add("schemaString", StringType.STRING, false /* nullable */)
        .add("partitionColumns",
            new ArrayType(StringType.STRING, false /* contains null */),
            false /* nullable */)
        .add("createdTime", LongType.LONG, true /* contains null */)
        .add("configuration",
            new MapType(StringType.STRING, StringType.STRING, false),
            false /* nullable */);

    private final String id;
    private final Optional<String> name;
    private final Optional<String> description;
    private final Format format;
    private final String schemaString;
    private final StructType schema;
    private final ArrayValue partitionColumns;
    private final Optional<Long> createdTime;
    private final MapValue configurationMapValue;
    private final Lazy<Map<String, String>> configuration;

    public Metadata(
        String id,
        Optional<String> name,
        Optional<String> description,
        Format format,
        String schemaString,
        StructType schema,
        ArrayValue partitionColumns,
        Optional<Long> createdTime,
        MapValue configurationMapValue) {
        this.id = requireNonNull(id, "id is null");
        this.name = name;
        this.description = requireNonNull(description, "description is null");
        this.format = requireNonNull(format, "format is null");
        this.schemaString = requireNonNull(schemaString, "schemaString is null");
        this.schema = schema;
        this.partitionColumns = requireNonNull(partitionColumns, "partitionColumns is null");
        this.createdTime = createdTime;
        this.configurationMapValue = requireNonNull(configurationMapValue, "configuration is null");
        this.configuration = new Lazy<>(() -> VectorUtils.toJavaMap(configurationMapValue));
    }

    public String getSchemaString() {
        return schemaString;
    }

    public StructType getSchema() {
        return schema;
    }

    public ArrayValue getPartitionColumns() {
        return partitionColumns;
    }

    public String getId() {
        return id;
    }

    public Optional<String> getName() {
        return name;
    }

    public Optional<String> getDescription() {
        return description;
    }

    public Format getFormat() {
        return format;
    }

    public Optional<Long> getCreatedTime() {
        return createdTime;
    }

    public MapValue getConfigurationMapValue() {
        return configurationMapValue;
    }

    public Map<String, String> getConfiguration() {
        return Collections.unmodifiableMap(configuration.get());
    }
}
