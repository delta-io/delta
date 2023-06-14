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
package io.delta.kernel.internal.data;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import io.delta.kernel.data.Row;
import io.delta.kernel.types.ArrayType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.MapType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;

import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;

/**
 * Expose the {@link Metadata}  as a {@link Row}.
 */
public class ScanStateRow
    implements Row
{
    private static final Map<Integer, Function<ScanStateRow, Object>>
        ordinalToAccessor = new HashMap<>();
    private static final Map<Integer, String> ordinalToColName = new HashMap<>();
    private static final StructType schema = new StructType()
        .add("configuration", new MapType(StringType.INSTANCE, StringType.INSTANCE, false))
        .add("schemaString", StringType.INSTANCE)
        .add("partitionColumns", new ArrayType(StringType.INSTANCE, false))
        .add("minReaderVersion", IntegerType.INSTANCE)
        .add("minWriterVersion", IntegerType.INSTANCE)
        .add("readSchemaString", StringType.INSTANCE)
        .add("tablePath", StringType.INSTANCE);

    static {
        ordinalToAccessor.put(0, (a) -> a.getConfiguration());
        ordinalToAccessor.put(1, (a) -> a.getSchemaString());
        ordinalToAccessor.put(2, (a) -> a.getPartitionColumns());
        ordinalToAccessor.put(3, (a) -> a.getMinReaderVersion());
        ordinalToAccessor.put(4, (a) -> a.getMinWriterVersion());
        ordinalToAccessor.put(5, (a) -> a.getReadSchemaString());
        ordinalToAccessor.put(6, (a) -> a.getTablePath());

        ordinalToColName.put(0, "configuration");
        ordinalToColName.put(1, "schemaString");
        ordinalToColName.put(2, "partitionColumns");
        ordinalToColName.put(3, "minReaderVersion");
        ordinalToColName.put(4, "minWriterVersion");
        ordinalToColName.put(5, "readSchemaString");
        ordinalToColName.put(6, "tablePath");
    }

    private final Map<String, String> configuration;
    private final String schemaString;
    private final List<String> partitionColumns;
    private final int minReaderVersion;
    private final int minWriterVersion;
    private final StructType readSchema;
    private final String readSchemaString;
    private String tablePath;

    public ScanStateRow(
        Metadata metadata,
        Protocol protocol,
        StructType readSchema,
        String tablePath)
    {
        this.configuration = metadata.getConfiguration();
        this.schemaString = metadata.getSchemaString();
        this.partitionColumns = metadata.getPartitionColumns();
        this.minReaderVersion = protocol.getMinReaderVersion();
        this.minWriterVersion = protocol.getMinWriterVersion();
        this.readSchema = readSchema;
        this.readSchemaString = readSchema.toString();
        this.tablePath = tablePath;
    }

    public Map<String, String> getConfiguration()
    {
        return configuration;
    }

    public String getSchemaString()
    {
        return schemaString;
    }

    public List<String> getPartitionColumns()
    {
        return partitionColumns;
    }

    public int getMinReaderVersion()
    {
        return minReaderVersion;
    }

    public int getMinWriterVersion()
    {
        return minWriterVersion;
    }

    public String getReadSchemaString()
    {
        return readSchemaString;
    }

    public String getTablePath()
    {
        return tablePath;
    }

    public StructType getReadSchema()
    {
        return readSchema;
    }

    @Override
    public StructType getSchema()
    {
        return schema;
    }

    @Override
    public boolean isNullAt(int ordinal)
    {
        return getValue(ordinal) == null;
    }

    @Override
    public boolean getBoolean(int ordinal)
    {
        return (boolean) getValue(ordinal);
    }

    @Override
    public int getInt(int ordinal)
    {
        return (int) getValue(ordinal);
    }

    @Override
    public long getLong(int ordinal)
    {
        return (long) getValue(ordinal);
    }

    @Override
    public String getString(int ordinal)
    {
        return (String) getValue(ordinal);
    }

    @Override
    public Row getRecord(int ordinal)
    {
        return (Row) getValue(ordinal);
    }

    @Override
    public <T> List<T> getList(int ordinal)
    {
        return (List<T>) getValue(ordinal);
    }

    @Override
    public <K, V> Map<K, V> getMap(int ordinal)
    {
        return (Map<K, V>) getValue(ordinal);
    }

    private Object getValue(int ordinal)
    {
        return ordinalToAccessor.get(ordinal).apply(this);
    }
}
